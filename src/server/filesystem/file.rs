use std::ffi::OsString;
use std::os::unix::fs::MetadataExt;
use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, UNIX_EPOCH};

use async_std::fs;
use async_std::path::Path;
use async_std::sync::RwLock;
use fuse::{FileAttr, FileType};
use log::debug;

use crate::Result;

use super::entry::Entry;
use super::file_handle::{FileHandle, FileHandleKind};
use super::inode::{Inode, InodeMap};

#[derive(Debug)]
struct InnerFile {
    inode: Inode,
    name: OsString,
    real_path: OsString,
    parent: Inode,
}

#[derive(Debug)]
pub struct File(RwLock<InnerFile>);

impl File {
    pub async fn from_exist<P: AsRef<Path>>(
        parent: Inode,
        real_path: P,
        inode_gen: &AtomicU64,
        inode_map: &mut InodeMap,
    ) -> Result<Arc<Self>> {
        fs::metadata(&real_path).await?;

        let real_path = real_path.as_ref().to_path_buf();

        debug!("create File from real path {:?}", real_path);

        let inode = inode_gen.fetch_add(1, Ordering::Relaxed);

        let file = Arc::new(File(RwLock::new(InnerFile {
            inode,
            name: real_path
                .file_name()
                .expect("name should be valid")
                .to_os_string(),
            real_path: real_path.as_os_str().to_os_string(),
            parent,
        })));

        inode_map.insert(inode, Entry::from(&file));

        Ok(file)
    }

    pub async fn get_attr(&self) -> Result<FileAttr> {
        let guard = self.0.read().await;

        let metadata = fs::metadata(&guard.real_path).await?;

        Ok(FileAttr {
            ino: guard.inode,
            size: metadata.len(),
            blocks: metadata.blocks(),
            kind: FileType::RegularFile,
            atime: metadata.accessed()?,
            mtime: metadata.modified()?,
            ctime: UNIX_EPOCH
                + Duration::new(metadata.ctime() as u64, metadata.ctime_nsec() as u32),
            perm: (metadata.permissions().mode() ^ libc::S_IFREG) as u16,
            uid: metadata.uid(),
            gid: metadata.gid(),
            rdev: metadata.rdev() as u32,
            flags: 0,
            nlink: 0,
        })
    }

    pub async fn open(&self, fh_id: u64, flags: u32) -> Result<FileHandle> {
        let guard = self.0.read().await;

        let mut options = fs::OpenOptions::new();

        let fh_kind = if flags & libc::O_RDWR as u32 > 0 {
            options.write(true);
            options.read(true);

            FileHandleKind::ReadWrite
        } else if flags & libc::O_WRONLY as u32 > 0 {
            options.write(true);
            options.read(false);

            FileHandleKind::WriteOnly
        } else {
            options.write(false);
            options.read(true);

            FileHandleKind::ReadOnly
        };

        let sys_file = options.open(&guard.real_path).await?;

        Ok(FileHandle::new(fh_id, guard.inode, sys_file, fh_kind))
    }

    pub async fn rename<P: AsRef<Path>>(&self, new_real_path: P) -> Result<()> {
        let mut guard = self.0.write().await;

        fs::rename(&guard.real_path, &new_real_path).await?;

        let new_real_path = new_real_path.as_ref();

        guard.real_path = new_real_path.as_os_str().to_os_string();
        guard.name = new_real_path
            .file_name()
            .expect("name should be valid")
            .to_os_string();

        Ok(())
    }

    #[inline]
    pub async fn set_new_parent(&self, new_parent: Inode) {
        self.0.write().await.parent = new_parent
    }

    #[inline]
    pub async fn get_inode(&self) -> Inode {
        self.0.read().await.inode
    }

    #[inline]
    pub async fn get_name(&self) -> OsString {
        self.0.read().await.name.to_os_string()
    }

    #[inline]
    pub async fn get_real_path(&self) -> OsString {
        self.0.read().await.real_path.to_os_string()
    }

    #[inline]
    pub async fn get_parent_inode(&self) -> Inode {
        self.0.read().await.parent
    }
}
