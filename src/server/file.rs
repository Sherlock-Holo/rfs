use std::ffi::OsString;
use std::os::unix::fs::MetadataExt;
use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_std::fs;
use async_std::fs::File as SysFile;
use async_std::path::Path;
use async_std::sync::RwLock;
use fuse::{FileAttr, FileType};
use time_old::Timespec;

use crate::helper::Apply;
use crate::Result;
use crate::server::entry::Entry;
use crate::server::file_handle::{FileHandle, FileHandleKind};
use crate::server::inode::{Inode, InodeMap};

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
    pub async fn from_exist<P: AsRef<Path>>(parent: Inode, real_path: P, inode_gen: &AtomicU64, inode_map: &mut InodeMap) -> Result<Arc<Self>> {
        fs::metadata(&real_path).await?;

        let real_path = real_path.as_ref().to_path_buf();

        let inode = inode_gen.fetch_add(1, Ordering::Relaxed);

        let file = Arc::new(File(RwLock::new(InnerFile {
            inode,
            name: real_path.file_name().expect("name should be valid").to_os_string(),
            real_path: real_path.as_os_str().to_os_string(),
            parent,
        })));

        inode_map.insert(inode, Entry::from(&file));

        Ok(file)
    }
    /*pub async fn create_file<P: AsRef<Path>>(
        parent: Inode,
        real_path: P,
        _uid: u32,
        _gid: u32,
        mode: u32,
        inode_gen: &AtomicU64,
        inode_map: &mut InodeMap,
    ) -> Result<Arc<Self>> {
        let file = SysFile::create(&real_path).await?;

        let perm = fs::metadata(&real_path).await?.permissions()
            .apply(|perm| perm.set_mode(mode));

        file.set_permissions(perm).await?;

        File::from_exist(parent, real_path, inode_gen, inode_map).await
    }

    pub async fn from_exist<P: AsRef<Path>>(parent: Inode, real_path: P, inode_gen: &AtomicU64, inode_map: &mut InodeMap) -> Result<Arc<Self>> {
        let real_path = real_path.as_ref();

        let file = Arc::new(File(RwLock::new(InnerFile {
            inode: inode_gen.fetch_add(1, Ordering::Relaxed),
            name: real_path.file_name().expect("it should be a valid path").to_os_string(),
            real_path: real_path.as_os_str().to_os_string(),
            parent,
        })));

        inode_map.insert(file.get_inode().await, Entry::from(&file));

        Ok(file)
    }*/

    pub async fn get_attr(self: &Arc<Self>) -> Result<FileAttr> {
        let guard = self.0.read().await;

        let metadata = fs::metadata(&guard.real_path).await?;

        Ok(FileAttr {
            ino: guard.inode,
            size: metadata.len(),
            blocks: metadata.blocks(),
            kind: FileType::RegularFile,
            atime: Timespec::new(metadata.atime(), metadata.atime_nsec() as i32),
            mtime: Timespec::new(metadata.mtime(), metadata.mtime_nsec() as i32),
            ctime: Timespec::new(metadata.ctime(), metadata.ctime_nsec() as i32),
            crtime: Timespec::new(metadata.atime(), metadata.atime_nsec() as i32),
            perm: metadata.permissions().mode() as u16,
            uid: metadata.uid(),
            gid: metadata.gid(),
            rdev: metadata.rdev() as u32,
            flags: 0,
            nlink: 0,
        })
    }

    pub async fn open(self: &Arc<Self>, fh_id: u64, flags: u32) -> Result<FileHandle> {
        let guard = self.0.read().await;

        let mut options = fs::OpenOptions::new();

        let fh_kind = if flags & libc::O_WRONLY as u32 > 0 {
            options.write(true);
            options.read(false);
            FileHandleKind::WriteOnly
        } else if flags & libc::O_RDONLY as u32 > 0 {
            options.write(false);
            options.read(true);
            FileHandleKind::ReadOnly
        } else {
            options.write(true);
            options.read(true);
            FileHandleKind::ReadWrite
        };

        let sys_file = options.open(&guard.real_path).await?;

        Ok(FileHandle::new(fh_id, sys_file, fh_kind))
    }

    #[inline]
    pub async fn get_name(self: &Arc<Self>) -> OsString {
        self.0.read().await.name.to_os_string()
    }

    #[inline]
    pub async fn get_real_path(self: &Arc<Self>) -> OsString {
        self.0.read().await.real_path.to_os_string()
    }

    #[inline]
    pub async fn get_inode(self: &Arc<Self>) -> Inode {
        self.0.read().await.inode
    }
}