use std::ffi::{OsStr, OsString};
use std::sync::atomic::{AtomicU64, Ordering};

use async_std::sync::{Arc, RwLock};
use fuse::{FileAttr, FileType};

use crate::errno::Errno;
use crate::path::PathClean;
use crate::Result;

use super::attr::SetAttr;
use super::dir::Dir;
use super::entry::Entry;
use super::file_handle::FileHandle;
use super::inode::{Inode, InodeMap};

pub struct Filesystem {
    inode_map: Arc<RwLock<InodeMap>>,
    file_handle_id_gen: Arc<AtomicU64>,
    inode_gen: Arc<AtomicU64>,
}

impl Filesystem {
    pub async fn new() -> Result<Self> {
        let inode_map = Arc::new(RwLock::new(InodeMap::new()));
        let inode_gen = Arc::new(AtomicU64::new(1));

        let _root = Dir::from_exist(1, "/", Arc::clone(&inode_gen), Arc::clone(&inode_map)).await?;

        Ok(Self {
            inode_map,
            file_handle_id_gen: Arc::new(AtomicU64::new(1)),
            inode_gen,
        })
    }

    pub async fn lookup(&self, parent: Inode, name: &OsStr) -> Result<FileAttr> {
        let name = name.clean()?;

        let guard = self.inode_map.read().await;

        if let Entry::Dir(dir) = guard.get(&parent).ok_or(Errno::from(libc::ENOENT))? {
            dir.lookup(OsStr::new(&name)).await
        } else {
            Err(Errno::from(libc::ENOTDIR))
        }
    }

    #[inline]
    pub async fn get_attr(&self, inode: Inode) -> Result<FileAttr> {
        self.inode_map.read().await.get(&inode).ok_or(Errno::from(libc::ENOENT))?.get_attr().await
    }

    pub async fn set_dir_attr(&self, inode: Inode, set_attr: SetAttr) -> Result<FileAttr> {
        let guard = self.inode_map.read().await;

        if let Entry::Dir(dir) = guard.get(&inode).ok_or(Errno::from(libc::ENOENT))? {
            dir.set_attr(set_attr).await
        } else {
            panic!("file must use file handle to set attr")
        }
    }

    pub async fn create_dir(&self, parent: Inode, name: &OsStr, mode: u32) -> Result<FileAttr> {
        let name = name.clean()?;

        let entry = self.inode_map.read().await.get(&parent).ok_or(Errno::from(libc::ENOENT))?.clone();

        match entry {
            Entry::File(_) => Err(Errno::from(libc::ENOTDIR)),
            Entry::Dir(dir) => dir.create_dir(OsStr::new(&name), mode).await?.get_attr().await
        }
    }

    pub async fn remove_entry(&self, parent: Inode, name: &OsStr, is_dir: bool) -> Result<()> {
        let name = name.clean()?;

        match self.inode_map.read().await
            .get(&parent)
            .ok_or(Errno::from(libc::ENOENT))? {
            Entry::File(_) => return Err(Errno::from(libc::ENOTDIR)),
            Entry::Dir(dir) => dir.remove_entry(OsStr::new(&name), is_dir).await?
        };

        Ok(())
    }

    pub async fn rename(&self, old_parent: Inode, old_name: &OsStr, new_parent: Inode, new_name: &OsStr) -> Result<()> {
        let old_name = old_name.clean()?;
        let new_name = new_name.clean()?;

        let guard = self.inode_map.read().await;

        let old_parent = match guard.get(&old_parent).ok_or(Errno::from(libc::ENOENT))? {
            Entry::File(_) => return Err(Errno::from(libc::ENOTDIR)),
            Entry::Dir(dir) => dir
        };

        let new_parent = match guard.get(&new_parent).ok_or(Errno::from(libc::ENOENT))? {
            Entry::File(_) => return Err(Errno::from(libc::ENOTDIR)),
            Entry::Dir(dir) => dir
        };

        new_parent.add_child_from(old_parent, OsStr::new(&old_name), OsStr::new(&new_name)).await
    }

    pub async fn open(&self, inode: Inode, flags: u32) -> Result<FileHandle> {
        if let Entry::File(file) = self.inode_map.read().await
            .get(&inode)
            .ok_or(Errno::from(libc::ENOENT))?
        {
            file.open(self.file_handle_id_gen.fetch_add(1, Ordering::Relaxed), flags).await
        } else {
            Err(Errno::from(libc::EISDIR))
        }
    }

    pub async fn read_dir(&self, inode: Inode, offset: i64) -> Result<Vec<(Inode, i64, FileType, OsString)>> {
        if let Entry::Dir(dir) = self.inode_map.read().await
            .get(&inode)
            .ok_or(Errno::from(libc::ENOENT))?
        {
            dir.read_dir(offset).await
        } else {
            Err(Errno::from(libc::ENOTDIR))
        }
    }

    pub async fn create_file(&self, parent: Inode, name: &OsStr, mode: u32, flags: u32) -> Result<FileHandle> {
        let name = name.clean()?;

        if let Entry::Dir(dir) = self.inode_map.read().await
            .get(&parent)
            .ok_or(Errno::from(libc::ENOENT))?
        {
            let file = dir.create_file(OsStr::new(&name), mode).await?;

            file.open(self.file_handle_id_gen.fetch_add(1, Ordering::Relaxed), flags).await
        } else {
            Err(Errno::from(libc::ENOTDIR))
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile;

    use crate::server::filesystem::chroot;

    use super::*;

    #[async_std::test]
    async fn init_filesystem() {
        let tmp_dir = tempfile::TempDir::new().unwrap();

        chroot(tmp_dir.path().to_path_buf()).expect_err("chroot failed");

        if let Err(errno) = Filesystem::new().await {
            panic!("new filesystem failed, errno: {:?}", errno);
        }
    }
}