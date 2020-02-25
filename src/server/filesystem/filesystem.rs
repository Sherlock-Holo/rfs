use std::ffi::{OsStr, OsString};
use std::sync::atomic::{AtomicU64, Ordering};

use async_std::sync::{Arc, RwLock};
use fuse::{FileAttr, FileType};
use log::debug;

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
}

impl Filesystem {
    pub async fn new() -> Result<Self> {
        let inode_map = Arc::new(RwLock::new(InodeMap::new()));
        let inode_gen = Arc::new(AtomicU64::new(1));

        let _root = Dir::from_exist(1, "/", inode_gen.clone(), inode_map.clone()).await?;

        Ok(Self {
            inode_map,
            file_handle_id_gen: Arc::new(AtomicU64::new(1)),
        })
    }

    pub async fn lookup(&self, parent: Inode, name: &OsStr) -> Result<FileAttr> {
        let name = name.clean()?;

        let entry = self
            .inode_map
            .read()
            .await
            .get(&parent)
            .ok_or(Errno::from(libc::ENOENT))?
            .clone();

        if let Entry::Dir(dir) = entry {
            dir.lookup(OsStr::new(&name)).await
        } else {
            Err(Errno::from(libc::ENOTDIR))
        }
    }

    #[inline]
    pub async fn get_attr(&self, inode: Inode) -> Result<FileAttr> {
        self.inode_map
            .read()
            .await
            .get(&inode)
            .ok_or(Errno::from(libc::ENOENT))?
            .get_attr()
            .await
    }

    #[inline]
    pub async fn get_name(&self, inode: Inode) -> Result<OsString> {
        Ok(self
            .inode_map
            .read()
            .await
            .get(&inode)
            .ok_or(Errno::from(libc::ENOENT))?
            .get_name()
            .await)
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

        let entry = self
            .inode_map
            .read()
            .await
            .get(&parent)
            .ok_or(Errno::from(libc::ENOENT))?
            .clone();

        match entry {
            Entry::File(_) => Err(Errno::from(libc::ENOTDIR)),
            Entry::Dir(dir) => {
                dir.create_dir(OsStr::new(&name), mode)
                    .await?
                    .get_attr()
                    .await
            }
        }
    }

    pub async fn remove_entry(&self, parent: Inode, name: &OsStr, is_dir: bool) -> Result<()> {
        let name = name.clean()?;

        let entry = self
            .inode_map
            .read()
            .await
            .get(&parent)
            .ok_or(Errno::from(libc::ENOENT))?
            .clone();

        if let Entry::Dir(dir) = entry {
            dir.remove_entry(OsStr::new(&name), is_dir).await?;
        } else {
            return Err(Errno::from(libc::ENOTDIR));
        }

        Ok(())
    }

    pub async fn rename(
        &self,
        old_parent: Inode,
        old_name: &OsStr,
        new_parent: Inode,
        new_name: &OsStr,
    ) -> Result<()> {
        let old_name = old_name.clean()?;
        let new_name = new_name.clean()?;

        let guard = self.inode_map.read().await;

        let old_parent = match guard.get(&old_parent).ok_or(Errno::from(libc::ENOENT))? {
            Entry::File(_) => return Err(Errno::from(libc::ENOTDIR)),
            Entry::Dir(dir) => dir.clone(),
        };

        let new_parent = match guard.get(&new_parent).ok_or(Errno::from(libc::ENOENT))? {
            Entry::File(_) => return Err(Errno::from(libc::ENOTDIR)),
            Entry::Dir(dir) => dir.clone(),
        };

        // release inode map lock
        drop(guard);

        new_parent
            .add_child_from(&old_parent, OsStr::new(&old_name), OsStr::new(&new_name))
            .await
    }

    pub async fn open(&self, inode: Inode, flags: u32) -> Result<FileHandle> {
        if let Entry::File(file) = self
            .inode_map
            .read()
            .await
            .get(&inode)
            .ok_or(Errno::from(libc::ENOENT))?
        {
            file.open(
                self.file_handle_id_gen.fetch_add(1, Ordering::Relaxed),
                flags,
            )
                .await
        } else {
            Err(Errno::from(libc::EISDIR))
        }
    }

    pub async fn read_dir(
        &self,
        inode: Inode,
        offset: i64,
    ) -> Result<Vec<(Inode, i64, FileType, OsString)>> {
        let entry = self
            .inode_map
            .read()
            .await
            .get(&inode)
            .ok_or(Errno::from(libc::ENOENT))?
            .clone();

        if let Entry::Dir(dir) = entry {
            dir.read_dir(offset).await
        } else {
            Err(Errno::from(libc::ENOTDIR))
        }
    }

    pub async fn create_file(
        &self,
        parent: Inode,
        name: &OsStr,
        mode: u32,
        flags: u32,
    ) -> Result<(FileHandle, FileAttr)> {
        let name = name.clean()?;

        let entry = self
            .inode_map
            .read()
            .await
            .get(&parent)
            .ok_or(Errno::from(libc::ENOENT))?
            .clone();

        let dir = match entry {
            Entry::File(_) => return Err(Errno::from(libc::ENOTDIR)),
            Entry::Dir(dir) => dir,
        };

        let file = dir.create_file(OsStr::new(&name), mode).await?;

        debug!("file created");

        let file_handle = file
            .open(
                self.file_handle_id_gen.fetch_add(1, Ordering::Relaxed),
                flags,
            )
            .await?;

        let attr = file_handle.get_attr().await?;

        Ok((file_handle, attr))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use async_std::sync::Mutex;
    use async_std::task::sleep;
    use futures::future::FutureExt;
    use futures::select;
    use tempfile;

    use crate::log_init;
    use crate::server::filesystem::{chroot, LockKind};
    use crate::server::filesystem::file_handle::FileHandleKind;

    use super::*;

    #[async_std::test]
    async fn init_filesystem() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        chroot(tmp_dir.path().to_path_buf()).unwrap();

        let filesystem = Filesystem::new().await.unwrap();

        let root = filesystem.inode_map.read().await.get(&1).unwrap().clone();

        let root_dir = if let Entry::Dir(dir) = root {
            dir
        } else {
            panic!("root is not Dir");
        };

        assert_eq!(root_dir.get_inode().await, 1);
        assert_eq!(root_dir.get_name().await, OsString::from("/"));
        assert_eq!(root_dir.get_real_path().await, OsString::from("/"));
        assert_eq!(root_dir.get_parent_inode().await, 1);
    }

    #[async_std::test]
    async fn create_dir() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        chroot(tmp_dir.path().to_path_buf()).unwrap();

        let filesystem = Filesystem::new().await.unwrap();

        let dir_attr = filesystem
            .create_dir(1, OsStr::new("test"), 0o755)
            .await
            .unwrap();

        assert_eq!(dir_attr.ino, 2);
        assert_eq!(dir_attr.kind, FileType::Directory);
        assert_eq!(dir_attr.perm, 0o755);
    }

    #[async_std::test]
    async fn create_file() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        chroot(tmp_dir.path().to_path_buf()).unwrap();

        let filesystem = Filesystem::new().await.unwrap();

        let (file_handle, _) = filesystem
            .create_file(1, OsStr::new("test"), 0o644, libc::O_RDONLY as u32)
            .await
            .unwrap();

        assert_eq!(file_handle.get_id(), 1);

        let attr = file_handle.get_attr().await.unwrap();

        assert_eq!(attr.ino, 2);
        assert_eq!(attr.kind, FileType::RegularFile);
        assert_eq!(attr.perm, 0o644);
    }

    #[async_std::test]
    async fn get_dir_name() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        chroot(tmp_dir.path().to_path_buf()).unwrap();

        let filesystem = Filesystem::new().await.unwrap();

        filesystem
            .create_dir(1, OsStr::new("test"), 0o644)
            .await
            .unwrap();

        assert_eq!(filesystem.get_name(2).await, Ok(OsString::from("test")));
    }

    #[async_std::test]
    async fn get_file_name() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        chroot(tmp_dir.path().to_path_buf()).unwrap();

        let filesystem = Filesystem::new().await.unwrap();

        filesystem
            .create_file(1, OsStr::new("test"), 0o644, libc::O_RDONLY as u32)
            .await
            .unwrap();

        assert_eq!(filesystem.get_name(2).await, Ok(OsString::from("test")));
    }

    #[async_std::test]
    async fn lookup_dir() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        chroot(tmp_dir.path().to_path_buf()).unwrap();

        let filesystem = Filesystem::new().await.unwrap();

        filesystem
            .create_dir(1, OsStr::new("test"), 0o755)
            .await
            .unwrap();

        let attr = filesystem.lookup(1, OsStr::new("test")).await.unwrap();

        assert_eq!(attr.ino, 2);
        assert_eq!(attr.kind, FileType::Directory);
        assert_eq!(attr.perm, 0o755);
    }

    #[async_std::test]
    async fn lookup_file() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        chroot(tmp_dir.path().to_path_buf()).unwrap();

        let filesystem = Filesystem::new().await.unwrap();

        filesystem
            .create_file(1, OsStr::new("test"), 0o644, libc::O_RDONLY as u32)
            .await
            .unwrap();

        let attr = filesystem.lookup(1, OsStr::new("test")).await.unwrap();

        assert_eq!(attr.ino, 2);
        assert_eq!(attr.kind, FileType::RegularFile);
        assert_eq!(attr.perm, 0o644);
    }

    #[async_std::test]
    async fn get_attr_dir() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        chroot(tmp_dir.path().to_path_buf()).unwrap();

        let filesystem = Filesystem::new().await.unwrap();

        filesystem
            .create_dir(1, OsStr::new("test"), 0o755)
            .await
            .unwrap();

        let attr = filesystem.get_attr(2).await.unwrap();

        assert_eq!(attr.ino, 2);
        assert_eq!(attr.kind, FileType::Directory);
        assert_eq!(attr.perm, 0o755);
    }

    #[async_std::test]
    async fn get_attr_file() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        chroot(tmp_dir.path().to_path_buf()).unwrap();

        let filesystem = Filesystem::new().await.unwrap();

        filesystem
            .create_file(1, OsStr::new("test"), 0o644, libc::O_RDONLY as u32)
            .await
            .unwrap();

        let attr = filesystem.get_attr(2).await.unwrap();

        assert_eq!(attr.ino, 2);
        assert_eq!(attr.kind, FileType::RegularFile);
        assert_eq!(attr.perm, 0o644);
    }

    #[async_std::test]
    async fn set_dir_attr() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        chroot(tmp_dir.path().to_path_buf()).unwrap();

        let filesystem = Filesystem::new().await.unwrap();

        filesystem
            .create_dir(1, OsStr::new("test"), 0o755)
            .await
            .unwrap();

        let set_attr = SetAttr {
            ctime: None,
            mtime: None,
            atime: None,
            flags: None,
            uid: None,
            gid: None,
            size: None,
            mode: Some(0o700),
        };

        filesystem.set_dir_attr(2, set_attr).await.unwrap();

        let attr = filesystem.get_attr(2).await.unwrap();

        assert_eq!(attr.ino, 2);
        assert_eq!(attr.kind, FileType::Directory);
        assert_eq!(attr.perm, 0o700);
    }

    #[async_std::test]
    async fn remove_dir() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        chroot(tmp_dir.path().to_path_buf()).unwrap();

        let filesystem = Filesystem::new().await.unwrap();

        filesystem
            .create_dir(1, OsStr::new("test"), 0o755)
            .await
            .unwrap();

        debug!("dir created");

        filesystem
            .remove_entry(1, OsStr::new("test"), true)
            .await
            .unwrap();

        assert_eq!(
            filesystem.lookup(1, OsStr::new("test")).await,
            Err(Errno::from(libc::ENOENT))
        );
    }

    #[async_std::test]
    async fn remove_file() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        chroot(tmp_dir.path().to_path_buf()).unwrap();

        let filesystem = Filesystem::new().await.unwrap();

        filesystem
            .create_file(1, OsStr::new("test"), 0o644, libc::O_RDONLY as u32)
            .await
            .unwrap();

        filesystem
            .remove_entry(1, OsStr::new("test"), false)
            .await
            .unwrap();

        assert_eq!(
            filesystem.lookup(1, OsStr::new("test")).await,
            Err(Errno::from(libc::ENOENT))
        );
    }

    #[async_std::test]
    async fn rename_dir() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        chroot(tmp_dir.path().to_path_buf()).unwrap();

        let filesystem = Filesystem::new().await.unwrap();

        filesystem
            .create_dir(1, OsStr::new("test"), 0o755)
            .await
            .unwrap();

        filesystem
            .rename(1, OsStr::new("test"), 1, OsStr::new("new-test"))
            .await
            .unwrap();

        let attr = filesystem.lookup(1, OsStr::new("new-test")).await.unwrap();

        assert_eq!(attr.ino, 2);
        assert_eq!(attr.kind, FileType::Directory);
        assert_eq!(attr.perm, 0o755);
    }

    #[async_std::test]
    async fn rename_file() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        chroot(tmp_dir.path().to_path_buf()).unwrap();

        let filesystem = Filesystem::new().await.unwrap();

        filesystem
            .create_file(1, OsStr::new("test"), 0o644, libc::O_RDWR as u32)
            .await
            .unwrap();

        filesystem
            .rename(1, OsStr::new("test"), 1, OsStr::new("new-test"))
            .await
            .unwrap();

        let attr = filesystem.lookup(1, OsStr::new("new-test")).await.unwrap();

        assert_eq!(attr.ino, 2);
        assert_eq!(attr.kind, FileType::RegularFile);
        assert_eq!(attr.perm, 0o644);
    }

    #[async_std::test]
    async fn move_dir() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        chroot(tmp_dir.path().to_path_buf()).unwrap();

        let filesystem = Filesystem::new().await.unwrap();

        filesystem
            .create_dir(1, OsStr::new("old"), 0o755)
            .await
            .unwrap(); // inode 2
        filesystem
            .create_dir(1, OsStr::new("new"), 0o755)
            .await
            .unwrap(); // inode 3

        filesystem
            .create_dir(2, OsStr::new("test"), 0o755)
            .await
            .unwrap(); // inode 4

        filesystem
            .rename(2, OsStr::new("test"), 3, OsStr::new("test"))
            .await
            .unwrap();

        let attr = filesystem.lookup(3, OsStr::new("test")).await.unwrap();

        assert_eq!(attr.ino, 4);
        assert_eq!(attr.kind, FileType::Directory);
        assert_eq!(attr.perm, 0o755);
    }

    #[async_std::test]
    async fn move_file() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        chroot(tmp_dir.path().to_path_buf()).unwrap();

        let filesystem = Filesystem::new().await.unwrap();

        filesystem
            .create_dir(1, OsStr::new("old"), 0o755)
            .await
            .unwrap(); // inode 2
        filesystem
            .create_dir(1, OsStr::new("new"), 0o755)
            .await
            .unwrap(); // inode 3

        filesystem
            .create_file(2, OsStr::new("test"), 0o644, libc::O_RDWR as u32)
            .await
            .unwrap(); // inode 4

        filesystem
            .rename(2, OsStr::new("test"), 3, OsStr::new("test"))
            .await
            .unwrap();

        let attr = filesystem.lookup(3, OsStr::new("test")).await.unwrap();

        assert_eq!(attr.ino, 4);
        assert_eq!(attr.kind, FileType::RegularFile);
        assert_eq!(attr.perm, 0o644);
    }

    #[async_std::test]
    async fn read_dir() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        chroot(tmp_dir.path().to_path_buf()).unwrap();

        let filesystem = Filesystem::new().await.unwrap();

        filesystem
            .create_dir(1, OsStr::new("test-1"), 0o755)
            .await
            .unwrap(); // inode 2
        filesystem
            .create_dir(1, OsStr::new("test-2"), 0o755)
            .await
            .unwrap(); // inode 3

        let child_info = filesystem.read_dir(1, 0).await.unwrap();

        assert_eq!(child_info.len(), 4); // include . and ..

        let (inode, _, kind, name) = &child_info[0];

        assert_eq!(*inode, 1);
        assert_eq!(*kind, FileType::Directory);
        assert_eq!(*name, OsString::from("."));

        let (inode, _, kind, name) = &child_info[1];

        assert_eq!(*inode, 1);
        assert_eq!(*kind, FileType::Directory);
        assert_eq!(*name, OsString::from(".."));

        let (inode, _, kind, name) = &child_info[2];

        assert!(*inode == 2 || *inode == 3);
        assert_eq!(*kind, FileType::Directory);
        assert!(*name == OsString::from("test-1") || *name == OsString::from("test-2"));

        let (_, _, kind, name) = &child_info[3];

        assert!(*inode == 2 || *inode == 3);
        assert_eq!(*kind, FileType::Directory);
        assert!(*name == OsString::from("test-1") || *name == OsString::from("test-2"));
    }

    #[async_std::test]
    async fn open_file_rw() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        chroot(tmp_dir.path().to_path_buf()).unwrap();

        let filesystem = Filesystem::new().await.unwrap();

        filesystem
            .create_file(1, OsStr::new("test"), 0o644, libc::O_RDONLY as u32)
            .await
            .unwrap(); // file handle id 1 used

        let file_handle = filesystem.open(2, libc::O_RDWR as u32).await.unwrap();

        assert_eq!(file_handle.get_id(), 2);
        assert_eq!(
            file_handle.get_file_handle_kind(),
            FileHandleKind::ReadWrite
        );

        let attr = file_handle.get_attr().await.unwrap();

        assert_eq!(attr.ino, 2);
        assert_eq!(attr.kind, FileType::RegularFile);
        assert_eq!(attr.perm, 0o644);
    }

    #[async_std::test]
    async fn open_file_ro() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        chroot(tmp_dir.path().to_path_buf()).unwrap();

        let filesystem = Filesystem::new().await.unwrap();

        filesystem
            .create_file(1, OsStr::new("test"), 0o644, libc::O_RDWR as u32)
            .await
            .unwrap(); // file handle id 1 used

        let file_handle = filesystem.open(2, libc::O_RDONLY as u32).await.unwrap();

        assert_eq!(file_handle.get_id(), 2);
        assert_eq!(file_handle.get_file_handle_kind(), FileHandleKind::ReadOnly);

        let attr = file_handle.get_attr().await.unwrap();

        assert_eq!(attr.ino, 2);
        assert_eq!(attr.kind, FileType::RegularFile);
        assert_eq!(attr.perm, 0o644);
    }

    #[async_std::test]
    async fn open_file_wo() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        chroot(tmp_dir.path().to_path_buf()).unwrap();

        let filesystem = Filesystem::new().await.unwrap();

        filesystem
            .create_file(1, OsStr::new("test"), 0o644, libc::O_RDWR as u32)
            .await
            .unwrap(); // file handle id 1 used

        let file_handle = filesystem.open(2, libc::O_WRONLY as u32).await.unwrap();

        assert_eq!(file_handle.get_id(), 2);
        assert_eq!(
            file_handle.get_file_handle_kind(),
            FileHandleKind::WriteOnly
        );

        let attr = file_handle.get_attr().await.unwrap();

        assert_eq!(attr.ino, 2);
        assert_eq!(attr.kind, FileType::RegularFile);
        assert_eq!(attr.perm, 0o644);
    }

    #[async_std::test]
    async fn write_file() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        chroot(tmp_dir.path().to_path_buf()).unwrap();

        let filesystem = Filesystem::new().await.unwrap();

        let (mut file_handle, _) = filesystem
            .create_file(1, OsStr::new("test"), 0o644, libc::O_RDWR as u32)
            .await
            .unwrap();

        let written = file_handle.write(b"test", 0).await.unwrap();
        file_handle.flush().await.unwrap();

        assert_eq!(written, 4);
        assert_eq!(file_handle.get_attr().await.unwrap().size, 4)
    }

    #[async_std::test]
    async fn read_file() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        chroot(tmp_dir.path().to_path_buf()).unwrap();

        let filesystem = Filesystem::new().await.unwrap();

        let (mut file_handle, _) = filesystem
            .create_file(1, OsStr::new("test"), 0o644, libc::O_RDWR as u32)
            .await
            .unwrap();

        let read = file_handle.read(&mut vec![0; 0], 0).await.unwrap();
        assert_eq!(read, 0);

        file_handle.write(b"test", 0).await.unwrap();
        file_handle.flush().await.unwrap();

        let mut buf = vec![0; 4];

        let read = file_handle.read(&mut buf, 0).await.unwrap();
        assert_eq!(read, 4);
        assert_eq!(&b"test"[..], &buf[..])
    }

    #[async_std::test]
    async fn set_attr_file() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        chroot(tmp_dir.path().to_path_buf()).unwrap();

        let filesystem = Filesystem::new().await.unwrap();

        let (mut file_handle, _) = filesystem
            .create_file(1, OsStr::new("test"), 0o644, libc::O_RDWR as u32)
            .await
            .unwrap();

        file_handle.write(b"test", 0).await.unwrap();
        file_handle.flush().await.unwrap();

        let set_attr = SetAttr {
            mode: Some(0o600),
            uid: None,
            gid: None,
            size: Some(2),
            atime: None,
            mtime: None,
            ctime: None,
            flags: None,
        };

        file_handle.set_attr(set_attr).await.unwrap();

        let attr = file_handle.get_attr().await.unwrap();

        assert_eq!(attr.perm, 0o600);
        assert_eq!(attr.size, 2);

        let mut buf = vec![0; 4];

        let read = file_handle.read(&mut buf, 0).await.unwrap();
        assert_eq!(read, 2);
        assert_eq!(&b"te"[..], &buf[..read])
    }

    #[async_std::test]
    async fn set_share_lock_success() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        chroot(tmp_dir.path().to_path_buf()).unwrap();

        let filesystem = Filesystem::new().await.unwrap();

        let (mut file_handle, _) = filesystem
            .create_file(1, OsStr::new("test"), 0o644, libc::O_RDWR as u32)
            .await
            .unwrap();

        let mut file_handle2 = filesystem.open(2, libc::O_RDWR as u32).await.unwrap();

        let lock_table = Arc::new(Mutex::new(BTreeMap::new()));

        let lock_job = file_handle
            .set_lock(1, true, lock_table.clone())
            .await
            .unwrap();

        let lock_job = select! {
            result = lock_job.fuse() => result,
            _ = sleep(Duration::from_secs(2)).fuse() => panic!("set share lock failed"),
        };

        assert!(lock_job);

        let lock_job = file_handle2.set_lock(2, true, lock_table).await.unwrap();

        let lock_job = select! {
            result = lock_job.fuse() => result,
            _ = sleep(Duration::from_secs(2)).fuse() => panic!("set another share lock failed"),
        };

        assert!(lock_job);
    }

    #[async_std::test]
    async fn set_share_lock_failed() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        chroot(tmp_dir.path().to_path_buf()).unwrap();

        let filesystem = Filesystem::new().await.unwrap();

        let (mut file_handle, _) = filesystem
            .create_file(1, OsStr::new("test"), 0o644, libc::O_RDWR as u32)
            .await
            .unwrap();

        let mut file_handle2 = filesystem.open(2, libc::O_RDWR as u32).await.unwrap();

        let lock_table = Arc::new(Mutex::new(BTreeMap::new()));

        let lock_job = file_handle
            .set_lock(1, true, lock_table.clone())
            .await
            .unwrap();

        let lock_job = select! {
            result = lock_job.fuse() => result,
            _ = sleep(Duration::from_secs(2)).fuse() => panic!("set share lock failed"),
        };

        assert!(lock_job);

        let lock_job = file_handle2.set_lock(2, false, lock_table).await.unwrap();

        select! {
            result = lock_job.fuse() => panic!("set not share lock success"),
            _ = sleep(Duration::from_secs(1)).fuse() => (),
        }
    }

    #[async_std::test]
    async fn try_set_share_lock_success() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        chroot(tmp_dir.path().to_path_buf()).unwrap();

        let filesystem = Filesystem::new().await.unwrap();

        let (file_handle, _) = filesystem
            .create_file(1, OsStr::new("test"), 0o644, libc::O_RDWR as u32)
            .await
            .unwrap();

        let file_handle2 = filesystem.open(2, libc::O_RDWR as u32).await.unwrap();

        assert_eq!(file_handle.try_set_lock(true).await, Ok(()));
        assert_eq!(file_handle2.try_set_lock(true).await, Ok(()));
    }

    #[async_std::test]
    async fn try_set_share_lock_failed() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        chroot(tmp_dir.path().to_path_buf()).unwrap();

        let filesystem = Filesystem::new().await.unwrap();

        let (file_handle, _) = filesystem
            .create_file(1, OsStr::new("test"), 0o644, libc::O_RDWR as u32)
            .await
            .unwrap();

        let file_handle2 = filesystem.open(2, libc::O_RDWR as u32).await.unwrap();

        assert_eq!(file_handle.try_set_lock(true).await, Ok(()));

        assert_eq!(
            file_handle2.try_set_lock(false).await,
            Err(Errno::from(libc::EWOULDBLOCK))
        )
    }

    #[async_std::test]
    async fn set_exclusive_lock() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        chroot(tmp_dir.path().to_path_buf()).unwrap();

        let filesystem = Filesystem::new().await.unwrap();

        let (mut file_handle, _) = filesystem
            .create_file(1, OsStr::new("test"), 0o644, libc::O_RDWR as u32)
            .await
            .unwrap();

        let mut file_handle2 = filesystem.open(2, libc::O_RDWR as u32).await.unwrap();

        let lock_table = Arc::new(Mutex::new(BTreeMap::new()));

        let lock_job = file_handle
            .set_lock(1, false, lock_table.clone())
            .await
            .unwrap();

        let lock_job = select! {
            result = lock_job.fuse() => result,
            _ = sleep(Duration::from_secs(2)).fuse() => panic!("set exclusive lock failed"),
        };

        assert!(lock_job);

        let lock_job = file_handle2
            .set_lock(2, false, lock_table.clone())
            .await
            .unwrap();

        select! {
            result = lock_job.fuse() => panic!("set exclusive lock should failed"),
            _ = sleep(Duration::from_secs(1)).fuse() => (),
        }
        ;

        let lock_job = file_handle2.set_lock(3, true, lock_table).await.unwrap();

        select! {
            result = lock_job.fuse() => panic!("set share lock should failed"),
            _ = sleep(Duration::from_secs(1)).fuse() => (),
        }
    }

    #[async_std::test]
    async fn try_set_exclusive_lock() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        chroot(tmp_dir.path().to_path_buf()).unwrap();

        let filesystem = Filesystem::new().await.unwrap();

        let (file_handle, _) = filesystem
            .create_file(1, OsStr::new("test"), 0o644, libc::O_RDWR as u32)
            .await
            .unwrap();

        let file_handle2 = filesystem.open(2, libc::O_RDWR as u32).await.unwrap();

        assert_eq!(file_handle.try_set_lock(false).await, Ok(()));
        assert_eq!(
            file_handle2.try_set_lock(true).await,
            Err(Errno::from(libc::EWOULDBLOCK))
        );
        assert_eq!(
            file_handle2.try_set_lock(false).await,
            Err(Errno::from(libc::EWOULDBLOCK))
        );
    }

    #[async_std::test]
    async fn release_share_lock() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        chroot(tmp_dir.path().to_path_buf()).unwrap();

        let filesystem = Filesystem::new().await.unwrap();

        let (mut file_handle, _) = filesystem
            .create_file(1, OsStr::new("test"), 0o644, libc::O_RDWR as u32)
            .await
            .unwrap();

        let file_handle2 = filesystem.open(2, libc::O_RDWR as u32).await.unwrap();

        let lock_queue = Arc::new(Mutex::new(BTreeMap::new()));

        let lock_job = file_handle.set_lock(1, true, lock_queue).await.unwrap();

        assert!(lock_job.await);
        assert_eq!(file_handle.release_lock().await, Ok(()));

        assert_eq!(file_handle2.try_set_lock(false).await, Ok(()));
    }

    #[async_std::test]
    async fn release_exclusive_lock() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        chroot(tmp_dir.path().to_path_buf()).unwrap();

        let filesystem = Filesystem::new().await.unwrap();

        let (mut file_handle, _) = filesystem
            .create_file(1, OsStr::new("test"), 0o644, libc::O_RDWR as u32)
            .await
            .unwrap();

        let file_handle2 = filesystem.open(2, libc::O_RDWR as u32).await.unwrap();

        let lock_queue = Arc::new(Mutex::new(BTreeMap::new()));

        let lock_job = file_handle.set_lock(1, false, lock_queue).await.unwrap();

        assert!(lock_job.await);
        assert_eq!(file_handle.release_lock().await, Ok(()));

        assert_eq!(file_handle2.try_set_lock(false).await, Ok(()));
    }

    #[async_std::test]
    async fn interrupt_lock() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        chroot(tmp_dir.path().to_path_buf()).unwrap();

        let filesystem = Filesystem::new().await.unwrap();

        let (mut file_handle, _) = filesystem
            .create_file(1, OsStr::new("test"), 0o644, libc::O_RDWR as u32)
            .await
            .unwrap();

        let mut file_handle2 = filesystem.open(2, libc::O_RDWR as u32).await.unwrap();

        let lock_table = Arc::new(Mutex::new(BTreeMap::new()));

        let lock_job = file_handle
            .set_lock(1, false, lock_table.clone())
            .await
            .unwrap();

        assert!(lock_job.await);

        let lock_job = file_handle2
            .set_lock(2, false, lock_table.clone())
            .await
            .unwrap();

        lock_table.lock().await.get(&2).unwrap().send(()).await;

        debug!("interrupt sent");

        assert!(!lock_job.await)
    }

    #[async_std::test]
    async fn get_lock_kind() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        chroot(tmp_dir.path().to_path_buf()).unwrap();

        let filesystem = Filesystem::new().await.unwrap();

        let (mut file_handle, _) = filesystem
            .create_file(1, OsStr::new("test"), 0o644, libc::O_RDWR as u32)
            .await
            .unwrap();

        assert_eq!(file_handle.get_lock_kind().await, LockKind::NoLock);

        let lock_table = Arc::new(Mutex::new(BTreeMap::new()));

        file_handle
            .set_lock(1, false, lock_table.clone())
            .await
            .unwrap()
            .await;

        assert_eq!(file_handle.get_lock_kind().await, LockKind::Exclusive);

        file_handle.release_lock().await.unwrap();

        assert_eq!(file_handle.get_lock_kind().await, LockKind::NoLock);

        file_handle
            .set_lock(1, true, lock_table.clone())
            .await
            .unwrap()
            .await;

        assert_eq!(file_handle.get_lock_kind().await, LockKind::Share);

        file_handle.release_lock().await.unwrap();

        assert_eq!(file_handle.get_lock_kind().await, LockKind::NoLock);
    }
}
