use std::env;
use std::ffi::{OsStr, OsString};
use std::path::Path;

use fuse::{FileAttr, FileType};
use futures::channel::mpsc::Receiver;
use futures::stream::StreamExt;
use log::{debug, info};
use nix::unistd;

pub use attr::SetAttr;
use dir::Dir;
use entry::Entry;
pub use file_handle::FileHandle;
pub use file_handle::LockKind;
pub use file_handle::LockTable;
use inode::{Inode, InodeMap};
pub use request::Request;

use crate::path::PathClean;
use crate::Result;

mod attr;
mod dir;
mod entry;
mod file;
mod file_handle;
mod inode;
mod request;

pub struct Filesystem {
    inode_map: InodeMap,
    inode_gen: Inode,
    file_handle_id_gen: u64,
    receiver: Receiver<Request>,
}

impl Filesystem {
    /// new a Filesystem will enter a new mount namespace, then chroot to root path
    /// ensure won't be affected by uds client fuse mount
    pub async fn new<P: AsRef<Path>>(root: P, receiver: Receiver<Request>) -> Result<Self> {
        info!("root is {:?}", root.as_ref());

        // pivot_root mode, butI think we don't need use pivot_root, chroot is safe enough
        /*sched::unshare(nix::sched::CloneFlags::CLONE_NEWNS)?;

        let old_root = tempfile::tempdir_in(root.as_ref())?;

        let old_root_path = old_root.path().file_name().expect("old root filename should be valid");

        debug!("old root {:?}", old_root_path);

        // env::set_current_dir(root.as_ref())?;

        mount::mount(Some(""), "/", Some(""), MsFlags::MS_PRIVATE | MsFlags::MS_REC, Some(""))?;

        mount::mount(Some(root.as_ref()), root.as_ref(), Some("bind"), MsFlags::MS_BIND | MsFlags::MS_REC, Some(""))?;

        unistd::pivot_root(root.as_ref(), old_root.path())?;

        info!("pivot root success");

        env::set_current_dir("/")?;

        mount::umount2(old_root_path, MntFlags::MNT_DETACH)?;

        info!("unmount old root success");

        drop(old_root);

        info!("remove old root dir success");

        async_std::fs::read_dir("/")
            .await?
            .for_each(|child| {
                let child = child.unwrap();

                info!("child {:?} in /", child.path());

                futures::future::ready(())
            })
            .await;*/

        Self::chroot(&root).await?;

        let mut inode_map = InodeMap::new();

        let root = Entry::from(Dir::new_root());

        inode_map.insert(1, root);

        Ok(Self {
            inode_gen: 1,
            file_handle_id_gen: 0,
            inode_map,
            receiver,
        })
    }

    async fn chroot<P: AsRef<Path>>(root: P) -> Result<()> {
        unistd::chroot(root.as_ref().as_os_str())?;

        info!("chroot {:?} success", root.as_ref());

        env::set_current_dir("/")?;

        Ok(())
    }

    pub async fn run(&mut self) {
        while let Some(request) = self.receiver.next().await {
            match request {
                Request::Lookup {
                    parent,
                    name,
                    mut response,
                } => {
                    let result = self.lookup(parent, &name).await;
                    let _ = response.try_send(result);
                }

                Request::GetAttr {
                    inode,
                    mut response,
                } => {
                    let result = self.get_attr(inode).await;
                    let _ = response.try_send(result);
                }

                Request::SetAttr {
                    inode,
                    new_attr,
                    mut response,
                } => {
                    let result = self.set_attr(inode, new_attr).await;
                    let _ = response.try_send(result);
                }

                Request::CreateDir {
                    parent,
                    name,
                    mode,
                    mut response,
                } => {
                    let result = self.create_dir(parent, &name, mode).await;
                    let _ = response.try_send(result);
                }

                Request::RemoveEntry {
                    parent,
                    name,
                    is_dir,
                    mut response,
                } => {
                    let result = self.remove_entry(parent, &name, is_dir).await;
                    let _ = response.try_send(result);
                }

                Request::Rename {
                    old_parent,
                    old_name,
                    new_parent,
                    new_name,
                    mut response,
                } => {
                    let result = self
                        .rename(old_parent, &old_name, new_parent, &new_name)
                        .await;
                    let _ = response.try_send(result);
                }

                Request::Open {
                    inode,
                    flags,
                    mut response,
                } => {
                    let result = self.open(inode, flags).await;
                    let _ = response.try_send(result);
                }

                Request::ReadDir {
                    inode,
                    offset,
                    mut response,
                } => {
                    let result = self.read_dir(inode, offset).await;
                    let _ = response.try_send(result);
                }

                Request::CreateFile {
                    parent,
                    name,
                    mode,
                    flags,
                    mut response,
                } => {
                    let result = self.create_file(parent, &name, mode, flags).await;
                    let _ = response.try_send(result);
                }
            }
        }
    }

    async fn lookup(&mut self, parent: Inode, name: &OsStr) -> Result<FileAttr> {
        let name = name.clean()?;

        debug!("lookup name {:?} in parent {}", name, parent);

        let entry = self.inode_map.get(&parent).ok_or(libc::ENOENT)?.clone();

        if let Entry::Dir(mut dir) = entry {
            let child_entry = dir
                .lookup(&name, &mut self.inode_map, &mut self.inode_gen)
                .await?;

            child_entry.get_attr().await
        } else {
            Err(libc::ENOTDIR.into())
        }
    }

    async fn get_attr(&mut self, inode: Inode) -> Result<FileAttr> {
        self.inode_map
            .get(&inode)
            .ok_or(libc::ENOENT)?
            .get_attr()
            .await
    }

    async fn set_attr(&mut self, inode: Inode, set_attr: SetAttr) -> Result<FileAttr> {
        self.inode_map
            .get(&inode)
            .ok_or(libc::ENOENT)?
            .set_attr(set_attr)
            .await
    }

    async fn create_dir(&mut self, parent: Inode, name: &OsStr, mode: u32) -> Result<FileAttr> {
        let name = name.clean()?;

        debug!("create dir name {:?} in parent {}", name, parent);

        let entry = self.inode_map.get(&parent).ok_or(libc::ENOENT)?.clone();

        if let Entry::Dir(mut dir) = entry {
            dir.create_dir(name, mode, &mut self.inode_map, &mut self.inode_gen)
                .await?
                .get_attr()
                .await
        } else {
            Err(libc::ENOTDIR.into())
        }
    }

    async fn remove_entry(&mut self, parent: Inode, name: &OsStr, is_dir: bool) -> Result<()> {
        let name = name.clean()?;

        debug!(
            "remove name {:?} from parent {}, is dir {}",
            name, parent, is_dir
        );

        let entry = self.inode_map.get(&parent).ok_or(libc::ENOENT)?.clone();

        if let Entry::Dir(mut dir) = entry {
            let kind = if is_dir {
                FileType::Directory
            } else {
                FileType::RegularFile
            };

            dir.remove_child(name, kind, &mut self.inode_map, &mut self.inode_gen)
                .await
        } else {
            Err(libc::ENOTDIR.into())
        }
    }

    async fn rename(
        &mut self,
        old_parent: Inode,
        old_name: &OsStr,
        new_parent: Inode,
        new_name: &OsStr,
    ) -> Result<()> {
        let old_name = old_name.clean()?;
        let new_name = new_name.clean()?;

        debug!(
            "rename {:?} from {} to {} as {:?}",
            old_name, old_parent, new_parent, new_name
        );

        let old_parent = self.inode_map.get(&old_parent).ok_or(libc::ENOENT)?.clone();
        let new_parent = self.inode_map.get(&new_parent).ok_or(libc::ENOENT)?.clone();

        if let Entry::Dir(mut old_parent) = old_parent {
            if let Entry::Dir(mut new_parent) = new_parent {
                return old_parent
                    .move_child_to_new_parent(
                        old_name,
                        &mut new_parent,
                        new_name,
                        &mut self.inode_map,
                        &mut self.inode_gen,
                    )
                    .await;
            }
        }

        Err(libc::ENOTDIR.into())
    }

    async fn open(&mut self, inode: Inode, flags: i32) -> Result<FileHandle> {
        debug!("open inode {} flags {}", inode, flags);

        let entry = self.inode_map.get(&inode).ok_or(libc::ENOENT)?;

        if let Entry::File(file) = entry {
            file.open(flags, &mut self.file_handle_id_gen).await
        } else {
            Err(libc::EISDIR.into())
        }
    }

    async fn read_dir(
        &mut self,
        inode: Inode,
        offset: i64,
    ) -> Result<Vec<(Inode, i64, FileType, OsString)>> {
        debug!("readdir in parent {}", inode);

        let entry = self.inode_map.get(&inode).ok_or(libc::ENOENT)?.clone();

        if let Entry::Dir(mut dir) = entry {
            let children = dir
                .readdir(&mut self.inode_map, &mut self.inode_gen)
                .await?;

            Ok(children
                .into_iter()
                .enumerate()
                .map(|(index, (inode, kind, name, _attr))| (inode, (index + 1) as i64, kind, name))
                .skip(offset as usize)
                .collect())
        } else {
            Err(libc::ENOTDIR.into())
        }
    }

    async fn create_file(
        &mut self,
        parent: Inode,
        name: &OsStr,
        mode: u32,
        flags: i32,
    ) -> Result<(FileHandle, FileAttr)> {
        let name = name.clean()?;

        debug!("create file name {:?} in parent {}", name, parent);

        let entry = self.inode_map.get(&parent).ok_or(libc::ENOENT)?.clone();

        if let Entry::Dir(mut dir) = entry {
            let attr = dir
                .create_file(name, mode, flags, &mut self.inode_map, &mut self.inode_gen)
                .await?
                .get_attr()
                .await?;

            let file_handle = self.open(attr.ino, flags).await?;

            Ok((file_handle, attr))
        } else {
            Err(libc::ENOTDIR.into())
        }
    }
}

#[cfg(features = "test")]
#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::os::unix::fs::PermissionsExt;
    use std::sync::Arc;
    use std::time::Duration;

    use async_std::fs;
    use async_std::future::timeout;
    use async_std::sync::Mutex;
    use async_std::task;
    use futures::channel::mpsc::channel;
    use futures::SinkExt;
    use tempfile;

    use crate::log_init;
    use crate::server::filesystem::file_handle::FileHandleKind;
    use crate::server::filesystem::LockKind;
    use crate::Errno;

    use super::*;

    #[async_std::test]
    async fn init_filesystem() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        let (mut fs_tx, fs_rx) = channel(1);

        let mut filesystem = Filesystem::new(tmp_dir.path(), fs_rx).await.unwrap();

        task::spawn(async move { filesystem.run().await });

        let (tx, mut rx) = channel(1);

        let req = Request::GetAttr {
            inode: 1,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let attr = rx.next().await.unwrap().unwrap();

        assert_eq!(attr.ino, 1);
        assert_eq!(attr.kind, FileType::Directory);
        assert_eq!(attr.perm, 0o755);
    }

    #[async_std::test]
    async fn create_dir() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        let (mut fs_tx, fs_rx) = channel(1);

        let mut filesystem = Filesystem::new(tmp_dir.path(), fs_rx).await.unwrap();

        task::spawn(async move { filesystem.run().await });

        let (tx, mut rx) = channel(1);

        let req = Request::CreateDir {
            parent: 1,
            name: OsString::from("test"),
            mode: 0o755,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let attr = rx.next().await.unwrap().unwrap();

        assert_eq!(attr.ino, 2);
        assert_eq!(attr.kind, FileType::Directory);
        assert_eq!(attr.perm, 0o755);
    }

    #[async_std::test]
    async fn create_file() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        let (mut fs_tx, fs_rx) = channel(1);

        let mut filesystem = Filesystem::new(tmp_dir.path(), fs_rx).await.unwrap();

        task::spawn(async move { filesystem.run().await });

        let (tx, mut rx) = channel(1);

        let req = Request::CreateFile {
            parent: 1,
            name: OsString::from("test"),
            mode: 0o644,
            flags: libc::O_RDWR,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let (_, attr) = rx.next().await.unwrap().unwrap();

        assert_eq!(attr.ino, 2);
        assert_eq!(attr.kind, FileType::RegularFile);
        assert_eq!(attr.perm, 0o644);
    }

    #[async_std::test]
    async fn lookup_dir() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        let (mut fs_tx, fs_rx) = channel(1);

        let mut filesystem = Filesystem::new(tmp_dir.path(), fs_rx).await.unwrap();

        task::spawn(async move { filesystem.run().await });

        // create dir
        let (tx, mut rx) = channel(1);

        let req = Request::CreateDir {
            parent: 1,
            name: OsString::from("test"),
            mode: 0o755,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::Lookup {
            parent: 1,
            name: OsString::from("test"),
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let attr = rx.next().await.unwrap().unwrap();

        assert_eq!(attr.ino, 2);
        assert_eq!(attr.kind, FileType::Directory);
        assert_eq!(attr.perm, 0o755);
    }

    #[async_std::test]
    async fn lookup_file() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        let (mut fs_tx, fs_rx) = channel(1);

        let mut filesystem = Filesystem::new(tmp_dir.path(), fs_rx).await.unwrap();

        task::spawn(async move { filesystem.run().await });

        // create file
        let (tx, mut rx) = channel(1);

        let req = Request::CreateFile {
            parent: 1,
            name: OsString::from("test"),
            mode: 0o644,
            flags: libc::O_RDWR,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::Lookup {
            parent: 1,
            name: OsString::from("test"),
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let attr = rx.next().await.unwrap().unwrap();

        assert_eq!(attr.ino, 2);
        assert_eq!(attr.kind, FileType::RegularFile);
        assert_eq!(attr.perm, 0o644);
    }

    #[async_std::test]
    async fn get_attr_dir() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        let (mut fs_tx, fs_rx) = channel(1);

        let mut filesystem = Filesystem::new(tmp_dir.path(), fs_rx).await.unwrap();

        task::spawn(async move { filesystem.run().await });

        // create dir
        let (tx, mut rx) = channel(1);

        let req = Request::CreateDir {
            parent: 1,
            name: OsString::from("test"),
            mode: 0o755,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::GetAttr {
            inode: 2,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let attr = rx.next().await.unwrap().unwrap();

        assert_eq!(attr.ino, 2);
        assert_eq!(attr.kind, FileType::Directory);
        assert_eq!(attr.perm, 0o755);
    }

    #[async_std::test]
    async fn get_attr_file() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        let (mut fs_tx, fs_rx) = channel(1);

        let mut filesystem = Filesystem::new(tmp_dir.path(), fs_rx).await.unwrap();

        task::spawn(async move { filesystem.run().await });

        // create dir
        let (tx, mut rx) = channel(1);

        let req = Request::CreateFile {
            parent: 1,
            name: OsString::from("test"),
            mode: 0o644,
            flags: libc::O_RDWR,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::GetAttr {
            inode: 2,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let attr = rx.next().await.unwrap().unwrap();

        assert_eq!(attr.ino, 2);
        assert_eq!(attr.kind, FileType::RegularFile);
        assert_eq!(attr.perm, 0o644);
    }

    #[async_std::test]
    async fn set_dir_attr() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        let (mut fs_tx, fs_rx) = channel(1);

        let mut filesystem = Filesystem::new(tmp_dir.path(), fs_rx).await.unwrap();

        task::spawn(async move { filesystem.run().await });

        // create dir
        let (tx, mut rx) = channel(1);

        let req = Request::CreateDir {
            parent: 1,
            name: OsString::from("test"),
            mode: 0o755,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::SetAttr {
            inode: 2,
            new_attr: SetAttr {
                ctime: None,
                mtime: None,
                atime: None,
                flags: None,
                uid: None,
                gid: None,
                size: None,
                mode: Some(0o700),
            },
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let attr = rx.next().await.unwrap().unwrap();

        assert_eq!(attr.ino, 2);
        assert_eq!(attr.kind, FileType::Directory);
        assert_eq!(attr.perm, 0o700);
    }

    #[async_std::test]
    async fn remove_dir() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        let (mut fs_tx, fs_rx) = channel(1);

        let mut filesystem = Filesystem::new(tmp_dir.path(), fs_rx).await.unwrap();

        task::spawn(async move { filesystem.run().await });

        // create dir
        let (tx, mut rx) = channel(1);

        let req = Request::CreateDir {
            parent: 1,
            name: OsString::from("test"),
            mode: 0o755,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::RemoveEntry {
            parent: 1,
            name: OsString::from("test"),
            is_dir: true,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::Lookup {
            parent: 1,
            name: OsString::from("test"),
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        assert_eq!(rx.next().await.unwrap(), Err(Errno::from(libc::ENOENT)))
    }

    #[async_std::test]
    async fn remove_file() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        let (mut fs_tx, fs_rx) = channel(1);

        let mut filesystem = Filesystem::new(tmp_dir.path(), fs_rx).await.unwrap();

        task::spawn(async move { filesystem.run().await });

        // create dir
        let (tx, mut rx) = channel(1);

        let req = Request::CreateFile {
            parent: 1,
            name: OsString::from("test"),
            mode: 0o644,
            flags: libc::O_RDWR,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::RemoveEntry {
            parent: 1,
            name: OsString::from("test"),
            is_dir: false,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::Lookup {
            parent: 1,
            name: OsString::from("test"),
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        assert_eq!(rx.next().await.unwrap(), Err(Errno::from(libc::ENOENT)))
    }

    #[async_std::test]
    async fn rename_dir() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        let (mut fs_tx, fs_rx) = channel(1);

        let mut filesystem = Filesystem::new(tmp_dir.path(), fs_rx).await.unwrap();

        task::spawn(async move { filesystem.run().await });

        // create dir
        let (tx, mut rx) = channel(1);

        let req = Request::CreateDir {
            parent: 1,
            name: OsString::from("test"),
            mode: 0o755,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::Rename {
            old_parent: 1,
            old_name: OsString::from("test"),
            new_parent: 1,
            new_name: OsString::from("new-test"),
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::Lookup {
            parent: 1,
            name: OsString::from("new-test"),
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let attr = rx.next().await.unwrap().unwrap();

        assert_eq!(attr.ino, 3);
        assert_eq!(attr.kind, FileType::Directory);
        assert_eq!(attr.perm, 0o755);
    }

    #[async_std::test]
    async fn rename_file() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        let (mut fs_tx, fs_rx) = channel(1);

        let mut filesystem = Filesystem::new(tmp_dir.path(), fs_rx).await.unwrap();

        task::spawn(async move { filesystem.run().await });

        // create dir
        let (tx, mut rx) = channel(1);

        let req = Request::CreateFile {
            parent: 1,
            name: OsString::from("test"),
            mode: 0o644,
            flags: libc::O_RDWR,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::Rename {
            old_parent: 1,
            old_name: OsString::from("test"),
            new_parent: 1,
            new_name: OsString::from("new-test"),
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::Lookup {
            parent: 1,
            name: OsString::from("new-test"),
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let attr = rx.next().await.unwrap().unwrap();

        assert_eq!(attr.ino, 3);
        assert_eq!(attr.kind, FileType::RegularFile);
        assert_eq!(attr.perm, 0o644);
    }

    #[async_std::test]
    async fn move_dir() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        let (mut fs_tx, fs_rx) = channel(1);

        let mut filesystem = Filesystem::new(tmp_dir.path(), fs_rx).await.unwrap();

        task::spawn(async move { filesystem.run().await });

        let (tx, mut rx) = channel(1);

        let req = Request::CreateDir {
            parent: 1,
            name: OsString::from("old"),
            mode: 0o755,
            response: tx,
        }; // inode 2

        fs_tx.send(req).await.unwrap();

        rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::CreateDir {
            parent: 1,
            name: OsString::from("new"),
            mode: 0o755,
            response: tx,
        }; // inode 3

        fs_tx.send(req).await.unwrap();

        rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::CreateDir {
            parent: 2,
            name: OsString::from("test"),
            mode: 0o755,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::Rename {
            old_parent: 2,
            old_name: OsString::from("test"),
            new_parent: 3,
            new_name: OsString::from("new-test"),
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::Lookup {
            parent: 3,
            name: OsString::from("new-test"),
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let attr = rx.next().await.unwrap().unwrap();

        assert_eq!(attr.ino, 5);
        assert_eq!(attr.kind, FileType::Directory);
        assert_eq!(attr.perm, 0o755);
    }

    #[async_std::test]
    async fn move_file() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        let (mut fs_tx, fs_rx) = channel(1);

        let mut filesystem = Filesystem::new(tmp_dir.path(), fs_rx).await.unwrap();

        task::spawn(async move { filesystem.run().await });

        let (tx, mut rx) = channel(1);

        let req = Request::CreateDir {
            parent: 1,
            name: OsString::from("old"),
            mode: 0o755,
            response: tx,
        }; // inode 2

        fs_tx.send(req).await.unwrap();

        rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::CreateDir {
            parent: 1,
            name: OsString::from("new"),
            mode: 0o755,
            response: tx,
        }; // inode 3

        fs_tx.send(req).await.unwrap();

        rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::CreateFile {
            parent: 2,
            name: OsString::from("test"),
            mode: 0o644,
            flags: libc::O_RDWR,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::Rename {
            old_parent: 2,
            old_name: OsString::from("test"),
            new_parent: 3,
            new_name: OsString::from("new-test"),
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::Lookup {
            parent: 3,
            name: OsString::from("new-test"),
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let attr = rx.next().await.unwrap().unwrap();

        assert_eq!(attr.ino, 5);
        assert_eq!(attr.kind, FileType::RegularFile);
        assert_eq!(attr.perm, 0o644);
    }

    #[async_std::test]
    async fn read_dir() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        let (mut fs_tx, fs_rx) = channel(1);

        let mut filesystem = Filesystem::new(tmp_dir.path(), fs_rx).await.unwrap();

        task::spawn(async move { filesystem.run().await });

        let (tx, mut rx) = channel(1);

        let req = Request::CreateDir {
            parent: 1,
            name: OsString::from("test"),
            mode: 0o755,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::ReadDir {
            inode: 1,
            offset: 0,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let children = rx.next().await.unwrap().unwrap();

        assert_eq!(children.len(), 3);

        let (inode, _, kind, name) = &children[0];

        assert_eq!(*inode, 1);
        assert_eq!(*kind, FileType::Directory);
        assert_eq!(*name, OsString::from("."));

        let (inode, _, kind, name) = &children[1];

        assert_eq!(*inode, 1);
        assert_eq!(*kind, FileType::Directory);
        assert_eq!(*name, OsString::from(".."));

        let (inode, _, kind, name) = &children[2];

        assert_eq!(*inode, 2);
        assert_eq!(*kind, FileType::Directory);
        assert_eq!(*name, OsString::from("test"));
    }

    #[async_std::test]
    async fn open_file_rw() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        let (mut fs_tx, fs_rx) = channel(1);

        let mut filesystem = Filesystem::new(tmp_dir.path(), fs_rx).await.unwrap();

        task::spawn(async move { filesystem.run().await });

        let (tx, mut rx) = channel(1);

        let req = Request::CreateFile {
            parent: 1,
            name: OsString::from("test"),
            mode: 0o644,
            flags: libc::O_RDWR,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::Open {
            inode: 2,
            flags: libc::O_RDWR,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let file_handle = rx.next().await.unwrap().unwrap();

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

        let (mut fs_tx, fs_rx) = channel(1);

        let mut filesystem = Filesystem::new(tmp_dir.path(), fs_rx).await.unwrap();

        task::spawn(async move { filesystem.run().await });

        let (tx, mut rx) = channel(1);

        let req = Request::CreateFile {
            parent: 1,
            name: OsString::from("test"),
            mode: 0o644,
            flags: libc::O_RDWR,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::Open {
            inode: 2,
            flags: libc::O_RDONLY,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let file_handle = rx.next().await.unwrap().unwrap();

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

        let (mut fs_tx, fs_rx) = channel(1);

        let mut filesystem = Filesystem::new(tmp_dir.path(), fs_rx).await.unwrap();

        task::spawn(async move { filesystem.run().await });

        let (tx, mut rx) = channel(1);

        let req = Request::CreateFile {
            parent: 1,
            name: OsString::from("test"),
            mode: 0o644,
            flags: libc::O_RDWR,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::Open {
            inode: 2,
            flags: libc::O_WRONLY,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let file_handle = rx.next().await.unwrap().unwrap();

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

        let (mut fs_tx, fs_rx) = channel(1);

        let mut filesystem = Filesystem::new(tmp_dir.path(), fs_rx).await.unwrap();

        task::spawn(async move { filesystem.run().await });

        let (tx, mut rx) = channel(1);

        let req = Request::CreateFile {
            parent: 1,
            name: OsString::from("test"),
            mode: 0o644,
            flags: libc::O_RDWR,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let (mut file_handle, _) = rx.next().await.unwrap().unwrap();

        let written = file_handle.write(b"test", 0).await.unwrap();
        file_handle.flush().await.unwrap();

        assert_eq!(written, 4);
        assert_eq!(file_handle.get_attr().await.unwrap().size, 4)
    }

    #[async_std::test]
    async fn read_file() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        let (mut fs_tx, fs_rx) = channel(1);

        let mut filesystem = Filesystem::new(tmp_dir.path(), fs_rx).await.unwrap();

        task::spawn(async move { filesystem.run().await });

        let (tx, mut rx) = channel(1);

        let req = Request::CreateFile {
            parent: 1,
            name: OsString::from("test"),
            mode: 0o644,
            flags: libc::O_RDWR,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let (mut file_handle, _) = rx.next().await.unwrap().unwrap();

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

        let (mut fs_tx, fs_rx) = channel(1);

        let mut filesystem = Filesystem::new(tmp_dir.path(), fs_rx).await.unwrap();

        task::spawn(async move { filesystem.run().await });

        let (tx, mut rx) = channel(1);

        let req = Request::CreateFile {
            parent: 1,
            name: OsString::from("test"),
            mode: 0o644,
            flags: libc::O_RDWR,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let (mut file_handle, _) = rx.next().await.unwrap().unwrap();

        assert_eq!(file_handle.write(b"test", 0).await, Ok(b"test".len()));

        file_handle.flush().await.unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::SetAttr {
            inode: 2,
            new_attr: SetAttr {
                mode: Some(0o600),
                uid: None,
                gid: None,
                size: Some(2),
                atime: None,
                mtime: None,
                ctime: None,
                flags: None,
            },
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let attr = rx.next().await.unwrap().unwrap();

        assert_eq!(attr.perm, 0o600);
        assert_eq!(attr.size, 2);

        let (tx, mut rx) = channel(1);

        let req = Request::Open {
            inode: 2,
            flags: libc::O_RDONLY,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let mut file_handle = rx.next().await.unwrap().unwrap();

        let mut buf = vec![0; 4];

        let read = file_handle.read(&mut buf, 0).await.unwrap();
        assert_eq!(read, 2);
        assert_eq!(&b"te"[..], &buf[..read])
    }

    #[async_std::test]
    async fn set_share_lock_success() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        let (mut fs_tx, fs_rx) = channel(1);

        let mut filesystem = Filesystem::new(tmp_dir.path(), fs_rx).await.unwrap();

        task::spawn(async move { filesystem.run().await });

        let (tx, mut rx) = channel(1);

        let req = Request::CreateFile {
            parent: 1,
            name: OsString::from("test"),
            mode: 0o644,
            flags: libc::O_RDWR,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let (mut file_handle, _) = rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::Open {
            inode: 2,
            flags: libc::O_RDWR,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let mut file_handle2 = rx.next().await.unwrap().unwrap();

        let lock_table = Arc::new(Mutex::new(BTreeMap::new()));

        let lock_job = file_handle
            .set_lock(1, true, lock_table.clone())
            .await
            .unwrap();

        let lock_job = timeout(Duration::from_secs(2), lock_job)
            .await
            .expect("set share lock failed")
            .unwrap();

        assert!(lock_job);

        let lock_job = file_handle2.set_lock(2, true, lock_table).await.unwrap();

        let lock_job = timeout(Duration::from_secs(2), lock_job)
            .await
            .expect("set another share lock failed")
            .unwrap();

        assert!(lock_job);
    }

    #[async_std::test]
    async fn set_share_lock_failed() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        let (mut fs_tx, fs_rx) = channel(1);

        let mut filesystem = Filesystem::new(tmp_dir.path(), fs_rx).await.unwrap();

        task::spawn(async move { filesystem.run().await });

        let (tx, mut rx) = channel(1);

        let req = Request::CreateFile {
            parent: 1,
            name: OsString::from("test"),
            mode: 0o644,
            flags: libc::O_RDWR,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let (mut file_handle, _) = rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::Open {
            inode: 2,
            flags: libc::O_RDWR,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let mut file_handle2 = rx.next().await.unwrap().unwrap();

        let lock_table = Arc::new(Mutex::new(BTreeMap::new()));

        let lock_job = file_handle
            .set_lock(1, true, lock_table.clone())
            .await
            .unwrap();

        let lock_job = timeout(Duration::from_secs(2), lock_job)
            .await
            .expect("set share lock failed")
            .unwrap();

        assert!(lock_job);

        let lock_job = file_handle2.set_lock(2, false, lock_table).await.unwrap();

        if timeout(Duration::from_secs(1), lock_job).await.is_ok() {
            panic!("set not share lock success")
        }
    }

    #[async_std::test]
    async fn try_set_share_lock_success() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        let (mut fs_tx, fs_rx) = channel(1);

        let mut filesystem = Filesystem::new(tmp_dir.path(), fs_rx).await.unwrap();

        task::spawn(async move { filesystem.run().await });

        let (tx, mut rx) = channel(1);

        let req = Request::CreateFile {
            parent: 1,
            name: OsString::from("test"),
            mode: 0o644,
            flags: libc::O_RDWR,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let (file_handle, _) = rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::Open {
            inode: 2,
            flags: libc::O_RDWR,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let file_handle2 = rx.next().await.unwrap().unwrap();

        assert_eq!(file_handle.try_set_lock(true).await, Ok(()));
        assert_eq!(file_handle2.try_set_lock(true).await, Ok(()));
    }

    #[async_std::test]
    async fn try_set_share_lock_failed() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        let (mut fs_tx, fs_rx) = channel(1);

        let mut filesystem = Filesystem::new(tmp_dir.path(), fs_rx).await.unwrap();

        task::spawn(async move { filesystem.run().await });

        let (tx, mut rx) = channel(1);

        let req = Request::CreateFile {
            parent: 1,
            name: OsString::from("test"),
            mode: 0o644,
            flags: libc::O_RDWR,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let (file_handle, _) = rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::Open {
            inode: 2,
            flags: libc::O_RDWR,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let file_handle2 = rx.next().await.unwrap().unwrap();

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

        let (mut fs_tx, fs_rx) = channel(1);

        let mut filesystem = Filesystem::new(tmp_dir.path(), fs_rx).await.unwrap();

        task::spawn(async move { filesystem.run().await });

        let (tx, mut rx) = channel(1);

        let req = Request::CreateFile {
            parent: 1,
            name: OsString::from("test"),
            mode: 0o644,
            flags: libc::O_RDWR,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let (mut file_handle, _) = rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::Open {
            inode: 2,
            flags: libc::O_RDWR,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let mut file_handle2 = rx.next().await.unwrap().unwrap();

        let lock_table = Arc::new(Mutex::new(BTreeMap::new()));

        let lock_job = file_handle
            .set_lock(1, false, lock_table.clone())
            .await
            .unwrap();

        let lock_job = timeout(Duration::from_secs(2), lock_job)
            .await
            .expect("set exclusive lock failed")
            .unwrap();

        assert!(lock_job);

        let lock_job = file_handle2
            .set_lock(2, false, lock_table.clone())
            .await
            .unwrap();

        if timeout(Duration::from_secs(1), lock_job).await.is_ok() {
            panic!("set exclusive lock should failed");
        }

        let lock_job = file_handle2.set_lock(3, true, lock_table).await.unwrap();

        if timeout(Duration::from_secs(1), lock_job).await.is_ok() {
            panic!("set share lock should failed");
        }
    }

    #[async_std::test]
    async fn try_set_exclusive_lock() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        let (mut fs_tx, fs_rx) = channel(1);

        let mut filesystem = Filesystem::new(tmp_dir.path(), fs_rx).await.unwrap();

        task::spawn(async move { filesystem.run().await });

        let (tx, mut rx) = channel(1);

        let req = Request::CreateFile {
            parent: 1,
            name: OsString::from("test"),
            mode: 0o644,
            flags: libc::O_RDWR,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let (file_handle, _) = rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::Open {
            inode: 2,
            flags: libc::O_RDWR,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let file_handle2 = rx.next().await.unwrap().unwrap();

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

        let (mut fs_tx, fs_rx) = channel(1);

        let mut filesystem = Filesystem::new(tmp_dir.path(), fs_rx).await.unwrap();

        task::spawn(async move { filesystem.run().await });

        let (tx, mut rx) = channel(1);

        let req = Request::CreateFile {
            parent: 1,
            name: OsString::from("test"),
            mode: 0o644,
            flags: libc::O_RDWR,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let (mut file_handle, _) = rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::Open {
            inode: 2,
            flags: libc::O_RDWR,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let file_handle2 = rx.next().await.unwrap().unwrap();

        let lock_queue = Arc::new(Mutex::new(BTreeMap::new()));

        let lock_job = file_handle.set_lock(1, true, lock_queue).await.unwrap();

        assert!(lock_job.await.unwrap());
        assert_eq!(file_handle.try_release_lock().await, Ok(()));

        assert_eq!(file_handle2.try_set_lock(false).await, Ok(()));
    }

    #[async_std::test]
    async fn release_exclusive_lock() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        let (mut fs_tx, fs_rx) = channel(1);

        let mut filesystem = Filesystem::new(tmp_dir.path(), fs_rx).await.unwrap();

        task::spawn(async move { filesystem.run().await });

        let (tx, mut rx) = channel(1);

        let req = Request::CreateFile {
            parent: 1,
            name: OsString::from("test"),
            mode: 0o644,
            flags: libc::O_RDWR,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let (mut file_handle, _) = rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::Open {
            inode: 2,
            flags: libc::O_RDWR,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let file_handle2 = rx.next().await.unwrap().unwrap();

        let lock_queue = Arc::new(Mutex::new(BTreeMap::new()));

        let lock_job = file_handle.set_lock(1, false, lock_queue).await.unwrap();

        assert!(lock_job.await.unwrap());
        assert_eq!(file_handle.try_release_lock().await, Ok(()));

        assert_eq!(file_handle2.try_set_lock(false).await, Ok(()));
    }

    #[async_std::test]
    async fn interrupt_lock() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        let (mut fs_tx, fs_rx) = channel(1);

        let mut filesystem = Filesystem::new(tmp_dir.path(), fs_rx).await.unwrap();

        task::spawn(async move { filesystem.run().await });

        let (tx, mut rx) = channel(1);

        let req = Request::CreateFile {
            parent: 1,
            name: OsString::from("test"),
            mode: 0o644,
            flags: libc::O_RDWR,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let (mut file_handle, _) = rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::Open {
            inode: 2,
            flags: libc::O_RDWR,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let mut file_handle2 = rx.next().await.unwrap().unwrap();

        let lock_table = Arc::new(Mutex::new(BTreeMap::new()));

        let lock_job = file_handle
            .set_lock(1, false, lock_table.clone())
            .await
            .unwrap();

        assert!(lock_job.await.unwrap());

        let lock_job = file_handle2
            .set_lock(2, false, lock_table.clone())
            .await
            .unwrap();

        lock_table.lock().await.get(&2).unwrap().notify();

        debug!("interrupt sent");

        assert!(!lock_job.await.unwrap())
    }

    #[async_std::test]
    async fn wait_exclusive_lock() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        let (mut fs_tx, fs_rx) = channel(1);

        let mut filesystem = Filesystem::new(tmp_dir.path(), fs_rx).await.unwrap();

        task::spawn(async move { filesystem.run().await });

        let (tx, mut rx) = channel(1);

        let req = Request::CreateFile {
            parent: 1,
            name: OsString::from("test"),
            mode: 0o644,
            flags: libc::O_RDWR,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let (mut file_handle, _) = rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::Open {
            inode: 2,
            flags: libc::O_RDWR,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let mut file_handle2 = rx.next().await.unwrap().unwrap();

        let lock_table = Arc::new(Mutex::new(BTreeMap::new()));

        assert_eq!(
            file_handle
                .set_lock(1, false, lock_table.clone())
                .await
                .unwrap()
                .await,
            Ok(true),
        );

        task::spawn(async move {
            task::sleep(Duration::from_secs(1)).await;

            file_handle.try_release_lock().await.unwrap();
        });

        let lock_job = file_handle2
            .set_lock(2, false, lock_table.clone())
            .await
            .unwrap();

        let lock_result = timeout(Duration::from_secs(2), lock_job)
            .await
            .expect("set exclusive lock should success");

        assert!(lock_result.unwrap())
    }

    #[async_std::test]
    async fn get_lock_kind() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        let (mut fs_tx, fs_rx) = channel(1);

        let mut filesystem = Filesystem::new(tmp_dir.path(), fs_rx).await.unwrap();

        task::spawn(async move { filesystem.run().await });

        let (tx, mut rx) = channel(1);

        let req = Request::CreateFile {
            parent: 1,
            name: OsString::from("test"),
            mode: 0o644,
            flags: libc::O_RDWR,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let (mut file_handle, _) = rx.next().await.unwrap().unwrap();

        assert_eq!(file_handle.get_lock_kind().await, LockKind::NoLock);

        let lock_table = Arc::new(Mutex::new(BTreeMap::new()));

        assert_eq!(
            file_handle
                .set_lock(1, false, lock_table.clone())
                .await
                .unwrap()
                .await,
            Ok(true),
        );

        assert_eq!(file_handle.get_lock_kind().await, LockKind::Exclusive);

        file_handle.try_release_lock().await.unwrap();

        assert_eq!(file_handle.get_lock_kind().await, LockKind::NoLock);

        assert_eq!(
            file_handle
                .set_lock(1, true, lock_table.clone())
                .await
                .unwrap()
                .await,
            Ok(true),
        );

        assert_eq!(file_handle.get_lock_kind().await, LockKind::Share);

        file_handle.try_release_lock().await.unwrap();

        assert_eq!(file_handle.get_lock_kind().await, LockKind::NoLock);
    }

    #[async_std::test]
    async fn lookup_exist_entry() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        let tmp_file = tempfile::NamedTempFile::new_in(&tmp_dir).unwrap();

        let perm = fs::metadata(tmp_file.path()).await.unwrap().permissions();
        let perm = perm.mode() ^ libc::S_IFREG;

        let (mut fs_tx, fs_rx) = channel(1);

        let mut filesystem = Filesystem::new(tmp_dir.path(), fs_rx).await.unwrap();

        task::spawn(async move { filesystem.run().await });

        let (tx, mut rx) = channel(1);

        let req = Request::Lookup {
            parent: 1,
            name: tmp_file.path().file_name().unwrap().to_os_string(),
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let attr = rx.next().await.unwrap().unwrap();

        assert_eq!(attr.ino, 2);
        assert_eq!(attr.kind, FileType::RegularFile);
        assert_eq!(attr.perm as u32, perm);
    }

    // issue #8
    #[async_std::test]
    async fn rename_to_exist_dir() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        let (mut fs_tx, fs_rx) = channel(1);

        let mut filesystem = Filesystem::new(tmp_dir.path(), fs_rx).await.unwrap();

        task::spawn(async move { filesystem.run().await });

        let (tx, mut rx) = channel(1);

        let req = Request::CreateDir {
            parent: 1,
            name: OsString::from("test"),
            mode: 0o755,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::CreateDir {
            parent: 1,
            name: OsString::from("exist"),
            mode: 0o755,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::Rename {
            old_parent: 1,
            old_name: OsString::from("test"),
            new_parent: 1,
            new_name: OsString::from("exist"),
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::Lookup {
            parent: 1,
            name: OsString::from("exist"),
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let attr = rx.next().await.unwrap().unwrap();

        assert_eq!(attr.ino, 4);
        assert_eq!(attr.kind, FileType::Directory);
        assert_eq!(attr.perm, 0o755);
    }

    // issue #8
    #[async_std::test]
    async fn rename_to_exist_file() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        let (mut fs_tx, fs_rx) = channel(1);

        let mut filesystem = Filesystem::new(tmp_dir.path(), fs_rx).await.unwrap();

        task::spawn(async move { filesystem.run().await });

        let (tx, mut rx) = channel(1);

        let req = Request::CreateFile {
            parent: 1,
            name: OsString::from("test"),
            mode: 0o644,
            flags: libc::O_RDWR,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::CreateFile {
            parent: 1,
            name: OsString::from("exist"),
            mode: 0o644,
            flags: libc::O_RDWR,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::Rename {
            old_parent: 1,
            old_name: OsString::from("test"),
            new_parent: 1,
            new_name: OsString::from("exist"),
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::Lookup {
            parent: 1,
            name: OsString::from("exist"),
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let attr = rx.next().await.unwrap().unwrap();

        assert_eq!(attr.ino, 4);
        assert_eq!(attr.kind, FileType::RegularFile);
        assert_eq!(attr.perm, 0o644);
    }
}
