use std::env;
use std::ffi::{OsStr, OsString};
use std::io::ErrorKind;
use std::os::raw::c_int;
use std::os::unix::fs::DirBuilderExt;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use async_std::fs;
use async_std::fs::{DirBuilder, Metadata, OpenOptions};
use fuse::{FileAttr, FileType};
use futures::channel::mpsc::Receiver;
use futures::stream::StreamExt;
use log::{debug, error, info};
use nix::unistd;

pub use attr::SetAttr;
use entry::EntryPath;
pub use file_handle::FileHandle;
pub use file_handle::LockKind;
pub use file_handle::LockTable;
use inode::{Inode, InodeToPath};
pub use request::Request;

use crate::path::PathClean;
use crate::server::filesystem::attr::metadata_to_file_attr;
use crate::server::filesystem::file_handle::FileHandleKind;
use crate::server::filesystem::inode::PathToInode;
use crate::{Apply, Result};

mod attr;
mod entry;
mod file_handle;

mod inode;
mod request;

pub struct Filesystem {
    inode_to_path: InodeToPath,
    path_to_inode: PathToInode,
    inode_gen: AtomicU64,
    file_handle_id_gen: AtomicU64,
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

        let mut inode_to_path = InodeToPath::new();
        let mut path_to_inode = PathToInode::new();

        let root_path = PathBuf::from("/");

        inode_to_path.insert(1, EntryPath::Dir(root_path.clone()));
        path_to_inode.insert(root_path, 1);

        Ok(Self {
            inode_to_path,
            path_to_inode,
            inode_gen: AtomicU64::new(2),
            file_handle_id_gen: AtomicU64::new(1),
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

        let entry_path = match self.inode_to_path.get(&parent) {
            None => return Err(libc::ENOENT.into()),
            Some(entry_path) => entry_path.clone(),
        };

        let path = entry_path.get_path();

        let metadata = self.get_metadata_with_inode_and_path(parent, path).await?;

        if self.check_and_fix_inode_and_path(parent, &entry_path, path, &metadata) {
            return Err(libc::ENOENT.into());
        }

        if metadata.is_file() {
            return Err(libc::ENOTDIR.into());
        }

        let lookup_path = path.to_path_buf().apply(|path| path.push(name));

        let metadata = match fs::metadata(&lookup_path).await {
            Err(err) => {
                return if let ErrorKind::NotFound = err.kind() {
                    if let Some(inode) = self.path_to_inode.remove(&lookup_path) {
                        self.inode_to_path.remove(&inode);
                    }

                    Err(libc::ENOENT.into())
                } else {
                    Err(err.into())
                };
            }

            Ok(metadata) => metadata,
        };

        let inode = match self.path_to_inode.get(&lookup_path) {
            None => {
                let new_inode = self.inode_gen.fetch_add(1, Ordering::Relaxed);

                self.path_to_inode.insert(lookup_path.clone(), new_inode);

                let entry_path = if metadata.is_file() {
                    EntryPath::File(lookup_path)
                } else {
                    EntryPath::Dir(lookup_path)
                };

                self.inode_to_path.insert(new_inode, entry_path);

                new_inode
            }

            Some(&inode) => {
                let entry_path = self.inode_to_path.get(&inode).expect("checked").clone();

                if self.check_and_fix_inode_and_path(inode, &entry_path, lookup_path, &metadata) {
                    return Err(libc::ENOENT.into());
                }

                inode
            }
        };

        metadata_to_file_attr(inode, metadata)
    }

    async fn get_attr(&mut self, inode: Inode) -> Result<FileAttr> {
        let entry_path = match self.inode_to_path.get(&inode) {
            None => return Err(libc::ENOENT.into()),
            Some(entry_path) => entry_path.clone(),
        };

        let path = entry_path.get_path();

        let metadata = self.get_metadata_with_inode_and_path(inode, path).await?;

        if self.check_and_fix_inode_and_path(inode, &entry_path, path, &metadata) {
            return Err(libc::ENOENT.into());
        }

        metadata_to_file_attr(inode, metadata)
    }

    async fn set_attr(&mut self, inode: Inode, set_attr: SetAttr) -> Result<FileAttr> {
        let entry_path = match self.inode_to_path.get(&inode) {
            None => return Err(libc::ENOENT.into()),
            Some(entry_path) => entry_path.clone(),
        };

        let path = entry_path.get_path();

        let metadata = self.get_metadata_with_inode_and_path(inode, path).await?;

        if self.check_and_fix_inode_and_path(inode, &entry_path, path, &metadata) {
            return Err(libc::ENOENT.into());
        }

        if let Some(mode) = set_attr.mode {
            let mut permissions = metadata.permissions();

            permissions.set_mode(mode);

            fs::set_permissions(path, permissions).await?;
        }

        if let Some(size) = set_attr.size {
            if entry_path.is_dir() {
                return Err(libc::EISDIR.into());
            }

            let truncate = size == 0;

            let file = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .truncate(truncate)
                .open(path)
                .await?;

            if size > 0 {
                debug!("set attr size {}", size);

                if let Err(err) = file.set_len(size).await {
                    error!("set inode {} size {} failed", inode, size);

                    return Err(err.into());
                }
            }
        }

        self.get_attr(inode).await
    }

    async fn create_dir(&mut self, parent: Inode, name: &OsStr, mode: u32) -> Result<FileAttr> {
        let name = name.clean()?;

        debug!("create dir name {:?} in parent {}", name, parent);

        let entry_path = match self.inode_to_path.get(&parent) {
            None => return Err(libc::ENOENT.into()),
            Some(entry_path) => entry_path.clone(),
        };

        let path = entry_path.get_path();

        let metadata = self.get_metadata_with_inode_and_path(parent, path).await?;

        if self.check_and_fix_inode_and_path(parent, &entry_path, path, &metadata) {
            return Err(libc::ENOENT.into());
        }

        if metadata.is_file() {
            return Err(libc::ENOTDIR.into());
        }

        let create_path = path.to_path_buf().apply(|path| path.push(name));

        match fs::metadata(&create_path).await {
            Ok(metadata) => {
                if let Some(&inode) = self.path_to_inode.get(&create_path) {
                    let entry_path = self.inode_to_path.get(&inode).expect("checked").clone();

                    self.check_and_fix_inode_and_path(inode, &entry_path, &create_path, &metadata);
                } else {
                    let new_inode = self.inode_gen.fetch_add(1, Ordering::Relaxed);

                    let entry_path = if metadata.is_file() {
                        EntryPath::File(create_path.clone())
                    } else {
                        EntryPath::Dir(create_path.clone())
                    };

                    self.inode_to_path.insert(new_inode, entry_path);
                    self.path_to_inode.insert(create_path, new_inode);
                }

                return Err(libc::EEXIST.into());
            }

            Err(err) => {
                if err.kind() != ErrorKind::NotFound {
                    return Err(err.into());
                }
            }
        }

        // clean old record
        if let Some(inode) = self.path_to_inode.remove(&create_path) {
            self.inode_to_path.remove(&inode);
        }

        DirBuilder::new().mode(mode).create(&create_path).await?;

        let new_inode = self.inode_gen.fetch_add(1, Ordering::Relaxed);

        let entry_path = EntryPath::Dir(create_path.clone());

        self.inode_to_path.insert(new_inode, entry_path);
        self.path_to_inode.insert(create_path.clone(), new_inode);

        let metadata = fs::metadata(create_path).await?;

        metadata_to_file_attr(new_inode, metadata)
    }

    async fn remove_entry(&mut self, parent: Inode, name: &OsStr, is_dir: bool) -> Result<()> {
        let name = name.clean()?;

        debug!(
            "remove name {:?} from parent {}, is dir {}",
            name, parent, is_dir
        );

        let entry_path = match self.inode_to_path.get(&parent) {
            None => return Err(libc::ENOENT.into()),
            Some(entry_path) => entry_path.clone(),
        };

        let path = entry_path.get_path();

        let metadata = self.get_metadata_with_inode_and_path(parent, path).await?;

        if self.check_and_fix_inode_and_path(parent, &entry_path, path, &metadata) {
            return Err(libc::ENOENT.into());
        }

        if metadata.is_file() {
            return Err(libc::ENOTDIR.into());
        }

        let remove_path = path.to_path_buf().apply(|path| path.push(name));

        let metadata = match fs::metadata(&remove_path).await {
            Err(err) => {
                return if let ErrorKind::NotFound = err.kind() {
                    if let Some(inode) = self.path_to_inode.remove(&remove_path) {
                        self.inode_to_path.remove(&inode);
                    }

                    Err(libc::ENOENT.into())
                } else {
                    Err(err.into())
                };
            }

            Ok(metadata) => metadata,
        };

        if metadata.is_file() && is_dir {
            return Err(libc::ENOTDIR.into());
        } else if metadata.is_dir() && !is_dir {
            return Err(libc::EISDIR.into());
        }

        if metadata.is_file() {
            fs::remove_file(&remove_path).await?;
        } else {
            fs::remove_dir_all(&remove_path).await?;
        }

        if let Some(inode) = self.path_to_inode.remove(&remove_path) {
            self.inode_to_path.remove(&inode);
        }

        Ok(())
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

        let old_parent_entry_path = match self.inode_to_path.get(&old_parent) {
            None => return Err(libc::ENOENT.into()),
            Some(entry_path) => entry_path.clone(),
        };

        let old_parent_path = old_parent_entry_path.get_path();

        let old_parent_metadata = self
            .get_metadata_with_inode_and_path(old_parent, old_parent_path)
            .await?;

        if self.check_and_fix_inode_and_path(
            old_parent,
            &old_parent_entry_path,
            old_parent_path,
            &old_parent_metadata,
        ) {
            return Err(libc::ENOENT.into());
        }

        if old_parent_metadata.is_file() {
            return Err(libc::ENOTDIR.into());
        }

        let old_child_path = old_parent_path
            .to_path_buf()
            .apply(|path| path.push(old_name));

        let old_child_metadata = match fs::metadata(&old_child_path).await {
            Err(err) => {
                return if err.kind() == ErrorKind::NotFound {
                    if let Some(inode) = self.path_to_inode.remove(&old_child_path) {
                        self.inode_to_path.remove(&inode);
                    }

                    Err(libc::ENOENT.into())
                } else {
                    Err(err.into())
                };
            }

            Ok(metadata) => metadata,
        };

        let new_parent_entry_path = match self.inode_to_path.get(&new_parent) {
            None => return Err(libc::ENOENT.into()),
            Some(entry_path) => entry_path.clone(),
        };

        let new_parent_path = new_parent_entry_path.get_path();

        let new_parent_metadata = self
            .get_metadata_with_inode_and_path(new_parent, new_parent_path)
            .await?;

        if self.check_and_fix_inode_and_path(
            new_parent,
            &new_parent_entry_path,
            new_parent_path,
            &new_parent_metadata,
        ) {
            return Err(libc::ENOENT.into());
        }

        if new_parent_metadata.is_file() {
            return Err(libc::ENOTDIR.into());
        }

        let new_child_path = new_parent_path
            .to_path_buf()
            .apply(|path| path.push(new_name));

        fs::rename(&old_child_path, &new_child_path).await?;

        // no matter exist or not, remove new child inode and path record
        if let Some(inode) = self.path_to_inode.remove(&new_child_path) {
            self.inode_to_path.remove(&inode);
        }

        let new_child_entry_path = if old_child_metadata.is_file() {
            EntryPath::File(new_child_path.clone())
        } else {
            EntryPath::Dir(new_child_path.clone())
        };

        if let Some(inode) = self.path_to_inode.remove(&old_child_path) {
            self.path_to_inode.insert(new_child_path, inode);

            self.inode_to_path.entry(inode).and_modify(|path| {
                *path = new_child_entry_path;
            });
        } else {
            // server side other people add a new 'old child', rfs server doesn't know it, to fix
            // it, we only can add a new inode and entry path

            let new_child_inode = self.inode_gen.fetch_add(1, Ordering::Relaxed);

            self.inode_to_path
                .insert(new_child_inode, new_child_entry_path);
            self.path_to_inode.insert(new_child_path, new_child_inode);
        }

        Ok(())
    }

    async fn open(&mut self, inode: Inode, flags: u32) -> Result<FileHandle> {
        let entry_path = match self.inode_to_path.get(&inode) {
            None => return Err(libc::ENOENT.into()),
            Some(entry_path) => entry_path.clone(),
        };

        let path = entry_path.get_path();

        let metadata = self.get_metadata_with_inode_and_path(inode, path).await?;

        if self.check_and_fix_inode_and_path(inode, &entry_path, path, &metadata) {
            return Err(libc::ENOENT.into());
        }

        if metadata.is_dir() {
            return Err(libc::EISDIR.into());
        }

        debug!("open {:?} flags {}", path, flags);

        let mut options = fs::OpenOptions::new();

        let flags = flags as c_int;

        let fh_kind = if flags & libc::O_RDWR > 0 {
            options.write(true);
            options.read(true);

            FileHandleKind::ReadWrite
        } else if flags & libc::O_WRONLY > 0 {
            options.write(true);
            options.read(false);

            FileHandleKind::WriteOnly
        } else {
            options.write(false);
            options.read(true);

            FileHandleKind::ReadOnly
        };

        if flags & libc::O_TRUNC > 0 {
            debug!("open {:?} flags have truncate", path);

            options.truncate(true);
        }

        let sys_file = options.open(path).await?;

        Ok(FileHandle::new(
            self.file_handle_id_gen.fetch_add(1, Ordering::Relaxed),
            #[cfg(features = "test")]
            inode,
            sys_file,
            fh_kind,
        ))
    }

    async fn read_dir(
        &mut self,
        inode: Inode,
        offset: i64,
    ) -> Result<Vec<(Inode, i64, FileType, OsString)>> {
        debug!("readdir in parent {}", inode);

        let entry_path = match self.inode_to_path.get(&inode) {
            None => return Err(libc::ENOENT.into()),
            Some(entry_path) => entry_path.clone(),
        };

        let path = entry_path.get_path();

        let metadata = self.get_metadata_with_inode_and_path(inode, path).await?;

        if self.check_and_fix_inode_and_path(inode, &entry_path, path, &metadata) {
            return Err(libc::ENOENT.into());
        }

        if metadata.is_file() {
            return Err(libc::ENOTDIR.into());
        }

        let parent_path = if let Some(parent_path) = path.parent() {
            parent_path
        } else if path == Path::new("/") {
            path
        } else {
            return Err(libc::EIO.into());
        };

        let parent_inode = if let Some(&parent_inode) = self.path_to_inode.get(parent_path) {
            parent_inode
        } else {
            // deleted by client, but recreate by someone
            let parent_inode = self.inode_gen.fetch_add(1, Ordering::Relaxed);

            self.inode_to_path
                .insert(parent_inode, EntryPath::Dir(parent_path.to_path_buf()));
            self.path_to_inode
                .insert(parent_path.to_path_buf(), parent_inode);

            parent_inode
        };

        let mut children = vec![
            (inode, FileType::Directory, OsString::from(".")),
            (parent_inode, FileType::Directory, OsString::from("..")),
        ];

        debug!("inode is {}, parnet inode is {}", inode, parent_inode);

        let mut dir_entries = fs::read_dir(path).await?;

        while let Some(child) = dir_entries.next().await {
            if let Ok(child) = child {
                let metadata = if let Ok(metadata) = child.metadata().await {
                    metadata
                } else {
                    continue;
                };

                let file_type = if metadata.is_file() {
                    FileType::RegularFile
                } else {
                    FileType::Directory
                };

                let child_name = child.file_name();

                let child_path = path.to_path_buf().apply(|path| path.push(&child_name));

                debug!(
                    "file type {:?}, child name {:?}, child path {:?}",
                    file_type, child_name, child_path
                );

                let child_inode = if let Some(&inode) = self.path_to_inode.get(&child_path) {
                    let entry_path = self.inode_to_path.get(&inode).expect("checked").clone();

                    self.check_and_fix_inode_and_path(inode, &entry_path, &child_path, &metadata);

                    *self.path_to_inode.get(&child_path).expect("checked")
                } else {
                    let new_child_inode = self.inode_gen.fetch_add(1, Ordering::Relaxed);

                    let entry_path = if file_type == FileType::RegularFile {
                        EntryPath::File(child_path.clone())
                    } else {
                        EntryPath::Dir(child_path.clone())
                    };

                    self.inode_to_path.insert(new_child_inode, entry_path);
                    self.path_to_inode
                        .insert(child_path.clone(), new_child_inode);

                    new_child_inode
                };

                children.push((child_inode, file_type, child_name));
            }
        }

        Ok(children
            .into_iter()
            .enumerate()
            .map(|(index, (inode, kind, name))| (inode, (index + 1) as i64, kind, name))
            .skip(offset as usize)
            .collect())
    }

    async fn create_file(
        &mut self,
        parent: Inode,
        name: &OsStr,
        mode: u32,
        _flags: u32,
    ) -> Result<(FileHandle, FileAttr)> {
        let name = name.clean()?;

        debug!("create file name {:?} in parent {}", name, parent);

        let entry_path = match self.inode_to_path.get(&parent) {
            None => return Err(libc::ENOENT.into()),
            Some(entry_path) => entry_path.clone(),
        };

        let path = entry_path.get_path();

        let metadata = self.get_metadata_with_inode_and_path(parent, path).await?;

        if self.check_and_fix_inode_and_path(parent, &entry_path, path, &metadata) {
            return Err(libc::ENOENT.into());
        }

        if metadata.is_file() {
            return Err(libc::ENOTDIR.into());
        }

        let create_path = path.to_path_buf().apply(|path| path.push(name));

        match fs::metadata(&create_path).await {
            Ok(metadata) => {
                if let Some(&inode) = self.path_to_inode.get(&create_path) {
                    let entry_path = self.inode_to_path.get(&inode).expect("checked").clone();

                    self.check_and_fix_inode_and_path(inode, &entry_path, &create_path, &metadata);
                } else {
                    let new_inode = self.inode_gen.fetch_add(1, Ordering::Relaxed);

                    let entry_path = if metadata.is_file() {
                        EntryPath::File(create_path.clone())
                    } else {
                        EntryPath::Dir(create_path.clone())
                    };

                    self.inode_to_path.insert(new_inode, entry_path);
                    self.path_to_inode.insert(create_path, new_inode);
                }

                return Err(libc::EEXIST.into());
            }

            Err(err) => {
                if err.kind() != ErrorKind::NotFound {
                    return Err(err.into());
                }
            }
        }

        let sys_file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .mode(mode)
            .open(&create_path)
            .await?;

        // clean old record
        if let Some(inode) = self.path_to_inode.remove(&create_path) {
            self.inode_to_path.remove(&inode);
        }

        let new_inode = self.inode_gen.fetch_add(1, Ordering::Relaxed);

        let entry_path = EntryPath::File(create_path.clone());

        debug!(
            "create file inode {}, entry path {:?}",
            new_inode, entry_path
        );

        self.inode_to_path.insert(new_inode, entry_path);
        self.path_to_inode.insert(create_path.clone(), new_inode);

        let metadata = fs::metadata(create_path).await?;

        let attr = metadata_to_file_attr(new_inode, metadata)?;

        let file_handle = FileHandle::new(
            self.file_handle_id_gen.fetch_add(1, Ordering::Relaxed),
            #[cfg(features = "test")]
            new_inode,
            sys_file,
            FileHandleKind::ReadWrite,
        );

        Ok((file_handle, attr))
    }

    /// when inode and path need fix, will return true
    fn check_and_fix_inode_and_path(
        &mut self,
        inode: Inode,
        entry_path: &EntryPath,
        path: impl AsRef<Path>,
        metadata: &Metadata,
    ) -> bool {
        if metadata.is_file() {
            if let EntryPath::Dir(_) = entry_path {
                let path = path.as_ref();

                self.inode_to_path.remove(&inode);
                self.path_to_inode.remove(path);

                let new_inode = self.inode_gen.fetch_add(1, Ordering::Relaxed);

                self.inode_to_path
                    .insert(new_inode, EntryPath::File(path.to_path_buf()));
                self.path_to_inode.insert(path.to_path_buf(), new_inode);

                return true;
            }
        } else if let EntryPath::File(_) = entry_path {
            let path = path.as_ref();

            self.inode_to_path.remove(&inode);
            self.path_to_inode.remove(path);

            let new_inode = self.inode_gen.fetch_add(1, Ordering::Relaxed);

            self.inode_to_path
                .insert(new_inode, EntryPath::Dir(path.to_path_buf()));
            self.path_to_inode.insert(path.to_path_buf(), new_inode);

            return true;
        }

        false
    }

    async fn get_metadata_with_inode_and_path(
        &mut self,
        inode: Inode,
        path: impl AsRef<Path>,
    ) -> Result<Metadata> {
        let path = path.as_ref();

        match fs::metadata(path).await {
            Err(err) => {
                if let ErrorKind::NotFound = err.kind() {
                    self.inode_to_path.remove(&inode);
                    self.path_to_inode.remove(path);

                    Err(libc::ENOENT.into())
                } else {
                    Err(err.into())
                }
            }

            Ok(metadata) => Ok(metadata),
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
            flags: libc::O_RDWR as u32,
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
            flags: libc::O_RDWR as u32,
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
            flags: libc::O_RDWR as u32,
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
            flags: libc::O_RDWR as u32,
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
            flags: libc::O_RDWR as u32,
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
            flags: libc::O_RDWR as u32,
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
            flags: libc::O_RDWR as u32,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::Open {
            inode: 2,
            flags: libc::O_RDWR as u32,
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
            flags: libc::O_RDWR as u32,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::Open {
            inode: 2,
            flags: libc::O_RDONLY as u32,
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
            flags: libc::O_RDWR as u32,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::Open {
            inode: 2,
            flags: libc::O_WRONLY as u32,
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
            flags: libc::O_RDWR as u32,
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
            flags: libc::O_RDWR as u32,
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
            flags: libc::O_RDWR as u32,
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
            flags: libc::O_RDONLY as u32,
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
            flags: libc::O_RDWR as u32,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let (mut file_handle, _) = rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::Open {
            inode: 2,
            flags: libc::O_RDWR as u32,
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
            flags: libc::O_RDWR as u32,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let (mut file_handle, _) = rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::Open {
            inode: 2,
            flags: libc::O_RDWR as u32,
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
            flags: libc::O_RDWR as u32,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let (file_handle, _) = rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::Open {
            inode: 2,
            flags: libc::O_RDWR as u32,
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
            flags: libc::O_RDWR as u32,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let (file_handle, _) = rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::Open {
            inode: 2,
            flags: libc::O_RDWR as u32,
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
            flags: libc::O_RDWR as u32,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let (mut file_handle, _) = rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::Open {
            inode: 2,
            flags: libc::O_RDWR as u32,
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
            flags: libc::O_RDWR as u32,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let (file_handle, _) = rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::Open {
            inode: 2,
            flags: libc::O_RDWR as u32,
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
            flags: libc::O_RDWR as u32,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let (mut file_handle, _) = rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::Open {
            inode: 2,
            flags: libc::O_RDWR as u32,
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
            flags: libc::O_RDWR as u32,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let (mut file_handle, _) = rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::Open {
            inode: 2,
            flags: libc::O_RDWR as u32,
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
            flags: libc::O_RDWR as u32,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let (mut file_handle, _) = rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::Open {
            inode: 2,
            flags: libc::O_RDWR as u32,
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
            flags: libc::O_RDWR as u32,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        let (mut file_handle, _) = rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::Open {
            inode: 2,
            flags: libc::O_RDWR as u32,
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
            flags: libc::O_RDWR as u32,
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
            flags: libc::O_RDWR as u32,
            response: tx,
        };

        fs_tx.send(req).await.unwrap();

        rx.next().await.unwrap().unwrap();

        let (tx, mut rx) = channel(1);

        let req = Request::CreateFile {
            parent: 1,
            name: OsString::from("exist"),
            mode: 0o644,
            flags: libc::O_RDWR as u32,
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
