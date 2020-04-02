use std::env;
use std::ffi::{OsStr, OsString};
use std::io::ErrorKind;
use std::os::unix::fs::DirBuilderExt;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::fs::PermissionsExt;
use std::sync::atomic::{AtomicU64, Ordering};

use async_std::fs;
use async_std::fs::{DirBuilder, Metadata, OpenOptions};
use async_std::path::{Path, PathBuf};
use async_std::sync::Receiver;
use fuse::{FileAttr, FileType};
use futures_util::StreamExt;
use log::{debug, error, info};
use nix::{sched, unistd};

use crate::{Apply, Result};
use crate::path::PathClean;
use crate::server::filesystem::attr::metadata_to_file_attr;
use crate::server::filesystem::file_handle::FileHandleKind;
use crate::server::filesystem::inode::PathToInode;

use super::attr::SetAttr;
use super::entry::EntryPath;
use super::file_handle::FileHandle;
use super::inode::{Inode, InodeToPath};
use super::Request;

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
        env::set_current_dir(root.as_ref().to_path_buf())?;

        unistd::chroot(".")?;

        info!("chroot {:?} success", root.as_ref());

        env::set_current_dir("/")?;

        sched::unshare(nix::sched::CloneFlags::CLONE_NEWNS)?;

        Ok(())
    }

    async fn run(&mut self) {
        while let Some(request) = self.receiver.recv().await {
            match request {
                Request::Lookup {
                    parent,
                    name,
                    response,
                } => {
                    let result = self.lookup(parent, &name).await;
                    response.send(result).await;
                }

                Request::GetAttr {
                    inode,
                    response
                } => {
                    let result = self.get_attr(inode).await;
                    response.send(result).await;
                }

                Request::SetAttr {
                    inode,
                    new_attr,
                    response
                } => {
                    let result = self.set_attr(inode, new_attr).await;
                    response.send(result).await;
                }

                Request::GetName { inode, response } => {
                    let result = self.get_name(inode).await;
                    response.send(result).await;
                }

                Request::CreateDir { parent, name, mode, response } => {
                    let result = self.create_dir(parent, &name, mode).await;
                    response.send(result).await;
                }

                Request::RemoveEntry { parent, name, is_dir, response } => {
                    let result = self.remove_entry(parent, &name, is_dir).await;
                    response.send(result).await;
                }

                Request::Rename { old_parent, old_name, new_parent, new_name, response } => {
                    let result = self.rename(old_parent, &old_name, new_parent, &new_name).await;
                    response.send(result).await;
                }

                Request::Open { inode, flags, response } => {
                    let result = self.open(inode, flags).await;
                    response.send(result).await;
                }

                Request::ReadDir { inode, offset, response } => {
                    let result = self.read_dir(inode, offset).await;
                    response.send(result).await;
                }

                Request::CreateFile { parent, name, mode, flags, response } => {
                    let result = self.create_file(parent, &name, mode, flags).await;
                    response.send(result).await;
                }
            }
        }
    }

    pub async fn lookup(&mut self, parent: Inode, name: &OsStr) -> Result<FileAttr> {
        let name = name.clean()?;

        debug!("lookup name {:?} in parent {}", name, parent);

        let entry_path = match self.inode_to_path.get(&parent) {
            None => return Err(libc::ENOENT.into()),
            Some(entry_path) => entry_path
        };

        let path = entry_path.get_path();

        /*let metadata = match fs::metadata(path).await {
            Err(err) => return if let ErrorKind::NotFound = err.kind() {
                self.inode_to_path.remove(&parent);
                self.path_to_inode.remove(path);

                Err(libc::ENOENT.into())
            } else {
                Err(err.into())
            },

            Ok(metadata) => metadata
        };*/
        let metadata = self.get_metadata_with_inode_and_path(parent, path).await?;

        if self.check_and_fix_inode_and_path(parent, entry_path, path, &metadata) {
            return Err(libc::ENOENT.into());
        }

        if metadata.is_file() {
            return Err(libc::ENOTDIR.into());
        }

        let lookup_path = path.to_path_buf()
            .apply(|path| path.push(name));

        let metadata = match fs::metadata(&lookup_path).await {
            Err(err) => return if let ErrorKind::NotFound = err.kind() {
                if let Some(inode) = self.path_to_inode.remove(&lookup_path) {
                    self.inode_to_path.remove(&inode);
                }

                Err(libc::ENOENT.into())
            } else {
                Err(err.into())
            },

            Ok(metadata) => metadata
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
                let entry_path = self.inode_to_path.get(&inode).expect("checked");

                if self.check_and_fix_inode_and_path(inode, entry_path, lookup_path, &metadata) {
                    return Err(libc::ENOENT.into());
                }

                inode
            }
        };

        metadata_to_file_attr(inode, metadata)
    }

    pub async fn get_attr(&mut self, inode: Inode) -> Result<FileAttr> {
        let entry_path = match self.inode_to_path.get(&inode) {
            None => return Err(libc::ENOENT.into()),
            Some(entry_path) => entry_path
        };

        let path = entry_path.get_path();

        /*let metadata = match fs::metadata(path).await {
            Err(err) => return if let ErrorKind::NotFound = err.kind() {
                self.inode_to_path.remove(&inode);
                self.path_to_inode.remove(path);

                Err(libc::ENOENT.into())
            } else {
                Err(err.into())
            },

            Ok(metadata) => metadata
        };*/
        let metadata = self.get_metadata_with_inode_and_path(inode, path).await?;

        if self.check_and_fix_inode_and_path(inode, entry_path, path, &metadata) {
            return Err(libc::ENOENT.into());
        }

        metadata_to_file_attr(inode, metadata)
    }

    pub async fn get_name(&mut self, inode: Inode) -> Result<OsString> {
        let entry_path = match self.inode_to_path.get(&inode) {
            None => return Err(libc::ENOENT.into()),
            Some(entry_path) => entry_path
        };

        let path = entry_path.get_path();

        /*let metadata = match fs::metadata(path).await {
            Err(err) => return if let ErrorKind::NotFound = err.kind() {
                self.inode_to_path.remove(&inode);
                self.path_to_inode.remove(path);

                Err(libc::ENOENT.into())
            } else {
                Err(err.into())
            },

            Ok(metadata) => metadata
        };*/
        let metadata = self.get_metadata_with_inode_and_path(inode, path).await?;

        if self.check_and_fix_inode_and_path(inode, entry_path, path, &metadata) {
            return Err(libc::ENOENT.into());
        }

        if let Some(filename) = path.file_name() {
            Ok(filename.to_os_string())
        } else if path == Path::new("/") {
            Ok(OsString::from("/"))
        } else {
            Err(libc::EIO.into())
        }
    }

    pub async fn set_attr(&mut self, mut inode: Inode, set_attr: SetAttr) -> Result<FileAttr> {
        let entry_path = match self.inode_to_path.get(&inode) {
            None => return Err(libc::ENOENT.into()),
            Some(entry_path) => entry_path
        };

        let path = entry_path.get_path();

        /*let metadata = match fs::metadata(path).await {
            Err(err) => return if let ErrorKind::NotFound = err.kind() {
                self.inode_to_path.remove(&inode);
                self.path_to_inode.remove(path);

                Err(libc::ENOENT.into())
            } else {
                Err(err.into())
            },

            Ok(metadata) => metadata
        };*/
        let metadata = self.get_metadata_with_inode_and_path(inode, path).await?;

        if self.check_and_fix_inode_and_path(inode, entry_path, path, &metadata) {
            return Err(libc::ENOENT.into());
        }

        if let Some(mode) = set_attr.mode {
            let mut permissions = metadata.permissions();

            permissions.set_mode(mode);

            fs::set_permissions(path, permissions).await?;
        }

        if let Some(size) = set_attr.size {
            if entry_path.is_file() {
                let truncate = size == 0;

                let file = fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .truncate(truncate)
                    .open(path)
                    .await?;

                if size > 0 {
                    if let Err(err) = file.set_len(size).await {
                        error!("set inode {} size {} failed", inode, size);

                        return Err(err.into());
                    }

                    debug!("set inode {} size success", inode);
                }
            } else {
                return Err(libc::EISDIR.into());
            }
        }

        self.get_attr(inode).await
    }

    pub async fn create_dir(&mut self, parent: Inode, name: &OsStr, mode: u32) -> Result<FileAttr> {
        let name = name.clean()?;

        debug!("create dir name {:?} in parent {}", name, parent);

        let entry_path = match self.inode_to_path.get(&parent) {
            None => return Err(libc::ENOENT.into()),
            Some(entry_path) => entry_path
        };

        let path = entry_path.get_path();

        /*let metadata = match fs::metadata(path).await {
            Err(err) => return if let ErrorKind::NotFound = err.kind() {
                self.inode_to_path.remove(&parent);
                self.path_to_inode.remove(path);

                Err(libc::ENOENT.into())
            } else {
                Err(err.into())
            },

            Ok(metadata) => metadata
        };*/
        let metadata = self.get_metadata_with_inode_and_path(parent, path).await?;

        if self.check_and_fix_inode_and_path(parent, entry_path, path, &metadata) {
            return Err(libc::ENOENT.into());
        }

        if metadata.is_file() {
            return Err(libc::ENOTDIR.into());
        }

        let create_path = path.to_path_buf()
            .apply(|path| path.push(name));

        match fs::metadata(&create_path).await {
            Ok(metadata) => {
                if let Some(&inode) = self.path_to_inode.get(&create_path) {
                    let entry_path = self.inode_to_path.get(&inode).expect("checked");

                    self.check_and_fix_inode_and_path(inode, entry_path, &create_path, &metadata);
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

            Err(err) => if err.kind() != ErrorKind::NotFound {
                return Err(err.into());
            }
        }

        DirBuilder::new().mode(mode).create(&create_path).await?;

        let new_inode = self.inode_gen.fetch_add(1, Ordering::Relaxed);

        let entry_path = EntryPath::Dir(create_path.clone());

        self.inode_to_path.insert(new_inode, entry_path);
        self.path_to_inode.insert(create_path.clone(), new_inode);

        let metadata = fs::metadata(create_path).await?;

        metadata_to_file_attr(new_inode, metadata)
    }

    pub async fn remove_entry(&mut self, parent: Inode, name: &OsStr, is_dir: bool) -> Result<()> {
        let name = name.clean()?;

        debug!("remove name {:?} from parent {}, is dir {}", name, parent, is_dir);

        let entry_path = match self.inode_to_path.get(&parent) {
            None => return Err(libc::ENOENT.into()),
            Some(entry_path) => entry_path
        };

        let path = entry_path.get_path();

        /*let metadata = match fs::metadata(path).await {
            Err(err) => return if let ErrorKind::NotFound = err.kind() {
                self.inode_to_path.remove(&parent);
                self.path_to_inode.remove(path);

                Err(libc::ENOENT.into())
            } else {
                Err(err.into())
            },

            Ok(metadata) => metadata
        };*/
        let metadata = self.get_metadata_with_inode_and_path(parent, path).await?;

        if self.check_and_fix_inode_and_path(parent, entry_path, path, &metadata) {
            return Err(libc::ENOENT.into());
        }

        if metadata.is_file() {
            return Err(libc::ENOTDIR.into());
        }

        let remove_path = path.to_path_buf()
            .apply(|path| path.push(name));

        let metadata = match fs::metadata(&remove_path).await {
            Err(err) => return if let ErrorKind::NotFound = err.kind() {
                if let Some(inode) = self.path_to_inode.remove(&remove_path) {
                    self.inode_to_path.remove(&inode);
                }

                Err(libc::ENOENT.into())
            } else {
                Err(err.into())
            },

            Ok(metadata) => metadata
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

    pub async fn rename(
        &mut self,
        old_parent: Inode,
        old_name: &OsStr,
        new_parent: Inode,
        new_name: &OsStr,
    ) -> Result<()> {
        let old_name = old_name.clean()?;
        let new_name = new_name.clean()?;

        // op means old parent
        // oc means old child
        // np means new parent
        // nc means new child

        debug!("rename {:?} from {} to {} as {:?}", old_name, old_parent, new_parent, new_name);

        let op_entry_path = match self.inode_to_path.get(&old_parent) {
            None => return Err(libc::ENOENT.into()),
            Some(entry_path) => entry_path
        };

        let op_path = op_entry_path.get_path();

        /*let op_metadata = match fs::metadata(op_path).await {
            Err(err) => return if let ErrorKind::NotFound = err.kind() {
                self.inode_to_path.remove(&old_parent);
                self.path_to_inode.remove(op_path);

                Err(libc::ENOENT.into())
            } else {
                Err(err.into())
            },

            Ok(metadata) => metadata
        };*/
        let op_metadata = self.get_metadata_with_inode_and_path(old_parent, op_path).await?;

        if self.check_and_fix_inode_and_path(old_parent, op_entry_path, op_path, &op_metadata) {
            return Err(libc::ENOENT.into());
        }

        if op_metadata.is_file() {
            return Err(libc::ENOTDIR.into());
        }

        let oc_path = op_path.to_path_buf()
            .apply(|path| path.push(old_name));

        let oc_metadata = match fs::metadata(&oc_path).await {
            Err(err) => return if err.kind() == ErrorKind::NotFound {
                if let Some(inode) = self.path_to_inode.remove(&oc_path) {
                    self.inode_to_path.remove(&inode);
                }

                Err(libc::ENOENT.into())
            } else {
                Err(err.into())
            },

            Ok(metadata) => metadata
        };

        let np_entry_path = match self.inode_to_path.get(&new_parent) {
            None => return Err(libc::ENOENT.into()),
            Some(entry_path) => entry_path
        };

        let np_path = np_entry_path.get_path();

        /*let np_metadata = match fs::metadata(np_path).await {
            Err(err) => return if let ErrorKind::NotFound = err.kind() {
                self.inode_to_path.remove(&new_parent);
                self.path_to_inode.remove(np_path);

                Err(libc::ENOENT.into())
            } else {
                Err(err.into())
            },

            Ok(metadata) => metadata
        };*/
        let np_metadata = self.get_metadata_with_inode_and_path(new_parent, np_path).await?;

        if self.check_and_fix_inode_and_path(new_parent, np_entry_path, np_path, &np_metadata) {
            return Err(libc::ENOENT.into());
        }

        if np_metadata.is_file() {
            return Err(libc::ENOTDIR.into());
        }

        let nc_path = np_path.to_path_buf()
            .apply(|path| path.push(new_name));

        if let Err(err) = fs::metadata(&nc_path).await {
            if err.kind() != ErrorKind::NotFound {
                return Err(err.into());
            }
        } else {
            return Err(libc::EEXIST.into());
        }

        fs::rename(&oc_path, &nc_path).await?;

        // no matter exist or not, remove old child inode and path record
        if let Some(inode) = self.path_to_inode.remove(&oc_path) {
            self.inode_to_path.remove(&inode);
        }

        // no matter exist or not, remove new child inode and path record
        if let Some(inode) = self.path_to_inode.remove(&nc_path) {
            self.inode_to_path.remove(&inode);
        }

        let nc_inode = self.inode_gen.fetch_add(1, Ordering::Relaxed);

        let nc_entry_path = if oc_metadata.is_file() {
            EntryPath::File(nc_path.clone())
        } else {
            EntryPath::Dir(nc_path.clone())
        };

        self.inode_to_path.insert(nc_inode, nc_entry_path);
        self.path_to_inode.insert(nc_path, nc_inode);

        Ok(())
    }

    pub async fn open(&mut self, inode: Inode, flags: u32) -> Result<FileHandle> {
        let entry_path = match self.inode_to_path.get(&inode) {
            None => return Err(libc::ENOENT.into()),
            Some(entry_path) => entry_path
        };

        let path = entry_path.get_path();

        /*let metadata = match fs::metadata(path).await {
            Err(err) => return if let ErrorKind::NotFound = err.kind() {
                self.inode_to_path.remove(&inode);
                self.path_to_inode.remove(path);

                Err(libc::ENOENT.into())
            } else {
                Err(err.into())
            },

            Ok(metadata) => metadata
        };*/
        let metadata = self.get_metadata_with_inode_and_path(inode, path).await?;

        if self.check_and_fix_inode_and_path(inode, entry_path, path, &metadata) {
            return Err(libc::ENOENT.into());
        }

        if metadata.is_dir() {
            return Err(libc::EISDIR.into());
        }

        debug!("open {:?} flags {}", path, flags);

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

        if flags & libc::O_TRUNC as u32 > 0 {
            debug!("open {:?} flags have truncate", path);

            options.truncate(true);
        }

        let sys_file = options.open(path).await?;

        let fh_id = self.file_handle_id_gen.fetch_add(1, Ordering::Relaxed);

        Ok(FileHandle::new(fh_id, inode, sys_file, fh_kind))
    }

    pub async fn read_dir(
        &mut self,
        inode: Inode,
        offset: i64,
    ) -> Result<Vec<(Inode, i64, FileType, OsString)>> {
        debug!("readdir in parent {}", inode);

        let entry_path = match self.inode_to_path.get(&inode) {
            None => return Err(libc::ENOENT.into()),
            Some(entry_path) => entry_path
        };

        let path = entry_path.get_path();

        /*let metadata = match fs::metadata(path).await {
            Err(err) => return if let ErrorKind::NotFound = err.kind() {
                self.inode_to_path.remove(&inode);
                self.path_to_inode.remove(path);

                Err(libc::ENOENT.into())
            } else {
                Err(err.into())
            },

            Ok(metadata) => metadata
        };*/
        let metadata = self.get_metadata_with_inode_and_path(inode, path).await?;

        if self.check_and_fix_inode_and_path(inode, entry_path, path, &metadata) {
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

            self.inode_to_path.insert(parent_inode, EntryPath::Dir(parent_path.to_path_buf()));
            self.path_to_inode.insert(parent_path.to_path_buf(), parent_inode);

            parent_inode
        };

        let mut children = vec![
            (inode, FileType::Directory, OsString::from(".")),
            (parent_inode, FileType::Directory, OsString::from("..")),
        ];

        while let Some(child) = fs::read_dir(path).await?.next().await {
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

                let child_path = path.to_path_buf()
                    .apply(|path| path.push(&child_name));

                let child_inode = if let Some(&inode) = self.path_to_inode.get(&child_path) {
                    let entry_path = self.inode_to_path.get(&inode).expect("checked");

                    self.check_and_fix_inode_and_path(inode, entry_path, &child_path, &metadata);

                    *self.path_to_inode.get(&child_path).expect("checked")
                } else {
                    let new_child_inode = self.inode_gen.fetch_add(1, Ordering::Relaxed);

                    let entry_path = if file_type == FileType::RegularFile {
                        EntryPath::File(child_path.clone())
                    } else {
                        EntryPath::Dir(child_path.clone())
                    };

                    self.inode_to_path.insert(new_child_inode, entry_path);
                    self.path_to_inode.insert(child_path.clone(), new_child_inode);

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

    pub async fn create_file(
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
            Some(entry_path) => entry_path
        };

        let path = entry_path.get_path();

        /*let metadata = match fs::metadata(path).await {
            Err(err) => return if let ErrorKind::NotFound = err.kind() {
                self.inode_to_path.remove(&parent);
                self.path_to_inode.remove(path);

                Err(libc::ENOENT.into())
            } else {
                Err(err.into())
            },

            Ok(metadata) => metadata
        };*/
        let metadata = self.get_metadata_with_inode_and_path(parent, path).await?;

        if self.check_and_fix_inode_and_path(parent, entry_path, path, &metadata) {
            return Err(libc::ENOENT.into());
        }

        if metadata.is_file() {
            return Err(libc::ENOTDIR.into());
        }

        let create_path = path.to_path_buf()
            .apply(|path| path.push(name));

        match fs::metadata(&create_path).await {
            Ok(metadata) => {
                if let Some(&inode) = self.path_to_inode.get(&create_path) {
                    let entry_path = self.inode_to_path.get(&inode).expect("checked");

                    self.check_and_fix_inode_and_path(inode, entry_path, &create_path, &metadata);
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

            Err(err) => if err.kind() != ErrorKind::NotFound {
                return Err(err.into());
            }
        }

        let sys_file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .mode(mode)
            .open(&create_path)
            .await?;

        let new_inode = self.inode_gen.fetch_add(1, Ordering::Relaxed);

        let entry_path = EntryPath::File(create_path.clone());

        self.inode_to_path.insert(new_inode, entry_path);
        self.path_to_inode.insert(create_path.clone(), new_inode);

        let metadata = fs::metadata(create_path).await?;

        let attr = metadata_to_file_attr(new_inode, metadata)?;

        let fh_id = self.file_handle_id_gen.fetch_add(1, Ordering::Relaxed);

        let file_handle = FileHandle::new(fh_id, new_inode, sys_file, FileHandleKind::ReadWrite);

        Ok((file_handle, attr))
    }

    /// when inode and path need fix, will return true
    fn check_and_fix_inode_and_path(&mut self, inode: Inode, entry_path: &EntryPath, path: impl AsRef<Path>, metadata: &Metadata) -> bool {
        if metadata.is_file() {
            if let EntryPath::Dir(_) = entry_path {
                let path = path.as_ref();

                self.inode_to_path.remove(&inode);
                self.path_to_inode.remove(path);

                let new_inode = self.inode_gen.fetch_add(1, Ordering::Relaxed);

                self.inode_to_path.insert(new_inode, EntryPath::File(path.to_path_buf()));
                self.path_to_inode.insert(path.to_path_buf(), new_inode);

                return true;
            }
        } else if let EntryPath::File(_) = entry_path {
            let path = path.as_ref();

            self.inode_to_path.remove(&inode);
            self.path_to_inode.remove(path);

            let new_inode = self.inode_gen.fetch_add(1, Ordering::Relaxed);

            self.inode_to_path.insert(new_inode, EntryPath::Dir(path.to_path_buf()));
            self.path_to_inode.insert(path.to_path_buf(), new_inode);

            return true;
        }

        false
    }

    async fn get_metadata_with_inode_and_path(&mut self, inode: Inode, path: impl AsRef<Path>) -> Result<Metadata> {
        let path = path.as_ref();

        match fs::metadata(path).await {
            Err(err) => if let ErrorKind::NotFound = err.kind() {
                self.inode_to_path.remove(&inode);
                self.path_to_inode.remove(path);

                Err(libc::ENOENT.into())
            } else {
                Err(err.into())
            },

            Ok(metadata) => Ok(metadata)
        }
    }
}

/*
#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::os::unix::fs::PermissionsExt;
    use std::time::Duration;

    use async_std::fs;
    use async_std::future::timeout;
    use async_std::sync::Mutex;
    use async_std::task;
    use tempfile;

    use crate::log_init;
    use crate::server::filesystem::file_handle::FileHandleKind;
    use crate::server::filesystem::LockKind;

    use super::*;

    #[async_std::test]
    async fn init_filesystem() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

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

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

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

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

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

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

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

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

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

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

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

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

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

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

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

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

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

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

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

        filesystem.set_attr(2, set_attr).await.unwrap();

        let attr = filesystem.get_attr(2).await.unwrap();

        assert_eq!(attr.ino, 2);
        assert_eq!(attr.kind, FileType::Directory);
        assert_eq!(attr.perm, 0o700);
    }

    #[async_std::test]
    async fn remove_dir() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

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

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

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

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

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

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

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

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

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

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

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

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

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
    async fn read_dir_deeply() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

        filesystem
            .create_dir(1, OsStr::new("test-1"), 0o755)
            .await
            .unwrap(); // inode 2
        filesystem
            .create_dir(2, OsStr::new("test-2"), 0o755)
            .await
            .unwrap(); // inode 3

        let child_info = filesystem.read_dir(2, 0).await.unwrap();

        assert_eq!(child_info.len(), 3); // include . and ..

        let (inode, _, kind, name) = &child_info[0];

        assert_eq!(*inode, 2);
        assert_eq!(*kind, FileType::Directory);
        assert_eq!(*name, OsString::from("."));

        let (inode, _, kind, name) = &child_info[1];

        assert_eq!(*inode, 1);
        assert_eq!(*kind, FileType::Directory);
        assert_eq!(*name, OsString::from(".."));

        let (inode, _, kind, name) = &child_info[2];

        assert_eq!(*inode, 3);
        assert_eq!(*kind, FileType::Directory);
        assert_eq!(*name, OsString::from("test-2"));
    }

    #[async_std::test]
    async fn open_file_rw() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

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

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

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

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

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

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

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

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

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

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

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

        let attr = filesystem.set_attr(2, set_attr).await.unwrap();

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

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

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

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

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

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

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

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

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

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

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

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

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

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

        let (mut file_handle, _) = filesystem
            .create_file(1, OsStr::new("test"), 0o644, libc::O_RDWR as u32)
            .await
            .unwrap();

        let file_handle2 = filesystem.open(2, libc::O_RDWR as u32).await.unwrap();

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

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

        let (mut file_handle, _) = filesystem
            .create_file(1, OsStr::new("test"), 0o644, libc::O_RDWR as u32)
            .await
            .unwrap();

        let file_handle2 = filesystem.open(2, libc::O_RDWR as u32).await.unwrap();

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

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

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

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

        let (mut file_handle, _) = filesystem
            .create_file(1, OsStr::new("test"), 0o644, libc::O_RDWR as u32)
            .await
            .unwrap();

        let mut file_handle2 = filesystem.open(2, libc::O_RDWR as u32).await.unwrap();

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

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

        let (mut file_handle, _) = filesystem
            .create_file(1, OsStr::new("test"), 0o644, libc::O_RDWR as u32)
            .await
            .unwrap();

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

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

        let attr = filesystem
            .lookup(1, tmp_file.path().file_name().unwrap())
            .await
            .unwrap();

        assert_eq!(attr.ino, 2);
        assert_eq!(attr.kind, FileType::RegularFile);
        assert_eq!(attr.perm as u32, perm);
    }

    // issue #8
    #[async_std::test]
    async fn rename_to_exist_dir() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

        filesystem
            .create_dir(1, OsStr::new("test"), 0o755)
            .await
            .unwrap();

        filesystem
            .create_dir(1, OsStr::new("exist"), 0o755)
            .await
            .unwrap();

        filesystem
            .rename(1, OsStr::new("test"), 1, OsStr::new("exist"))
            .await
            .unwrap();

        let attr = filesystem.lookup(1, OsStr::new("exist")).await.unwrap();

        assert_eq!(attr.ino, 2);
        assert_eq!(attr.kind, FileType::Directory);
        assert_eq!(attr.perm, 0o755);
    }

    // issue #8
    #[async_std::test]
    async fn rename_to_exist_file() {
        log_init(true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

        filesystem
            .create_file(1, OsStr::new("test"), 0o644, libc::O_RDWR as u32)
            .await
            .unwrap();

        filesystem
            .create_file(1, OsStr::new("exist"), 0o644, libc::O_RDWR as u32)
            .await
            .unwrap();

        filesystem
            .rename(1, OsStr::new("test"), 1, OsStr::new("exist"))
            .await
            .unwrap();

        let attr = filesystem.lookup(1, OsStr::new("exist")).await.unwrap();

        assert_eq!(attr.ino, 2);
        assert_eq!(attr.kind, FileType::RegularFile);
        assert_eq!(attr.perm, 0o644);
    }
}
*/
