use std::cell::RefCell;
use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::io::ErrorKind;
use std::ops::DerefMut;
use std::os::unix::fs::DirBuilderExt;
use std::os::unix::fs::MetadataExt;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::fs::PermissionsExt;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, UNIX_EPOCH};

use async_std::fs;
use async_std::fs::{DirBuilder, OpenOptions};
use async_std::path::{Path, PathBuf};
use async_std::stream;
use async_std::sync::RwLock;
use fuse::{FileAttr, FileType};
use futures_util::future::try_join_all;
use futures_util::stream::{FuturesOrdered, StreamExt};
use log::debug;

use crate::errno::Errno;
use crate::helper::Apply;
use crate::Result;

use super::attr::SetAttr;
use super::entry::EntryPath;
use super::file::File;
use super::inode::{Inode, InodeToPath};

#[derive(Debug)]
struct InnerDir {
    inode: Inode,
    name: OsString,
    real_path: OsString,
    parent: Rc<RefCell<InnerDir>>,
    children: Option<BTreeMap<OsString, Path>>,
}

#[derive(Debug, Clone)]
pub struct Dir(Rc<RefCell<InnerDir>>);

impl Dir {
    pub async fn from_exist<P: AsRef<Path>>(
        parent: &Self,
        real_path: P,
        inode_gen: &AtomicU64,
        inode_map: &mut InodeToPath,
    ) -> Result<Self> {
        if fs::metadata(&real_path).await?.is_file() {
            return Err(Errno::from(libc::ENOTDIR));
        }

        let real_path = real_path.as_ref();

        debug!("create Dir from exist path {:?}", real_path);

        let inode = inode_gen.fetch_add(1, Ordering::Relaxed);

        debug!("new dir inode generated");

        let dir = Dir(Rc::new(RefCell::new(InnerDir {
            inode,
            name: if real_path == Path::new("/") {
                OsString::from("/")
            } else {
                real_path
                    .file_name()
                    .expect("name should be valid")
                    .to_os_string()
            },
            real_path: real_path.as_os_str().to_os_string(),
            parent: parent.0.clone(),
            children: None,
        })));

        debug!("Dir created");

        inode_map.insert(inode, Path::from(&dir));

        debug!("inode map wrote");

        Ok(dir)
    }

    pub async fn get_attr(&self, inode_map: &mut InodeToPath) -> Result<FileAttr> {
        let inner_dir = self.0.borrow();

        let metadata = match fs::metadata(&inner_dir.real_path).await {
            Err(err) => if let ErrorKind::NotFound = err.kind() {
                unimplemented!("TODO parent delete this dir")
            } else {
                return Err(Errno::from(err));
            },

            Ok(metadata) => metadata
        };

        Ok(FileAttr {
            ino: guard.inode,
            size: metadata.len(),
            blocks: metadata.blocks(),
            kind: FileType::Directory,
            atime: metadata.accessed()?,
            mtime: metadata.modified()?,
            ctime: UNIX_EPOCH
                + Duration::new(metadata.ctime() as u64, metadata.ctime_nsec() as u32),
            perm: (metadata.permissions().mode() ^ libc::S_IFDIR) as u16,
            uid: metadata.uid(),
            gid: metadata.gid(),
            rdev: metadata.rdev() as u32,
            flags: 0,
            nlink: 2,
        })
    }

    pub async fn set_attr(&self, set_attr: SetAttr, inode_map: &mut InodeToPath) -> Result<FileAttr> {
        let inner_dir = self.0.borrow();

        let metadata = match fs::metadata(&inner_dir.real_path).await {
            Err(err) => if let ErrorKind::NotFound = err.kind() {
                unimplemented!("TODO parent delete this dir")
            } else {
                return Err(Errno::from(err));
            },

            Ok(metadata) => metadata
        };

        if let Some(mode) = set_attr.mode {
            let mut permissions = metadata.permissions();

            permissions.set_mode(mode);

            fs::set_permissions(&inner_dir.real_path, permissions).await?;
        }

        if set_attr.size.is_some() {
            return Err(Errno::from(libc::EISDIR));
        }

        self.get_attr(inode_map).await
    }

    pub async fn lookup(&mut self, name: &OsStr, inode_map: &mut InodeToPath, inode_gen: &AtomicU64) -> Result<FileAttr> {
        self.init_children_map(inode_map, inode_gen).await?;

        let mut inner_dir = self.0.borrow_mut();

        if let Err(err) = fs::metadata(&inner_dir.real_path).await {
            if let ErrorKind::NotFound = err.kind() {
                unimplemented!("TODO parent delete this dir")
            } else {
                return Err(Errno::from(err));
            }
        }

        let real_path = PathBuf::from(&inner_dir.real_path)
            .apply(|path| path.push(name));

        let children = inner_dir.children.as_mut().expect("initialize");

        match fs::metadata(&real_path).await {
            // if entry deleted by someone
            Err(err) => return if let ErrorKind::NotFound = err.kind() {
                match children.get_mut(name) {
                    None => Err(Errno::from(libc::ENOENT)),
                    Some(entry) => {
                        let inode = entry.get_inode();

                        match entry {
                            Path::File(_) => {
                                children.remove(name);

                                inode_map.remove(&inode);
                            }
                            Path::Dir(dir) => dir.remove_self(inode_map),
                        }

                        Err(Errno::from(libc::ENOENT))
                    }
                }
            } else {
                Err(Errno::from(err))
            },

            Ok(metadata) => match children.get(name) {
                Some(entry) => entry.get_attr().await,
                None => {
                    // if entry create by someone
                    let new_child = if metadata.is_dir() {
                        Path::from(Dir::from_exist(self, &real_path, inode_gen, inode_map).await?)
                    } else {
                        Path::from(File::from_exist(self, &real_path, inode_gen, inode_map).await?)
                    };

                    children.insert(name.to_os_string(), new_child.clone());

                    new_child.get_attr().await
                }
            },
        }
    }

    pub async fn read_dir(&mut self, offset: i64, inode_map: &mut InodeToPath, inode_gen: &AtomicU64) -> Result<Vec<(Inode, i64, FileType, OsString)>> {
        self.init_children_map(inode_map, inode_gen).await?;

        let inner_dir = self.0.borrow_mut();

        if let Err(err) = fs::metadata(&inner_dir.real_path).await {
            if let ErrorKind::NotFound = err.kind() {
                unimplemented!("TODO parent delete this dir")
            } else {
                return Err(Errno::from(err));
            }
        }

        let children_map = inner_dir.children.as_mut().expect("initialize");

        let prefix_children = stream::from_iter(vec![
            (inner_dir.inode, FileType::Directory, OsString::from(".")),
            (inner_dir.parent.borrow().inode, FileType::Directory, OsString::from("..")),
        ]);

        fs::read_dir(&inner_dir.real_path).await?.for_each(|real_child|async move {
            let real_child = real_child?;
            let metadata = real_child.metadata().await?;
        })

        let children = children_map
            .iter()
            .map(|(name, child)| async move { (name, child.get_attr().await) })
            .collect::<FuturesOrdered<_>>()
            .filter_map(|(name, result)| async move {
                if let Ok(attr) = result {
                    Some((attr.ino, attr.kind, name.to_os_string()))
                } else {
                    None
                }
            });

        Ok(prefix_children
            .chain(children)
            .enumerate()
            .map(|(index, (inode, kind, name))| (inode, (index + 1) as i64, kind, name))
            .skip(offset as usize)
            .collect()
            .await)
    }

    pub async fn create_dir(&self, name: &OsStr, mode: u32) -> Result<Path> {
        self.init_children_map().await?;

        let mut guard = self.0.write().await;

        if guard
            .children
            .as_ref()
            .expect("children should be initialized")
            .get(name)
            .is_some()
        {
            return Err(Errno::from(libc::EEXIST));
        }

        let parent_path = PathBuf::from(guard.real_path.clone());
        let inode_map = guard.inode_map.clone();
        let inode_gen = guard.inode_gen.clone();
        let parent_inode = guard.parent;

        let children_map = &mut guard
            .children
            .as_mut()
            .expect("children should be initialized");

        if children_map.get(name).is_some() {
            return Err(Errno::from(libc::EEXIST));
        }

        let new_dir_path = PathBuf::from(parent_path).apply(|path| path.push(name));

        DirBuilder::new().mode(mode).create(&new_dir_path).await?;

        let dir = Dir::from_exist(parent_inode, &new_dir_path, inode_gen, inode_map).await?;
        let dir = Path::from(dir);

        children_map.insert(name.to_os_string(), dir.clone());

        Ok(dir)
    }

    pub async fn create_file(&self, name: &OsStr, mode: u32) -> Result<Arc<File>> {
        debug!("init children map");

        self.init_children_map().await?;

        debug!("children map is initialize");

        let mut guard = self.0.write().await;

        if guard
            .children
            .as_ref()
            .expect("children should be initialized")
            .get(name)
            .is_some()
        {
            return Err(Errno::from(libc::EEXIST));
        }

        debug!("guard acquired");

        let parent_path = PathBuf::from(guard.real_path.clone());
        let inode_map = guard.inode_map.clone();
        let parent_inode = guard.parent;

        let inode_gen = guard.inode_gen.clone();

        let children_map = guard
            .children
            .as_mut()
            .expect("children should be initialized");

        debug!("get children map");

        if children_map.get(name).is_some() {
            return Err(Errno::from(libc::EEXIST));
        }

        let new_file_path = parent_path.apply(|path| path.push(name));

        debug!("new file path {:?}", new_file_path);

        OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(mode)
            .open(&new_file_path)
            .await?;

        debug!("created real file {:?}", new_file_path);

        let file = File::from_exist(
            parent_inode,
            &new_file_path,
            &inode_gen,
            inode_map.write().await.deref_mut(),
        )
            .await?;

        children_map.insert(name.to_os_string(), Path::from(&file));

        Ok(file)
    }

    pub async fn remove_entry(&self, name: &OsStr, is_dir: bool) -> Result<Path> {
        self.init_children_map().await?;

        debug!("remove entry children map initialize");

        let mut guard = self.0.write().await;

        let inode_map = guard.inode_map.clone();
        let mut inode_map = inode_map.write().await;

        let children_map = guard
            .children
            .as_mut()
            .expect("children map should be initialized");

        match children_map.get(name).ok_or(Errno::from(libc::ENOENT))? {
            Path::Dir(dir) => {
                if !is_dir {
                    return Err(Errno::from(libc::EISDIR));
                }

                fs::remove_dir(dir.get_real_path().await).await?;
            }

            Path::File(file) => {
                if is_dir {
                    return Err(Errno::from(libc::ENOTDIR));
                } else {
                    fs::remove_file(file.get_real_path().await).await?;
                }
            }
        }

        let entry = children_map.remove(name).expect("child exists");

        inode_map.remove(&entry.get_inode().await);

        Ok(entry)
    }

    pub async fn add_child_from(
        &self,
        old_parent: &Self,
        old_name: &OsStr,
        new_name: &OsStr,
    ) -> Result<()> {
        self.init_children_map().await?;

        let new_parent_inode = self.get_inode().await;
        let old_parent_inode = old_parent.get_inode().await;

        if old_parent_inode == new_parent_inode {
            let mut guard = self.0.write().await;

            let inode_map = guard.inode_map.clone(); // fix immutable borrow can't stay with mutable borrow
            let mut inode_map = inode_map.write().await;

            let new_real_path =
                PathBuf::from(guard.real_path.clone()).apply(|path| path.push(new_name));

            debug!("new real path {:?}", new_real_path);

            let children_map = guard
                .children
                .as_mut()
                .expect("children map should be initialized");

            if children_map.get(old_name).is_none() {
                return Err(Errno::from(libc::ENOENT));
            }

            // if new_name exists, remove it at first
            if let Some(exist_entry) = children_map.remove(new_name) {
                match &exist_entry {
                    Path::Dir(dir) => {
                        let real_path = dir.get_real_path().await;
                        fs::remove_dir_all(real_path).await?;
                    }

                    Path::File(file) => {
                        let real_path = file.get_real_path().await;
                        fs::remove_file(real_path).await?;
                    }
                }

                inode_map.remove(&exist_entry.get_inode().await);
            }

            let entry = children_map.remove(old_name).expect("checked");

            match &entry {
                Path::Dir(child_dir) => child_dir.rename(&new_real_path).await?,
                Path::File(child_file) => child_file.rename(&new_real_path).await?,
            }

            children_map.insert(new_name.to_os_string(), entry);

            return Ok(());
        }

        old_parent.init_children_map().await?;

        let mut old_parent = old_parent.0.write().await;
        let mut new_parent = self.0.write().await;

        let inode_map = new_parent.inode_map.clone();
        let mut inode_map = inode_map.write().await;

        let new_real_path =
            PathBuf::from(new_parent.real_path.clone()).apply(|path| path.push(new_name));

        let old_children_map = old_parent
            .children
            .as_mut()
            .expect("children map should be initialized");

        let new_children_map = new_parent
            .children
            .as_mut()
            .expect("children map should be initialized");

        let entry = old_children_map
            .remove(old_name)
            .ok_or(Errno::from(libc::ENOENT))?;

        // if new_name exists, remove it at first
        if let Some(exist_entry) = new_children_map.remove(new_name) {
            match &exist_entry {
                Path::Dir(dir) => {
                    let real_path = dir.get_real_path().await;
                    fs::remove_dir_all(real_path).await?;
                }

                Path::File(file) => {
                    let real_path = file.get_real_path().await;
                    fs::remove_file(real_path).await?;
                }
            }

            inode_map.remove(&exist_entry.get_inode().await);
        }

        match &entry {
            Path::Dir(child_dir) => {
                child_dir.set_new_parent(new_parent_inode).await;
                child_dir.rename(&new_real_path).await?;
            }
            Path::File(child_file) => {
                child_file.set_new_parent(new_parent_inode).await;
                child_file.rename(&new_real_path).await?;
            }
        }

        new_children_map.insert(new_name.to_os_string(), entry);

        Ok(())
    }

    pub async fn rename<P: AsRef<Path>>(&self, new_real_path: P) -> Result<()> {
        let mut guard = self.0.write().await;

        fs::rename(&guard.real_path, &new_real_path).await?;

        let new_real_path = new_real_path.as_ref();

        debug!(
            "rename dir from {:?} to {:?}",
            guard.real_path, new_real_path
        );

        guard.real_path = new_real_path.as_os_str().to_os_string();
        guard.name = new_real_path
            .file_name()
            .expect("name should be valid")
            .to_os_string();

        Ok(())
    }

    //#[inline]
    pub async fn set_new_parent(&self, new_parent: Inode) {
        self.0.write().await.parent = new_parent
    }

    //#[inline]
    pub fn get_inode(&self) -> Inode {
        self.0.borrow().inode
    }

    async fn init_children_map(&mut self, inode_map: &mut InodeToPath, inode_gen: &AtomicU64) -> Result<()> {
        let mut inner_dir = self.0.borrow_mut();

        if inner_dir.children.is_some() {
            return Ok(());
        }

        debug!("children map not init");

        let mut dir_entries = fs::read_dir(&inner_dir.real_path).await?;

        // let parent_inode = guard.parent;

        let mut futures = vec![];

        while let Some(dir_entry) = dir_entries.next().await {
            let parent_path = PathBuf::from(&inner_dir.real_path);

            futures.push(Box::pin(async move {
                let dir_entry = match dir_entry {
                    Err(err) => return Err(Errno::from(err)),
                    Ok(dir_entry) => dir_entry,
                };

                let dir_entry_real_path =
                    PathBuf::from(&parent_path).apply(|path| path.push(dir_entry.file_name()));

                let child = if dir_entry.file_type().await?.is_dir() {
                    Path::from(
                        Dir::from_exist(self, &dir_entry_real_path, inode_gen, inode_map)
                            .await?,
                    )
                } else {
                    Path::from(
                        File::from_exist(
                            self,
                            &dir_entry_real_path,
                            &inode_gen,
                            inode_map,
                        )
                            .await?,
                    )
                };

                Ok((dir_entry.file_name(), child))
            }))
        }

        let mut children_map = BTreeMap::new();

        for (name, entry) in try_join_all(futures).await? {
            children_map.insert(name, entry);
        }

        // disable this debug log, it may downgrade performance
        /*for (name, _entry) in children_map.iter() {
            debug!("name is {:?}", name);
        }*/

        inner_dir.children.replace(children_map);

        debug!("children map init success");

        Ok(())
    }

    //#[inline]
    pub async fn get_name(&self) -> OsString {
        self.0.read().await.name.to_os_string()
    }

    #[inline]
    pub async fn get_real_path(&self) -> OsString {
        self.0.read().await.real_path.to_os_string()
    }

    //#[inline]
    pub async fn get_parent_inode(&self) -> Inode {
        self.0.read().await.parent
    }

    fn remove_self(&mut self, inode_map: &mut InodeToPath) {
        let inner_dir = self.0.borrow();

        inode_map.remove(&inner_dir.inode);

        for (_, entry) in inner_dir.children.as_mut().expect("initialize").iter_mut() {
            match entry {
                Path::Dir(dir) => dir.remove_self(inode_map),
                Path::File(file) => inode_map.remove(file.get_inode()),
            }
        }
    }
}
