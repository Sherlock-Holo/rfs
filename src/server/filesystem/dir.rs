use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::ops::DerefMut;
use std::os::unix::fs::DirBuilderExt;
use std::os::unix::fs::MetadataExt;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, UNIX_EPOCH};

use async_std::fs;
use async_std::fs::{DirBuilder, OpenOptions};
use async_std::path::{Path, PathBuf};
use async_std::stream;
use async_std::sync::RwLock;
use fuse::{FileAttr, FileType};
use futures::stream::StreamExt;
use log::debug;

use crate::errno::Errno;
use crate::helper::Apply;
use crate::Result;

use super::attr::SetAttr;
use super::entry::Entry;
use super::file::File;
use super::inode::{Inode, InodeMap};

#[derive(Debug)]
struct InnerDir {
    inode: Inode,
    name: OsString,
    real_path: OsString,
    parent: Inode,
    children: Option<BTreeMap<OsString, Entry>>,
    inode_gen: Arc<AtomicU64>,
    inode_map: Arc<RwLock<InodeMap>>,
}

#[derive(Debug)]
pub struct Dir(RwLock<InnerDir>);

impl Dir {
    pub async fn from_exist<P: AsRef<Path>>(
        parent: Inode,
        real_path: P,
        inode_gen: Arc<AtomicU64>,
        inode_map: Arc<RwLock<InodeMap>>,
    ) -> Result<Arc<Self>> {
        if fs::metadata(&real_path).await?.is_file() {
            return Err(Errno::from(libc::ENOTDIR));
        }

        let real_path = real_path.as_ref();

        debug!("create Dir from exist path {:?}", real_path);

        let inode = inode_gen.fetch_add(1, Ordering::Relaxed);

        let dir = Arc::new(Dir(RwLock::new(InnerDir {
            inode,
            name: if real_path == Path::new("/") {
                real_path.as_os_str().to_os_string()
            } else {
                real_path
                    .file_name()
                    .expect("name should be valid")
                    .to_os_string()
            },
            real_path: real_path.as_os_str().to_os_string(),
            parent,
            children: None,
            inode_gen,
            inode_map: Arc::clone(&inode_map),
        })));

        inode_map.write().await.insert(inode, Entry::from(&dir));

        Ok(dir)
    }

    pub async fn get_attr(self: &Arc<Self>) -> Result<FileAttr> {
        let guard = self.0.read().await;

        let metadata = fs::metadata(&guard.real_path).await?;

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

    pub async fn set_attr(self: &Arc<Self>, set_attr: SetAttr) -> Result<FileAttr> {
        {
            let write_guard = self.0.write().await;

            if let Some(mode) = set_attr.mode {
                let metadata = fs::metadata(&write_guard.real_path).await?;

                let mut permissions = metadata.permissions();

                permissions.set_mode(mode);

                fs::set_permissions(&write_guard.real_path, permissions).await?;
            }

            if set_attr.size.is_some() {
                return Err(Errno::from(libc::EISDIR));
            }
        }

        self.get_attr().await
    }

    pub async fn lookup(&self, name: &OsStr) -> Result<FileAttr> {
        self.init_children_map().await?;

        self.0
            .read()
            .await
            .children
            .as_ref()
            .expect("children map should be initialized")
            .get(name)
            .ok_or(Errno::from(libc::ENOENT))?
            .get_attr()
            .await
    }

    pub async fn read_dir(&self, offset: i64) -> Result<Vec<(Inode, i64, FileType, OsString)>> {
        self.init_children_map().await?;

        let guard = self.0.read().await;

        let children_map = guard
            .children
            .as_ref()
            .expect("children map should be initialized");

        let prefix_children = stream::from_iter(vec![
            (guard.inode, FileType::Directory, OsString::from(".")),
            (guard.parent, FileType::Directory, OsString::from("..")),
        ]);

        let children =
            stream::from_iter(children_map.iter()).filter_map(|(name, child)| async move {
                match child.get_attr().await {
                    Err(_err) => None, // ignore error child
                    Ok(attr) => Some((attr.ino, attr.kind, name.to_os_string())),
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

    pub async fn create_dir(&self, name: &OsStr, mode: u32) -> Result<Entry> {
        self.init_children_map().await?;

        if self
            .0
            .read()
            .await
            .children
            .as_ref()
            .expect("children should be initialized")
            .get(name)
            .is_some()
        {
            return Err(Errno::from(libc::EEXIST));
        }

        let mut guard = self.0.write().await;

        let parent_path = PathBuf::from(guard.real_path.clone());
        let inode_map = Arc::clone(&guard.inode_map);
        let inode_gen = Arc::clone(&guard.inode_gen);
        let parent_inode = guard.parent;

        let children_map = &mut guard
            .children
            .as_mut()
            .expect("children should be initialized");

        if children_map.get(name).is_some() {
            return Err(Errno::from(libc::EEXIST));
        }

        let new_dir_path = PathBuf::from(parent_path).apply(|path| path.push(name));

        // fs::create_dir(&new_dir_path).await?;
        DirBuilder::new().mode(mode).create(&new_dir_path).await?;

        let dir = Dir::from_exist(parent_inode, &new_dir_path, inode_gen, inode_map).await?;
        let dir = Entry::from(dir);

        children_map.insert(name.to_os_string(), dir.clone());

        Ok(dir)
    }

    pub async fn create_file(&self, name: &OsStr, mode: u32) -> Result<Arc<File>> {
        debug!("init children map");

        self.init_children_map().await?;

        if self
            .0
            .read()
            .await
            .children
            .as_ref()
            .expect("children should be initialized")
            .get(name)
            .is_some()
        {
            return Err(Errno::from(libc::EEXIST));
        }

        debug!("children map is initialize");

        let mut guard = self.0.write().await;

        debug!("guard acquired");

        let parent_path = PathBuf::from(guard.real_path.clone());
        let inode_map = Arc::clone(&guard.inode_map);
        let parent_inode = guard.parent;

        let inode_gen = Arc::clone(&guard.inode_gen);

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

        children_map.insert(name.to_os_string(), Entry::from(&file));

        Ok(file)
    }

    pub async fn remove_entry(&self, name: &OsStr, is_dir: bool) -> Result<Entry> {
        self.init_children_map().await?;

        debug!("remove entry children map initialize");

        let mut guard = self.0.write().await;

        let inode_map = Arc::clone(&guard.inode_map);
        let mut inode_map = inode_map.write().await;

        let children_map = guard
            .children
            .as_mut()
            .expect("children map should be initialized");

        match children_map.get(name).ok_or(Errno::from(libc::ENOENT))? {
            Entry::Dir(dir) => {
                if !is_dir {
                    return Err(Errno::from(libc::EISDIR));
                }

                /*// always contains . and ..
                if !dir.read_dir(0).await?.len() > 2 {
                    return Err(Errno::from(libc::ENOTEMPTY));
                }*/

                fs::remove_dir(dir.get_real_path().await).await?;
            }

            Entry::File(file) => {
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

            let new_real_path =
                PathBuf::from(guard.real_path.clone()).apply(|path| path.push(new_name));

            debug!("new real path {:?}", new_real_path);

            let children_map = guard
                .children
                .as_mut()
                .expect("children map should be initialized");

            // let entry = children_map.remove(old_name).ok_or(Errno(libc::ENOENT))?;
            if children_map.get(old_name).is_none() {
                return Err(Errno::from(libc::ENOENT));
            }

            if children_map.get(new_name).is_some() {
                return Err(Errno::from(libc::EEXIST));
            }

            let entry = children_map.remove(old_name).expect("checked");

            match &entry {
                Entry::Dir(child_dir) => child_dir.rename(&new_real_path).await?,
                Entry::File(child_file) => child_file.rename(&new_real_path).await?,
            }

            children_map.insert(new_name.to_os_string(), entry);

            return Ok(());
        }

        old_parent.init_children_map().await?;

        let mut old_parent = old_parent.0.write().await;
        let mut new_parent = self.0.write().await;

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

        old_children_map
            .get(old_name)
            .ok_or(Errno::from(libc::ENOENT))?;

        if new_children_map.get(new_name).is_some() {
            return Err(Errno::from(libc::EEXIST));
        }

        let entry = old_children_map.remove(old_name).unwrap();

        match &entry {
            Entry::Dir(child_dir) => {
                child_dir.set_new_parent(new_parent_inode).await;
                child_dir.rename(&new_real_path).await?;
            }
            Entry::File(child_file) => {
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

    #[inline]
    pub async fn set_new_parent(&self, new_parent: Inode) {
        self.0.write().await.parent = new_parent
    }

    #[inline]
    pub async fn get_inode(&self) -> Inode {
        self.0.read().await.inode
    }

    async fn init_children_map(&self) -> Result<()> {
        if self.0.read().await.children.is_some() {
            return Ok(());
        }

        let mut guard = self.0.write().await;

        // avoid useless init
        if guard.children.is_some() {
            return Ok(());
        }

        debug!("children map not init");

        let mut children_map = BTreeMap::new();

        let mut dir_entries = fs::read_dir(&guard.real_path).await?;

        let parent_path = PathBuf::from(guard.real_path.clone());
        let inode_map = Arc::clone(&guard.inode_map);

        debug!("locking inode map");

        let mut inode_map = inode_map.write().await;

        debug!("lock inode map");

        while let Some(dir_entry) = dir_entries.next().await {
            let dir_entry = dir_entry?;

            let dir_entry_real_path =
                PathBuf::from(&parent_path).apply(|path| path.push(dir_entry.file_name()));

            let child = if dir_entry.file_type().await?.is_dir() {
                Entry::from(
                    Dir::from_exist(
                        guard.parent,
                        &dir_entry_real_path,
                        Arc::clone(&guard.inode_gen),
                        Arc::clone(&guard.inode_map),
                    )
                        .await?,
                )
            } else {
                Entry::from(
                    File::from_exist(
                        guard.parent,
                        &dir_entry_real_path,
                        &guard.inode_gen,
                        &mut inode_map,
                    )
                        .await?,
                )
            };

            children_map.insert(dir_entry_real_path.into_os_string(), child);
        }

        guard.children.replace(children_map);

        debug!("children map init success");

        Ok(())
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
