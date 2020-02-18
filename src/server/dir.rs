use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::ops::DerefMut;
use std::os::unix::fs::MetadataExt;
use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_std::fs;
use async_std::fs::File as SysFile;
use async_std::path::{Path, PathBuf};
use async_std::stream;
use async_std::sync::RwLock;
use fuse::{FileAttr, FileType};
use futures_util::stream::StreamExt;
use time_old::Timespec;

use crate::errno::Errno;
use crate::helper::Apply;
use crate::Result;
use crate::server::attr::SetAttr;
use crate::server::entry::Entry;
use crate::server::file::File;
use crate::server::inode::{Inode, InodeMap};

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
    pub async fn from_exist<P: AsRef<Path>>(parent: Inode, real_path: P, inode_gen: Arc<AtomicU64>, inode_map: Arc<RwLock<InodeMap>>) -> Result<Arc<Self>> {
        if fs::metadata(&real_path).await?.is_file() {
            return Err(Errno(libc::ENOTDIR));
        }

        let real_path = real_path.as_ref();

        let inode = inode_gen.fetch_add(1, Ordering::Relaxed);

        let dir = Arc::new(Dir(RwLock::new(InnerDir {
            inode,
            name: real_path.file_name().expect("name should be valid").to_os_string(),
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
            atime: Timespec::new(metadata.atime(), metadata.atime_nsec() as i32),
            mtime: Timespec::new(metadata.mtime(), metadata.mtime_nsec() as i32),
            ctime: Timespec::new(metadata.ctime(), metadata.ctime_nsec() as i32),
            crtime: Timespec::new(metadata.atime(), metadata.atime_nsec() as i32),
            perm: metadata.permissions().mode() as u16,
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

    async fn init_children_map(&self) -> Result<()> {
        if self.0.read().await.children.is_some() {
            return Ok(());
        }

        let guard = self.0.write().await;

        // avoid useless init
        if guard.children.is_some() {
            return Ok(());
        }

        let mut children_map = BTreeMap::new();

        let mut dir_entries = fs::read_dir(&guard.real_path).await?;

        let mut inode_map = guard.inode_map.write().await;

        while let Some(dir_entry) = dir_entries.next().await {
            let dir_entry = dir_entry?;

            let dir_entry_real_path = PathBuf::from(guard.real_path.clone()).apply(|path| path.push(dir_entry.file_name()));

            let child = if dir_entry.file_type().await?.is_dir() {
                Entry::from(Dir::from_exist(
                    guard.parent,
                    &dir_entry_real_path,
                    Arc::clone(&guard.inode_gen),
                    Arc::clone(&guard.inode_map),
                ).await?)
            } else {
                Entry::from(File::from_exist(
                    guard.parent,
                    &dir_entry_real_path,
                    &guard.inode_gen,
                    &mut inode_map,
                ).await?)
            };

            children_map.insert(dir_entry_real_path.into_os_string(), child);
        }

        guard.children.replace(children_map);

        Ok(())
    }

    pub async fn readdir(&self, offset: i64) -> Result<Vec<(Inode, i64, FileType, OsString)>> {
        self.init_children_map().await?;

        let guard = self.0.read().await;

        let children_map = &mut guard.children.expect("children map should be initialized");

        let prefix_children = stream::from_iter(vec![
            (guard.inode, FileType::Directory, OsString::from(".")),
            (guard.parent, FileType::Directory, OsString::from("..")),
        ]);

        let children = stream::from_iter(children_map.iter())
            .filter_map(|(name, child)| async move {
                match child.get_attr().await {
                    Err(err) => None, // ignore error child
                    Ok(attr) => Some((attr.ino, attr.kind, name.to_os_string()))
                }
            });

        Ok(prefix_children.chain(children)
            .enumerate()
            .map(|(index, (inode, kind, name))| (inode, (index + 1) as i64, kind, name))
            .skip(offset as usize)
            .collect().await)
    }

    pub async fn create_dir(&self, name: &OsStr) -> Result<Entry> {
        self.init_children_map().await?;

        if self.0.read().await.children.expect("children should be initialized").get(name).is_some() {
            return Err(Errno(libc::EEXIST));
        }

        let guard = self.0.write().await;

        let children_map = &mut guard.children.expect("children should be initialized");

        if children_map.get(name).is_some() {
            return Err(Errno(libc::EEXIST));
        }

        let new_dir_path = PathBuf::from(guard.real_path.clone()).apply(|path| path.push(name));

        fs::create_dir(&new_dir_path).await?;

        let dir = Dir::from_exist(guard.parent, &new_dir_path, Arc::clone(&guard.inode_gen), Arc::clone(&guard.inode_map)).await?;
        let dir = Entry::from(dir);

        children_map.insert(name.to_os_string(), dir.clone());

        Ok(dir)
    }

    pub async fn create_file(&self, name: &OsStr) -> Result<Entry> {
        self.init_children_map().await?;

        if self.0.read().await.children.expect("children should be initialized").get(name).is_some() {
            return Err(Errno(libc::EEXIST));
        }

        let guard = self.0.write().await;

        let children_map = &mut guard.children.expect("children should be initialized");

        if children_map.get(name).is_some() {
            return Err(Errno(libc::EEXIST));
        }

        let new_file_path = PathBuf::from(guard.real_path.clone()).apply(|path| path.push(name));

        SysFile::create(&new_file_path).await?;

        let file = File::from_exist(guard.parent, &new_file_path, &guard.inode_gen, guard.inode_map.write().await.deref_mut()).await?;
        let file = Entry::from(&file);

        children_map.insert(name.to_os_string(), file.clone());

        Ok(file)
    }

    pub async fn remove_entry(&self, name: &OsStr, is_dir: bool) -> Result<Entry> {
        self.init_children_map().await?;

        let guard = self.0.write().await;

        let mut inode_map = guard.inode_map.write().await;

        let children_map = &mut guard.children.expect("children map should be initialized");

        match children_map.get(name).ok_or(Errno(libc::ENOENT))? {
            Entry::Dir(dir) => {
                if !is_dir {
                    return Err(Errno(libc::EISDIR));
                }

                // always contains . and ..
                if !dir.readdir(0).await?.len() > 2 {
                    return Err(Errno(libc::ENOTEMPTY));
                }
            }

            Entry::File(_) => if is_dir {
                return Err(Errno(libc::ENOTDIR));
            }
        }

        let entry = children_map.remove(name).expect("child exists");

        inode_map.remove(&entry.get_inode().await);

        Ok(entry)
    }
}
/*pub async fn create_dir<P>(
    parent: Inode,
    inode: Inode,
    real_path: P,
    _uid: u32,
    _gid: u32,
    mode: u32,
) -> Result<Arc<Self>>
    where P: AsRef<Path>
{
    fs::create_dir(&real_path).await?;

    let perm = fs::metadata(&real_path).await?.permissions()
        .apply(|perm| perm.set_mode(mode));

    fs::set_permissions(&real_path, perm).await?;

    let real_path = real_path.as_ref();

    Ok(Arc::new(Self(RwLock::new(InnerDir {
        inode,
        name: real_path.file_name().expect("it should be a valid path").to_os_string(),
        real_path: real_path.as_os_str().to_os_string(),
        parent,
        children: BTreeMap::new(),
    }))))
}*/
/*pub async fn create_dir<P: AsRef<Path>>(
    parent: Inode,
    real_path: P,
    _uid: u32,
    _gid: u32,
    mode: u32,
    inode_gen: &AtomicU64,
    inode_map: &mut InodeMap,
) -> Result<Arc<Self>> {
    fs::create_dir(&real_path).await?;

    let perm = fs::metadata(&real_path).await?.permissions()
        .apply(|perm| perm.set_mode(mode));

    fs::set_permissions(&real_path, perm).await?;

    Self::from_exist(parent, real_path, inode_gen, inode_map).await
}

pub async fn from_exist<P: AsRef<Path>>(parent: Inode, real_path: P, inode_gen: &AtomicU64, inode_map: &mut InodeMap) -> Result<Arc<Self>> {
    let real_path = real_path.as_ref();



    let dir = Arc::new(Dir(RwLock::new(InnerDir {
        inode: inode_gen.fetch_add(1, Ordering::Relaxed),
        name: real_path.file_name().expect("it should be a valid path").to_os_string(),
        real_path: real_path.as_os_str().to_os_string(),
        parent,
        children: BTreeMap::new(),
    })));
}*//*

    pub async fn get_attr(self: &Arc<Self>) -> Result<FileAttr> {
        let guard = self.0.read().await;

        let metadata = fs::metadata(&guard.real_path).await?;

        Ok(FileAttr {
            ino: guard.inode,
            size: metadata.len(),
            blocks: metadata.blocks(),
            kind: FileType::Directory,
            atime: Timespec::new(metadata.atime(), metadata.atime_nsec() as i32),
            mtime: Timespec::new(metadata.mtime(), metadata.mtime_nsec() as i32),
            ctime: Timespec::new(metadata.ctime(), metadata.ctime_nsec() as i32),
            crtime: Timespec::new(metadata.atime(), metadata.atime_nsec() as i32),
            perm: metadata.permissions().mode() as u16,
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

    pub async fn readdir(self: &Arc<Self>, offset: usize, inode_gen: &AtomicU64, inode_map: &mut InodeMap) -> Result<Vec<(Inode, i64, FileType, OsString)>> {
        *//*let guard = self.0.read().await;

        *//**//*let mut children = vec![
            (guard.inode, FileType::Directory, OsString::from(".")),
            (guard.parent, FileType::Directory, OsString::from("..")),
        ];

        for (name, child) in guard.children.iter() {
            let attr = child.get_attr().await?;
            children.push((attr.ino, attr.kind, name.to_os_string()));
        }

        Ok(children.into_iter().enumerate().map(|(index, (inode, kind, name))| {
            (inode, (index + 1) as i64, kind, name)
        }).collect())*//*

        {
            let guard = self.0.read().await;

            if let Some(children_map) = &guard.children {
                let prefix_children = stream::from_iter(vec![
                    (guard.inode, FileType::Directory, OsString::from(".")),
                    (guard.parent, FileType::Directory, OsString::from("..")),
                ]);

                let children = stream::from_iter(children_map.iter())
                    .filter_map(|(name, child)| async move {
                        match child.get_attr().await {
                            Err(err) => None, // ignore error child
                            Ok(attr) => Some((attr.ino, attr.kind, name.to_os_string()))
                        }
                    });

                return Ok(prefix_children.chain(children)
                    .enumerate()
                    .map(|(index, (inode, kind, name))| (inode, (index + 1) as i64, kind, name))
                    .skip(offset)
                    .collect().await);
            }
        }

        self.init_children_map(inode_gen, inode_map).await?;
        self.readdir(offset, inode_gen, inode_map).await
    }

    pub async fn lookup(self: &Arc<Self>, name: &OsStr, inode_gen: &AtomicU64, inode_map: &mut InodeMap) -> Result<Entry> {
        if let Some(children_map) = &self.0.read().await.children {
            return children_map
                .get(name)
                .ok_or(Errno(libc::ENOENT))
                .map(|entry| entry.clone());
        }

        // after init children map, lookup again
        self.init_children_map(inode_gen, inode_map).await?;
        self.lookup(name, inode_gen, inode_map).await
    }

    async fn init_children_map<P: AsRef<Path>>(
        inode: Inode,
        real_path: P,
        inode_gen: &AtomicU64,
        inode_map: &mut InodeMap,
    ) -> Result<BTreeMap<OsString, Entry>> {
        let mut children_map = BTreeMap::new();

        let mut children_stream = fs::read_dir(&real_path).await?;

        while let Some(child) = children_stream.next().await {
            let child = child?;

            let metadata = child.metadata().await?;

            let child_path = real_path.as_ref().to_path_buf().apply(|path| path.push(child.file_name()));

            let child_entry = if metadata.is_dir() {
                Entry::from(Dir::from_exist(inode, child_path, inode_gen, inode_map).await?)
            } else {
                Entry::from(File::from_exist(inode, child_path, inode_gen, inode_map).await?)
            };

            children_map.insert(child.file_name(), child_entry.clone());
        }

        Ok(children_map)
    }
}*/
