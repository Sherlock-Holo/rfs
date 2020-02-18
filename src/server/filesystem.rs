use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::sync::atomic::{AtomicU64, Ordering};

use async_std::sync::{Arc, RwLock};
use fuse::{FileAttr, FileType};

use crate::errno::Errno;
use crate::helper::Apply;
use crate::path::PathClean;
use crate::Result;
use crate::server::attr::SetAttr;
use crate::server::dir::Dir;
use crate::server::entry::Entry;
use crate::server::file_handle::FileHandle;
use crate::server::inode::{Inode, InodeMap};

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

        if let Entry::Dir(dir) = guard.get(&parent).ok_or(Errno(libc::ENOENT))? {
            dir.lookup(OsStr::new(&name)).await
        } else {
            Err(Errno(libc::ENOTDIR))
        }
    }

    #[inline]
    pub async fn get_attr(&self, inode: Inode) -> Result<FileAttr> {
        self.inode_map.read().await.get(&inode).ok_or(Errno(libc::ENOENT))?.get_attr().await
    }

    pub async fn set_dir_attr(&self, inode: Inode, set_attr: SetAttr) -> Result<FileAttr> {
        let guard = self.inode_map.read().await;

        if let Entry::Dir(dir) = guard.get(&inode).ok_or(Errno(libc::ENOENT))? {
            dir.set_attr(set_attr).await
        } else {
            panic!("file must use file handle to set attr")
        }
    }

    pub async fn create_dir(&self, parent: Inode, name: &OsStr, mode: u32) -> Result<FileAttr> {
        let name = name.clean()?;

        let entry = self.inode_map.read().await.get(&parent).ok_or(Errno(libc::ENOENT))?.clone();

        match entry {
            Entry::File(_) => Err(Errno(libc::ENOTDIR)),
            Entry::Dir(dir) => dir.create_dir(OsStr::new(&name), mode).await?.get_attr().await
        }
    }

    pub async fn remove_entry(&self, parent: Inode, name: &OsStr, is_dir: bool) -> Result<()> {
        let name = name.clean()?;

        match self.inode_map.read().await
            .get(&parent)
            .ok_or(Errno(libc::ENOENT))? {
            Entry::File(_) => return Err(Errno(libc::ENOTDIR)),
            Entry::Dir(dir) => dir.remove_entry(OsStr::new(&name), is_dir).await?
        };

        Ok(())
    }

    pub async fn rename(&self, old_parent: Inode, old_name: &OsStr, new_parent: Inode, new_name: &OsStr) -> Result<()> {
        let old_name = old_name.clean()?;
        let new_name = new_name.clean()?;

        let guard = self.inode_map.read().await;

        let old_parent = match guard.get(&old_parent).ok_or(Errno(libc::ENOENT))? {
            Entry::File(_) => return Err(Errno(libc::ENOTDIR)),
            Entry::Dir(dir) => dir
        };

        let new_parent = match guard.get(&new_parent).ok_or(Errno(libc::ENOENT))? {
            Entry::File(_) => return Err(Errno(libc::ENOTDIR)),
            Entry::Dir(dir) => dir
        };

        new_parent.add_child_from(old_parent, OsStr::new(&old_name), OsStr::new(&new_name)).await
    }

    pub async fn open(&self, inode: Inode, flags: u32) -> Result<FileHandle> {
        if let Entry::File(file) = self.inode_map.read().await
            .get(&inode)
            .ok_or(Errno(libc::ENOENT))?
        {
            file.open(self.file_handle_id_gen.fetch_add(1, Ordering::Relaxed), flags).await
        } else {
            Err(Errno(libc::EISDIR))
        }
    }

    pub async fn read_dir(&self, inode: Inode, offset: i64) -> Result<Vec<(Inode, i64, FileType, OsString)>> {
        if let Entry::Dir(dir) = self.inode_map.read().await
            .get(&inode)
            .ok_or(Errno(libc::ENOENT))?
        {
            dir.read_dir(offset).await
        } else {
            Err(Errno(libc::ENOTDIR))
        }
    }

    pub async fn create(&self, parent: Inode, name: &OsStr, mode: u32, flags: u32) -> Result<FileHandle> {
        let name = name.clean()?;

        if let Entry::Dir(dir) = self.inode_map.read().await
            .get(&parent)
            .ok_or(Errno(libc::ENOENT))?
        {
            let file = dir.create_file(OsStr::new(&name), mode).await?;

            file.open(self.file_handle_id_gen.fetch_add(1, Ordering::Relaxed), flags).await
        } else {
            Err(Errno(libc::ENOTDIR))
        }
    }
}

/*
*//*pub struct Filesystem {
    inode_map: RwLock<InodeMap>,
    inode_generator: AtomicU64,
}

impl Filesystem {
    pub async fn new() -> Result<Self> {
        let inode_generator = AtomicU64::new(1);

        let root = Dir::from_exist(1, "/", &inode_generator).await?;

        let inode_map = InodeMap::new().apply(|inode_map|{
            inode_map.insert(1, Entry::from(root));
        });

        Ok(Self {
            inode_map: RwLock::new(inode_map),
            inode_generator,
        })
    }

    pub async fn lookup(&self, parent: Inode, name: &OsStr) -> Result<FileAttr> {
        self.inode_map.read().await
    }
}*//*

struct InnerFilesystem {
    root: Arc<Dir>,
    inode_map: BTreeMap<Inode, Entry>,
    inode_generator: AtomicU64,
}

impl InnerFilesystem {
    async fn new() -> Result<RwLock<Self>> {
        let inode_generator = AtomicU64::new(1);

        Ok(RwLock::new(Self {
            root: Dir::from_exist(1, "/", &inode_generator).await?,
            inode_map: BTreeMap::new(),
            inode_generator: AtomicU64::new(2),
        }))
    }
}

pub struct Filesystem(RwLock<InnerFilesystem>);

impl Filesystem {
    pub async fn new() -> Result<Arc<Self>> {
        Ok(Arc::new(Self(InnerFilesystem::new().await?)))
    }

    pub async fn lookup(self: &Arc<Self>, parent: Inode, name: &OsStr) -> Result<FileAttr> {
        let guard = self.0.read().await;

        let parent = guard.inode_map.get(&parent).ok_or(Errno(libc::ENOENT))?;

        let parent = match parent {
            Entry::File(_) => return Err(Errno(libc::ENOTDIR)),
            Entry::Dir(dir) => dir
        };

        let entry = parent.lookup(name, &guard.inode_generator, &mut guard.inode_map).await?;

        entry.get_attr().await
    }

    pub async fn getattr(self: &Arc<Self>, inode: Inode) -> Result<FileAttr> {
        let entry = self.0.read().await.inode_map.get(&inode).ok_or(Errno(libc::ENOENT))?;
        entry.get_attr().await
    }
}
*/
