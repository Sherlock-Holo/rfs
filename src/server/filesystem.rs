use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::sync::atomic::AtomicU64;

use async_std::sync::{Arc, RwLock};
use fuse::FileAttr;

use crate::errno::Errno;
use crate::helper::Apply;
use crate::Result;
use crate::server::dir::Dir;
use crate::server::entry::Entry;
use crate::server::inode::{Inode, InodeMap};

/*pub struct Filesystem {
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
}*/

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
