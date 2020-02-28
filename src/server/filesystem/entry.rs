use std::ffi::OsString;
use std::sync::Arc;

use fuse::FileAttr;

use crate::Result;

use super::dir::Dir;
use super::file::File;
use super::inode::Inode;
use super::SetAttr;

#[derive(Debug)]
pub enum Entry {
    Dir(Arc<Dir>),
    File(Arc<File>),
}

impl Clone for Entry {
    fn clone(&self) -> Self {
        match self {
            Entry::Dir(dir) => Entry::Dir(dir.clone()),
            Entry::File(file) => Entry::File(file.clone()),
        }
    }
}

impl Entry {
    pub async fn get_attr(&self) -> Result<FileAttr> {
        match self {
            Entry::Dir(dir) => dir.get_attr().await,
            Entry::File(file) => file.get_attr().await,
        }
    }

    pub async fn set_attr(&self, set_attr: SetAttr) -> Result<FileAttr> {
        match self {
            Entry::Dir(dir) => dir.set_attr(set_attr).await,
            Entry::File(file) => file.set_attr(set_attr).await,
        }
    }

    pub async fn get_inode(&self) -> Inode {
        match self {
            Entry::Dir(dir) => dir.get_inode().await,
            Entry::File(file) => file.get_inode().await,
        }
    }

    pub async fn get_name(&self) -> OsString {
        match self {
            Entry::Dir(dir) => dir.get_name().await,
            Entry::File(file) => file.get_name().await,
        }
    }
}

impl From<&Arc<Dir>> for Entry {
    fn from(dir: &Arc<Dir>) -> Self {
        Entry::Dir(dir.clone())
    }
}

impl From<Arc<Dir>> for Entry {
    fn from(dir: Arc<Dir>) -> Self {
        Entry::Dir(dir)
    }
}

impl From<&Arc<File>> for Entry {
    fn from(file: &Arc<File>) -> Self {
        Entry::File(file.clone())
    }
}

impl From<Arc<File>> for Entry {
    fn from(file: Arc<File>) -> Self {
        Entry::File(file)
    }
}
