use std::cmp::Ordering;
use std::ffi::OsString;
use std::sync::Arc;

use async_std::path::{Path, PathBuf};
use fuse::FileAttr;

use crate::Result;

use super::dir::Dir;
use super::file::File;
use super::inode::Inode;
use super::SetAttr;

#[derive(Debug, Clone)]
pub enum EntryPath {
    Dir(PathBuf),
    File(PathBuf),
}

impl EntryPath {
    pub fn get_path(&self) -> &Path {
        match self {
            EntryPath::Dir(path) => path,
            EntryPath::File(path) => path
        }
    }

    #[inline]
    pub fn is_file(&self) -> bool {
        matches!(*self, EntryPath::File(_))
    }

    #[inline]
    pub fn is_dir(&self) -> bool {
        !self.is_file()
    }
}

/*impl Entry {
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

    pub fn get_inode(&self) -> Inode {
        match self {
            Entry::Dir(dir) => dir.get_inode(),
            Entry::File(file) => file.get_inode(),
        }
    }

    pub async fn get_name(&self) -> OsString {
        match self {
            Entry::Dir(dir) => dir.get_name().await,
            Entry::File(file) => file.get_name().await,
        }
    }
}

impl From<&Dir> for Entry {
    fn from(dir: &Dir) -> Self {
        Entry::Dir(dir.clone())
    }
}

impl From<Dir> for Entry {
    fn from(dir: Dir) -> Self {
        Entry::Dir(dir)
    }
}

impl From<&File> for Entry {
    fn from(file: &File) -> Self {
        Entry::File(file.clone())
    }
}

impl From<File> for Entry {
    fn from(file: File) -> Self {
        Entry::File(file)
    }
}*/
