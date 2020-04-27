use std::ffi::OsStr;
use std::fs::Metadata;

use fuse::{FileAttr, FileType};

use crate::Result;

use super::dir::Dir;
use super::file::File;
use super::inode::Inode;
use super::SetAttr;

#[derive(Debug, Clone)]
pub enum Entry {
    Dir(Dir),
    File(File),
}

impl Entry {
    pub fn new(new_inode: Inode, name: &OsStr, parent: &Dir, metadata: &Metadata) -> Self {
        if metadata.is_dir() {
            Entry::from(Dir::from_exist(parent, &name, new_inode))
        } else {
            Entry::from(File::from_exist(parent, &name, new_inode))
        }
    }

    pub fn is_dir(&self) -> bool {
        matches!(self, Entry::Dir(_))
    }

    pub fn is_file(&self) -> bool {
        !self.is_dir()
    }

    pub fn get_inode(&self) -> Inode {
        match self {
            Entry::Dir(dir) => dir.get_inode(),
            Entry::File(file) => file.get_inode(),
        }
    }

    pub fn get_kind(&self) -> FileType {
        if self.is_dir() {
            FileType::Directory
        } else {
            FileType::RegularFile
        }
    }

    pub fn set_new_parent(&mut self, new_parent: &Dir) {
        match self {
            Entry::Dir(dir) => dir.set_new_parent(new_parent),
            Entry::File(file) => file.set_new_parent(new_parent),
        }
    }

    pub fn set_new_name(&mut self, new_name: &OsStr) {
        match self {
            Entry::Dir(dir) => dir.set_new_name(new_name),
            Entry::File(file) => file.set_new_name(new_name),
        }
    }

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
}

impl From<Dir> for Entry {
    fn from(dir: Dir) -> Self {
        Entry::Dir(dir)
    }
}

impl From<File> for Entry {
    fn from(file: File) -> Self {
        Entry::File(file)
    }
}

impl From<&Dir> for Entry {
    fn from(dir: &Dir) -> Self {
        Entry::Dir(dir.clone())
    }
}

impl From<&File> for Entry {
    fn from(file: &File) -> Self {
        Entry::File(file.clone())
    }
}
