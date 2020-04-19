use std::ffi::OsStr;
use std::path::{Path, PathBuf};

use async_std::fs::Metadata;
use fuse::FileType;

use super::dir::Dir;
use super::file::File;
use super::inode::Inode;

#[derive(Debug, Clone)]
pub enum EntryPath {
    Dir(PathBuf),
    File(PathBuf),
}

impl EntryPath {
    pub fn get_path(&self) -> &Path {
        match self {
            EntryPath::Dir(path) => path,
            EntryPath::File(path) => path,
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
