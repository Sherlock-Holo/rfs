use std::ffi::OsString;

use async_std::sync::Sender;
use fuse::{FileAttr, FileType};

use crate::Result;

use super::inode::Inode;
use super::{FileHandle, SetAttr};

pub enum Request {
    Lookup {
        parent: Inode,
        name: OsString,
        response: Sender<Result<FileAttr>>,
    },

    GetAttr {
        inode: Inode,
        response: Sender<Result<FileAttr>>,
    },

    SetAttr {
        inode: Inode,
        new_attr: SetAttr,
        response: Sender<Result<FileAttr>>,
    },

    CreateDir {
        parent: Inode,
        name: OsString,
        mode: u32,
        response: Sender<Result<FileAttr>>,
    },

    RemoveEntry {
        parent: Inode,
        name: OsString,
        is_dir: bool,
        response: Sender<Result<()>>,
    },

    Rename {
        old_parent: Inode,
        old_name: OsString,
        new_parent: Inode,
        new_name: OsString,
        response: Sender<Result<()>>,
    },

    Open {
        inode: Inode,
        flags: u32,
        response: Sender<Result<FileHandle>>,
    },

    ReadDir {
        inode: Inode,
        offset: i64,
        response: Sender<Result<Vec<(Inode, i64, FileType, OsString)>>>,
    },

    CreateFile {
        parent: Inode,
        name: OsString,
        mode: u32,
        flags: u32,
        response: Sender<Result<(FileHandle, FileAttr)>>,
    },
}
