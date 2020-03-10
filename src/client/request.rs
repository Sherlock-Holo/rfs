use std::ffi::OsString;
use std::time::SystemTime;

use async_std::sync::Sender;
use fuse::{
    ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyLock,
    ReplyOpen, ReplyWrite,
};

#[derive(Copy, Clone)]
pub struct BasicMessage {
    pub uid: u32,
    pub gid: u32,
    pub unique: u64,
}

pub enum FuseRequest {
    Init(BasicMessage, Sender<Result<(), libc::c_int>>),

    Lookup {
        basic: BasicMessage,
        parent: u64,
        name: OsString,
        reply: ReplyEntry,
    },

    Getattr {
        basic: BasicMessage,
        inode: u64,
        reply: ReplyAttr,
    },

    SetAttr {
        basic: BasicMessage,
        inode: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<SystemTime>,
        mtime: Option<SystemTime>,
        fh: Option<u64>,
        crtime: Option<SystemTime>,
        chgtime: Option<SystemTime>,
        bkuptime: Option<SystemTime>,
        flags: Option<u32>,
        reply: ReplyAttr,
    },

    Mkdir {
        basic: BasicMessage,
        parent: u64,
        name: OsString,
        mode: u32,
        reply: ReplyEntry,
    },

    Unlink {
        basic: BasicMessage,
        parent: u64,
        name: OsString,
        reply: ReplyEmpty,
    },

    Rmdir {
        basic: BasicMessage,
        parent: u64,
        name: OsString,
        reply: ReplyEmpty,
    },

    Rename {
        basic: BasicMessage,
        parent: u64,
        name: OsString,
        new_parent: u64,
        new_name: OsString,
        reply: ReplyEmpty,
    },

    Open {
        basic: BasicMessage,
        inode: u64,
        flags: u32,
        reply: ReplyOpen,
    },

    Read {
        basic: BasicMessage,
        inode: u64,
        fh: u64,
        offset: i64,
        size: u32,
        reply: ReplyData,
    },

    Write {
        basic: BasicMessage,
        inode: u64,
        fh: u64,
        offset: i64,
        data: Vec<u8>,
        flags: u32,
        reply: ReplyWrite,
    },

    Flush {
        basic: BasicMessage,
        inode: u64,
        fh: u64,
        lock_owner: u64,
        reply: ReplyEmpty,
    },

    Release {
        basic: BasicMessage,
        inode: u64,
        fh: u64,
        flags: u32,
        lock_owner: u64,
        flush: bool,
        reply: ReplyEmpty,
    },

    Fsync {
        basic: BasicMessage,
        inode: u64,
        fh: u64,
        data_sync: bool,
        reply: ReplyEmpty,
    },

    Readdir {
        basic: BasicMessage,
        inode: u64,
        offset: i64,
        reply: ReplyDirectory,
    },

    Create {
        basic: BasicMessage,
        parent: u64,
        name: OsString,
        mode: u32,
        flags: u32,
        reply: ReplyCreate,
    },

    Getlk {
        basic: BasicMessage,
        inode: u64,
        fh: u64,
        lock_owner: u64,
        pid: u32,
        reply: ReplyLock,
    },

    Setlk {
        basic: BasicMessage,
        inode: u64,
        fh: u64,
        lock_owner: u64,
        r#type: u32,
        sleep: bool,
        reply: ReplyEmpty,
    },

    Interrupt {
        basic: BasicMessage,
        reply: ReplyEmpty,
    },
}
