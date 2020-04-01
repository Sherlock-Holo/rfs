use std::os::unix::fs::MetadataExt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_std::fs::Metadata;
use fuse::{FileAttr, FileType};

use crate::Result;

use super::inode::Inode;

#[derive(Debug, Copy, Clone)]
pub struct SetAttr {
    pub mode: Option<u32>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub size: Option<u64>,
    pub atime: Option<SystemTime>,
    pub mtime: Option<SystemTime>,
    pub ctime: Option<SystemTime>,
    pub flags: Option<u32>,
}

pub fn metadata_to_file_attr(inode: Inode, metadata: Metadata) -> Result<FileAttr> {
    Ok(FileAttr {
        ino: inode,
        size: metadata.len(),
        blocks: metadata.blocks(),
        kind: if metadata.is_dir() {
            FileType::Directory
        } else {
            FileType::RegularFile
        },
        atime: metadata.accessed()?,
        mtime: metadata.modified()?,
        ctime: UNIX_EPOCH
            + Duration::new(metadata.ctime() as u64, metadata.ctime_nsec() as u32),
        perm: if metadata.is_dir() {
            (metadata.permissions().mode() ^ libc::S_IFDIR) as u16
        } else {
            (metadata.permissions().mode() ^ libc::S_IFREG) as u16
        },
        uid: metadata.uid(),
        gid: metadata.gid(),
        rdev: metadata.rdev() as u32,
        flags: 0,
        nlink: metadata.nlink(),
    })
}