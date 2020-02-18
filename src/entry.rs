use fuse::FileAttr;
use std::os::raw::c_int;
use time_old::Timespec;

use crate::dir::Dir;
use crate::file::File;

#[derive(Debug, Clone)]
pub enum Entry {
    Dir(Dir),
    File(File),
}

impl Entry {
    pub fn get_attr(&self) -> FileAttr {
        match self {
            Entry::Dir(dir) => dir.get_attr(),
            Entry::File(file) => file.get_attr()
        }
    }

    pub fn set_attr(
        &self,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<Timespec>,
        mtime: Option<Timespec>,
        fh: Option<u64>,
        crtime: Option<Timespec>,
        chgtime: Option<Timespec>,
        kuptime: Option<Timespec>,
        flags: Option<u32>,
    ) -> Result<FileAttr, c_int> {
        match self {
            Entry::Dir(dir) => dir.set_attr(
                mode,
                uid,
                gid,
                size,
                atime,
                mtime,
                fh,
                crtime,
                chgtime,
                kuptime,
                flags,
            ),
            Entry::File(file) => file.set_attr(
                mode,
                uid,
                gid,
                size,
                atime,
                mtime,
                fh,
                crtime,
                chgtime,
                kuptime,
                flags,
            )
        }
    }
}
