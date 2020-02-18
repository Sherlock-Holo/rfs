use fuse::{FileAttr, FileType};
use std::ffi::OsString;
use std::os::raw::c_int;
use std::sync::{Arc, RwLock};
use time_old::Timespec;

use crate::dir::Dir;
use crate::entry::Entry;
use crate::file_handle::FileHandler;
use crate::inode::Inode;

pub const BLOCK_SIZE: u64 = 4096;

#[derive(Debug)]
pub struct File(Arc<RwLock<InnerFile>>);

#[derive(Debug)]
pub struct InnerFile {
    inode: Inode,
    name: OsString,
    pub parent: Dir,
    data: Vec<u8>,
    mode: u32,
    uid: u32,
    gid: u32,
}

impl InnerFile {
    fn get_attr(&self) -> FileAttr {
        let data_size = self.data.len() as u64;

        FileAttr {
            ino: self.inode,
            size: data_size,
            blocks: if data_size % BLOCK_SIZE != 0 {
                data_size / BLOCK_SIZE + 1
            } else {
                data_size / BLOCK_SIZE
            },
            kind: FileType::RegularFile,
            atime: Timespec::new(0, 0),
            mtime: Timespec::new(0, 0),
            ctime: Timespec::new(0, 0),
            crtime: Timespec::new(0, 0),
            perm: self.mode as u16,
            uid: self.uid,
            gid: self.gid,
            rdev: 0,
            flags: 0,
            nlink: 2,
        }
    }

    fn set_attr(
        &mut self,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<Timespec>,
        _mtime: Option<Timespec>,
        _fh: Option<u64>,
        _crtime: Option<Timespec>,
        _chgtime: Option<Timespec>,
        _kuptime: Option<Timespec>,
        _flags: Option<u32>,
    ) -> Result<FileAttr, c_int> {
        if let Some(mode) = mode {
            self.mode = mode;
        }

        if let Some(uid) = uid {
            self.uid = uid;
        }

        if let Some(gid) = gid {
            self.gid = gid;
        }

        if let Some(size) = size {
            self.data.truncate(size as usize);
        }

        Ok(self.get_attr())
    }

    fn read(&self, offset: i64, size: u32) -> Result<Vec<u8>, c_int> {
        let readable = self.data
            .iter()
            .skip(offset as usize)
            .take(size as usize)
            .count();

        if readable == 0 {
            return Ok(vec![]);
        }

        let mut buf = vec![0; readable];

        buf.copy_from_slice(&self.data[offset as usize..readable]);

        Ok(buf)
    }

    fn write(&mut self, offset: i64, data: &[u8]) -> Result<usize, c_int> {
        let offset = offset as usize;

        if self.data.len() < offset {
            return Err(libc::EINVAL);
        }

        let buf = &mut self.data;

        if buf[offset..].len() < data.len() {
            buf.resize(data.len() - buf[offset..].len(), 0);
        }

        buf[offset..].copy_from_slice(data);

        Ok(data.len())
    }

    fn truncate(&mut self) {
        self.data.clear();
    }
}

impl File {
    pub fn new(inode: Inode, name: OsString, parent: Dir, mode: u32, uid: u32, gid: u32) -> Self {
        Self(Arc::new(RwLock::new(InnerFile {
            inode,
            name,
            parent,
            data: vec![],
            mode,
            uid,
            gid,
        })))
    }

    pub fn get_attr(&self) -> FileAttr {
        self.0.read().unwrap().get_attr()
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
        self.0.write().unwrap().set_attr(
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

    pub fn open(&self, fh_id: u64, flags: u32) -> FileHandler {
        FileHandler::new(fh_id, File(Arc::clone(&self.0)), flags)
    }

    pub fn read(&self, offset: i64, size: u32) -> Result<Vec<u8>, c_int> {
        self.0.read().unwrap().read(offset, size)
    }

    pub fn write(&self, offset: i64, data: &[u8]) -> Result<usize, c_int> {
        self.0.write().unwrap().write(offset, data)
    }

    pub fn truncate(&self) {
        self.0.write().unwrap().truncate()
    }

    pub fn set_new_parent(&self, new_parent: Dir) {
        self.0.write().unwrap().parent = new_parent
    }

    pub fn get_inode(&self) -> Inode {
        self.0.read().unwrap().inode
    }

    pub fn get_name(&self) -> OsString {
        self.0.read().unwrap().name.to_os_string()
    }
}

impl Clone for File {
    fn clone(&self) -> Self {
        File(Arc::clone(&self.0))
    }
}

impl From<File> for Entry {
    fn from(file: File) -> Self {
        Entry::File(file)
    }
}

impl From<Arc<RwLock<InnerFile>>> for File {
    fn from(file: Arc<RwLock<InnerFile>>) -> Self {
        File(file)
    }
}