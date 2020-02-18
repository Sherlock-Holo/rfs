use fuse::{FileAttr, FileType};
use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::os::raw::c_int;
use std::sync::{Arc, RwLock};
use time_old::Timespec;

use crate::entry::Entry;
use crate::file::File;
use crate::inode::Inode;

pub const DIR_SIZE: u64 = 4096;

#[derive(Debug)]
pub struct Dir(Arc<RwLock<InnerDir>>);

#[derive(Debug)]
struct InnerDir {
    inode: Inode,
    name: OsString,
    parent: Option<Dir>,
    children: HashMap<OsString, Entry>,
    mode: u32,
    uid: u32,
    gid: u32,
}

impl InnerDir {
    /*fn get_attr(&self) -> FileAttr {
        FileAttr {
            ino: self.inode,
            size: DIR_SIZE,
            blocks: 1,
            kind: FileType::Directory,
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

        if size.is_some() {
            return Err(libc::EISDIR);
        }

        Ok(self.get_attr())
    }

    fn readdir(&self, offset: i64) -> Vec<(Inode, i64, FileType, OsString)> {
        let parent_inode = match &self.parent {
            None => 1,
            Some(parent) => parent.get_inode(),
        };

        let hard_link = vec![
            (self.inode, FileType::Directory, OsString::from(".")),
            (parent_inode, FileType::Directory, OsString::from("..")),
        ];

        let children: Vec<_> = self.children
            .iter()
            .map(|(.., child)| {
                let name = match child {
                    Entry::Dir(dir) => dir.get_name(),
                    Entry::File(file) => file.get_name(),
                };

                let attr = child.get_attr();

                (attr.ino, attr.kind, name)
            })
            .collect();

        hard_link
            .into_iter()
            .chain(children.into_iter())
            .skip(offset as usize)
            .enumerate()
            .map(|(index, (inode, kind, name))| {
                (inode, (index + 1) as i64, kind, name)
            })
            .collect()
    }

    fn mkdir(&mut self, inode: Inode, name: &OsStr, mode: u32, uid: u32, gid: u32) -> Result<FileAttr, c_int> {
        if self.children.get(name).is_some() {
            return Err(libc::EEXIST),
        }

        let dir = Dir::new(inode, name.to_os_string(), Some(Dir::), mode, uid, gid);

        let attr = dir.get_attr();

        guard.children.insert(name.to_os_string(), dir.into());

        Ok(attr)
    }

    fn get_child(&self, name: &OsStr) -> Option<Entry> {
        self.children.get(name).map(|entry| entry.clone())
    }*/
}

impl Dir {
    pub fn new(inode: Inode, name: OsString, parent: Option<Dir>, mode: u32, uid: u32, gid: u32) -> Self {
        Dir(Arc::new(RwLock::new(InnerDir {
            inode,
            name,
            parent,
            children: HashMap::new(),
            mode,
            uid,
            gid,
        })))
    }

    pub fn get_child(&self, name: &OsStr) -> Option<Entry> {
        self.0.read().unwrap().children.get(name).map(|entry| entry.clone())
    }

    pub fn get_attr(&self) -> FileAttr {
        let guard = self.0.read().unwrap();

        FileAttr {
            ino: guard.inode,
            size: DIR_SIZE,
            blocks: 1,
            kind: FileType::Directory,
            atime: Timespec::new(0, 0),
            mtime: Timespec::new(0, 0),
            ctime: Timespec::new(0, 0),
            crtime: Timespec::new(0, 0),
            perm: guard.mode as u16,
            uid: guard.uid,
            gid: guard.gid,
            rdev: 0,
            flags: 0,
            nlink: 2,
        }
    }

    pub fn set_attr(
        &self,
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
        let mut guard = self.0.write().unwrap();

        if let Some(mode) = mode {
            guard.mode = mode;
        }

        if let Some(uid) = uid {
            guard.uid = uid;
        }

        if let Some(gid) = gid {
            guard.gid = gid;
        }

        if size.is_some() {
            return Err(libc::EISDIR);
        }

        Ok(self.get_attr())
    }

    pub fn mkdir(&self, inode: Inode, name: &OsStr, mode: u32, uid: u32, gid: u32) -> Result<Dir, c_int> {
        let mut guard = self.0.write().unwrap();

        if guard.children.get(name).is_some() {
            return Err(libc::EEXIST);
        }

        let dir = Dir::new(inode, name.to_os_string(), Some(Dir(Arc::clone(&self.0))), mode, uid, gid);

        guard.children.insert(name.to_os_string(), dir.clone().into());

        Ok(dir)
    }

    pub fn create(&self, inode: Inode, name: &OsStr, mode: u32, _flags: u32, uid: u32, gid: u32) -> Result<File, c_int> {
        let mut guard = self.0.write().unwrap();

        if guard.children.get(name).is_some() {
            return Err(libc::EEXIST);
        }

        let file = File::new(inode, name.to_os_string(), Dir(Arc::clone(&self.0)), mode, uid, gid);

        guard.children.insert(name.to_os_string(), file.clone().into());

        Ok(file)
    }

    pub fn unlink(&self, name: &OsStr) -> Result<File, c_int> {
        if let Entry::File(file) = self.remove_child(name, false)? {
            return Ok(file);
        }

        unreachable!()
    }

    pub fn rmdir(&self, name: &OsStr) -> Result<Dir, c_int> {
        if let Entry::Dir(dir) = self.remove_child(name, true)? {
            return Ok(dir);
        }

        unreachable!()
    }

    fn remove_child(&self, name: &OsStr, is_dir: bool) -> Result<Entry, c_int> {
        let mut guard = self.0.write().unwrap();

        let entry = match guard.children.get(name) {
            None => return Err(libc::ENOENT),
            Some(entry) => entry
        };

        match entry {
            Entry::Dir(_) => {
                if is_dir {
                    Ok(guard.children.remove(name).unwrap())
                } else {
                    Err(libc::EISDIR)
                }
            }

            Entry::File(_) => {
                if is_dir {
                    Err(libc::ENOTDIR)
                } else {
                    Ok(guard.children.remove(name).unwrap())
                }
            }
        }
    }

    pub fn readdir(&self, offset: i64) -> Vec<(Inode, i64, FileType, OsString)> {
        let guard = self.0.read().unwrap();

        let parent_inode = match &guard.parent {
            None => 1,
            Some(parent) => parent.get_inode(),
        };

        let hard_link = vec![
            (guard.inode, FileType::Directory, OsString::from(".")),
            (parent_inode, FileType::Directory, OsString::from("..")),
        ];

        let children: Vec<_> = guard.children
            .iter()
            .map(|(.., child)| {
                let name = match child {
                    Entry::Dir(dir) => dir.get_name(),
                    Entry::File(file) => file.get_name(),
                };

                let attr = child.get_attr();

                (attr.ino, attr.kind, name)
            })
            .collect();

        hard_link
            .into_iter()
            .chain(children.into_iter())
            .skip(offset as usize)
            .enumerate()
            .map(|(index, (inode, kind, name))| {
                (inode, (index + 1) as i64, kind, name)
            })
            .collect()
    }

    pub fn rename_child_from(&self, from: &Self, old_name: &OsStr, new_name: &OsStr) -> Result<FileAttr, c_int> {
        if self.0.read().unwrap().inode == from.0.read().unwrap().inode {
            let mut guard = self.0.write().unwrap();

            if guard.children.get(old_name).is_none() {
                return Err(libc::ENOENT);
            }

            if guard.children.get(new_name).is_some() {
                return Err(libc::EEXIST);
            }

            let child = guard.children.remove(old_name).unwrap();

            match &child {
                Entry::Dir(dir) => {
                    dir.set_new_parent(Dir(Arc::clone(&self.0)))
                }
                Entry::File(file) => {
                    file.set_new_parent(Dir(Arc::clone(&self.0)))
                }
            }

            let attr = child.get_attr();

            guard.children.insert(new_name.to_os_string(), child);

            return Ok(attr);
        }

        let mut to_guard = self.0.write().unwrap();
        let mut from_guard = from.0.write().unwrap();

        if from_guard.children.get(old_name).is_none() {
            return Err(libc::ENOENT);
        }

        if to_guard.children.get(new_name).is_some() {
            return Err(libc::EEXIST);
        }

        let child = from_guard.children.remove(old_name).unwrap();

        match &child {
            Entry::Dir(dir) => {
                dir.set_new_parent(Dir(Arc::clone(&self.0)))
            }
            Entry::File(file) => {
                file.set_new_parent(Dir(Arc::clone(&self.0)))
            }
        }

        let attr = child.get_attr();

        to_guard.children.insert(new_name.to_os_string(), child);

        Ok(attr)
    }

    pub fn set_new_parent(&self, new_parent: Dir) {
        self.0.write().unwrap().parent = Some(new_parent)
    }

    pub fn get_inode(&self) -> Inode {
        self.0.read().unwrap().inode
    }

    pub fn get_name(&self) -> OsString {
        self.0.read().unwrap().name.to_os_string()
    }
}

impl Clone for Dir {
    fn clone(&self) -> Self {
        Dir(Arc::clone(&self.0))
    }
}

impl From<Dir> for Entry {
    fn from(dir: Dir) -> Self {
        Entry::Dir(dir)
    }
}

impl From<Arc<RwLock<InnerDir>>> for Dir {
    fn from(dir: Arc<RwLock<InnerDir>>) -> Self {
        Dir(dir)
    }
}