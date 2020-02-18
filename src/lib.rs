use errno::Errno;

mod server;
mod errno;
mod path;
mod helper;

pub type Result<T> = std::result::Result<T, Errno>;

// use std::collections::HashMap;
// use std::ffi::{OsStr, OsString};
// use std::sync::{Arc, RwLock};
// use std::sync::atomic::{AtomicU64, Ordering};
//
// use fuse::{Filesystem as FuseFilesystem, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite, Request};
// use log::LevelFilter;
// use time_old::Timespec;
//
// use crate::dir::Dir;
// use crate::entry::Entry;
// use crate::file_handle::FileHandler;
// use crate::helper::*;
// use crate::inode::{Inode, InodeMap};
//
// mod helper;
// mod entry;
// mod inode;
// mod file_handle;
// mod dir;
// mod file;
//
// pub struct Filesystem {
//     inode_generator: AtomicU64,
//     file_handler_id_generator: AtomicU64,
//     inode_map: InodeMap,
//     file_handler_map: Arc<RwLock<HashMap<u64, FileHandler>>>,
//     root: Dir,
// }
//
// impl Filesystem {
//     pub fn new(uid: u32, gid: u32) -> Self {
//         log::info!("uid: {} gid: {}",uid,gid);
//
//         Filesystem {
//             inode_generator: AtomicU64::new(2),
//             file_handler_id_generator: AtomicU64::new(1),
//             inode_map: InodeMap::new(),
//             file_handler_map: Arc::new(RwLock::new(HashMap::new())),
//             root: Dir::new(1, OsString::from("/"), None, 0o755, uid, gid),
//         }.apply(|filesystem| {
//             filesystem.inode_map.add_entry(1, filesystem.root.clone().into());
//         })
//     }
// }
//
// impl FuseFilesystem for Filesystem {
//     fn lookup(&mut self, _req: &Request, parent: Inode, name: &OsStr, reply: ReplyEntry) {
//         let parent = match self.inode_map.get_entry(parent) {
//             None => return reply.error(libc::ENOENT),
//             Some(parent) => if let Entry::Dir(dir) = parent {
//                 dir
//             } else {
//                 return reply.error(libc::ENOTDIR);
//             }
//         };
//
//         if let Some(entry) = parent.get_child(name) {
//             log::debug!("lookup result {:?}",entry.get_attr());
//
//             reply.entry(&Timespec::new(0, 0), &entry.get_attr(), 0);
//             return;
//         }
//
//         reply.error(libc::ENOENT);
//     }
//
//     fn getattr(&mut self, _req: &Request, inode: Inode, reply: ReplyAttr) {
//         let entry = match self.inode_map.get_entry(inode) {
//             None => return reply.error(libc::ENOENT),
//             Some(entry) => entry
//         };
//
//         reply.attr(&Timespec::new(0, 0), &entry.get_attr())
//     }
//
//     fn setattr(
//         &mut self,
//         _req: &Request,
//         inode: u64,
//         mode: Option<u32>,
//         uid: Option<u32>,
//         gid: Option<u32>,
//         size: Option<u64>,
//         atime: Option<Timespec>,
//         mtime: Option<Timespec>,
//         fh: Option<u64>,
//         crtime: Option<Timespec>,
//         chgtime: Option<Timespec>,
//         kuptime: Option<Timespec>,
//         flags: Option<u32>,
//         reply: ReplyAttr,
//     ) {
//         let entry = match self.inode_map.get_entry(inode) {
//             None => return reply.error(libc::ENOENT),
//             Some(entry) => entry
//         };
//
//         if let Err(err) = entry.set_attr(
//             mode,
//             uid,
//             gid,
//             size,
//             atime,
//             mtime,
//             fh,
//             crtime,
//             chgtime,
//             kuptime,
//             flags,
//         ) {
//             return reply.error(err);
//         }
//
//         reply.attr(&Timespec::new(0, 0), &entry.get_attr())
//     }
//
//     fn mkdir(&mut self, req: &Request, parent: u64, name: &OsStr, mode: u32, reply: ReplyEntry) {
//         let parent = match self.inode_map.get_entry(parent) {
//             None => return reply.error(libc::ENOENT),
//             Some(parent) => if let Entry::Dir(dir) = parent {
//                 dir
//             } else {
//                 return reply.error(libc::ENOTDIR);
//             }
//         };
//
//         let new_inode = self.inode_generator.fetch_add(1, Ordering::Relaxed);
//
//         match parent.mkdir(new_inode, name, mode, req.uid(), req.gid()) {
//             Err(err) => return reply.error(err),
//             Ok(dir) => {
//                 let attr = dir.get_attr();
//
//                 self.inode_map.add_entry(new_inode, dir.into());
//
//                 reply.entry(&Timespec::new(0, 0), &attr, 0)
//             }
//         }
//     }
//
//     fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
//         let parent = match self.inode_map.get_entry(parent) {
//             None => return reply.error(libc::ENOENT),
//             Some(parent) => if let Entry::Dir(dir) = parent {
//                 dir
//             } else {
//                 return reply.error(libc::ENOTDIR);
//             }
//         };
//
//         match parent.unlink(name) {
//             Err(err) => reply.error(err),
//             Ok(file) => {
//                 self.inode_map.remove_entry(file.get_inode());
//
//                 reply.ok()
//             }
//         }
//     }
//
//     fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
//         let parent = match self.inode_map.get_entry(parent) {
//             None => return reply.error(libc::ENOENT),
//             Some(parent) => if let Entry::Dir(dir) = parent {
//                 dir
//             } else {
//                 return reply.error(libc::ENOTDIR);
//             }
//         };
//
//         match parent.rmdir(name) {
//             Err(err) => reply.error(err),
//             Ok(dir) => {
//                 self.inode_map.remove_entry(dir.get_inode());
//
//                 reply.ok()
//             }
//         }
//     }
//
//     fn rename(&mut self, _req: &Request, parent: u64, name: &OsStr, new_parent: u64, new_name: &OsStr, reply: ReplyEmpty) {
//         let old_parent = match self.inode_map.get_entry(parent) {
//             None => return reply.error(libc::ENOENT),
//             Some(parent) => if let Entry::Dir(dir) = parent {
//                 dir
//             } else {
//                 return reply.error(libc::ENOTDIR);
//             }
//         };
//
//         let new_parent = match self.inode_map.get_entry(new_parent) {
//             None => return reply.error(libc::ENOENT),
//             Some(parent) => if let Entry::Dir(dir) = parent {
//                 dir
//             } else {
//                 return reply.error(libc::ENOTDIR);
//             }
//         };
//
//         match new_parent.rename_child_from(&old_parent, name, new_name) {
//             Err(err) => reply.error(err),
//             Ok(_) => reply.ok()
//         }
//     }
//
//     fn open(&mut self, _req: &Request, inode: u64, flags: u32, reply: ReplyOpen) {
//         let file = match self.inode_map.get_entry(inode) {
//             None => return reply.error(libc::ENOENT),
//             Some(entry) => if let Entry::File(file) = entry {
//                 file
//             } else {
//                 return reply.error(libc::EISDIR);
//             }
//         };
//
//         let fh_id = self.file_handler_id_generator.fetch_add(1, Ordering::Relaxed);
//
//         let file_handler = file.open(fh_id, flags);
//
//         self.file_handler_map.write().unwrap().insert(fh_id, file_handler);
//
//         reply.opened(fh_id, flags)
//     }
//
//     fn read(&mut self, _req: &Request, inode: u64, fh: u64, offset: i64, size: u32, reply: ReplyData) {
//         let file_handler_map_guard = self.file_handler_map.read().unwrap();
//
//         let file_handler = match file_handler_map_guard.get(&fh) {
//             None => return reply.error(libc::EBADF),
//             Some(file_handler) => file_handler
//         };
//
//         let data = match file_handler.read(offset, size) {
//             Err(err) => return reply.error(err),
//             Ok(data) => data
//         };
//
//         log::debug!("read inode: {}, fh id: {}", inode, fh);
//
//         reply.data(&data)
//     }
//
//     fn write(&mut self, _req: &Request, inode: u64, fh: u64, offset: i64, data: &[u8], _flags: u32, reply: ReplyWrite) {
//         let mut file_handler_map_guard = self.file_handler_map.write().unwrap();
//
//         let file_handler = match file_handler_map_guard.get_mut(&fh) {
//             None => return reply.error(libc::EBADF),
//             Some(file_handler) => file_handler
//         };
//
//         match file_handler.write(offset, data) {
//             Err(err) => reply.error(err),
//             Ok(written) => {
//                 log::debug!("write inode: {}, fh id: {}", inode, fh);
//                 reply.written(written as u32)
//             }
//         }
//     }
//
//     fn flush(&mut self, _req: &Request, _ino: u64, fh: u64, _lock_owner: u64, reply: ReplyEmpty) {
//         if self.file_handler_map.read().unwrap().get(&fh).is_none() {
//             reply.error(libc::EBADF);
//             return;
//         }
//
//         reply.ok()
//     }
//
//     fn release(&mut self, _req: &Request, _ino: u64, fh: u64, _flags: u32, _lock_owner: u64, _flush: bool, reply: ReplyEmpty) {
//         if self.file_handler_map.write().unwrap().remove(&fh).is_some() {
//             reply.ok();
//             return;
//         }
//
//         reply.error(libc::EBADF)
//     }
//
//     fn fsync(&mut self, _req: &Request, _ino: u64, fh: u64, _datasync: bool, reply: ReplyEmpty) {
//         if self.file_handler_map.read().unwrap().get(&fh).is_none() {
//             reply.error(libc::EBADF);
//             return;
//         }
//
//         reply.ok()
//     }
//
//     /*fn opendir(&mut self, _req: &Request, ino: u64, _flags: u32, reply: ReplyOpen) {
//
//     }*/
//
//     fn readdir(&mut self, _req: &Request, inode: u64, _fh: u64, offset: i64, mut reply: ReplyDirectory) {
//         let dir = match self.inode_map.get_entry(inode) {
//             None => return reply.error(libc::ENOENT),
//             Some(entry) => if let Entry::Dir(dir) = entry {
//                 dir
//             } else {
//                 return reply.error(libc::ENOTDIR);
//             }
//         };
//
//         for (inode, index, kind, name) in dir.readdir(offset) {
//             if reply.add(inode, index, kind, name) {
//                 break;
//             }
//         }
//
//         reply.ok()
//     }
//
//     /*fn access(&mut self, _req: &Request, inode: u64, mask: u32, reply: ReplyEmpty) {
//         let paths = match self.get_paths_after_split(inode) {
//             Err(err) => return reply.error(err),
//             Ok(paths) => paths
//         };
//
//         log::debug!("access {:?}",paths);
//
//         let attr = match self.root.get_child(&paths) {
//             None => return reply.error(libc::ENOENT),
//             Some(entry) => entry.get_attr()
//         };
//
//         if attr.perm as u32 & mask > 0 {
//             reply.ok()
//         } else {
//             reply.error(libc::EPERM)
//         }
//     }*/
//
//     fn create(&mut self, req: &Request, parent: u64, name: &OsStr, mode: u32, flags: u32, reply: ReplyCreate) {
//         let dir = match self.inode_map.get_entry(parent) {
//             None => return reply.error(libc::ENOENT),
//             Some(entry) => if let Entry::Dir(dir) = entry {
//                 dir
//             } else {
//                 return reply.error(libc::ENOTDIR);
//             }
//         };
//
//         let new_inode = self.inode_generator.fetch_add(1, Ordering::Relaxed);
//
//         match dir.create(new_inode, name, mode, flags, req.uid(), req.gid()) {
//             Err(err) => reply.error(err),
//             Ok(file) => {
//                 self.inode_map.add_entry(new_inode, file.clone().into());
//
//                 let fh_id = self.file_handler_id_generator.fetch_add(1, Ordering::Relaxed);
//                 let file_handler = file.open(fh_id, flags);
//
//                 self.file_handler_map.write().unwrap().insert(fh_id, file_handler);
//
//                 reply.created(&Timespec::new(0, 0), &file.get_attr(), 0, fh_id, flags)
//             }
//         }
//     }
// }
//
// pub fn log_init(debug: bool) {
//     let mut builder = pretty_env_logger::formatted_timed_builder();
//
//     if debug {
//         builder.filter_level(LevelFilter::Trace);
//     } else {
//         builder.filter_level(LevelFilter::Info);
//     }
//
//     builder.init();
// }