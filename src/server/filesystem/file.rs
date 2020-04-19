use std::cell::RefCell;
use std::ffi::{OsStr, OsString};
use std::ops::Deref;
use std::os::raw::c_int;
use std::os::unix::fs::PermissionsExt;
use std::rc::Rc;

use async_std::fs;
use fuse::FileAttr;
use log::{debug, error};

use crate::{Apply, Result};

use super::attr::metadata_to_file_attr;
use super::dir::Dir;
use super::file_handle::FileHandleKind;
use super::inode::Inode;
use super::FileHandle;
use super::SetAttr;

#[derive(Debug, Clone)]
pub struct File(Rc<RefCell<InnerFile>>);

#[derive(Debug)]
struct InnerFile {
    parent: Dir,
    inode: Inode,
    name: OsString,
}

impl File {
    pub fn from_exist(parent: &Dir, name: &OsStr, inode: Inode) -> Self {
        Self(Rc::new(RefCell::new(InnerFile {
            parent: parent.clone(),
            inode,
            name: name.to_os_string(),
        })))
    }

    pub fn get_inode(&self) -> Inode {
        self.0.borrow().inode
    }

    pub async fn open(&self, flags: u32, file_handle_id_gen: &mut u64) -> Result<FileHandle> {
        let inner = self.0.borrow();

        let path = inner
            .parent
            .get_absolute_path()
            .apply(|path| path.push(&inner.name));

        debug!("open {:?} flags {}", path, flags);

        let mut options = fs::OpenOptions::new();

        let flags = flags as c_int;

        let fh_kind = if flags & libc::O_RDWR > 0 {
            options.write(true);
            options.read(true);

            FileHandleKind::ReadWrite
        } else if flags & libc::O_WRONLY > 0 {
            options.write(true);
            options.read(false);

            FileHandleKind::WriteOnly
        } else {
            options.write(false);
            options.read(true);

            FileHandleKind::ReadOnly
        };

        if flags & libc::O_TRUNC > 0 {
            debug!("open {:?} flags have truncate", path);

            options.truncate(true);
        }

        let sys_file = options.open(path).await?;

        *file_handle_id_gen += 1;
        let file_handle_id = *file_handle_id_gen.deref();

        Ok(FileHandle::new(
            file_handle_id,
            #[cfg(features = "test")]
            inode,
            sys_file,
            fh_kind,
        ))
    }

    pub async fn get_attr(&self) -> Result<FileAttr> {
        let inner = self.0.borrow();

        let path = inner
            .parent
            .get_absolute_path()
            .apply(|path| path.push(&inner.name));

        metadata_to_file_attr(inner.inode, fs::metadata(path).await?)
    }

    pub async fn set_attr(&self, set_attr: SetAttr) -> Result<FileAttr> {
        let inner = self.0.borrow();

        let path = inner
            .parent
            .get_absolute_path()
            .apply(|path| path.push(&inner.name));

        let metadata = fs::metadata(&path).await?;

        if let Some(mode) = set_attr.mode {
            let mut permissions = metadata.permissions();

            permissions.set_mode(mode);

            fs::set_permissions(&path, permissions).await?;
        }

        if let Some(size) = set_attr.size {
            let truncate = size == 0;

            let file = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .truncate(truncate)
                .open(&path)
                .await?;

            if size > 0 {
                debug!("set attr size {}", size);

                if let Err(err) = file.set_len(size).await {
                    error!("set inode {} size {} failed", inner.inode, size);

                    return Err(err.into());
                }
            }
        }

        self.get_attr().await
    }

    pub fn set_new_parent(&mut self, new_parent: &Dir) {
        self.0.borrow_mut().parent = new_parent.clone();
    }

    pub fn set_new_name(&mut self, new_name: &OsStr) {
        self.0.borrow_mut().name = new_name.to_os_string();
    }
}
