use std::collections::BTreeMap;
use std::io::SeekFrom;
use std::ops::Deref;
use std::os::unix::fs::MetadataExt;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::io::AsRawFd;
use std::time::{Duration, UNIX_EPOCH};

use async_std::fs::File as SysFile;
use async_std::prelude::*;
use async_std::sync;
use async_std::sync::{Arc, Mutex, RwLock, Sender};
use async_std::task;
use async_std::task::JoinHandle;
use fuse::{FileAttr, FileType};
use futures::future::FutureExt;
use futures::select;
use log::debug;
use nix::fcntl;
use nix::fcntl::FlockArg;

use crate::errno::Errno;
use crate::Result;

use super::attr::SetAttr;
use super::inode::Inode;

pub type LockTable = Arc<Mutex<BTreeMap<u64, Sender<()>>>>;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum FileHandleKind {
    ReadOnly,
    WriteOnly,
    ReadWrite,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum LockKind {
    NoLock,
    Share,
    Exclusive,
}

pub struct FileHandle {
    id: u64,
    inode: Inode,
    sys_file: SysFile,

    // avoid useless read/write syscall to improve performance
    kind: FileHandleKind,

    lock_queue: Option<LockTable>,

    /// record file handle is locked or not
    lock_kind: Arc<RwLock<LockKind>>,
}

impl FileHandle {
    pub fn new(id: u64, inode: Inode, sys_file: SysFile, kind: FileHandleKind) -> Self {
        Self {
            id,
            inode,
            sys_file,
            kind,
            lock_queue: None,
            lock_kind: Arc::new(RwLock::new(LockKind::NoLock)),
        }
    }

    pub async fn read(&mut self, buf: &mut [u8], offset: i64) -> Result<usize> {
        if let FileHandleKind::WriteOnly = self.kind {
            return Err(Errno::from(libc::EBADF));
        }

        let seek_from = if offset < 0 {
            SeekFrom::End(offset)
        } else {
            SeekFrom::Start(offset as u64)
        };

        self.sys_file.seek(seek_from).await?;

        let n = self.sys_file.read(buf).await?;

        Ok(n)
    }

    pub async fn write(&mut self, data: &[u8], offset: i64) -> Result<usize> {
        if let FileHandleKind::ReadOnly = self.kind {
            return Err(Errno::from(libc::EBADF));
        }

        let seek_from = if offset < 0 {
            SeekFrom::End(offset)
        } else {
            SeekFrom::Start(offset as u64)
        };

        self.sys_file.seek(seek_from).await?;

        self.sys_file.write_all(data).await?;

        Ok(data.len())
    }

    pub async fn get_attr(&self) -> Result<FileAttr> {
        let metadata = self.sys_file.metadata().await?;

        Ok(FileAttr {
            ino: self.inode,
            size: metadata.len(),
            blocks: metadata.blocks(),
            kind: FileType::RegularFile,
            atime: metadata.accessed()?,
            mtime: metadata.modified()?,
            ctime: UNIX_EPOCH
                + Duration::new(metadata.ctime() as u64, metadata.ctime_nsec() as u32),
            perm: (metadata.permissions().mode() ^ libc::S_IFREG) as u16,
            uid: metadata.uid(),
            gid: metadata.gid(),
            rdev: metadata.rdev() as u32,
            flags: 0,
            nlink: 0,
        })
    }

    pub async fn set_attr(&mut self, set_attr: SetAttr) -> Result<FileAttr> {
        if let FileHandleKind::ReadOnly = self.kind {
            return Err(Errno::from(libc::EBADF));
        }

        if let Some(mode) = set_attr.mode {
            let mut permissions = self.sys_file.metadata().await?.permissions();

            permissions.set_mode(mode);

            self.sys_file.set_permissions(permissions).await?;
        }

        if let Some(size) = set_attr.size {
            self.sys_file.set_len(size).await?;
        }

        self.get_attr().await
    }

    pub async fn set_lock(
        &mut self,
        unique: u64,
        share: bool,
        lock_table: LockTable,
    ) -> Result<JoinHandle<bool>> {
        let raw_fd = self.sys_file.as_raw_fd();

        let flock_arg = if share {
            FlockArg::LockShared
        } else {
            FlockArg::LockExclusive
        };

        if self.lock_queue.is_none() {
            self.lock_queue.replace(lock_table.clone());
        }

        let lock_kind = self.lock_kind.clone();

        let (sender, receiver) = sync::channel(1);

        // save lock canceler at first, ensure when return JoinHandle, lock canceler is usable
        lock_table.lock().await.insert(unique, sender);

        debug!("save unique {} lock canceler", unique);

        Ok(task::spawn(async move {
            let lock_job = async_std::task::spawn_blocking(move || fcntl::flock(raw_fd, flock_arg));

            let lock_success = select! {
                _ = receiver.recv().fuse() => false,
                _ = lock_job.fuse() => true,
            };

            if lock_success {
                let mut lock_kind = lock_kind.write().await;

                *lock_kind = if share {
                    LockKind::Share
                } else {
                    LockKind::Exclusive
                };
            }

            lock_table.lock().await.remove(&unique);

            lock_success
        }))
    }

    pub async fn try_set_lock(&self, share: bool) -> Result<()> {
        let raw_fd = self.sys_file.as_raw_fd();

        let flock_arg = if share {
            FlockArg::LockSharedNonblock
        } else {
            FlockArg::LockExclusiveNonblock
        };

        fcntl::flock(raw_fd, flock_arg)?;

        let mut lock_kind = self.lock_kind.write().await;

        *lock_kind = if share {
            LockKind::Share
        } else {
            LockKind::Exclusive
        };

        Ok(())
    }

    pub async fn release_lock(&self) -> Result<()> {
        let raw_fd = self.sys_file.as_raw_fd();

        async_std::task::spawn_blocking(move || fcntl::flock(raw_fd, FlockArg::Unlock)).await?;

        let mut lock_kind = self.lock_kind.write().await;

        *lock_kind = LockKind::NoLock;

        Ok(())
    }

    pub async fn try_release_lock(&self) -> Result<()> {
        let raw_fd = self.sys_file.as_raw_fd();

        fcntl::flock(raw_fd, FlockArg::UnlockNonblock)?;

        let mut lock_kind = self.lock_kind.write().await;

        *lock_kind = LockKind::NoLock;

        Ok(())
    }

    // flush should release all lock
    pub async fn flush(&mut self) -> Result<()> {
        self.sys_file.flush().await?;

        if let Some(lock_queue) = self.lock_queue.as_ref() {
            for (_, lock_canceler) in lock_queue.lock().await.iter() {
                lock_canceler.send(()).await;
            }
        }

        let mut lock_kind = self.lock_kind.write().await;

        *lock_kind = LockKind::NoLock;

        Ok(())
    }

    pub async fn fsync(&mut self, only_data_sync: bool) -> Result<()> {
        if only_data_sync {
            self.sys_file.sync_data().await?;
        } else {
            self.sys_file.sync_all().await?;
        }

        Ok(())
    }

    pub fn get_id(&self) -> u64 {
        self.id
    }

    pub fn get_file_handle_kind(&self) -> FileHandleKind {
        self.kind
    }

    #[inline]
    pub async fn get_lock_kind(&self) -> LockKind {
        self.lock_kind.read().await.deref().clone()
    }
}

impl Drop for FileHandle {
    fn drop(&mut self) {
        task::block_on(async {
            let _ = self.flush().await;
        })
    }
}
