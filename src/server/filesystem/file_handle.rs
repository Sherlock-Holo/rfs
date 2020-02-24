use std::collections::BTreeMap;
use std::io::SeekFrom;
use std::os::unix::fs::MetadataExt;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::io::AsRawFd;
use std::time::{Duration, UNIX_EPOCH};

use async_std::fs::File as SysFile;
use async_std::prelude::*;
use async_std::sync;
use async_std::sync::{Arc, Mutex, Sender};
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

type LockQueue = Arc<Mutex<BTreeMap<u64, Sender<()>>>>;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum FileHandleKind {
    ReadOnly,
    WriteOnly,
    ReadWrite,
}

pub struct FileHandle {
    id: u64,
    inode: Inode,
    sys_file: SysFile,

    // avoid useless read/write syscall to improve performance
    kind: FileHandleKind,

    lock_queue: Option<LockQueue>,
}

impl FileHandle {
    pub fn new(id: u64, inode: Inode, sys_file: SysFile, kind: FileHandleKind) -> Self {
        Self {
            id,
            inode,
            sys_file,
            kind,
            lock_queue: None,
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
        lock_queue: LockQueue,
    ) -> Result<JoinHandle<bool>> {
        let raw_fd = self.sys_file.as_raw_fd();

        let flock_arg = if share {
            FlockArg::LockShared
        } else {
            FlockArg::LockExclusive
        };

        if self.lock_queue.is_none() {
            self.lock_queue.replace(Arc::clone(&lock_queue));
        }

        Ok(task::spawn(async move {
            let (sender, receiver) = sync::channel(1);

            lock_queue.lock().await.insert(unique, sender);

            debug!("save unique {} lock canceler", unique);

            let lock_job = async_std::task::spawn_blocking(move || fcntl::flock(raw_fd, flock_arg));

            let lock_success = select! {
                _ = receiver.recv().fuse() => false,
                _ = lock_job.fuse() => true,
            };

            lock_queue.lock().await.remove(&unique);

            lock_success
        }))
    }

    pub fn try_set_lock(&self, share: bool) -> Result<()> {
        let raw_fd = self.sys_file.as_raw_fd();

        let flock_arg = if share {
            FlockArg::LockSharedNonblock
        } else {
            FlockArg::LockExclusiveNonblock
        };

        fcntl::flock(raw_fd, flock_arg)?;

        Ok(())
    }

    pub async fn release_lock(&self) -> Result<()> {
        let raw_fd = self.sys_file.as_raw_fd();

        async_std::task::spawn_blocking(move || fcntl::flock(raw_fd, FlockArg::Unlock)).await?;

        Ok(())
    }

    pub fn try_release_lock(&self) -> Result<()> {
        let raw_fd = self.sys_file.as_raw_fd();

        fcntl::flock(raw_fd, FlockArg::UnlockNonblock)?;

        Ok(())
    }

    //TODO should I remove this method?
    pub async fn interrupt_lock(&self, unique: u64) {
        debug!("try to cancel unique {} lock", unique);

        if let Some(lock_queue) = self.lock_queue.as_ref() {
            if let Some(lock_canceler) = lock_queue.lock().await.get(&unique) {
                debug!("unique {} lock canceler found", unique);

                lock_canceler.send(()).await;

                debug!("lock cancel");
            } else {
                debug!("unique {} lock canceler not found", unique);
            }
        }
    }

    // flush should release all lock
    pub async fn flush(&mut self) -> Result<()> {
        self.sys_file.flush().await?;

        if let Some(lock_queue) = self.lock_queue.as_ref() {
            for (_, lock_canceler) in lock_queue.lock().await.iter() {
                lock_canceler.send(()).await;
            }
        }

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
}

impl Drop for FileHandle {
    fn drop(&mut self) {
        task::block_on(async {
            let _ = self.flush().await;
        })
    }
}
