use std::collections::BTreeMap;
use std::future::Future;
use std::io::SeekFrom;
use std::os::raw::c_int;
#[cfg(test)]
use std::os::unix::fs::MetadataExt;
#[cfg(test)]
use std::os::unix::fs::PermissionsExt;
use std::os::unix::io::AsRawFd;
use std::os::unix::io::RawFd;
use std::sync::Arc;
#[cfg(test)]
use std::time::{Duration, UNIX_EPOCH};

use async_notify::Notify;
#[cfg(test)]
use fuse3::{path::reply::FileAttr, FileType};
use fuse3::{Errno, Result};
use futures_util::future::FutureExt;
use nix::fcntl;
use nix::fcntl::{FallocateFlags, FlockArg};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::{Mutex, RwLock};
use tokio::task;
use tracing::{debug, error, instrument};

#[cfg(test)]
use crate::BLOCK_SIZE;

pub type LockTable = Arc<Mutex<BTreeMap<u64, Arc<Notify>>>>;

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

#[derive(Debug)]
pub struct FileHandle {
    id: u64,

    sys_file: File,

    // avoid useless read/write syscall to improve performance
    kind: FileHandleKind,

    lock_table: Option<LockTable>,

    /// record file handle is locked or not
    lock_kind: Arc<RwLock<LockKind>>,
}

impl FileHandle {
    pub fn new(id: u64, sys_file: File, kind: FileHandleKind) -> Self {
        Self {
            id,
            sys_file,
            kind,
            lock_table: None,
            lock_kind: Arc::new(RwLock::new(LockKind::NoLock)),
        }
    }

    #[instrument(skip(buf))]
    pub async fn read(&mut self, mut buf: &mut [u8], offset: i64) -> Result<usize> {
        if let FileHandleKind::WriteOnly = self.kind {
            return Err(Errno::from(libc::EBADF));
        }

        let seek_from = if offset < 0 {
            SeekFrom::End(offset)
        } else {
            SeekFrom::Start(offset as u64)
        };

        self.sys_file.seek(seek_from).await?;

        let mut read = 0;

        while !buf.is_empty() {
            let n = self.sys_file.read(buf).await?;
            if n == 0 {
                break;
            }

            read += n;
            buf = &mut buf[n..];
        }

        Ok(read)
    }

    #[instrument(skip(data))]
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

    #[cfg(test)]
    pub async fn get_attr(&self) -> Result<FileAttr> {
        let metadata = self.sys_file.metadata().await?;

        Ok(FileAttr {
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
            nlink: 0,
            blksize: BLOCK_SIZE,
        })
    }

    /*pub async fn set_attr(&mut self, set_attr: SetAttr) -> Result<FileAttr> {
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
    }*/

    #[instrument]
    pub async fn set_lock(
        &mut self,
        unique: u64,
        share: bool,
        lock_table: LockTable,
    ) -> Result<impl Future<Output = Result<bool>>> {
        let raw_fd = self.sys_file.as_raw_fd();

        let flock_arg = if share {
            FlockArg::LockShared
        } else {
            FlockArg::LockExclusive
        };

        if self.lock_table.is_none() {
            self.lock_table.replace(lock_table.clone());
        }

        let lock_kind = self.lock_kind.clone();

        let lock_canceler = Arc::new(Notify::new());

        // save lock canceler at first, ensure when return JoinHandle, lock canceler is usable
        lock_table
            .lock()
            .await
            .insert(unique, lock_canceler.clone());

        debug!("save unique {} lock canceler", unique);

        Ok(async move {
            tokio::spawn(async move {
                let lock_success = loop {
                    let lock_job = async move {
                        task::spawn_blocking(move || fcntl::flock(raw_fd, flock_arg))
                            .await
                            .unwrap()
                    };

                    let result = futures_util::select! {
                        _ = lock_canceler.notified().fuse() => break false,
                        result = lock_job.fuse() => result,
                    };

                    if let Err(err) = result {
                        error!("nix lock failed, error is {}", err);

                        if err as c_int == libc::EINTR {
                            debug!("nix lock is interrupted, retry it");

                            continue;
                        } else {
                            return Err(Errno::from(err));
                        }
                    } else {
                        break true;
                    }
                };

                if lock_success {
                    let mut lock_kind = lock_kind.write().await;

                    *lock_kind = if share {
                        LockKind::Share
                    } else {
                        LockKind::Exclusive
                    };

                    debug!("unique {} set lock success", unique);
                }

                lock_table.lock().await.remove(&unique);

                Ok(lock_success)
            })
            .await
            .unwrap()
        })
    }

    #[instrument]
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

    /*pub async fn release_lock(&self) -> Result<()> {
        let raw_fd = self.sys_file.as_raw_fd();

        async_std::task::spawn_blocking(move || fcntl::flock(raw_fd, FlockArg::Unlock)).await?;

        let mut lock_kind = self.lock_kind.write().await;

        *lock_kind = LockKind::NoLock;

        Ok(())
    }*/

    #[instrument]
    pub async fn release_lock(&self) -> Result<()> {
        let raw_fd = self.sys_file.as_raw_fd();

        fcntl::flock(raw_fd, FlockArg::UnlockNonblock)?;

        let mut lock_kind = self.lock_kind.write().await;

        *lock_kind = LockKind::NoLock;

        Ok(())
    }

    // flush should release all lock
    #[instrument]
    pub async fn flush(&mut self) -> Result<()> {
        self.sys_file.flush().await?;

        self.release_lock().await?;

        Ok(())
    }

    #[instrument]
    pub async fn fsync(&mut self, only_data_sync: bool) -> Result<()> {
        if only_data_sync {
            self.sys_file.sync_data().await?;
        } else {
            self.sys_file.sync_all().await?;
        }

        Ok(())
    }

    #[instrument]
    pub async fn fallocate(&mut self, offset: u64, size: u64, mode: u32) -> Result<()> {
        let fd = self.sys_file.as_raw_fd();

        let mut fallocate_flags: FallocateFlags = FallocateFlags::empty();

        let mode = mode as c_int;

        if libc::FALLOC_FL_KEEP_SIZE & mode > 0 {
            fallocate_flags |= FallocateFlags::FALLOC_FL_KEEP_SIZE;
        }

        if libc::FALLOC_FL_PUNCH_HOLE & mode > 0 {
            fallocate_flags |= FallocateFlags::FALLOC_FL_PUNCH_HOLE;
        }

        if libc::FALLOC_FL_COLLAPSE_RANGE & mode > 0 {
            fallocate_flags |= FallocateFlags::FALLOC_FL_COLLAPSE_RANGE;
        }

        if libc::FALLOC_FL_ZERO_RANGE & mode > 0 {
            fallocate_flags |= FallocateFlags::FALLOC_FL_ZERO_RANGE;
        }

        if libc::FALLOC_FL_INSERT_RANGE & mode > 0 {
            fallocate_flags |= FallocateFlags::FALLOC_FL_INSERT_RANGE;
        }

        if libc::FALLOC_FL_UNSHARE_RANGE & mode > 0 {
            fallocate_flags |= FallocateFlags::FALLOC_FL_UNSHARE_RANGE;
        }

        task::spawn_blocking(move || {
            fcntl::fallocate(fd, fallocate_flags, offset as _, size as _)?;

            Ok(())
        })
        .await
        .unwrap()
    }

    #[instrument]
    pub async fn copy_to(
        &self,
        offset_in: u64,
        offset_out: u64,
        size: usize,
        fh: Option<&Self>,
    ) -> Result<usize> {
        let fd_in = self.as_raw_fd();

        let fd_out = if let Some(fh) = fh {
            fh.as_raw_fd()
        } else {
            fd_in
        };

        let size = task::spawn_blocking(move || {
            let mut offset_in = offset_in as i64;
            let mut offset_out = offset_out as i64;

            fcntl::copy_file_range(
                fd_in,
                Some(&mut offset_in),
                fd_out,
                Some(&mut offset_out),
                size,
            )
        })
        .await
        .unwrap()?;

        Ok(size)
    }

    pub fn get_id(&self) -> u64 {
        self.id
    }

    #[cfg(test)]
    pub fn get_file_handle_kind(&self) -> FileHandleKind {
        self.kind
    }

    pub async fn get_lock_kind(&self) -> LockKind {
        *self.lock_kind.read().await
    }
}

impl AsRawFd for FileHandle {
    fn as_raw_fd(&self) -> RawFd {
        self.sys_file.as_raw_fd()
    }
}

// drop not grantee will flush
/*impl Drop for FileHandle {
    fn drop(&mut self) {
        smol::block_on(async {
            let _ = self.flush().await;
        })
    }
}*/
