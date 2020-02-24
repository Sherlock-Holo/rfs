use std::collections::BTreeMap;

use async_std::sync::{Arc, Mutex, RwLock, Sender};
use async_std::task::JoinHandle;
use chrono::prelude::*;
use fuse::FileAttr;
use futures::future::FutureExt;
use futures::select;
use uuid::Uuid;

use crate::errno::Errno;
use crate::Result;

use super::super::filesystem::FileHandle;
use super::super::filesystem::LockKind;
use super::super::filesystem::SetAttr;

struct InnerUser {
    uuid: Uuid,
    file_handle_map: BTreeMap<u64, Arc<Mutex<FileHandle>>>,
    last_alive_time: DateTime<Local>,
    lock_queue: Arc<Mutex<BTreeMap<u64, Sender<()>>>>,
}

pub struct User(RwLock<InnerUser>);

// TODO fix may block user lock
impl User {
    pub fn new(uuid: Uuid) -> Self {
        Self(RwLock::new(InnerUser {
            uuid,
            file_handle_map: BTreeMap::new(),
            last_alive_time: Local::now(),
            lock_queue: Arc::new(Mutex::new(BTreeMap::new())),
        }))
    }

    #[inline]
    pub async fn update_last_alive_time(&self, now: DateTime<Local>) {
        self.0.write().await.last_alive_time = now;
    }

    #[inline]
    pub async fn add_file_handle(&self, file_handle: FileHandle) {
        self.0
            .write()
            .await
            .file_handle_map
            .insert(file_handle.get_id(), Arc::new(Mutex::new(file_handle)));
    }

    pub async fn read_file(&self, fh_id: u64, offset: i64, size: u64) -> Result<Vec<u8>> {
        if let Some(file_handle) = self.0.read().await.file_handle_map.get(&fh_id) {
            let mut buf = vec![0; size as usize - offset as usize];

            let n = file_handle.lock().await.read(&mut buf, offset).await?;

            buf.truncate(n);

            Ok(buf)
        } else {
            Err(Errno::from(libc::EBADF))
        }
    }

    pub async fn write_file(&self, fh_id: u64, offset: i64, data: &[u8]) -> Result<usize> {
        if let Some(file_handle) = self.0.read().await.file_handle_map.get(&fh_id) {
            file_handle.lock().await.write(data, offset).await
        } else {
            Err(Errno::from(libc::EBADF))
        }
    }

    pub async fn close_file(&self, fh_id: u64) -> Result<()> {
        let mut guard = self.0.write().await;

        if let Some(file_handle) = guard.file_handle_map.remove(&fh_id) {
            // before close file handle, flush data which may still in kernel buffer
            file_handle.lock().await.flush().await
        } else {
            Err(Errno::from(libc::EBADF))
        }
    }

    pub async fn sync_file(&self, fh_id: u64) -> Result<()> {
        if let Some(file_handle) = self.0.read().await.file_handle_map.get(&fh_id) {
            file_handle.lock().await.fsync(false).await
        } else {
            Err(Errno::from(libc::EBADF))
        }
    }

    pub async fn flush(&self, fh_id: u64) -> Result<()> {
        if let Some(file_handle) = self.0.read().await.file_handle_map.get(&fh_id) {
            file_handle.lock().await.flush().await
        } else {
            Err(Errno::from(libc::EBADF))
        }
    }

    pub async fn set_file_attr(&self, fh_id: u64, set_attr: SetAttr) -> Result<FileAttr> {
        if let Some(file_handle) = self.0.read().await.file_handle_map.get(&fh_id) {
            file_handle.lock().await.set_attr(set_attr).await
        } else {
            Err(Errno::from(libc::EBADF))
        }
    }

    pub async fn set_lock(
        self: &Arc<Self>,
        fh_id: u64,
        unique: u64,
        share: bool,
    ) -> Result<JoinHandle<bool>> {
        let guard = self.0.read().await;

        let file_handle = guard
            .file_handle_map
            .get(&fh_id)
            .ok_or(Errno::from(libc::EBADF))?;

        let lock_job = file_handle
            .lock()
            .await
            .set_lock(unique, share, Arc::clone(&guard.lock_queue))
            .await?;

        Ok(lock_job)
    }

    pub async fn try_set_lock(&self, fh_id: u64, share: bool) -> Result<()> {
        let guard = self.0.read().await;

        let file_handle = guard
            .file_handle_map
            .get(&fh_id)
            .ok_or(Errno::from(libc::EBADF))?;

        let file_handle = select! {
            file_handle = file_handle.lock().fuse() => file_handle,
            default => return Err(Errno::from(libc::EWOULDBLOCK)),
        };

        file_handle.try_set_lock(share).await
    }

    pub async fn release_lock(&self, fh_id: u64) -> Result<()> {
        let guard = self.0.read().await;

        let file_handle = guard
            .file_handle_map
            .get(&fh_id)
            .ok_or(Errno::from(libc::EBADF))?;

        file_handle.lock().await.try_release_lock().await?;

        Ok(())
    }

    #[inline]
    pub async fn interrupt_lock(&self, unique: u64) -> Result<()> {
        self.0
            .read()
            .await
            .lock_queue
            .lock()
            .await
            .get(&unique)
            .ok_or(Errno::from(libc::EBADF))?
            .send(())
            .await;

        Ok(())
    }

    #[inline]
    pub async fn get_lock_kind(&self, fh_id: u64) -> Result<LockKind> {
        Ok(self
            .0
            .read()
            .await
            .file_handle_map
            .get(&fh_id)
            .ok_or(Errno::from(libc::EBADF))?
            .lock()
            .await
            .get_lock_kind()
            .await)
    }
}
