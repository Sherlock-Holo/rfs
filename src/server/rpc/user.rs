use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_std::sync::{Mutex, RwLock};
use fuse3::{Errno, Result};
use futures_util::future::FutureExt;
use futures_util::select;
use log::debug;
use smol::Task;
use uuid::Uuid;

use super::super::filesystem::FileHandle;
use super::super::filesystem::LockKind;
use super::super::filesystem::LockTable;

struct InnerUser {
    uuid: Uuid,
    file_handle_map: BTreeMap<u64, Arc<Mutex<FileHandle>>>,
    last_alive_time: Instant,
    lock_table: LockTable,
}

pub struct User {
    inner: RwLock<InnerUser>,
    enable_compress: bool,
}

impl User {
    pub fn new(uuid: Uuid, enable_compress: bool) -> Self {
        Self {
            inner: RwLock::new(InnerUser {
                uuid,
                file_handle_map: BTreeMap::new(),
                last_alive_time: Instant::now(),
                lock_table: Arc::new(Mutex::new(BTreeMap::new())),
            }),
            enable_compress,
        }
    }

    //#[inline]
    pub async fn add_file_handle(&self, file_handle: FileHandle) {
        self.inner
            .write()
            .await
            .file_handle_map
            .insert(file_handle.get_id(), Arc::new(Mutex::new(file_handle)));
    }

    pub async fn read_file(&self, fh_id: u64, offset: i64, size: u64) -> Result<Vec<u8>> {
        let file_handle = self
            .inner
            .read()
            .await
            .file_handle_map
            .get(&fh_id)
            .ok_or_else(|| Errno::from(libc::EBADF))?
            .clone();

        let mut buf = vec![0; size as usize];

        let n = file_handle.lock().await.read(&mut buf, offset).await?;

        buf.truncate(n);

        Ok(buf)
    }

    pub async fn write_file(&self, fh_id: u64, offset: i64, data: &[u8]) -> Result<usize> {
        let file_handle = self
            .inner
            .read()
            .await
            .file_handle_map
            .get(&fh_id)
            .ok_or_else(|| Errno::from(libc::EBADF))?
            .clone();

        let written = file_handle.lock().await.write(data, offset).await?;

        Ok(written)
    }

    pub async fn close_file(&self, fh_id: u64) -> Result<()> {
        let mut guard = self.inner.write().await;

        if let Some(file_handle) = guard.file_handle_map.remove(&fh_id) {
            drop(guard); // release lock as soon as possible

            // before close file handle, flush data which may still in kernel buffer
            file_handle.lock().await.flush().await
        } else {
            Err(Errno::from(libc::EBADF))
        }
    }

    pub async fn sync_file(&self, fh_id: u64) -> Result<()> {
        let file_handle = self
            .inner
            .read()
            .await
            .file_handle_map
            .get(&fh_id)
            .ok_or_else(|| Errno::from(libc::EBADF))?
            .clone();

        file_handle.lock().await.fsync(false).await?;

        Ok(())
    }

    pub async fn flush(&self, fh_id: u64) -> Result<()> {
        let file_handle = self
            .inner
            .read()
            .await
            .file_handle_map
            .get(&fh_id)
            .ok_or_else(|| Errno::from(libc::EBADF))?
            .clone();

        file_handle.lock().await.flush().await?;

        Ok(())
    }

    /*pub async fn set_file_attr(&self, fh_id: u64, set_attr: SetAttr) -> Result<FileAttr> {
        let file_handle = self
            .inner
            .read()
            .await
            .file_handle_map
            .get(&fh_id)
            .ok_or(Errno::from(libc::EBADF))?
            .clone();

        let attr = file_handle.lock().await.set_attr(set_attr).await?;

        Ok(attr)
    }*/

    pub async fn set_lock(
        self: &Arc<Self>,
        fh_id: u64,
        unique: u64,
        share: bool,
    ) -> Result<Task<Result<bool>>> {
        let guard = self.inner.read().await;

        let lock_table = guard.lock_table.clone();

        let file_handle = guard
            .file_handle_map
            .get(&fh_id)
            .ok_or_else(|| Errno::from(libc::EBADF))?
            .clone();

        drop(guard); // release lock as soon as possible

        let lock_job = file_handle
            .lock()
            .await
            .set_lock(unique, share, lock_table)
            .await?;

        Ok(lock_job)
    }

    pub async fn try_set_lock(&self, fh_id: u64, share: bool) -> Result<()> {
        let file_handle = self
            .inner
            .read()
            .await
            .file_handle_map
            .get(&fh_id)
            .ok_or_else(|| Errno::from(libc::EBADF))?
            .clone();

        let file_handle = select! {
            file_handle = file_handle.lock().fuse() => file_handle,
            default => return Err(Errno::from(libc::EWOULDBLOCK)),
        };

        file_handle.try_set_lock(share).await
    }

    pub async fn release_lock(&self, fh_id: u64) -> Result<()> {
        let file_handle = self
            .inner
            .read()
            .await
            .file_handle_map
            .get(&fh_id)
            .ok_or_else(|| Errno::from(libc::EBADF))?
            .clone();

        file_handle.lock().await.try_release_lock().await?;

        Ok(())
    }

    //#[inline]
    pub async fn interrupt_lock(&self, unique: u64) -> Result<()> {
        debug!("interrupt unique {} lock", unique);

        self.inner
            .read()
            .await
            .lock_table
            .lock()
            .await
            .get(&unique)
            .ok_or_else(|| Errno::from(libc::EBADF))?
            .notify();

        Ok(())
    }

    //#[inline]
    pub async fn get_lock_kind(&self, fh_id: u64) -> Result<LockKind> {
        let file_handle = self
            .inner
            .read()
            .await
            .file_handle_map
            .get(&fh_id)
            .ok_or_else(|| Errno::from(libc::EBADF))?
            .clone();

        let lock_kind = file_handle.lock().await.get_lock_kind().await;

        Ok(lock_kind)
    }

    pub async fn fallocate(&self, fh_id: u64, offset: u64, size: u64, mode: u32) -> Result<()> {
        let file_handle = self
            .inner
            .read()
            .await
            .file_handle_map
            .get(&fh_id)
            .ok_or_else(|| Errno::from(libc::EBADF))?
            .clone();

        file_handle
            .lock()
            .await
            .fallocate(offset, size, mode)
            .await?;

        Ok(())
    }

    pub async fn copy_file_range(
        &self,
        fh_in: u64,
        off_in: u64,
        fh_out: u64,
        off_out: u64,
        length: u64,
        _flags: u64,
    ) -> Result<usize> {
        let guard = self.inner.read().await;

        let file_handle_in = guard
            .file_handle_map
            .get(&fh_in)
            .ok_or_else(|| Errno::from(libc::EBADF))?;

        let file_handle_in = file_handle_in.lock().await;

        if fh_in == fh_out {
            file_handle_in
                .copy_to(off_in, off_out, length as _, None)
                .await
        } else {
            let file_handle_out = guard
                .file_handle_map
                .get(&fh_out)
                .ok_or_else(|| Errno::from(libc::EBADF))?;

            let file_handle_out = file_handle_out.lock().await;

            file_handle_in
                .copy_to(off_in, off_out, length as _, Some(&file_handle_out))
                .await
        }
    }

    pub async fn is_online(&self, interval: Duration) -> bool {
        self.inner.read().await.last_alive_time.elapsed() < interval
    }

    pub async fn update_last_alive_time(&self) {
        self.inner.write().await.last_alive_time = Instant::now();
    }

    pub async fn get_id(&self) -> Uuid {
        self.inner.read().await.uuid
    }

    #[inline]
    pub fn support_compress(&self) -> bool {
        self.enable_compress
    }
}
