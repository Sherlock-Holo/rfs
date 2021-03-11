use std::env;
use std::mem;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::io::AsRawFd;
use std::os::unix::io::RawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use fuse3::path::reply::{FileAttr, ReplyStatFs};
use fuse3::{Errno, Result};
use futures_util::{stream, StreamExt, TryStreamExt};
use nix::dir::Dir as NixDir;
use nix::fcntl::OFlag;
use nix::sys::stat::Mode;
use nix::sys::statfs;
use nix::unistd;
use tokio::fs;
use tokio::fs::{DirBuilder, OpenOptions};
use tokio_stream::wrappers::ReadDirStream;
use tracing::{debug, error, info, instrument};

pub use attr::SetAttr;
pub use file_handle::FileHandle;
pub use file_handle::LockKind;
pub use file_handle::LockTable;

use crate::path::PathClean;
use crate::server::filesystem::attr::metadata_to_file_attr;
use crate::server::filesystem::file_handle::FileHandleKind;
use crate::Apply;

mod attr;
mod file_handle;

pub struct Filesystem {
    file_handle_id_gen: AtomicU64,
    root_dir_fd: RawFd,
}

impl Filesystem {
    /// new a Filesystem will enter a new mount namespace, then chroot to root path
    /// ensure won't be affected by uds client fuse mount
    pub async fn new<P: AsRef<Path>>(root: P) -> Result<Self> {
        info!("root is {:?}", root.as_ref());

        let root_dir = NixDir::open(root.as_ref(), OFlag::O_DIRECTORY, Mode::S_IRWXU)?;
        let root_dir_fd = root_dir.as_raw_fd();

        // avoid the root dir fd be closed
        mem::forget(root_dir);

        // pivot_root mode, butI think we don't need use pivot_root, chroot is safe enough
        /*sched::unshare(nix::sched::CloneFlags::CLONE_NEWNS)?;

        let old_root = tempfile::tempdir_in(root.as_ref())?;

        let old_root_path = old_root.path().file_name().expect("old root filename should be valid");

        debug!("old root {:?}", old_root_path);

        // env::set_current_dir(root.as_ref())?;

        mount::mount(Some(""), "/", Some(""), MsFlags::MS_PRIVATE | MsFlags::MS_REC, Some(""))?;

        mount::mount(Some(root.as_ref()), root.as_ref(), Some("bind"), MsFlags::MS_BIND | MsFlags::MS_REC, Some(""))?;

        unistd::pivot_root(root.as_ref(), old_root.path())?;

        info!("pivot root success");

        env::set_current_dir("/")?;

        mount::umount2(old_root_path, MntFlags::MNT_DETACH)?;

        info!("unmount old root success");

        drop(old_root);

        info!("remove old root dir success");

        async_std::fs::read_dir("/")
            .await?
            .for_each(|child| {
                let child = child.unwrap();

                info!("child {:?} in /", child.path());

                futures::future::ready(())
            })
            .await;*/

        Self::chroot(root.as_ref()).await?;

        Ok(Self {
            file_handle_id_gen: AtomicU64::new(1),
            root_dir_fd,
        })
    }

    #[instrument]
    async fn chroot(root: &Path) -> Result<()> {
        unistd::chroot(root.as_os_str())?;

        info!("chroot {:?} success", root);

        env::set_current_dir("/")?;

        Ok(())
    }

    fn get_fh_id(&self) -> u64 {
        self.file_handle_id_gen.fetch_add(1, Ordering::Relaxed)
    }

    #[instrument(skip(self))]
    pub async fn lookup(&self, parent: String, name: &str) -> Result<FileAttr> {
        debug!("lookup name {} in parent {}", name, parent);

        let path = PathBuf::from(parent)
            .apply(|path| path.push(name))
            .clean()?;

        metadata_to_file_attr(fs::metadata(path).await?)
    }

    #[inline]
    #[instrument(skip(self))]
    pub async fn get_attr(&self, path: &str) -> Result<FileAttr> {
        let path: PathBuf = path.clean();

        metadata_to_file_attr(fs::metadata(path).await?)
    }

    #[instrument(skip(self))]
    pub async fn set_attr(&self, path: &str, set_attr: SetAttr) -> Result<FileAttr> {
        let path: PathBuf = path.clean();

        let metadata = fs::metadata(&path).await?;

        if let Some(mode) = set_attr.mode {
            let mut permissions = metadata.permissions();

            permissions.set_mode(mode);

            fs::set_permissions(&path, permissions).await?;
        }

        if let Some(size) = set_attr.size {
            if metadata.is_dir() {
                return Err(Errno::new_is_dir());
            }

            let truncate = size == 0;

            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .truncate(truncate)
                .open(&path)
                .await?;

            if size > 0 {
                debug!("set attr size {}", size);

                if let Err(err) = file.set_len(size).await {
                    error!("set path {:?} size {} failed", path, size);

                    return Err(err.into());
                }
            }
        }

        metadata_to_file_attr(fs::metadata(path).await?)
    }

    #[instrument(skip(self))]
    pub async fn create_dir(&self, parent: String, name: &str, mode: u32) -> Result<FileAttr> {
        debug!("create dir name {:?} in parent {:?}", name, parent);

        let path = PathBuf::from(parent)
            .apply(|path| path.push(name))
            .clean()?;

        DirBuilder::new().mode(mode).create(&path).await?;

        metadata_to_file_attr(fs::metadata(path).await?)
    }

    #[instrument(skip(self))]
    pub async fn remove_entry(&self, parent: String, name: &str, remove_dir: bool) -> Result<()> {
        debug!(
            "remove name {:?} from parent {:?}, remove dir {}",
            name, parent, remove_dir
        );

        let path = PathBuf::from(parent)
            .apply(|path| path.push(name))
            .clean()?;

        if remove_dir {
            Ok(fs::remove_dir(path).await?)
        } else {
            Ok(fs::remove_file(path).await?)
        }
    }

    #[instrument(skip(self))]
    pub async fn rename(
        &self,
        old_parent: String,
        old_name: &str,
        new_parent: String,
        new_name: &str,
    ) -> Result<()> {
        debug!(
            "rename {:?} from {:?} to {:?} as {:?}",
            old_name, old_parent, new_parent, new_name
        );

        let old_path = PathBuf::from(old_parent)
            .apply(|path| path.push(old_name))
            .clean()?;
        let new_path = PathBuf::from(new_parent)
            .apply(|path| path.push(new_name))
            .clean()?;

        Ok(fs::rename(old_path, new_path).await?)
    }

    #[instrument(skip(self))]
    pub async fn open(&self, path: &str, flags: i32) -> Result<FileHandle> {
        let path: PathBuf = path.clean();

        debug!("open path {:?} flags {}", path, flags);

        let mut options = OpenOptions::new();

        let fh_kind = if flags & libc::O_RDWR > 0 {
            options.write(true);
            options.read(true);

            debug!("open {:?} with O_RDWR", path);

            FileHandleKind::ReadWrite
        } else if flags & libc::O_WRONLY > 0 {
            options.write(true);
            options.read(false);

            debug!("open {:?} with O_WRONLY", path);

            FileHandleKind::WriteOnly
        } else {
            options.write(false);
            options.read(true);

            debug!("open {:?} read only", path);

            FileHandleKind::ReadOnly
        };

        if flags & libc::O_TRUNC > 0 {
            debug!("open {:?} with O_TRUNC", path);

            options.truncate(true);
        }

        let sys_file = options.open(path).await?;

        let file_handle_id = self.get_fh_id();

        Ok(FileHandle::new(file_handle_id, sys_file, fh_kind))
    }

    #[instrument(skip(self))]
    pub async fn read_dir(
        &self,
        path: &str,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<(FileAttr, String)>> {
        let path: PathBuf = path.clean();

        debug!("readdir in parent {:?}", path);

        let current_attr = metadata_to_file_attr(fs::metadata(&path).await?)?;
        let parent_attr = match path.parent() {
            None => current_attr,
            Some(parent_path) => metadata_to_file_attr(fs::metadata(parent_path).await?)?,
        };

        let entries = ReadDirStream::new(fs::read_dir(&path).await?).and_then(|entry| async move {
            let attr = metadata_to_file_attr(entry.metadata().await?)?;

            Ok((attr, entry.file_name().to_string_lossy().to_string()))
        });

        Ok(stream::iter(vec![
            Ok((current_attr, String::from("."))),
            Ok((parent_attr, String::from(".."))),
        ])
        .chain(entries)
        .skip(offset)
        .take(limit)
        .try_collect::<Vec<_>>()
        .await?)
    }

    #[instrument(skip(self))]
    pub async fn create_file(
        &self,
        parent: String,
        name: &str,
        mode: u32,
        flags: i32,
    ) -> Result<(FileHandle, FileAttr)> {
        debug!("create file name {} in parent {}", name, parent);

        let path = PathBuf::from(parent)
            .apply(|path| path.push(name))
            .clean()?;

        let sys_file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .mode(mode)
            .custom_flags(flags)
            .open(path)
            .await?;

        let attr = metadata_to_file_attr(sys_file.metadata().await?)?;

        let fd_id = self.get_fh_id();

        Ok((
            FileHandle::new(fd_id, sys_file, FileHandleKind::ReadWrite),
            attr,
        ))
    }

    #[instrument(skip(self))]
    pub async fn statfs(&self) -> Result<ReplyStatFs> {
        let statfs = statfs::fstatfs(&self.root_dir_fd)?;

        Ok(ReplyStatFs {
            blocks: statfs.blocks(),
            bfree: statfs.blocks_free(),
            bavail: statfs.blocks_available(),
            files: statfs.files(),
            ffree: statfs.files_free(),
            bsize: statfs.block_size() as _,
            namelen: statfs.maximum_name_length() as _,
            frsize: 0,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::Write;
    use std::os::unix::fs::PermissionsExt;
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::{BufMut, BytesMut};
    use fuse3::{Errno, FileType};
    use tokio::io::AsyncWriteExt;
    use tokio::sync::Mutex;
    use tokio::task;
    use tokio::time::timeout;
    use tokio::{fs, time};

    use super::*;

    #[tokio::test]
    async fn init_filesystem() {
        let tmp_dir = tempfile::TempDir::new().unwrap();

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

        let attr = filesystem.get_attr("/").await.unwrap();

        assert_eq!(attr.kind, FileType::Directory);
        assert_eq!(attr.perm, 0o755);
    }

    #[tokio::test]
    async fn create_dir() {
        let tmp_dir = tempfile::TempDir::new().unwrap();

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

        let attr = filesystem
            .create_dir("/".to_string(), "test", 0o755)
            .await
            .unwrap();

        assert_eq!(attr.kind, FileType::Directory);
        assert_eq!(attr.perm, 0o755);
    }

    #[tokio::test]
    async fn create_file() {
        let tmp_dir = tempfile::TempDir::new().unwrap();

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

        let (_, attr) = filesystem
            .create_file("/".to_string(), "test", 0o644, libc::O_RDWR)
            .await
            .unwrap();

        assert_eq!(attr.kind, FileType::RegularFile);
        assert_eq!(attr.perm, 0o644);
    }

    #[tokio::test]
    async fn lookup_dir() {
        let tmp_dir = tempfile::TempDir::new().unwrap();

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

        filesystem
            .create_dir("/".to_string(), "test", 0o755)
            .await
            .unwrap();
        let attr = filesystem.lookup("/".to_string(), "test").await.unwrap();

        assert_eq!(attr.kind, FileType::Directory);
        assert_eq!(attr.perm, 0o755);
    }

    #[tokio::test]
    async fn lookup_file() {
        let tmp_dir = tempfile::TempDir::new().unwrap();

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();
        filesystem
            .create_file("/".to_string(), "test", 0o644, libc::O_RDWR)
            .await
            .unwrap();
        let attr = filesystem.lookup("/".to_string(), "test").await.unwrap();

        assert_eq!(attr.kind, FileType::RegularFile);
        assert_eq!(attr.perm, 0o644);
    }

    #[tokio::test]
    async fn get_attr_dir() {
        let tmp_dir = tempfile::TempDir::new().unwrap();

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

        filesystem
            .create_dir("/".to_string(), "test", 0o755)
            .await
            .unwrap();
        let attr = filesystem.get_attr("/test").await.unwrap();

        assert_eq!(attr.kind, FileType::Directory);
        assert_eq!(attr.perm, 0o755);
    }

    #[tokio::test]
    async fn get_attr_file() {
        let tmp_dir = tempfile::TempDir::new().unwrap();

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

        filesystem
            .create_file("/".to_string(), "test", 0o644, libc::O_RDWR)
            .await
            .unwrap();
        let attr = filesystem.get_attr("/test").await.unwrap();

        assert_eq!(attr.kind, FileType::RegularFile);
        assert_eq!(attr.perm, 0o644);
    }

    #[tokio::test]
    async fn set_dir_attr() {
        let tmp_dir = tempfile::TempDir::new().unwrap();

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

        filesystem
            .create_dir("/".to_string(), "test", 0o755)
            .await
            .unwrap();
        let attr = filesystem
            .set_attr(
                "/test",
                SetAttr {
                    ctime: None,
                    mtime: None,
                    atime: None,
                    flags: None,
                    uid: None,
                    gid: None,
                    size: None,
                    mode: Some(0o700),
                },
            )
            .await
            .unwrap();

        assert_eq!(attr.kind, FileType::Directory);
        assert_eq!(attr.perm, 0o700);
    }

    #[tokio::test]
    async fn remove_dir() {
        let tmp_dir = tempfile::TempDir::new().unwrap();

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

        filesystem
            .create_dir("/".to_string(), "test", 0o755)
            .await
            .unwrap();
        filesystem
            .remove_entry("/".to_string(), "test", true)
            .await
            .unwrap();

        assert_eq!(
            filesystem.lookup("/".to_string(), "test").await,
            Err(Errno::new_not_exist())
        );
    }

    #[tokio::test]
    async fn remove_file() {
        let tmp_dir = tempfile::TempDir::new().unwrap();

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

        filesystem
            .create_file("/".to_string(), "test", 0o644, libc::O_RDWR)
            .await
            .unwrap();
        filesystem
            .remove_entry("/".to_string(), "test", false)
            .await
            .unwrap();

        assert_eq!(
            filesystem.lookup("/".to_string(), "test").await,
            Err(Errno::new_not_exist())
        );
    }

    #[tokio::test]
    async fn rename_dir() {
        let tmp_dir = tempfile::TempDir::new().unwrap();

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

        filesystem
            .create_dir("/".to_string(), "test", 0o755)
            .await
            .unwrap();
        filesystem
            .rename("/".to_string(), "test", "/".to_string(), "new-test")
            .await
            .unwrap();
        let attr = filesystem
            .lookup("/".to_string(), "new-test")
            .await
            .unwrap();

        assert_eq!(attr.kind, FileType::Directory);
        assert_eq!(attr.perm, 0o755);
    }

    #[tokio::test]
    async fn rename_file() {
        let tmp_dir = tempfile::TempDir::new().unwrap();

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

        filesystem
            .create_file("/".to_string(), "test", 0o644, libc::O_RDWR)
            .await
            .unwrap();
        filesystem
            .rename("/".to_string(), "test", "/".to_string(), "new-test")
            .await
            .unwrap();
        let attr = filesystem
            .lookup("/".to_string(), "new-test")
            .await
            .unwrap();

        assert_eq!(attr.kind, FileType::RegularFile);
        assert_eq!(attr.perm, 0o644);
    }

    #[tokio::test]
    async fn move_dir() {
        let tmp_dir = tempfile::TempDir::new().unwrap();

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

        filesystem
            .create_dir("/".to_string(), "old", 0o755)
            .await
            .unwrap();
        filesystem
            .create_dir("/".to_string(), "new", 0o755)
            .await
            .unwrap();
        filesystem
            .create_dir("/old".to_string(), "test", 0o755)
            .await
            .unwrap();
        filesystem
            .rename("/old".to_string(), "test", "/new".to_string(), "new-test")
            .await
            .unwrap();
        let attr = filesystem
            .lookup("/new".to_string(), "new-test")
            .await
            .unwrap();

        assert_eq!(attr.kind, FileType::Directory);
        assert_eq!(attr.perm, 0o755);
    }

    #[tokio::test]
    async fn move_file() {
        let tmp_dir = tempfile::TempDir::new().unwrap();

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

        filesystem
            .create_dir("/".to_string(), "old", 0o755)
            .await
            .unwrap();
        filesystem
            .create_dir("/".to_string(), "new", 0o755)
            .await
            .unwrap();
        filesystem
            .create_file("/old".to_string(), "test", 0o644, libc::O_RDWR)
            .await
            .unwrap();
        filesystem
            .rename("/old".to_string(), "test", "/new".to_string(), "new-test")
            .await
            .unwrap();
        let attr = filesystem
            .lookup("/new".to_string(), "new-test")
            .await
            .unwrap();

        assert_eq!(attr.kind, FileType::RegularFile);
        assert_eq!(attr.perm, 0o644);
    }

    #[tokio::test]
    async fn read_dir() {
        let tmp_dir = tempfile::TempDir::new().unwrap();

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

        filesystem
            .create_dir("/".to_string(), "test", 0o755)
            .await
            .unwrap();
        let children = filesystem.read_dir("/", 0, 10).await.unwrap();

        assert_eq!(children.len(), 3);

        let (attr, name) = &children[0];
        assert_eq!(attr.kind, FileType::Directory);
        assert_eq!(*name, String::from("."));

        let (attr, name) = &children[1];
        assert_eq!(attr.kind, FileType::Directory);
        assert_eq!(*name, String::from(".."));

        let (attr, name) = &children[2];
        assert_eq!(attr.kind, FileType::Directory);
        assert_eq!(*name, String::from("test"));
    }

    #[tokio::test]
    async fn open_file_rw() {
        let tmp_dir = tempfile::TempDir::new().unwrap();

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

        filesystem
            .create_file("/".to_string(), "test", 0o644, libc::O_RDWR)
            .await
            .unwrap();
        let file_handle = filesystem.open("/test", libc::O_RDWR).await.unwrap();

        assert_eq!(file_handle.get_id(), 2);
        assert_eq!(
            file_handle.get_file_handle_kind(),
            FileHandleKind::ReadWrite
        );

        let attr = file_handle.get_attr().await.unwrap();

        assert_eq!(attr.kind, FileType::RegularFile);
        assert_eq!(attr.perm, 0o644);
    }

    #[tokio::test]
    async fn open_file_ro() {
        let tmp_dir = tempfile::TempDir::new().unwrap();

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

        filesystem
            .create_file("/".to_string(), "test", 0o644, libc::O_RDWR)
            .await
            .unwrap();
        let file_handle = filesystem.open("/test", libc::O_RDONLY).await.unwrap();

        assert_eq!(file_handle.get_id(), 2);
        assert_eq!(file_handle.get_file_handle_kind(), FileHandleKind::ReadOnly);

        let attr = file_handle.get_attr().await.unwrap();

        assert_eq!(attr.kind, FileType::RegularFile);
        assert_eq!(attr.perm, 0o644);
    }

    #[tokio::test]
    async fn open_file_wo() {
        let tmp_dir = tempfile::TempDir::new().unwrap();

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

        filesystem
            .create_file("/".to_string(), "test", 0o644, libc::O_RDWR)
            .await
            .unwrap();
        let file_handle = filesystem.open("/test", libc::O_WRONLY).await.unwrap();

        assert_eq!(file_handle.get_id(), 2);
        assert_eq!(
            file_handle.get_file_handle_kind(),
            FileHandleKind::WriteOnly
        );

        let attr = file_handle.get_attr().await.unwrap();

        assert_eq!(attr.kind, FileType::RegularFile);
        assert_eq!(attr.perm, 0o644);
    }

    #[tokio::test]
    async fn write_file() {
        let tmp_dir = tempfile::TempDir::new().unwrap();

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

        let (mut file_handle, _) = filesystem
            .create_file("/".to_string(), "test", 0o644, libc::O_RDWR)
            .await
            .unwrap();

        let written = file_handle.write(b"test", 0).await.unwrap();
        file_handle.flush().await.unwrap();

        assert_eq!(written, 4);
        assert_eq!(file_handle.get_attr().await.unwrap().size, 4)
    }

    #[tokio::test]
    async fn read_file() {
        let tmp_dir = tempfile::TempDir::new().unwrap();

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

        let (mut file_handle, _) = filesystem
            .create_file("/".to_string(), "test", 0o644, libc::O_RDWR)
            .await
            .unwrap();

        let read = file_handle.read(&mut [0], 0).await.unwrap();
        assert_eq!(read, 0);

        file_handle.write(b"test", 0).await.unwrap();
        file_handle.flush().await.unwrap();

        let mut buf = vec![0; 4];

        let read = file_handle.read(&mut buf, 0).await.unwrap();
        assert_eq!(read, 4);
        assert_eq!(&b"test"[..], &buf[..])
    }

    #[tokio::test]
    async fn set_attr_file() {
        let tmp_dir = tempfile::TempDir::new().unwrap();

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

        let (mut file_handle, _) = filesystem
            .create_file("/".to_string(), "test", 0o644, libc::O_RDWR)
            .await
            .unwrap();

        assert_eq!(file_handle.write(b"test", 0).await, Ok(b"test".len()));

        file_handle.flush().await.unwrap();

        let attr = filesystem
            .set_attr(
                "/test",
                SetAttr {
                    mode: Some(0o600),
                    uid: None,
                    gid: None,
                    size: Some(2),
                    atime: None,
                    mtime: None,
                    ctime: None,
                    flags: None,
                },
            )
            .await
            .unwrap();

        assert_eq!(attr.perm, 0o600);
        assert_eq!(attr.size, 2);

        let mut file_handle = filesystem.open("/test", libc::O_RDONLY).await.unwrap();

        let mut buf = vec![0; 4];

        let read = file_handle.read(&mut buf, 0).await.unwrap();
        assert_eq!(read, 2);
        assert_eq!(&b"te"[..], &buf[..read])
    }

    #[tokio::test]
    async fn set_share_lock_success() {
        let tmp_dir = tempfile::TempDir::new().unwrap();

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

        let mut file_handle = filesystem
            .create_file("/".to_string(), "test", 0o644, libc::O_RDWR)
            .await
            .unwrap()
            .0;
        let mut file_handle2 = filesystem.open("/test", libc::O_RDWR).await.unwrap();

        let lock_table = Arc::new(Mutex::new(BTreeMap::new()));

        let lock_job = file_handle
            .set_lock(1, true, lock_table.clone())
            .await
            .unwrap();

        let lock_job = timeout(Duration::from_secs(2), lock_job)
            .await
            .expect("set share lock failed")
            .unwrap();

        assert!(lock_job);

        let lock_job = file_handle2.set_lock(2, true, lock_table).await.unwrap();

        let lock_job = timeout(Duration::from_secs(2), lock_job)
            .await
            .expect("set another share lock failed")
            .unwrap();

        assert!(lock_job);
    }

    #[tokio::test]
    async fn set_share_lock_failed() {
        let tmp_dir = tempfile::TempDir::new().unwrap();

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

        let mut file_handle = filesystem
            .create_file("/".to_string(), "test", 0o644, libc::O_RDWR)
            .await
            .unwrap()
            .0;
        let mut file_handle2 = filesystem.open("/test", libc::O_RDWR).await.unwrap();

        let lock_table = Arc::new(Mutex::new(BTreeMap::new()));

        let lock_job = file_handle
            .set_lock(1, true, lock_table.clone())
            .await
            .unwrap();

        let lock_job = timeout(Duration::from_secs(2), lock_job)
            .await
            .expect("set share lock failed")
            .unwrap();

        assert!(lock_job);

        let lock_job = file_handle2.set_lock(2, false, lock_table).await.unwrap();

        if timeout(Duration::from_secs(1), lock_job).await.is_ok() {
            panic!("set not share lock success")
        }
    }

    #[tokio::test]
    async fn try_set_share_lock_success() {
        let tmp_dir = tempfile::TempDir::new().unwrap();

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

        let file_handle = filesystem
            .create_file("/".to_string(), "test", 0o644, libc::O_RDWR)
            .await
            .unwrap()
            .0;
        let file_handle2 = filesystem.open("/test", libc::O_RDWR).await.unwrap();

        assert_eq!(file_handle.try_set_lock(true).await, Ok(()));
        assert_eq!(file_handle2.try_set_lock(true).await, Ok(()));
    }

    #[tokio::test]
    async fn try_set_share_lock_failed() {
        let tmp_dir = tempfile::TempDir::new().unwrap();

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

        let file_handle = filesystem
            .create_file("/".to_string(), "test", 0o644, libc::O_RDWR)
            .await
            .unwrap()
            .0;
        let file_handle2 = filesystem.open("/test", libc::O_RDWR).await.unwrap();

        assert_eq!(file_handle.try_set_lock(true).await, Ok(()));

        assert_eq!(
            file_handle2.try_set_lock(false).await,
            Err(Errno::from(libc::EWOULDBLOCK))
        )
    }

    #[tokio::test]
    async fn set_exclusive_lock() {
        let tmp_dir = tempfile::TempDir::new().unwrap();

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

        let mut file_handle = filesystem
            .create_file("/".to_string(), "test", 0o644, libc::O_RDWR)
            .await
            .unwrap()
            .0;
        let mut file_handle2 = filesystem.open("/test", libc::O_RDWR).await.unwrap();

        let lock_table = Arc::new(Mutex::new(BTreeMap::new()));

        let lock_job = file_handle
            .set_lock(1, false, lock_table.clone())
            .await
            .unwrap();

        let lock_job = timeout(Duration::from_secs(2), lock_job)
            .await
            .expect("set exclusive lock failed")
            .unwrap();

        assert!(lock_job);

        let lock_job = file_handle2
            .set_lock(2, false, lock_table.clone())
            .await
            .unwrap();

        if timeout(Duration::from_secs(1), lock_job).await.is_ok() {
            panic!("set exclusive lock should failed");
        }

        let lock_job = file_handle2.set_lock(3, true, lock_table).await.unwrap();

        if timeout(Duration::from_secs(1), lock_job).await.is_ok() {
            panic!("set share lock should failed");
        }
    }

    #[tokio::test]
    async fn try_set_exclusive_lock() {
        let tmp_dir = tempfile::TempDir::new().unwrap();

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

        let file_handle = filesystem
            .create_file("/".to_string(), "test", 0o644, libc::O_RDWR)
            .await
            .unwrap()
            .0;
        let file_handle2 = filesystem.open("/test", libc::O_RDWR).await.unwrap();

        assert_eq!(file_handle.try_set_lock(false).await, Ok(()));
        assert_eq!(
            file_handle2.try_set_lock(true).await,
            Err(Errno::from(libc::EWOULDBLOCK))
        );
        assert_eq!(
            file_handle2.try_set_lock(false).await,
            Err(Errno::from(libc::EWOULDBLOCK))
        );
    }

    #[tokio::test]
    async fn release_share_lock() {
        let tmp_dir = tempfile::TempDir::new().unwrap();

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

        let mut file_handle = filesystem
            .create_file("/".to_string(), "test", 0o644, libc::O_RDWR)
            .await
            .unwrap()
            .0;
        let file_handle2 = filesystem.open("/test", libc::O_RDWR).await.unwrap();

        let lock_queue = Arc::new(Mutex::new(BTreeMap::new()));

        let lock_job = file_handle.set_lock(1, true, lock_queue).await.unwrap();

        assert!(lock_job.await.unwrap());
        assert_eq!(file_handle.release_lock().await, Ok(()));

        assert_eq!(file_handle2.try_set_lock(false).await, Ok(()));
    }

    #[tokio::test]
    async fn release_exclusive_lock() {
        let tmp_dir = tempfile::TempDir::new().unwrap();

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

        let mut file_handle = filesystem
            .create_file("/".to_string(), "test", 0o644, libc::O_RDWR)
            .await
            .unwrap()
            .0;
        let file_handle2 = filesystem.open("/test", libc::O_RDWR).await.unwrap();

        let lock_queue = Arc::new(Mutex::new(BTreeMap::new()));

        let lock_job = file_handle.set_lock(1, false, lock_queue).await.unwrap();

        assert!(lock_job.await.unwrap());
        assert_eq!(file_handle.release_lock().await, Ok(()));

        assert_eq!(file_handle2.try_set_lock(false).await, Ok(()));
    }

    #[tokio::test]
    async fn interrupt_lock() {
        let tmp_dir = tempfile::TempDir::new().unwrap();

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

        let mut file_handle = filesystem
            .create_file("/".to_string(), "test", 0o644, libc::O_RDWR)
            .await
            .unwrap()
            .0;
        let mut file_handle2 = filesystem.open("/test", libc::O_RDWR).await.unwrap();

        let lock_table = Arc::new(Mutex::new(BTreeMap::new()));

        let lock_job = file_handle
            .set_lock(1, false, lock_table.clone())
            .await
            .unwrap();

        assert!(lock_job.await.unwrap());

        let lock_job = file_handle2
            .set_lock(2, false, lock_table.clone())
            .await
            .unwrap();

        lock_table.lock().await.get(&2).unwrap().notify();

        debug!("interrupt sent");

        assert!(!lock_job.await.unwrap())
    }

    #[tokio::test]
    async fn wait_exclusive_lock() {
        let tmp_dir = tempfile::TempDir::new().unwrap();

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

        let mut file_handle = filesystem
            .create_file("/".to_string(), "test", 0o644, libc::O_RDWR)
            .await
            .unwrap()
            .0;
        let mut file_handle2 = filesystem.open("/test", libc::O_RDWR).await.unwrap();

        let lock_table = Arc::new(Mutex::new(BTreeMap::new()));

        assert_eq!(
            file_handle
                .set_lock(1, false, lock_table.clone())
                .await
                .unwrap()
                .await,
            Ok(true),
        );

        task::spawn(async move {
            time::sleep(Duration::from_secs(1)).await;

            file_handle.release_lock().await.unwrap();
        });

        let lock_job = file_handle2
            .set_lock(2, false, lock_table.clone())
            .await
            .unwrap();

        let lock_result = timeout(Duration::from_secs(2), lock_job)
            .await
            .expect("set exclusive lock should success");

        assert!(lock_result.unwrap())
    }

    #[tokio::test]
    async fn get_lock_kind() {
        let tmp_dir = tempfile::TempDir::new().unwrap();

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

        let mut file_handle = filesystem
            .create_file("/".to_string(), "test", 0o644, libc::O_RDWR)
            .await
            .unwrap()
            .0;

        assert_eq!(file_handle.get_lock_kind().await, LockKind::NoLock);

        let lock_table = Arc::new(Mutex::new(BTreeMap::new()));

        assert_eq!(
            file_handle
                .set_lock(1, false, lock_table.clone())
                .await
                .unwrap()
                .await,
            Ok(true),
        );

        assert_eq!(file_handle.get_lock_kind().await, LockKind::Exclusive);

        file_handle.release_lock().await.unwrap();

        assert_eq!(file_handle.get_lock_kind().await, LockKind::NoLock);

        assert_eq!(
            file_handle
                .set_lock(1, true, lock_table.clone())
                .await
                .unwrap()
                .await,
            Ok(true),
        );

        assert_eq!(file_handle.get_lock_kind().await, LockKind::Share);

        file_handle.release_lock().await.unwrap();

        assert_eq!(file_handle.get_lock_kind().await, LockKind::NoLock);
    }

    #[tokio::test]
    async fn lookup_exist_entry() {
        let tmp_dir = tempfile::TempDir::new().unwrap();

        let tmp_file = tempfile::NamedTempFile::new_in(&tmp_dir).unwrap();

        let perm = fs::metadata(tmp_file.path()).await.unwrap().permissions();
        let perm = perm.mode() ^ libc::S_IFREG;

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

        let attr = filesystem
            .lookup(
                "/".to_string(),
                tmp_file.path().file_name().unwrap().to_str().unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(attr.kind, FileType::RegularFile);
        assert_eq!(attr.perm as u32, perm);
    }

    // issue #8
    #[tokio::test]
    async fn rename_to_exist_dir() {
        let tmp_dir = tempfile::TempDir::new().unwrap();

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

        filesystem
            .create_dir("/".to_string(), "test", 0o755)
            .await
            .unwrap();
        filesystem
            .create_dir("/".to_string(), "exist", 0o755)
            .await
            .unwrap();
        filesystem
            .rename("/".to_string(), "test", "/".to_string(), "exist")
            .await
            .unwrap();
        let attr = filesystem.lookup("/".to_string(), "exist").await.unwrap();

        assert_eq!(attr.kind, FileType::Directory);
        assert_eq!(attr.perm, 0o755);
    }

    // issue #8
    #[tokio::test]
    async fn rename_to_exist_file() {
        let tmp_dir = tempfile::TempDir::new().unwrap();

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

        filesystem
            .create_file("/".to_string(), "test", 0o644, libc::O_RDWR)
            .await
            .unwrap();
        filesystem
            .create_file("/".to_string(), "exist", 0o644, libc::O_RDWR)
            .await
            .unwrap();
        filesystem
            .rename("/".to_string(), "test", "/".to_string(), "exist")
            .await
            .unwrap();
        let attr = filesystem.lookup("/".to_string(), "exist").await.unwrap();

        assert_eq!(attr.kind, FileType::RegularFile);
        assert_eq!(attr.perm, 0o644);
    }

    #[tokio::test]
    async fn fallocate() {
        let tmp_dir = tempfile::TempDir::new().unwrap();

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

        let mut fh = filesystem
            .create_file("/".to_string(), "test", 0o644, libc::O_RDWR)
            .await
            .unwrap()
            .0;
        fh.fallocate(0, 100, 0).await.unwrap();

        let attr = filesystem.get_attr("/test").await.unwrap();

        assert_eq!(attr.size, 100);
    }

    #[tokio::test]
    async fn copy_file_range() {
        let tmp_dir = tempfile::TempDir::new().unwrap();

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

        let mut fh1 = filesystem
            .create_file("/".to_string(), "test", 0o644, libc::O_RDWR)
            .await
            .unwrap()
            .0;

        assert_eq!(fh1.write(b"test", 0).await.unwrap(), 4);

        fh1.flush().await.unwrap();

        let mut fh2 = filesystem
            .create_file("/".to_string(), "test2", 0o644, libc::O_RDWR)
            .await
            .unwrap()
            .0;

        let copied = fh1.copy_to(0, 0, 4, Some(&fh2)).await.unwrap();

        assert_eq!(copied, 4);

        let mut buf = vec![0; 100];

        let n = fh2.read(&mut buf, 0).await.unwrap();
        assert_eq!(n, 4);

        assert_eq!(&buf[..n], b"test");
    }

    #[tokio::test]
    async fn remove_dir_deep() {
        let tmp_dir = tempfile::TempDir::new().unwrap();

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

        filesystem
            .create_dir("/".to_string(), "test", 0o755)
            .await
            .unwrap();
        filesystem
            .create_dir("/test".to_string(), "sub-dir", 0o755)
            .await
            .unwrap();

        assert_eq!(
            filesystem.remove_entry("/".to_string(), "test", true).await,
            Err(Errno::from(libc::ENOTEMPTY))
        );
    }

    #[tokio::test]
    async fn read_large_file() {
        crate::log_init("test".to_owned(), true);

        let tmp_dir = tempfile::TempDir::new().unwrap();

        let test_file_path = tmp_dir
            .path()
            .to_path_buf()
            .apply(|path| path.push("test-file"));

        let buffer = BytesMut::with_capacity(4 * 20 * 1024 * 1024);
        let mut writer = buffer.writer();

        for _ in 0..1024 * 1024 * 20 {
            writer.write_all(b"test").unwrap();
        }

        let buffer = writer.into_inner();

        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&test_file_path)
            .await
            .unwrap();
        file.write_all(&buffer).await.unwrap();
        file.flush().await.unwrap();

        debug!("write data into prepare file");

        let filesystem = Filesystem::new(tmp_dir.path()).await.unwrap();

        let mut file_handle = filesystem.open("test-file", libc::O_RDONLY).await.unwrap();

        let read_data = BytesMut::with_capacity(buffer.len());
        let mut read_data = read_data.writer();

        let mut read_buf = vec![0; 1024 * 1024 * 10];
        let mut read = 0;

        while read < buffer.len() {
            let n = file_handle.read(&mut read_buf, read as i64).await.unwrap();
            if n == 0 {
                panic!("unexpected EOF");
            }

            read += n;

            read_data.write_all(&read_buf[..n]).unwrap();
        }

        assert_eq!(
            String::from_utf8_lossy(buffer.as_ref()),
            String::from_utf8_lossy(read_data.into_inner().as_ref())
        );
    }
}
