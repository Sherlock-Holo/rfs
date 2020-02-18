use std::io::SeekFrom;
use std::os::unix::fs::PermissionsExt;

use async_std::fs::File as SysFile;
use async_std::prelude::*;

use crate::errno::Errno;
use crate::Result;
use crate::server::attr::SetAttr;

pub enum FileHandleKind {
    ReadOnly,
    WriteOnly,
    ReadWrite,
}

pub struct FileHandle {
    id: u64,
    sys_file: SysFile,
    kind: FileHandleKind, // avoid useless read/write syscall to improve performance
}

impl FileHandle {
    pub fn new(id: u64, sys_file: SysFile, kind: FileHandleKind) -> Self {
        Self {
            id,
            sys_file,
            kind,
        }
    }

    pub async fn read(&mut self, buf: &mut [u8], offset: i64) -> Result<usize> {
        if let FileHandleKind::WriteOnly = self.kind {
            return Err(Errno(libc::EBADF));
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
            return Err(Errno(libc::EBADF));
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

    pub async fn set_attr(&mut self, set_attr: SetAttr) -> Result<()> {
        if let FileHandleKind::ReadOnly = self.kind {
            return Err(Errno(libc::EBADF));
        }

        if let Some(mode) = set_attr.mode {
            let mut permissions = self.sys_file.metadata().await?.permissions();

            permissions.set_mode(mode);

            self.sys_file.set_permissions(permissions).await?;
        }

        if let Some(size) = set_attr.size {
            self.sys_file.set_len(size).await?;
        }

        Ok(())
    }
}