use std::os::raw::c_int;

use crate::file::File;

enum Status {
    ReadOnly,
    WriteOnly,
    ReadWrite,
}

pub struct FileHandler {
    id: u64,
    file: File,
    status: Status,
}

impl FileHandler {
    pub fn new(id: u64, file: File, flags: u32) -> Self {
        if flags & libc::O_TRUNC as u32 > 0 {
            file.truncate();
        }

        let status = if flags & libc::O_WRONLY as u32 > 0 {
            Status::WriteOnly
        } else if flags & libc::O_RDONLY as u32 > 0 {
            Status::ReadOnly
        } else {
            Status::ReadWrite
        };

        Self {
            id,
            file,
            status,
        }
    }

    pub fn get_id(&self) -> u64 { self.id }

    pub fn read(&self, offset: i64, size: u32) -> Result<Vec<u8>, c_int> {
        if let Status::WriteOnly = self.status {
            return Err(libc::EBADF);
        }

        self.file.read(offset, size)
    }

    pub fn write(&self, offset: i64, data: &[u8]) -> Result<usize, c_int> {
        if let Status::ReadOnly = self.status {
            return Err(libc::EBADF);
        }

        self.file.write(offset, data)
    }
}
