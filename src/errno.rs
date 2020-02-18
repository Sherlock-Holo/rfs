use std::io::Error as IoError;
use std::os::raw::c_int;

use nix::errno::Errno as NixErrno;
use nix::Error as NixError;

pub struct Errno(pub c_int);

impl From<IoError> for Errno {
    fn from(err: IoError) -> Self {
        if let Some(errno) = err.raw_os_error() {
            return Errno(errno);
        }

        return Errno(libc::EINVAL);
    }
}

impl From<NixError> for Errno {
    fn from(err: NixError) -> Self {
        match err {
            NixError::Sys(errno) => Errno(errno as libc::c_int),
            NixError::InvalidPath | NixError::InvalidUtf8 => Errno(libc::EINVAL),
            NixError::UnsupportedOperation => Errno(libc::ENOTSUP),
        }
    }
}

impl From<Errno> for c_int {
    fn from(errno: Errno) -> Self {
        errno.0
    }
}

impl From<c_int> for Errno {
    fn from(errno: i32) -> Self {
        Self(errno)
    }
}