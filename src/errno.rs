use std::backtrace::{Backtrace, BacktraceStatus};
use std::fmt;
use std::fmt::{Debug, Display, Formatter};
use std::io::Error as IoError;
use std::os::raw::c_int;

use nix::Error as NixError;
use thiserror::Error;

use crate::pb::Error as PbError;

#[derive(Error)]
pub struct Errno(pub c_int, Backtrace);

impl Debug for Errno {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if let BacktraceStatus::Captured = self.1.status() {
            write!(f, "errno is {}\n{}", self.0, self.1)
        } else {
            write!(f, "errno is {}", self.0)
        }
    }
}

impl Display for Errno {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Debug::fmt(self, f)
    }
}

impl PartialEq for Errno {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl From<IoError> for Errno {
    fn from(err: IoError) -> Self {
        if let Some(errno) = err.raw_os_error() {
            return Errno(errno, Backtrace::capture());
        }

        return Errno(libc::EINVAL, Backtrace::capture());
    }
}

impl From<NixError> for Errno {
    fn from(err: NixError) -> Self {
        match err {
            NixError::Sys(errno) => Errno(errno as libc::c_int, Backtrace::capture()),
            NixError::InvalidPath | NixError::InvalidUtf8 => {
                Errno(libc::EINVAL, Backtrace::capture())
            }
            NixError::UnsupportedOperation => Errno(libc::ENOTSUP, Backtrace::capture()),
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
        Self(errno, Backtrace::capture())
    }
}

impl From<Errno> for PbError {
    fn from(err: Errno) -> Self {
        PbError { errno: err.0 as u32 }
    }
}
