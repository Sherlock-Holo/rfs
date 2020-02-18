use std::io::Error;
use std::os::raw::c_int;

pub struct Errno(pub c_int);

impl From<Error> for Errno {
    fn from(err: Error) -> Self {
        if let Some(errno) = err.raw_os_error() {
            return Errno(errno);
        }

        return Errno(libc::EINVAL);
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