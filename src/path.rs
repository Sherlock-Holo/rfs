use std::fmt::{Debug, Display, Formatter};
use std::path::{Path, PathBuf};

use fuse3::Errno;
use path_clean::clean as original_clean;

pub struct Error(String);

impl std::error::Error for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl Debug for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

pub trait PathClean<P> {
    fn clean(&self) -> P;
}

impl<P: AsRef<Path>> PathClean<Result<PathBuf, Error>> for P {
    fn clean(&self) -> Result<PathBuf, Error> {
        match self.as_ref().as_os_str().to_os_string().into_string() {
            Err(original_path) => Err(Error(format!(
                "path \"{:?}\" has invalid char",
                original_path
            ))),
            Ok(path) => Ok(PathBuf::from(original_clean(&path))),
        }
    }
}

impl<P: AsRef<str>> PathClean<PathBuf> for P {
    fn clean(&self) -> PathBuf {
        PathBuf::from(original_clean(self.as_ref()))
    }
}

impl From<Error> for Errno {
    fn from(_: Error) -> Self {
        Errno::from(libc::EINVAL)
    }
}
