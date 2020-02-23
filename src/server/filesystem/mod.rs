use std::env;

use nix::NixPath;
use nix::unistd;

pub use attr::SetAttr;
pub use file_handle::FileHandle;
pub use filesystem::Filesystem;

use crate::Result;

mod dir;
mod entry;
mod file;
mod inode;
mod file_handle;
mod attr;
mod filesystem;

pub fn chroot<P: NixPath>(new_root: P) -> Result<()> {
    unistd::chroot(&new_root)?;

    Ok(env::set_current_dir("/")?)
}