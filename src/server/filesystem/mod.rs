use std::env;

use nix::NixPath;
use nix::unistd;

pub use attr::SetAttr;
pub use file_handle::FileHandle;
pub use filesystem::Filesystem;

use crate::Result;

mod attr;
mod dir;
mod entry;
mod file;
mod file_handle;
mod filesystem;
mod inode;

pub fn chroot<P: NixPath>(new_root: P) -> Result<()> {
    unistd::chroot(&new_root)?;

    Ok(env::set_current_dir("/")?)
}
