use std::collections::BTreeMap;
use std::path::PathBuf;

use super::entry::EntryPath;

pub type Inode = u64;
pub type InodeToPath = BTreeMap<Inode, EntryPath>;
pub type PathToInode = BTreeMap<PathBuf, Inode>;
