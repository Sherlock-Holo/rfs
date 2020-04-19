use std::collections::BTreeMap;
use std::path::PathBuf;

use super::entry::Entry;
use super::entry::EntryPath;

pub type Inode = u64;
pub type InodeMap = BTreeMap<Inode, Entry>;
pub type InodeToPath = BTreeMap<Inode, EntryPath>;
pub type PathToInode = BTreeMap<PathBuf, Inode>;
