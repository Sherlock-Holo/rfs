use std::collections::BTreeMap;

use super::entry::Entry;

pub type Inode = u64;
pub type InodeMap = BTreeMap<Inode, Entry>;