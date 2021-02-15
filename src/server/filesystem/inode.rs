use std::collections::BTreeMap;

use super::entry::Entry;

pub type Inode = fuse3::Inode;
pub type InodeMap = BTreeMap<Inode, Entry>;
