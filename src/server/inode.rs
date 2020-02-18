use std::collections::BTreeMap;

use crate::server::entry::Entry;

pub type Inode = u64;
pub type InodeMap = BTreeMap<Inode, Entry>;