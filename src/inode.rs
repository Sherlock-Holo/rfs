use std::collections::HashMap;
use std::sync::RwLock;

use crate::entry::Entry;

pub type Inode = u64;

pub struct InodeMap(RwLock<HashMap<Inode, Entry>>);

impl InodeMap {
    pub fn new() -> Self {
        Self(RwLock::new(HashMap::new()))
    }

    pub fn get_entry(&self, inode: Inode) -> Option<Entry> {
        self.0.read().unwrap().get(&inode).map(|entry| {
            entry.clone()
        })
    }

    pub fn add_entry(&self, inode: Inode, entry: Entry) {
        self.0.write().unwrap().insert(inode, entry);
    }

    pub fn remove_entry(&self, inode: Inode) -> bool {
        self.0.write().unwrap().remove(&inode).is_some()
    }
}