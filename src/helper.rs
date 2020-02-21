use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fuse::{FileAttr, FileType};

use crate::server::{PbAttr, PbEntryType};

pub trait Apply: Sized {
    fn apply<F>(mut self, f: F) -> Self
        where F: FnOnce(&mut Self),
    {
        f(&mut self);
        self
    }
}

impl<T> Apply for T {}

fn convert_system_time_to_proto_time(sys_time: SystemTime) -> Option<prost_types::Timestamp> {
    sys_time.duration_since(UNIX_EPOCH)
        .map_or(None, |duration| {
            Some(prost_types::Timestamp {
                seconds: duration.as_secs() as i64,
                nanos: duration.as_nanos() as i32,
            })
        })
}

#[inline]
pub fn convert_proto_time_to_system_time(proto_time: prost_types::Timestamp) -> SystemTime {
    UNIX_EPOCH + Duration::new(proto_time.seconds as u64, proto_time.nanos as u32)
}

pub fn convert_fuse_attr_to_proto_attr(fuse_attr: FileAttr, name: &str) -> PbAttr {
    PbAttr {
        inode: fuse_attr.ino,
        name: name.to_string(),
        r#type: match fuse_attr.kind {
            FileType::Directory => PbEntryType::Dir.into(),
            FileType::RegularFile => PbEntryType::File.into(),
            _ => unreachable!()
        },
        access_time: convert_system_time_to_proto_time(fuse_attr.atime),
        modify_time: convert_system_time_to_proto_time(fuse_attr.mtime),
        change_time: convert_system_time_to_proto_time(fuse_attr.ctime),
        mode: fuse_attr.perm as i32,
        size: fuse_attr.size as i64,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        let x = 100.apply(|myself| {
            *myself += 1;
        });

        assert_eq!(x, 101);
    }
}