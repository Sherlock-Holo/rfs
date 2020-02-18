use std::ffi::OsStr;
use std::path::Path;

use rfs::{Filesystem, log_init};

fn main() {
    log_init(true);

    let opts: Vec<_> = vec!["-o", "fsname=fuse-test"]
        .into_iter()
        .map(|opt| OsStr::new(opt))
        .collect();

    let uid = unsafe { libc::getuid() };
    let gid = unsafe { libc::getgid() };

    fuse::mount(Filesystem::new(uid, gid), &Path::new("/tmp/fuse-test"), &opts).unwrap();
}
