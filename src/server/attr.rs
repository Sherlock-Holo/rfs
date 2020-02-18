use time_old::Timespec;

pub struct SetAttr {
    pub mode: Option<u32>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub size: Option<u64>,
    pub atime: Option<Timespec>,
    pub mtime: Option<Timespec>,
    pub fh: Option<u64>,
    pub crtime: Option<Timespec>,
    pub chgtime: Option<Timespec>,
    pub kuptime: Option<Timespec>,
    pub flags: Option<u32>,
}