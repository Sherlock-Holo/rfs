use std::convert::TryFrom;
use std::ffi::OsStr;
use std::fmt::{self, Debug, Display};
use std::io;
use std::io::ErrorKind;
use std::path::Path;
use std::time::{Duration, SystemTime};

use anyhow::Result;
use fuse::{
    Filesystem as FuseFilesystem, FileType, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory,
    ReplyEmpty, ReplyEntry, ReplyLock, ReplyOpen, ReplyWrite, Request,
};
use libc::c_int;
use log::{debug, error, info, warn};
use nix::unistd;
use serde::export::Formatter;
use tokio::fs;
use tokio::net::UnixStream;
use tokio::task;
use tonic::Request as TonicRequest;
use tonic::transport::{Channel, Uri};
use tonic::transport::ClientTlsConfig;
use tonic::transport::Endpoint;
use tower::service_fn;
use uuid::Uuid;

use lazy_static::lazy_static;

use crate::block_on;
use crate::helper::proto_attr_into_fuse_attr;
use crate::pb::*;
use crate::pb::rfs_client::RfsClient;

lazy_static! {
    static ref TTL: Duration = Duration::new(1, 0);
}

enum ClientKind {
    Rpc,
    Uds,
}

impl Debug for ClientKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ClientKind::Rpc => f.write_str("rpc"),
            ClientKind::Uds => f.write_str("uds"),
        }
    }
}

impl Display for ClientKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Debug::fmt(self, f)
    }
}

pub struct Filesystem {
    uuid: Option<Uuid>,
    rpc_client: RfsClient<Channel>,
    client_kind: ClientKind,
}

impl Filesystem {
    fn get_rpc_header(&self) -> Option<Header> {
        if let Some(uuid) = self.uuid {
            Some(Header {
                uuid: uuid.to_hyphenated().to_string(),
            })
        } else {
            None
        }
    }
}

impl Filesystem {
    pub async fn new_uds<P: AsRef<Path>>(uds_path: P) -> Result<Self> {
        let uds_path = uds_path.as_ref().to_path_buf();

        // check if uds inits or not
        loop {
            if let Err(err) = fs::metadata(&uds_path).await {
                if let ErrorKind::NotFound = err.kind() {
                    info!("waiting for uds creating");

                    continue;
                } else {
                    return Err(err.into());
                };
            }

            break;
        }

        debug!("uds connected");

        let uds_path = uds_path.to_str().expect("invalid unix path").to_string();

        fn string_to_static_str(s: String) -> &'static str {
            Box::leak(s.into_boxed_str())
        };

        let uds_path: &'static str = string_to_static_str(uds_path);

        let channel = Endpoint::try_from("http://[::]:50051")?
            .connect_with_connector(service_fn(move |_: Uri| UnixStream::connect(uds_path)))
            .await?;

        Ok(Filesystem {
            uuid: None,
            rpc_client: RfsClient::new(channel),
            client_kind: ClientKind::Uds,
        })
    }

    pub async fn new(uri: Uri, tls_cfg: ClientTlsConfig) -> Result<Self, tonic::transport::Error> {
        let channel = Channel::builder(uri)
            .tls_config(tls_cfg)
            .tcp_keepalive(Some(Duration::from_secs(5)))
            .connect()
            .await?;

        Ok(Filesystem {
            uuid: None,
            rpc_client: RfsClient::new(channel),
            client_kind: ClientKind::Rpc,
        })
    }

    pub fn mount<P: AsRef<Path>>(self, mount_point: P) -> io::Result<()> {
        let uid = unistd::getuid();
        let gid = unistd::getgid();

        let opts: Vec<_> = vec![
            format!("fsname=rfs-{:?}", self.client_kind),
            "nonempty".to_string(),
            "auto_cache".to_string(),
            format!("uid={}", uid),
            format!("gid={}", gid),
        ]
            .into_iter()
            .map(|opt| vec!["-o".to_string(), opt])
            .flatten()
            .collect();

        let opts: Vec<_> = opts.iter().map(|opt| opt.as_ref()).collect();

        fuse::mount(self, mount_point, &opts)
    }
}

impl FuseFilesystem for Filesystem {
    fn init(&mut self, _req: &Request) -> Result<(), libc::c_int> {
        block_on(async {
            let req = TonicRequest::new(RegisterRequest {});

            match self.rpc_client.register(req).await {
                Err(err) => {
                    error!("register failed {}", err);

                    return Err(libc::EINVAL);
                }

                Ok(resp) => {
                    let resp = resp.into_inner();

                    let uuid: Uuid = match resp.uuid.parse() {
                        Err(_) => return Err(libc::EINVAL),
                        Ok(uuid) => uuid,
                    };

                    self.uuid.replace(uuid);

                    Ok(())
                }
            }
        })
    }

    fn destroy(&mut self, _req: &Request) {
        let uuid = self
            .uuid
            .as_ref()
            .expect("uuid should initialize")
            .to_string();

        block_on(async {
            let req = TonicRequest::new(LogoutRequest { uuid });

            if let Err(err) = self.rpc_client.logout(req).await {
                error!("logout failed {}", err)
            }
        })
    }

    fn lookup(&mut self, req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name = match name.to_str() {
            None => {
                reply.error(libc::EINVAL);
                return;
            }

            Some(name) => name.to_string(),
        };

        let header = self.get_rpc_header();

        let mut client = self.rpc_client.clone();

        let rpc_req = TonicRequest::new(LookupRequest {
            head: header,
            inode: parent,
            name,
        });

        let uid = req.uid();
        let gid = req.gid();

        task::spawn(async move {
            let result = match client.lookup(rpc_req).await {
                Err(err) => {
                    error!("lookup rpc has error {}", err);
                    reply.error(libc::EIO);

                    return;
                }

                Ok(resp) => {
                    let resp = resp.into_inner();

                    if let Some(result) = resp.result {
                        result
                    } else {
                        error!("lookup result is None");
                        reply.error(libc::EIO);

                        return;
                    }
                }
            };

            match result {
                lookup_response::Result::Error(err) => reply.error(err.errno as i32),

                lookup_response::Result::Attr(attr) => {
                    match proto_attr_into_fuse_attr(attr, uid, gid) {
                        Err(err) => reply.error(err.into()),
                        Ok(attr) => reply.entry(&TTL, &attr, 0),
                    }
                }
            }
        });
    }

    fn getattr(&mut self, req: &Request, inode: u64, reply: ReplyAttr) {
        let header = self.get_rpc_header();

        let mut client = self.rpc_client.clone();

        let rpc_req = TonicRequest::new(GetAttrRequest {
            head: header,
            inode,
        });

        let uid = req.uid();
        let gid = req.gid();

        task::spawn(async move {
            let result = match client.get_attr(rpc_req).await {
                Err(err) => {
                    error!("getattr rpc has error {}", err);
                    reply.error(libc::EIO);

                    return;
                }

                Ok(resp) => {
                    let resp = resp.into_inner();

                    if let Some(result) = resp.result {
                        result
                    } else {
                        error!("getattr result is None");
                        reply.error(libc::EIO);

                        return;
                    }
                }
            };

            match result {
                get_attr_response::Result::Error(err) => reply.error(err.errno as i32),

                get_attr_response::Result::Attr(attr) => {
                    match proto_attr_into_fuse_attr(attr, uid, gid) {
                        Err(err) => reply.error(err.into()),
                        Ok(attr) => reply.attr(&TTL, &attr),
                    }
                }
            }
        });
    }

    fn setattr(
        &mut self,
        req: &Request,
        inode: u64,
        mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<SystemTime>,
        _mtime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        let header = self.get_rpc_header();

        let mut client = self.rpc_client.clone();

        let rpc_req = TonicRequest::new(SetAttrRequest {
            head: header,
            inode,
            attr: Some(Attr {
                inode,
                name: String::new(),
                mode: if let Some(mode) = mode {
                    mode as i32
                } else {
                    -1
                },
                size: if let Some(size) = size {
                    size as i64
                } else {
                    -1
                },
                r#type: 0,
                access_time: None,
                modify_time: None,
                change_time: None,
            }),
        });

        let uid = req.uid();
        let gid = req.gid();

        task::spawn(async move {
            let result = match client.set_attr(rpc_req).await {
                Err(err) => {
                    error!("setattr rpc has error {}", err);
                    reply.error(libc::EIO);

                    return;
                }

                Ok(resp) => {
                    if let Some(result) = resp.into_inner().result {
                        result
                    } else {
                        error!("setattr result is None");
                        reply.error(libc::EIO);

                        return;
                    }
                }
            };

            match result {
                set_attr_response::Result::Error(err) => {
                    error!("setattr failed errno {}", err.errno);

                    reply.error(err.errno as i32)
                }

                set_attr_response::Result::Attr(attr) => {
                    match proto_attr_into_fuse_attr(attr, uid, gid) {
                        Err(err) => reply.error(err.into()),
                        Ok(attr) => reply.attr(&TTL, &attr),
                    }
                }
            }
        });
    }

    fn mkdir(&mut self, req: &Request, parent: u64, name: &OsStr, mode: u32, reply: ReplyEntry) {
        let name = match name.to_str() {
            None => {
                reply.error(libc::EINVAL);
                return;
            }

            Some(name) => name.to_string(),
        };

        let header = self.get_rpc_header();

        let mut client = self.rpc_client.clone();

        let rpc_req = TonicRequest::new(MkdirRequest {
            head: header,
            inode: parent,
            name,
            mode,
        });

        let uid = req.uid();
        let gid = req.gid();

        task::spawn(async move {
            let result = match client.mkdir(rpc_req).await {
                Err(err) => {
                    error!("mkdir rpc has error {}", err);
                    reply.error(libc::EIO);

                    return;
                }

                Ok(resp) => {
                    if let Some(result) = resp.into_inner().result {
                        result
                    } else {
                        error!("mkdir result is None");
                        reply.error(libc::EIO);

                        return;
                    }
                }
            };

            match result {
                mkdir_response::Result::Error(err) => reply.error(err.errno as i32),

                mkdir_response::Result::Attr(attr) => {
                    match proto_attr_into_fuse_attr(attr, uid, gid) {
                        Err(err) => reply.error(err.into()),
                        Ok(attr) => reply.entry(&TTL, &attr, 0),
                    }
                }
            }
        });
    }

    fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let name = match name.to_str() {
            None => {
                reply.error(libc::EINVAL);
                return;
            }

            Some(name) => name.to_string(),
        };

        let header = self.get_rpc_header();

        let mut client = self.rpc_client.clone();

        let rpc_req = TonicRequest::new(UnlinkRequest {
            head: header,
            inode: parent,
            name,
        });

        task::spawn(async move {
            match client.unlink(rpc_req).await {
                Err(err) => {
                    error!("unlink rpc has error {}", err);
                    reply.error(libc::EIO);

                    return;
                }

                Ok(resp) => {
                    if let Some(error) = resp.into_inner().error {
                        reply.error(error.errno as c_int);
                        return;
                    } else {
                        reply.ok()
                    }
                }
            };
        });
    }

    fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let name = match name.to_str() {
            None => {
                reply.error(libc::EINVAL);
                return;
            }

            Some(name) => name.to_string(),
        };

        let header = self.get_rpc_header();

        let mut client = self.rpc_client.clone();

        let rpc_req = TonicRequest::new(RmDirRequest {
            head: header,
            inode: parent,
            name,
        });

        task::spawn(async move {
            match client.rm_dir(rpc_req).await {
                Err(err) => {
                    error!("rmdir rpc has error {}", err);
                    reply.error(libc::EIO);

                    return;
                }

                Ok(resp) => {
                    if let Some(error) = resp.into_inner().error {
                        reply.error(error.errno as c_int);
                        return;
                    } else {
                        reply.ok()
                    }
                }
            };
        });
    }

    fn rename(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        new_parent: u64,
        new_name: &OsStr,
        reply: ReplyEmpty,
    ) {
        let name = match name.to_str() {
            None => {
                reply.error(libc::EINVAL);
                return;
            }

            Some(name) => name.to_string(),
        };

        let new_name = match new_name.to_str() {
            None => {
                reply.error(libc::EINVAL);
                return;
            }

            Some(new_name) => new_name.to_string(),
        };

        let header = self.get_rpc_header();

        let mut client = self.rpc_client.clone();

        let rpc_req = TonicRequest::new(RenameRequest {
            head: header,
            old_parent: parent,
            old_name: name,
            new_parent,
            new_name,
        });

        task::spawn(async move {
            match client.rename(rpc_req).await {
                Err(err) => {
                    error!("rename rpc has error {}", err);
                    reply.error(libc::EIO);

                    return;
                }

                Ok(resp) => {
                    if let Some(error) = resp.into_inner().error {
                        reply.error(error.errno as c_int);
                        return;
                    } else {
                        reply.ok()
                    }
                }
            }
        });
    }

    fn open(&mut self, _req: &Request, inode: u64, flags: u32, reply: ReplyOpen) {
        let header = self.get_rpc_header();

        let mut client = self.rpc_client.clone();

        let rpc_req = TonicRequest::new(OpenFileRequest {
            head: header,
            inode,
            flags,
        });

        debug!("client open inode {} flags {}", inode, flags);

        task::spawn(async move {
            let result = match client.open_file(rpc_req).await {
                Err(err) => {
                    error!("open rpc has error {}", err);
                    reply.error(libc::EIO);

                    return;
                }

                Ok(resp) => {
                    if let Some(result) = resp.into_inner().result {
                        result
                    } else {
                        error!("open result is None");
                        reply.error(libc::EIO);

                        return;
                    }
                }
            };

            match result {
                open_file_response::Result::Error(err) => reply.error(err.errno as i32),

                open_file_response::Result::FileHandleId(fh_id) => reply.opened(fh_id, flags),
            }
        });
    }

    fn read(
        &mut self,
        _req: &Request,
        _ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        reply: ReplyData,
    ) {
        let header = self.get_rpc_header();

        let mut client = self.rpc_client.clone();

        let rpc_req = TonicRequest::new(ReadFileRequest {
            head: header,
            file_handle_id: fh,
            offset,
            size: size as u64,
        });

        task::spawn(async move {
            let result = match client.read_file(rpc_req).await {
                Err(err) => {
                    error!("read_file rpc has error {}", err);
                    reply.error(libc::EIO);

                    return;
                }

                Ok(resp) => {
                    if let Some(result) = resp.into_inner().result {
                        result
                    } else {
                        error!("open result is None");
                        reply.error(libc::EIO);

                        return;
                    }
                }
            };

            match result {
                read_file_response::Result::Error(err) => reply.error(err.errno as i32),

                read_file_response::Result::Data(data) => reply.data(&data),
            }
        });
    }

    fn write(
        &mut self,
        _req: &Request,
        _ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _flags: u32,
        reply: ReplyWrite,
    ) {
        let header = self.get_rpc_header();

        let mut client = self.rpc_client.clone();

        let rpc_req = TonicRequest::new(WriteFileRequest {
            head: header,
            file_handle_id: fh,
            offset,
            data: data.to_vec(),
        });

        task::spawn(async move {
            let result = match client.write_file(rpc_req).await {
                Err(err) => {
                    error!("write_file rpc has error {}", err);
                    reply.error(libc::EIO);

                    return;
                }

                Ok(resp) => {
                    if let Some(result) = resp.into_inner().result {
                        result
                    } else {
                        error!("open result is None");
                        reply.error(libc::EIO);

                        return;
                    }
                }
            };

            match result {
                write_file_response::Result::Error(err) => reply.error(err.errno as i32),

                write_file_response::Result::Written(written) => reply.written(written as u32),
            }
        });
    }

    fn flush(&mut self, _req: &Request, _ino: u64, fh: u64, _lock_owner: u64, reply: ReplyEmpty) {
        let header = self.get_rpc_header();

        let mut client = self.rpc_client.clone();

        let rpc_req = TonicRequest::new(FlushRequest {
            head: header,
            file_handle_id: fh,
        });

        task::spawn(async move {
            match client.flush(rpc_req).await {
                Err(err) => {
                    error!("flush rpc has error {}", err);
                    reply.error(libc::EIO);

                    return;
                }

                Ok(resp) => {
                    if let Some(err) = resp.into_inner().error {
                        reply.error(err.errno as c_int)
                    } else {
                        reply.ok()
                    }
                }
            }
        });
    }

    fn release(
        &mut self,
        _req: &Request,
        _ino: u64,
        fh: u64,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let header = self.get_rpc_header();

        let mut client = self.rpc_client.clone();

        let rpc_req = TonicRequest::new(CloseFileRequest {
            head: header,
            file_handle_id: fh,
        });

        task::spawn(async move {
            match client.close_file(rpc_req).await {
                Err(err) => {
                    error!("close_file rpc has error {}", err);
                    reply.error(libc::EIO);

                    return;
                }

                Ok(resp) => {
                    if let Some(err) = resp.into_inner().error {
                        reply.error(err.errno as c_int)
                    } else {
                        reply.ok()
                    }
                }
            }
        });
    }

    fn fsync(&mut self, _req: &Request, _inode: u64, fh: u64, _datasync: bool, reply: ReplyEmpty) {
        let header = self.get_rpc_header();

        let mut client = self.rpc_client.clone();

        let rpc_req = TonicRequest::new(SyncFileRequest {
            head: header,
            file_handle_id: fh,
        });

        task::spawn(async move {
            match client.sync_file(rpc_req).await {
                Err(err) => {
                    error!("sync_file rpc has error {}", err);
                    reply.error(libc::EIO);

                    return;
                }

                Ok(resp) => {
                    if let Some(err) = resp.into_inner().error {
                        reply.error(err.errno as c_int)
                    } else {
                        reply.ok()
                    }
                }
            }
        });
    }

    fn readdir(
        &mut self,
        _req: &Request,
        inode: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        debug!("readdir inode {}, offset {}", inode, offset);

        let header = self.get_rpc_header();

        let mut client = self.rpc_client.clone();

        let rpc_req = TonicRequest::new(ReadDirRequest {
            head: header,
            inode,
        });

        task::spawn(async move {
            let dir_entries: Vec<read_dir_response::DirEntry> = match client.read_dir(rpc_req).await
            {
                Err(err) => {
                    error!("read_dir rpc has error {}", err);
                    reply.error(libc::EIO);

                    return;
                }

                Ok(resp) => {
                    let resp = resp.into_inner();

                    if let Some(error) = resp.error {
                        reply.error(error.errno as c_int);
                        return;
                    }

                    resp.dir_entries
                }
            };

            debug!("got readdir result");

            for (inode, index, kind, name) in dir_entries
                .into_iter()
                .enumerate()
                .skip(offset as usize)
                .map(|(index, dir_entry)| {
                    let dir = EntryType::Dir as i32;
                    let file = EntryType::File as i32;

                    let kind = if dir_entry.r#type == dir {
                        Some(FileType::Directory)
                    } else if dir_entry.r#type == file {
                        Some(FileType::RegularFile)
                    } else {
                        error!("unexpect file type {}", dir_entry.r#type);

                        None
                    };

                    (dir_entry.inode, index + 1, kind, dir_entry.name)
                })
            {
                let kind = match kind {
                    None => {
                        error!(
                            "unexpect file type in inode {}, index {}, name {}",
                            inode, index, name
                        );

                        // we got unknown entry type, it should not happened
                        reply.error(libc::EIO);
                        return;
                    }
                    Some(kind) => kind,
                };

                debug!(
                    "file type {:?}, inode {}, index {}, name {}",
                    kind, inode, index, name
                );

                if reply.add(inode, index as i64, kind, name) {
                    break;
                }
            }

            reply.ok();

            debug!("readdir success")
        });
    }

    // TODO next version to handle it
    fn access(&mut self, _req: &Request, _ino: u64, _mask: u32, reply: ReplyEmpty) {
        reply.ok()
    }

    fn create(
        &mut self,
        req: &Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        flags: u32,
        reply: ReplyCreate,
    ) {
        let name = match name.to_str() {
            None => {
                reply.error(libc::EINVAL);
                return;
            }

            Some(name) => name.to_string(),
        };

        let header = self.get_rpc_header();

        let mut client = self.rpc_client.clone();

        let rpc_req = TonicRequest::new(CreateFileRequest {
            head: header,
            inode: parent,
            name,
            mode,
            flags,
        });

        let uid = req.uid();
        let gid = req.gid();

        task::spawn(async move {
            let (fh_id, attr) = match client.create_file(rpc_req).await {
                Err(err) => {
                    error!("create_file rpc has error {}", err);
                    reply.error(libc::EIO);

                    return;
                }

                Ok(resp) => {
                    let resp = resp.into_inner();

                    if let Some(error) = resp.error {
                        reply.error(error.errno as c_int);
                        return;
                    }

                    if resp.attr.is_none() {
                        error!("create_file attr is None");
                        reply.error(libc::EIO);

                        return;
                    }

                    (resp.file_handle_id, resp.attr.unwrap())
                }
            };

            match proto_attr_into_fuse_attr(attr, uid, gid) {
                Err(err) => reply.error(err.into()),
                Ok(attr) => reply.created(&TTL, &attr, 0, fh_id, flags),
            }
        });
    }

    fn getlk(
        &mut self,
        _req: &Request,
        _ino: u64,
        fh: u64,
        _lock_owner: u64,
        start: u64,
        end: u64,
        _typ: u32,
        pid: u32,
        reply: ReplyLock,
    ) {
        let header = self.get_rpc_header();

        let mut client = self.rpc_client.clone();

        let rpc_req = TonicRequest::new(GetLockRequest {
            head: header,
            file_handle_id: fh,
        });

        task::spawn(async move {
            let result = match client.get_lock(rpc_req).await {
                Err(err) => {
                    error!("get_lock rpc has error {}", err);
                    reply.error(libc::EIO);

                    return;
                }

                Ok(resp) => {
                    if let Some(result) = resp.into_inner().result {
                        result
                    } else {
                        error!("get_lock result is None");
                        reply.error(libc::EIO);

                        return;
                    }
                }
            };

            match result {
                get_lock_response::Result::Error(err) => reply.error(err.errno as i32),

                get_lock_response::Result::LockType(lock_type) => {
                    let lock_type = match lock_type {
                        n if n == LockType::ReadLock as i32 => libc::F_RDLCK,
                        n if n == LockType::WriteLock as i32 => libc::F_WRLCK,
                        n if n == LockType::NoLock as i32 => libc::F_UNLCK, // TODO is it right way?
                        _ => {
                            error!("unknown lock type {}", lock_type);

                            reply.error(libc::EIO);

                            return;
                        }
                    };

                    reply.locked(start, end, lock_type as u32, pid);
                }
            }
        });
    }

    fn setlk(
        &mut self,
        req: &Request,
        _ino: u64,
        fh: u64,
        _lock_owner: u64,
        _start: u64,
        _end: u64,
        typ: u32,
        _pid: u32,
        sleep: bool,
        reply: ReplyEmpty,
    ) {
        let header = self.get_rpc_header();

        let mut client = self.rpc_client.clone();

        if typ as i32 == libc::F_UNLCK {
            let rpc_req = TonicRequest::new(ReleaseLockRequest {
                head: header,
                file_handle_id: fh,
                block: false,
            });

            task::spawn(async move {
                match client.release_lock(rpc_req).await {
                    Err(err) => {
                        error!("release_lock rpc has error {}", err);
                        reply.error(libc::EIO);

                        return;
                    }

                    Ok(resp) => {
                        if let Some(error) = resp.into_inner().error {
                            reply.error(error.errno as c_int);
                            return;
                        } else {
                            reply.ok()
                        }
                    }
                }
            });

            return;
        }

        let lock_kind = {
            match typ as i32 {
                libc::F_RDLCK => LockType::ReadLock,
                libc::F_WRLCK => LockType::WriteLock,

                _ => {
                    reply.error(libc::EINVAL);
                    return;
                }
            }
        };

        let unique = req.unique();

        let rpc_req = TonicRequest::new(SetLockRequest {
            head: header,
            file_handle_id: fh,
            unique,
            lock_kind: lock_kind.into(),
            block: sleep,
        });

        task::spawn(async move {
            match client.set_lock(rpc_req).await {
                Err(err) => {
                    error!("set_lock rpc has error {}", err);
                    reply.error(libc::EIO);

                    return;
                }

                Ok(resp) => {
                    if let Some(error) = resp.into_inner().error {
                        warn!(
                            "set lock failed, uiqueue {} errno is {}",
                            unique, error.errno
                        );

                        reply.error(error.errno as c_int)
                    } else {
                        reply.ok()
                    }
                }
            }
        });
    }

    fn interrupt(&mut self, _req: &Request, unique: u64, reply: ReplyEmpty) {
        let header = self.get_rpc_header();

        let mut client = self.rpc_client.clone();

        let rpc_req = TonicRequest::new(InterruptRequest {
            head: header,
            unique,
        });

        debug!("interrupt unique {}", unique);

        task::spawn(async move {
            match client.interrupt(rpc_req).await {
                Err(err) => {
                    error!("interrupt rpc has error {}", err);
                    reply.error(libc::EIO);

                    return;
                }

                Ok(resp) => {
                    if let Some(err) = resp.into_inner().error {
                        reply.error(err.errno as c_int)
                    } else {
                        reply.ok()
                    }
                }
            }
        });
    }
}
