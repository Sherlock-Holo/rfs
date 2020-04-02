use std::ffi::OsStr;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::Result;
use async_notify::Notify;
use async_signals::Signals;
use async_std::future::timeout;
use async_std::sync;
use async_std::sync::{RwLock, Sender};
use async_std::task;
use fuse::{
    FileType, Filesystem as FuseFilesystem, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory,
    ReplyEmpty, ReplyEntry, ReplyLock, ReplyOpen, ReplyWrite, Request,
};
use futures_util::StreamExt;
use libc::c_int;
use log::{debug, error, info, warn};
use nix::mount;
use nix::mount::MntFlags;
use nix::unistd;
use snap::read::FrameDecoder;
use snap::write::FrameEncoder;
use tonic::transport::ClientTlsConfig;
use tonic::transport::{Channel, Uri};
use tonic::{Code, Request as TonicRequest};
use uuid::Uuid;

use crate::helper::proto_attr_into_fuse_attr;
use crate::pb::rfs_client::RfsClient;
use crate::pb::*;

const TTL: Duration = Duration::from_secs(1);
const INITIAL_TIMEOUT: Duration = Duration::from_secs(10);
const MULTIPLIER: f64 = 1.5;
const MIN_COMPRESS_SIZE: usize = 2048;

pub struct Filesystem {
    uuid: Option<Uuid>,
    client: Arc<RwLock<RfsClient<Channel>>>,
    id_sender: Option<Sender<Uuid>>,
    tokio_handle: Option<tokio::runtime::Handle>,
    failed_notify: Option<Notify>,
    compress: bool,
    uri: Uri,
    tls_cfg: ClientTlsConfig,
}

impl Filesystem {
    pub async fn new(
        uri: Uri,
        tls_cfg: ClientTlsConfig,
        handle: tokio::runtime::Handle,
        compress: bool,
    ) -> Result<Self> {
        if compress {
            info!("try to enable compress");
        }

        info!("connecting server");

        let channel = Channel::builder(uri.clone())
            .tls_config(tls_cfg.clone())
            .tcp_keepalive(Some(Duration::from_secs(5)))
            .connect()
            .await?;

        info!("server connected");

        let client = Arc::new(RwLock::new(RfsClient::new(channel)));

        Ok(Filesystem {
            uuid: None,
            client,
            id_sender: None,
            tokio_handle: Some(handle),
            failed_notify: Some(Notify::new()),
            compress,
            uri,
            tls_cfg,
        })
    }

    pub async fn mount<P: AsRef<Path>>(mut self, mount_point: P) -> Result<()> {
        let uid = unistd::getuid();
        let gid = unistd::getgid();

        let opts: Vec<_> = vec![
            "fsname=rfs".to_string(),
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

        let mut stop_signal = Signals::new(vec![libc::SIGINT, libc::SIGTERM])?;

        let mount_point = mount_point.as_ref();

        let unmount_point = mount_point.to_path_buf();

        task::spawn(async move {
            stop_signal.next().await;

            drop(stop_signal); // in case release signal handle

            info!("stopping rfs");

            while let Err(err) = mount::umount2(&unmount_point, MntFlags::MNT_DETACH) {
                error!("lazy unmount failed {}", err);

                task::sleep(Duration::from_secs(1)).await;
            }
        });

        let (sender, receiver) = sync::channel(1);

        self.id_sender.replace(sender);

        let client = self.client.clone();

        fuse::mount(self, mount_point, &opts)?;

        if let Some(uuid) = receiver.recv().await {
            let req = TonicRequest::new(LogoutRequest {
                uuid: uuid.to_hyphenated().to_string(),
            });

            info!("sending logout request");

            let mut client = client.read().await.clone();

            match timeout(Duration::from_secs(10), client.logout(req)).await {
                Err(err) => {
                    error!("logout timeout {}", err);

                    std::process::exit(1);
                }

                Ok(result) => {
                    if let Err(err) = result {
                        error!("logout failed {}", err);

                        std::process::exit(1);
                    }
                }
            }

            info!("logout success")
        }

        // ensure rfs unmount
        let _ = mount::umount2(mount_point, MntFlags::MNT_DETACH);

        Ok(())
    }

    fn get_rpc_header(&self) -> Header {
        let uuid = self
            .uuid
            .expect("uuid must be initialize")
            .as_bytes()
            .to_vec();

        Header {
            uuid,
            version: VERSION.to_string(),
        }
    }

    async fn reconnect_loop(
        client: Arc<RwLock<RfsClient<Channel>>>,
        uri: Uri,
        tls_cfg: ClientTlsConfig,
        handle: tokio::runtime::Handle,
        failed_notify: Notify,
    ) {
        loop {
            failed_notify.notified().await;

            warn!("rpc failed, need reconnect");

            let channel = loop {
                let uri = uri.clone();
                let tls_cfg = tls_cfg.clone();

                match handle
                    .spawn(async move {
                        Channel::builder(uri)
                            .tls_config(tls_cfg)
                            .tcp_keepalive(Some(Duration::from_secs(5)))
                            .connect()
                            .await
                    })
                    .await
                    .expect("tokio task panic")
                {
                    Err(err) => {
                        error!("reconnect failed {}", err);

                        task::sleep(Duration::from_millis(500)).await;

                        continue;
                    }

                    Ok(channel) => break channel,
                };
            };

            *client.write().await = RfsClient::new(channel);

            info!("reconnect success");
        }
    }

    async fn ping_loop(client: Arc<RwLock<RfsClient<Channel>>>, uuid: Uuid, failed_notify: Notify) {
        let mut rpc_timeout = INITIAL_TIMEOUT;

        'outer: loop {
            for _ in 0..3 {
                task::sleep(Duration::from_secs(60)).await;

                let ping_req = TonicRequest::new(PingRequest {
                    header: Some(Header {
                        uuid: uuid.as_bytes().to_vec(),
                        version: VERSION.to_string(),
                    }),
                });

                let mut client = client.read().await.clone();

                if timeout(rpc_timeout, client.ping(ping_req)).await.is_ok() {
                    rpc_timeout = INITIAL_TIMEOUT;

                    debug!("sent ping message");

                    continue 'outer;
                } else {
                    rpc_timeout = rpc_timeout.mul_f64(MULTIPLIER);
                }
            }

            // ping failed 3 times, reset rpc timeout and notify to reconnect,
            // wait for next ping round
            rpc_timeout = INITIAL_TIMEOUT;

            failed_notify.notify();
        }
    }
}

impl FuseFilesystem for Filesystem {
    fn init(&mut self, _req: &Request) -> Result<(), libc::c_int> {
        task::block_on(async {
            let mut client = self.client.read().await.clone();

            for _ in 0..3 {
                let req = TonicRequest::new(RegisterRequest {
                    support_compress: self.compress,
                });

                return match client.register(req).await {
                    Err(err) => {
                        if code_can_retry(err.code()) {
                            warn!("register failed {}", err);

                            task::sleep(Duration::from_secs(1)).await;

                            continue;
                        }

                        error!("register failed {}", err);

                        Err(libc::EIO)
                    }

                    Ok(resp) => {
                        let resp = resp.into_inner();

                        let uuid = if let Ok(uuid) = Uuid::from_slice(&resp.uuid) {
                            uuid
                        } else {
                            return Err(libc::EINVAL);
                        };

                        self.uuid.replace(uuid);

                        // in case server report allow_compress when client disable compress
                        self.compress = self.compress && resp.allow_compress;

                        if self.compress {
                            info!("compress enabled");
                        }

                        self.id_sender
                            .as_ref()
                            .expect("id sender must be initialize")
                            .send(uuid)
                            .await;

                        let failed_notify =
                            self.failed_notify.clone().expect("not None in rpc mode");

                        let uri = self.uri.clone();
                        let tls_cfg = self.tls_cfg.clone();
                        let handle = self.tokio_handle.take().expect("not None in rpc mode");

                        task::spawn(Self::ping_loop(
                            self.client.clone(),
                            uuid,
                            failed_notify.clone(),
                        ));

                        task::spawn(Self::reconnect_loop(
                            self.client.clone(),
                            uri,
                            tls_cfg,
                            handle,
                            failed_notify,
                        ));

                        Ok(())
                    }
                };
            }

            error!("register fails more than 3 times");

            Err(libc::ETIMEDOUT)
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

        let client = self.client.clone();

        let uid = req.uid();
        let gid = req.gid();

        let failed_notify = self.failed_notify.clone();

        task::spawn(async move {
            let mut rpc_timeout = INITIAL_TIMEOUT;

            for _ in 0..3 {
                let rpc_req = TonicRequest::new(LookupRequest {
                    head: Some(header.clone()),
                    inode: parent,
                    name: name.to_string(),
                });

                let mut client = client.read().await.clone();

                let result = match timeout(rpc_timeout, client.lookup(rpc_req)).await {
                    Err(err) => {
                        warn!("lookup rpc timeout {}", err);

                        rpc_timeout = rpc_timeout.mul_f64(MULTIPLIER);

                        continue;
                    }

                    Ok(result) => result,
                };

                let result = match result {
                    Err(err) => {
                        if code_can_retry(err.code()) {
                            warn!("lookup rpc has error {}", err);

                            task::sleep(Duration::from_secs(1)).await;

                            continue;
                        }

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

                return;
            }

            error!("lookup failed more than 3 times");
            reply.error(libc::ETIMEDOUT);

            if let Some(failed_notify) = &failed_notify {
                failed_notify.notify();
            }
        });
    }

    fn getattr(&mut self, req: &Request, inode: u64, reply: ReplyAttr) {
        let header = self.get_rpc_header();

        let client = self.client.clone();

        let uid = req.uid();
        let gid = req.gid();

        let failed_notify = self.failed_notify.clone();

        task::spawn(async move {
            let mut rpc_timeout = INITIAL_TIMEOUT;

            for _ in 0..3 {
                let rpc_req = TonicRequest::new(GetAttrRequest {
                    head: Some(header.clone()),
                    inode,
                });

                let mut client = client.read().await.clone();

                let result = match timeout(rpc_timeout, client.get_attr(rpc_req)).await {
                    Err(err) => {
                        warn!("getattr rpc timeout {}", err);

                        rpc_timeout = rpc_timeout.mul_f64(MULTIPLIER);

                        continue;
                    }

                    Ok(result) => result,
                };

                let result = match result {
                    Err(err) => {
                        if code_can_retry(err.code()) {
                            warn!("getattr rpc has error {}", err);

                            task::sleep(Duration::from_secs(1)).await;

                            continue;
                        }

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

                return;
            }

            error!("getattr failed more than 3 times");
            reply.error(libc::ETIMEDOUT);

            if let Some(failed_notify) = &failed_notify {
                failed_notify.notify();
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

        let client = self.client.clone();

        let uid = req.uid();
        let gid = req.gid();

        let failed_notify = self.failed_notify.clone();

        task::spawn(async move {
            let mut rpc_timeout = INITIAL_TIMEOUT;

            for _ in 0..3 {
                let rpc_req = TonicRequest::new(SetAttrRequest {
                    head: Some(header.clone()),
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

                let mut client = client.read().await.clone();

                let result = match timeout(rpc_timeout, client.set_attr(rpc_req)).await {
                    Err(err) => {
                        warn!("setattr rpc timeout {}", err);

                        rpc_timeout = rpc_timeout.mul_f64(MULTIPLIER);

                        continue;
                    }

                    Ok(result) => result,
                };

                let result = match result {
                    Err(err) => {
                        if code_can_retry(err.code()) {
                            warn!("setattr rpc has error {}", err);

                            task::sleep(Duration::from_secs(1)).await;

                            continue;
                        }

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

                return;
            }

            error!("setattr failed more than 3 times");
            reply.error(libc::ETIMEDOUT);

            if let Some(failed_notify) = &failed_notify {
                failed_notify.notify();
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

        let client = self.client.clone();

        let failed_notify = self.failed_notify.clone();

        let uid = req.uid();
        let gid = req.gid();

        task::spawn(async move {
            let mut rpc_timeout = INITIAL_TIMEOUT;

            for _ in 0..3 {
                let rpc_req = TonicRequest::new(MkdirRequest {
                    head: Some(header.clone()),
                    inode: parent,
                    name: name.to_string(),
                    mode,
                });

                let mut client = client.read().await.clone();

                let result = match timeout(rpc_timeout, client.mkdir(rpc_req)).await {
                    Err(err) => {
                        warn!("mkdir rpc timeout {}", err);

                        rpc_timeout = rpc_timeout.mul_f64(MULTIPLIER);

                        continue;
                    }

                    Ok(result) => result,
                };

                let result = match result {
                    Err(err) => {
                        if code_can_retry(err.code()) {
                            warn!("mkdir rpc has error {}", err);

                            task::sleep(Duration::from_secs(1)).await;

                            continue;
                        }

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

                return;
            }

            error!("mkdir failed more than 3 times");
            reply.error(libc::ETIMEDOUT);

            if let Some(failed_notify) = &failed_notify {
                failed_notify.notify()
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

        let client = self.client.clone();

        let failed_notify = self.failed_notify.clone();

        task::spawn(async move {
            let mut rpc_timeout = INITIAL_TIMEOUT;

            for _ in 0..3 {
                let rpc_req = TonicRequest::new(UnlinkRequest {
                    head: Some(header.clone()),
                    inode: parent,
                    name: name.to_string(),
                });

                let mut client = client.read().await.clone();

                let result = match timeout(rpc_timeout, client.unlink(rpc_req)).await {
                    Err(err) => {
                        warn!("unlink rpc timeout {}", err);

                        rpc_timeout = rpc_timeout.mul_f64(MULTIPLIER);

                        continue;
                    }

                    Ok(result) => result,
                };

                match result {
                    Err(err) => {
                        if code_can_retry(err.code()) {
                            warn!("unlink rpc has error {}", err);

                            task::sleep(Duration::from_secs(1)).await;

                            continue;
                        }

                        error!("unlink rpc has error {}", err);
                        reply.error(libc::EIO);

                        return;
                    }

                    Ok(resp) => {
                        if let Some(error) = resp.into_inner().error {
                            reply.error(error.errno as c_int);
                        } else {
                            reply.ok()
                        }

                        return;
                    }
                };
            }

            error!("unlink failed more than 3 times");
            reply.error(libc::ETIMEDOUT);

            if let Some(failed_notify) = &failed_notify {
                failed_notify.notify()
            }
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

        let client = self.client.clone();

        let failed_notify = self.failed_notify.clone();

        task::spawn(async move {
            let mut rpc_timeout = INITIAL_TIMEOUT;

            for _ in 0..3 {
                let rpc_req = TonicRequest::new(RmDirRequest {
                    head: Some(header.clone()),
                    inode: parent,
                    name: name.to_string(),
                });

                let mut client = client.read().await.clone();

                let result = match timeout(rpc_timeout, client.rm_dir(rpc_req)).await {
                    Err(err) => {
                        warn!("rmdir rpc timeout {}", err);

                        rpc_timeout = rpc_timeout.mul_f64(MULTIPLIER);

                        continue;
                    }

                    Ok(result) => result,
                };

                match result {
                    Err(err) => {
                        if code_can_retry(err.code()) {
                            warn!("rmdir rpc has error {}", err);

                            task::sleep(Duration::from_secs(1)).await;

                            continue;
                        }

                        error!("rmdir rpc has error {}", err);
                        reply.error(libc::EIO);

                        return;
                    }

                    Ok(resp) => {
                        if let Some(error) = resp.into_inner().error {
                            reply.error(error.errno as c_int);
                        } else {
                            reply.ok()
                        }

                        return;
                    }
                };
            }

            error!("rmdir failed more than 3 times");
            reply.error(libc::ETIMEDOUT);

            if let Some(failed_notify) = &failed_notify {
                failed_notify.notify()
            }
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

        let client = self.client.clone();

        let failed_notify = self.failed_notify.clone();

        task::spawn(async move {
            let mut rpc_timeout = INITIAL_TIMEOUT;

            for _ in 0..3 {
                let rpc_req = TonicRequest::new(RenameRequest {
                    head: Some(header.clone()),
                    old_parent: parent,
                    old_name: name.to_string(),
                    new_parent,
                    new_name: new_name.to_string(),
                });

                let mut client = client.read().await.clone();

                let result = match timeout(rpc_timeout, client.rename(rpc_req)).await {
                    Err(err) => {
                        warn!("rename rpc timeout {}", err);

                        rpc_timeout = rpc_timeout.mul_f64(MULTIPLIER);

                        continue;
                    }

                    Ok(result) => result,
                };

                match result {
                    Err(err) => {
                        if code_can_retry(err.code()) {
                            warn!("rename rpc has error {}", err);

                            task::sleep(Duration::from_secs(1)).await;

                            continue;
                        }

                        error!("rename rpc has error {}", err);
                        reply.error(libc::EIO);

                        return;
                    }

                    Ok(resp) => {
                        if let Some(error) = resp.into_inner().error {
                            reply.error(error.errno as c_int);
                        } else {
                            reply.ok()
                        }

                        return;
                    }
                }
            }

            error!("rename failed more than 3 times");
            reply.error(libc::ETIMEDOUT);

            if let Some(failed_notify) = &failed_notify {
                failed_notify.notify()
            }
        });
    }

    fn open(&mut self, _req: &Request, inode: u64, flags: u32, reply: ReplyOpen) {
        let header = self.get_rpc_header();

        let client = self.client.clone();

        let failed_notify = self.failed_notify.clone();

        debug!("client open inode {} flags {}", inode, flags);

        task::spawn(async move {
            let mut rpc_timeout = INITIAL_TIMEOUT;

            for _ in 0..3 {
                let rpc_req = TonicRequest::new(OpenFileRequest {
                    head: Some(header.clone()),
                    inode,
                    flags,
                });

                let mut client = client.read().await.clone();

                let result = match timeout(rpc_timeout, client.open_file(rpc_req)).await {
                    Err(err) => {
                        warn!("open file rpc timeout {}", err);

                        rpc_timeout = rpc_timeout.mul_f64(MULTIPLIER);

                        continue;
                    }

                    Ok(result) => result,
                };

                let result = match result {
                    Err(err) => {
                        if code_can_retry(err.code()) {
                            warn!("open rpc has error {}", err);

                            task::sleep(Duration::from_secs(1)).await;

                            continue;
                        }

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

                return;
            }

            error!("open failed more than 3 times");
            reply.error(libc::ETIMEDOUT);

            if let Some(failed_notify) = &failed_notify {
                failed_notify.notify()
            };
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

        let client = self.client.clone();

        let failed_notify = self.failed_notify.clone();

        task::spawn(async move {
            let mut rpc_timeout = INITIAL_TIMEOUT;

            for _ in 0..3 {
                let rpc_req = TonicRequest::new(ReadFileRequest {
                    head: Some(header.clone()),
                    file_handle_id: fh,
                    offset,
                    size: size as u64,
                });

                let mut client = client.read().await.clone();

                let result = match timeout(rpc_timeout, client.read_file(rpc_req)).await {
                    Err(err) => {
                        warn!("read_file rpc timeout {}", err);

                        rpc_timeout = rpc_timeout.mul_f64(MULTIPLIER);

                        continue;
                    }

                    Ok(result) => result,
                };

                match result {
                    Err(err) => {
                        if code_can_retry(err.code()) {
                            warn!("read_file rpc has error {}", err);

                            task::sleep(Duration::from_secs(1)).await;

                            continue;
                        }

                        error!("read_file rpc has error {}", err);
                        reply.error(libc::EIO);

                        return;
                    }

                    Ok(resp) => {
                        let result = resp.into_inner();

                        if let Some(err) = result.error {
                            reply.error(err.errno as i32);

                            return;
                        }

                        if result.compressed {
                            match task::spawn_blocking(move || {
                                let mut decoder = FrameDecoder::new(result.data.as_slice());

                                let mut data = Vec::with_capacity(result.data.len());

                                if let Err(err) = decoder.read_to_end(&mut data) {
                                    error!("decompress read data failed {}", err);

                                    return Err(libc::EIO);
                                }

                                Ok(data)
                            })
                            .await
                            {
                                Err(err) => reply.error(err),
                                Ok(data) => reply.data(&data),
                            }
                        } else {
                            reply.data(&result.data);
                        }

                        return;
                    }
                }
            }

            error!("read_file failed more than 3 times");
            reply.error(libc::ETIMEDOUT);

            if let Some(failed_notify) = &failed_notify {
                failed_notify.notify()
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

        let client = self.client.clone();

        let failed_notify = self.failed_notify.clone();

        let data = data.to_vec();

        let enable_compress = self.compress;

        task::spawn(async move {
            let (data, compressed) = task::spawn_blocking(move || {
                if enable_compress && data.len() > MIN_COMPRESS_SIZE {
                    let mut encoder = FrameEncoder::new(Vec::with_capacity(MIN_COMPRESS_SIZE)); // should I choose a better size?

                    if let Err(err) = encoder.write_all(&data) {
                        warn!("compress write data failed {}", err);

                        (data.to_vec(), false)
                    } else {
                        match encoder.into_inner() {
                            Err(err) => {
                                warn!("get compress data failed {}", err);

                                (data.to_vec(), false)
                            }

                            Ok(compressed_data) => {
                                // sometimes compressed data is bigger than original data, so we should
                                // use original data directly
                                if compressed_data.len() < data.len() {
                                    (compressed_data, true)
                                } else {
                                    debug!("compress is bad");

                                    (data.to_vec(), false)
                                }
                            }
                        }
                    }
                } else {
                    (data.to_vec(), false)
                }
            })
            .await;

            let mut rpc_timeout = INITIAL_TIMEOUT;

            for _ in 0..3 {
                let rpc_req = TonicRequest::new(WriteFileRequest {
                    head: Some(header.clone()),
                    file_handle_id: fh,
                    offset,
                    data: data.clone(),
                    compressed,
                });

                let mut client = client.read().await.clone();

                let result = match timeout(rpc_timeout, client.write_file(rpc_req)).await {
                    Err(err) => {
                        warn!("write file rpc timeout {}", err);

                        rpc_timeout = rpc_timeout.mul_f64(MULTIPLIER);

                        continue;
                    }

                    Ok(result) => result,
                };

                let result = match result {
                    Err(err) => {
                        if code_can_retry(err.code()) {
                            warn!("write_file rpc has error {}", err);

                            task::sleep(Duration::from_secs(1)).await;

                            continue;
                        }

                        error!("write_file rpc has error {}", err);
                        reply.error(libc::EIO);

                        return;
                    }

                    Ok(resp) => {
                        if let Some(result) = resp.into_inner().result {
                            result
                        } else {
                            error!("write_file result is None");
                            reply.error(libc::EIO);

                            return;
                        }
                    }
                };

                match result {
                    write_file_response::Result::Error(err) => reply.error(err.errno as i32),

                    write_file_response::Result::Written(written) => reply.written(written as u32),
                }

                return;
            }

            error!("write_file failed more than 3 times");
            reply.error(libc::ETIMEDOUT);

            if let Some(failed_notify) = &failed_notify {
                failed_notify.notify()
            }
        });
    }

    fn flush(&mut self, _req: &Request, _ino: u64, fh: u64, _lock_owner: u64, reply: ReplyEmpty) {
        let header = self.get_rpc_header();

        let client = self.client.clone();

        let failed_notify = self.failed_notify.clone();

        task::spawn(async move {
            let mut rpc_timeout = INITIAL_TIMEOUT;

            for _ in 0..3 {
                let rpc_req = TonicRequest::new(FlushRequest {
                    head: Some(header.clone()),
                    file_handle_id: fh,
                });

                let mut client = client.read().await.clone();

                let result = match timeout(rpc_timeout, client.flush(rpc_req)).await {
                    Err(err) => {
                        warn!("flush rpc timeout {}", err);

                        rpc_timeout = rpc_timeout.mul_f64(MULTIPLIER);

                        continue;
                    }

                    Ok(result) => result,
                };

                match result {
                    Err(err) => {
                        if code_can_retry(err.code()) {
                            warn!("flush rpc has error {}", err);

                            task::sleep(Duration::from_secs(1)).await;

                            continue;
                        }

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

                        return;
                    }
                }
            }

            error!("flush failed more than 3 times");
            reply.error(libc::ETIMEDOUT);

            if let Some(failed_notify) = &failed_notify {
                failed_notify.notify()
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

        let client = self.client.clone();

        let failed_notify = self.failed_notify.clone();

        task::spawn(async move {
            let mut rpc_timeout = INITIAL_TIMEOUT;

            for _ in 0..3 {
                let rpc_req = TonicRequest::new(CloseFileRequest {
                    head: Some(header.clone()),
                    file_handle_id: fh,
                });

                let mut client = client.read().await.clone();

                let result = match timeout(rpc_timeout, client.close_file(rpc_req)).await {
                    Err(err) => {
                        warn!("close file rpc timeout {}", err);

                        rpc_timeout = rpc_timeout.mul_f64(MULTIPLIER);

                        continue;
                    }

                    Ok(result) => result,
                };

                match result {
                    Err(err) => {
                        if code_can_retry(err.code()) {
                            warn!("close_file rpc has error {}", err);

                            task::sleep(Duration::from_secs(1)).await;

                            continue;
                        }

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

                        return;
                    }
                }
            }

            error!("close_file failed more than 3 times");
            reply.error(libc::ETIMEDOUT);

            if let Some(failed_notify) = &failed_notify {
                failed_notify.notify()
            }
        });
    }

    fn fsync(&mut self, _req: &Request, _inode: u64, fh: u64, _datasync: bool, reply: ReplyEmpty) {
        let header = self.get_rpc_header();

        let client = self.client.clone();

        let failed_notify = self.failed_notify.clone();

        task::spawn(async move {
            let mut rpc_timeout = INITIAL_TIMEOUT;

            for _ in 0..3 {
                let rpc_req = TonicRequest::new(SyncFileRequest {
                    head: Some(header.clone()),
                    file_handle_id: fh,
                });

                let mut client = client.read().await.clone();

                let result = match timeout(rpc_timeout, client.sync_file(rpc_req)).await {
                    Err(err) => {
                        warn!("sync file rpc timeout {}", err);

                        rpc_timeout = rpc_timeout.mul_f64(MULTIPLIER);

                        continue;
                    }

                    Ok(result) => result,
                };

                match result {
                    Err(err) => {
                        if code_can_retry(err.code()) {
                            warn!("sync_file rpc has error {}", err);

                            task::sleep(Duration::from_secs(1)).await;

                            continue;
                        }

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

                        return;
                    }
                }
            }

            error!("sync_file failed more than 3 times");
            reply.error(libc::ETIMEDOUT);

            if let Some(failed_notify) = &failed_notify {
                failed_notify.notify()
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

        let client = self.client.clone();

        let failed_notify = self.failed_notify.clone();

        task::spawn(async move {
            let mut rpc_timeout = INITIAL_TIMEOUT;

            for _ in 0..3 {
                let rpc_req = TonicRequest::new(ReadDirRequest {
                    head: Some(header.clone()),
                    inode,
                });

                let mut client = client.read().await.clone();

                let result = match timeout(rpc_timeout, client.read_dir(rpc_req)).await {
                    Err(err) => {
                        warn!("readdir rpc timeout {}", err);

                        rpc_timeout = rpc_timeout.mul_f64(MULTIPLIER);

                        continue;
                    }

                    Ok(result) => result,
                };

                let dir_entries: Vec<read_dir_response::DirEntry> = match result {
                    Err(err) => {
                        if code_can_retry(err.code()) {
                            warn!("read_dir rpc has error {}", err);

                            task::sleep(Duration::from_secs(1)).await;

                            continue;
                        }

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

                debug!("readdir success");

                return;
            }

            error!("read_dir failed more than 3 times");
            reply.error(libc::ETIMEDOUT);

            if let Some(failed_notify) = &failed_notify {
                failed_notify.notify()
            }
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

        let client = self.client.clone();

        let failed_notify = self.failed_notify.clone();

        let uid = req.uid();
        let gid = req.gid();

        task::spawn(async move {
            let mut rpc_timeout = INITIAL_TIMEOUT;

            for _ in 0..3 {
                let rpc_req = TonicRequest::new(CreateFileRequest {
                    head: Some(header.clone()),
                    inode: parent,
                    name: name.to_string(),
                    mode,
                    flags,
                });

                let mut client = client.read().await.clone();

                let result = match timeout(rpc_timeout, client.create_file(rpc_req)).await {
                    Err(err) => {
                        warn!("create file rpc timeout {}", err);

                        rpc_timeout = rpc_timeout.mul_f64(MULTIPLIER);

                        continue;
                    }

                    Ok(result) => result,
                };

                let (fh_id, attr) = match result {
                    Err(err) => {
                        if code_can_retry(err.code()) {
                            warn!("create_file rpc has error {}", err);

                            task::sleep(Duration::from_secs(1)).await;

                            continue;
                        }

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

                return;
            }

            error!("create_file failed more than 3 times");
            reply.error(libc::ETIMEDOUT);

            if let Some(failed_notify) = &failed_notify {
                failed_notify.notify()
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

        let client = self.client.clone();

        let failed_notify = self.failed_notify.clone();

        task::spawn(async move {
            let mut rpc_timeout = INITIAL_TIMEOUT;

            for _ in 0..3 {
                let rpc_req = TonicRequest::new(GetLockRequest {
                    head: Some(header.clone()),
                    file_handle_id: fh,
                });

                let mut client = client.read().await.clone();

                let result = match timeout(rpc_timeout, client.get_lock(rpc_req)).await {
                    Err(err) => {
                        warn!("getlk rpc timeout {}", err);

                        rpc_timeout = rpc_timeout.mul_f64(MULTIPLIER);

                        continue;
                    }

                    Ok(result) => result,
                };

                let result = match result {
                    Err(err) => {
                        if code_can_retry(err.code()) {
                            warn!("get_lock rpc has error {}", err);

                            task::sleep(Duration::from_secs(1)).await;

                            continue;
                        }

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

                return;
            }

            error!("get_lock failed more than 3 times");
            reply.error(libc::ETIMEDOUT);

            if let Some(failed_notify) = &failed_notify {
                failed_notify.notify()
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

        let client = self.client.clone();
        let failed_notify = self.failed_notify.clone();

        if typ as i32 == libc::F_UNLCK {
            task::spawn(async move {
                for _ in 0..3 {
                    let rpc_req = TonicRequest::new(ReleaseLockRequest {
                        head: Some(header.clone()),
                        file_handle_id: fh,
                        block: false,
                    });

                    let mut client = client.read().await.clone();

                    match client.release_lock(rpc_req).await {
                        Err(err) => {
                            if code_can_retry(err.code()) {
                                warn!("release_lock rpc has error {}", err);

                                task::sleep(Duration::from_secs(1)).await;

                                continue;
                            }

                            error!("release_lock rpc has error {}", err);
                            reply.error(libc::EIO);

                            return;
                        }

                        Ok(resp) => {
                            if let Some(error) = resp.into_inner().error {
                                reply.error(error.errno as c_int);
                            } else {
                                reply.ok()
                            }

                            return;
                        }
                    }
                }

                error!("release_lock failed more than 3 times");
                reply.error(libc::ETIMEDOUT);

                if let Some(failed_notify) = &failed_notify {
                    failed_notify.notify();
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

        task::spawn(async move {
            for _ in 0..3 {
                let rpc_req = TonicRequest::new(SetLockRequest {
                    head: Some(header.clone()),
                    file_handle_id: fh,
                    unique,
                    lock_kind: lock_kind.into(),
                    block: sleep,
                });

                let mut client = client.read().await.clone();

                match client.set_lock(rpc_req).await {
                    Err(err) => {
                        if code_can_retry(err.code()) {
                            warn!("set_lock rpc has error {}", err);

                            task::sleep(Duration::from_secs(1)).await;

                            continue;
                        }

                        error!("set_lock rpc has error {}", err);
                        reply.error(libc::EIO);

                        return;
                    }

                    Ok(resp) => {
                        if let Some(error) = resp.into_inner().error {
                            warn!(
                                "set lock failed, unique {} errno is {}",
                                unique, error.errno
                            );

                            reply.error(error.errno as c_int)
                        } else {
                            reply.ok()
                        }

                        return;
                    }
                }
            }

            error!("set_lock failed more than 3 times");
            reply.error(libc::ETIMEDOUT);

            if let Some(failed_notify) = &failed_notify {
                failed_notify.notify();
            }
        });
    }

    fn interrupt(&mut self, _req: &Request, unique: u64, reply: ReplyEmpty) {
        let header = self.get_rpc_header();

        let client = self.client.clone();

        let failed_notify = self.failed_notify.clone();

        debug!("interrupt unique {}", unique);

        task::spawn(async move {
            let mut rpc_timeout = INITIAL_TIMEOUT;

            for _ in 0..3 {
                let rpc_req = TonicRequest::new(InterruptRequest {
                    head: Some(header.clone()),
                    unique,
                });

                let mut client = client.read().await.clone();

                let result = match timeout(rpc_timeout, client.interrupt(rpc_req)).await {
                    Err(err) => {
                        warn!("interrupt rpc timeout {}", err);

                        rpc_timeout = rpc_timeout.mul_f64(MULTIPLIER);

                        continue;
                    }

                    Ok(result) => result,
                };

                match result {
                    Err(err) => {
                        if code_can_retry(err.code()) {
                            warn!("interrupt rpc has error {}", err);

                            task::sleep(Duration::from_secs(1)).await;

                            continue;
                        }

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

                        return;
                    }
                }
            }

            error!("interrupt failed more than 3 times");
            reply.error(libc::ETIMEDOUT);

            if let Some(failed_notify) = &failed_notify {
                failed_notify.notify()
            }
        });
    }
}

fn code_can_retry(code: Code) -> bool {
    code == Code::Unavailable || code == Code::DeadlineExceeded
}
