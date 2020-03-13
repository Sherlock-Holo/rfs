use std::convert::TryFrom;
use std::ffi::OsStr;
use std::fmt::{self, Debug};
use std::io;
use std::io::ErrorKind;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::Result;
use async_signals::Signals;
use async_std::fs;
use async_std::future::timeout;
use async_std::os::unix::net::UnixStream;
use async_std::sync;
use async_std::sync::{Receiver, RwLock, Sender};
use async_std::task;
use chrono::Local;
use fuse::{
    FileType, Filesystem as FuseFilesystem, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory,
    ReplyEmpty, ReplyEntry, ReplyLock, ReplyOpen, ReplyWrite, Request,
};
use futures_util::future::ready;
use futures_util::future::FutureExt;
use futures_util::select;
use futures_util::StreamExt;
use libc::c_int;
use log::{debug, error, info, warn};
use nix::mount;
use nix::mount::MntFlags;
use nix::unistd;
use serde::export::Formatter;
use tonic::transport::ClientTlsConfig;
use tonic::transport::Endpoint;
use tonic::transport::{Channel, Uri};
use tonic::{Code, Request as TonicRequest};
use tower::service_fn;
use uuid::Uuid;

use crate::helper::proto_attr_into_fuse_attr;
use crate::pb::rfs_client::RfsClient;
use crate::pb::*;
use crate::TokioUnixStream;

const TTL: Duration = Duration::from_secs(1);
const MIN_RTT: Duration = Duration::from_secs(10);

#[derive(Clone)]
enum ClientKind {
    Rpc(Arc<RwLock<RfsClient<Channel>>>, Uri, ClientTlsConfig),
    Uds(RfsClient<Channel>),
}

impl ClientKind {
    #[inline]
    async fn get_client(&self) -> RfsClient<Channel> {
        match self {
            ClientKind::Uds(client) => client.clone(),
            ClientKind::Rpc(client, ..) => client.read().await.clone(),
        }
    }
}

impl Debug for ClientKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ClientKind::Rpc(..) => f.write_str("rpc"),
            ClientKind::Uds(_) => f.write_str("uds"),
        }
    }
}

pub struct Filesystem {
    uuid: Option<Uuid>,
    client_kind: ClientKind,
    id_sender: Option<Sender<Uuid>>,
    rtt: Arc<RwLock<Duration>>,
    tokio_handle: Option<tokio::runtime::Handle>,
    rpc_failed_receiver: Option<Receiver<bool>>,
    rpc_failed_sender: Option<Sender<bool>>,
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
            .connect_with_connector(service_fn(move |_: Uri| {
                task::block_on(async {
                    match UnixStream::connect(uds_path).await {
                        Err(err) => ready(Err(err)),
                        Ok(unix_stream) => ready(Ok(TokioUnixStream(unix_stream))),
                    }
                })
            }))
            .await?;

        Ok(Filesystem {
            uuid: None,
            client_kind: ClientKind::Uds(RfsClient::new(channel)),
            id_sender: None,
            rtt: Arc::new(RwLock::new(Duration::from_millis(100))),
            tokio_handle: None,
            rpc_failed_receiver: None,
            rpc_failed_sender: None,
        })
    }

    pub async fn new(
        uri: Uri,
        tls_cfg: ClientTlsConfig,
        handle: tokio::runtime::Handle,
    ) -> Result<Self, tonic::transport::Error> {
        info!("connecting server");

        let channel = Channel::builder(uri.clone())
            .tls_config(tls_cfg.clone())
            .tcp_keepalive(Some(Duration::from_secs(5)))
            .connect()
            .await?;

        info!("server connected");

        let client = Arc::new(RwLock::new(RfsClient::new(channel)));

        let (sender, receiver) = sync::channel(3);

        Ok(Filesystem {
            uuid: None,
            client_kind: ClientKind::Rpc(client.clone(), uri.clone(), tls_cfg),
            id_sender: None,
            rtt: Arc::new(RwLock::new(Duration::from_millis(100))),
            tokio_handle: Some(handle),
            rpc_failed_receiver: Some(receiver),
            rpc_failed_sender: Some(sender),
        })
    }

    pub async fn mount<P: AsRef<Path>>(mut self, mount_point: P) -> io::Result<()> {
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

        let mut stop_signal = Signals::new(vec![libc::SIGINT, libc::SIGTERM])?;

        let unmount_point = mount_point.as_ref().to_path_buf();

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

        let mut client = self.client_kind.get_client().await;

        fuse::mount(self, mount_point, &opts)?;

        if let Some(uuid) = receiver.recv().await {
            let req = TonicRequest::new(LogoutRequest {
                uuid: uuid.to_hyphenated().to_string(),
            });

            info!("sending logout request");

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

        Ok(())
    }

    fn get_rpc_header(&self) -> Header {
        let uuid = self
            .uuid
            .expect("uuid must be initialize")
            .to_hyphenated()
            .to_string();

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
        receiver: Receiver<bool>,
    ) {
        let mut failed_count = 0;

        loop {
            while let Some(fail) = receiver.recv().await {
                if fail {
                    failed_count += 1;

                    if failed_count >= 3 {
                        warn!("rpc failed more then 3 times, need reconnect");

                        let uri = uri.clone();
                        let tls_cfg = tls_cfg.clone();

                        let channel = match handle
                            .spawn(async move {
                                Channel::builder(uri)
                                    .tls_config(tls_cfg)
                                    .tcp_keepalive(Some(Duration::from_secs(5)))
                                    .connect()
                                    .await
                            })
                            .await
                            .unwrap()
                        {
                            Err(err) => {
                                error!("reconnect failed {}", err);

                                continue;
                            }

                            Ok(channel) => channel,
                        };

                        failed_count = 0;

                        *client.write().await = RfsClient::new(channel);

                        info!("reconnect success");
                    }
                } else {
                    failed_count = 0;
                }
            }
        }
    }

    async fn ping_loop(
        client: Arc<RwLock<RfsClient<Channel>>>,
        uuid: Uuid,
        rtt: Arc<RwLock<Duration>>,
        failed_sender: Sender<bool>,
    ) {
        loop {
            task::sleep(Duration::from_secs(60)).await;

            let before_ping = Local::now();

            let ping_req = TonicRequest::new(PingRequest {
                header: Some(Header {
                    uuid: uuid.to_hyphenated().to_string(),
                    version: VERSION.to_string(),
                }),
            });

            let mut client = client.read().await.clone();

            if timeout(*rtt.read().await * 2, client.ping(ping_req))
                .await
                .is_ok()
            {
                if let Ok(new_rtt) = (Local::now() - before_ping).to_std() {
                    *rtt.write().await = new_rtt;
                }

                select! {
                    _ = failed_sender.send(false).fuse() => (),
                    default => ()
                };

                debug!("sent ping message");
            } else {
                select! {
                    _ = failed_sender.send(true).fuse() => (),
                    default => ()
                };
            }
        }
    }
}

impl FuseFilesystem for Filesystem {
    fn init(&mut self, _req: &Request) -> Result<(), libc::c_int> {
        task::block_on(async {
            for _ in 0..3 {
                let req = TonicRequest::new(RegisterRequest {});

                return match self.client_kind.get_client().await.register(req).await {
                    Err(err) => {
                        if code_can_retry(err.code()) {
                            task::sleep(Duration::from_secs(1)).await;

                            warn!("register failed {}", err);

                            continue;
                        }

                        error!("register failed {}", err);

                        Err(libc::EIO)
                    }

                    Ok(resp) => {
                        let resp = resp.into_inner();

                        let uuid: Uuid = match resp.uuid.parse() {
                            Err(_) => return Err(libc::EINVAL),
                            Ok(uuid) => uuid,
                        };

                        self.uuid.replace(uuid);

                        self.id_sender
                            .as_ref()
                            .expect("id sender must be initialize")
                            .send(uuid)
                            .await;

                        // uds client doesn't need send ping
                        if let ClientKind::Rpc(client, uri, tls_cfg) = &self.client_kind {
                            let client = client.clone();

                            let rtt = self.rtt.clone();

                            let failed_sender = self
                                .rpc_failed_sender
                                .clone()
                                .expect("not None in rpc mode");

                            let uri = uri.clone();
                            let tls_cfg = tls_cfg.clone();
                            let handle = self.tokio_handle.take().expect("not None in rpc mode");
                            let receiver = self
                                .rpc_failed_receiver
                                .clone()
                                .expect("not None in rpc mode");

                            task::spawn(Self::ping_loop(client.clone(), uuid, rtt, failed_sender));

                            task::spawn(Self::reconnect_loop(
                                client, uri, tls_cfg, handle, receiver,
                            ));
                        }

                        Ok(())
                    }
                };
            }

            error!("register fails more than 3 times");

            Err(libc::EIO)
        })
    }

    /*fn destroy(&mut self, _req: &Request) {
        let uuid = self
            .uuid
            .as_ref()
            .expect("uuid should initialize")
            .to_string();

        task::block_on(async {
            let req = TonicRequest::new(LogoutRequest { uuid });

            info!("sending logout request");

            if let Err(err) = self.rpc_client.logout(req).await {
                error!("logout failed {}", err)
            }

            info!("logout success")
        })
    }*/

    fn lookup(&mut self, req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name = match name.to_str() {
            None => {
                reply.error(libc::EINVAL);
                return;
            }

            Some(name) => name.to_string(),
        };

        let header = self.get_rpc_header();

        let client_kind = self.client_kind.clone();

        let uid = req.uid();
        let gid = req.gid();

        let rtt = self.rtt.clone();

        let failed_sender = self.rpc_failed_sender.clone();

        task::spawn(async move {
            for _ in 0..3 {
                let rpc_req = TonicRequest::new(LookupRequest {
                    head: Some(header.clone()),
                    inode: parent,
                    name: name.to_string(),
                });

                let mut client = client_kind.get_client().await.clone();

                let mut rtt = *rtt.read().await * 2;
                if rtt < MIN_RTT {
                    rtt = MIN_RTT;
                }

                let result = match timeout(rtt, client.lookup(rpc_req)).await {
                    Err(err) => {
                        warn!("lookup rpc timeout {}", err);

                        if let Some(failed_sender) = &failed_sender {
                            select! {
                                _ = failed_sender.send(true).fuse() => (),
                                default => ()
                            };
                        }

                        continue;
                    }

                    Ok(result) => result,
                };

                let result = match result {
                    Err(err) => {
                        if code_can_retry(err.code()) {
                            warn!("lookup rpc has error {}", err);

                            if let Some(failed_sender) = &failed_sender {
                                select! {
                                    _ = failed_sender.send(true).fuse() => (),
                                    default => ()
                                };
                            }

                            task::sleep(Duration::from_secs(1)).await;

                            continue;
                        }

                        error!("lookup rpc has error {}", err);
                        reply.error(libc::EIO);

                        return;
                    }

                    Ok(resp) => {
                        if let Some(failed_sender) = &failed_sender {
                            select! {
                                _ = failed_sender.send(false).fuse() => (),
                                default => ()
                            };
                        }

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
            reply.error(libc::EIO);
        });
    }

    fn getattr(&mut self, req: &Request, inode: u64, reply: ReplyAttr) {
        let header = self.get_rpc_header();

        let client_kind = self.client_kind.clone();

        let uid = req.uid();
        let gid = req.gid();

        let rtt = self.rtt.clone();

        let failed_sender = self.rpc_failed_sender.clone();

        task::spawn(async move {
            for _ in 0..3 {
                let rpc_req = TonicRequest::new(GetAttrRequest {
                    head: Some(header.clone()),
                    inode,
                });

                let mut client = client_kind.get_client().await.clone();

                let mut rtt = *rtt.read().await * 2;
                if rtt < MIN_RTT {
                    rtt = MIN_RTT;
                }

                let result = match timeout(rtt, client.get_attr(rpc_req)).await {
                    Err(err) => {
                        warn!("getattr rpc timeout {}", err);

                        if let Some(failed_sender) = &failed_sender {
                            select! {
                                _ = failed_sender.send(true).fuse() => (),
                                default => ()
                            };
                        }

                        continue;
                    }

                    Ok(result) => result,
                };

                let result = match result {
                    Err(err) => {
                        if code_can_retry(err.code()) {
                            warn!("getattr rpc has error {}", err);

                            if let Some(failed_sender) = &failed_sender {
                                select! {
                                    _ = failed_sender.send(true).fuse() => (),
                                    default => ()
                                };
                            }

                            task::sleep(Duration::from_secs(1)).await;

                            continue;
                        }

                        error!("getattr rpc has error {}", err);
                        reply.error(libc::EIO);

                        return;
                    }

                    Ok(resp) => {
                        if let Some(failed_sender) = &failed_sender {
                            select! {
                                _ = failed_sender.send(false).fuse() => (),
                                default => ()
                            };
                        }

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
            reply.error(libc::EIO);
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

        let client_kind = self.client_kind.clone();

        let uid = req.uid();
        let gid = req.gid();

        let rtt = self.rtt.clone();

        let failed_sender = self.rpc_failed_sender.clone();

        task::spawn(async move {
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

                let mut client = client_kind.get_client().await.clone();

                let mut rtt = *rtt.read().await * 2;
                if rtt < MIN_RTT {
                    rtt = MIN_RTT;
                }

                let result = match timeout(rtt, client.set_attr(rpc_req)).await {
                    Err(err) => {
                        warn!("setattr rpc timeout {}", err);

                        if let Some(failed_sender) = &failed_sender {
                            select! {
                                _ = failed_sender.send(true).fuse() => (),
                                default => ()
                            };
                        }

                        continue;
                    }

                    Ok(result) => result,
                };

                let result = match result {
                    Err(err) => {
                        if code_can_retry(err.code()) {
                            warn!("setattr rpc has error {}", err);

                            if let Some(failed_sender) = &failed_sender {
                                select! {
                                    _ = failed_sender.send(true).fuse() => (),
                                    default => ()
                                };
                            }

                            task::sleep(Duration::from_secs(1)).await;

                            continue;
                        }

                        error!("setattr rpc has error {}", err);
                        reply.error(libc::EIO);

                        return;
                    }

                    Ok(resp) => {
                        if let Some(failed_sender) = &failed_sender {
                            select! {
                                _ = failed_sender.send(false).fuse() => (),
                                default => ()
                            };
                        }

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
            reply.error(libc::EIO);
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

        let client_kind = self.client_kind.clone();

        let rtt = self.rtt.clone();

        let failed_sender = self.rpc_failed_sender.clone();

        let uid = req.uid();
        let gid = req.gid();

        task::spawn(async move {
            for _ in 0..3 {
                let rpc_req = TonicRequest::new(MkdirRequest {
                    head: Some(header.clone()),
                    inode: parent,
                    name: name.to_string(),
                    mode,
                });

                let mut client = client_kind.get_client().await.clone();

                let mut rtt = *rtt.read().await * 2;
                if rtt < MIN_RTT {
                    rtt = MIN_RTT;
                }

                let result = match timeout(rtt, client.mkdir(rpc_req)).await {
                    Err(err) => {
                        warn!("mkdir rpc timeout {}", err);

                        if let Some(failed_sender) = &failed_sender {
                            select! {
                                _ = failed_sender.send(true).fuse() => (),
                                default => ()
                            };
                        }

                        continue;
                    }

                    Ok(result) => result,
                };

                let result = match result {
                    Err(err) => {
                        if code_can_retry(err.code()) {
                            warn!("mkdir rpc has error {}", err);

                            if let Some(failed_sender) = &failed_sender {
                                select! {
                                    _ = failed_sender.send(true).fuse() => (),
                                    default => ()
                                };
                            }

                            task::sleep(Duration::from_secs(1)).await;

                            continue;
                        }

                        error!("mkdir rpc has error {}", err);
                        reply.error(libc::EIO);

                        return;
                    }

                    Ok(resp) => {
                        if let Some(failed_sender) = &failed_sender {
                            select! {
                                _ = failed_sender.send(false).fuse() => (),
                                default => ()
                            };
                        }

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
            reply.error(libc::EIO);

            return;
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

        let client_kind = self.client_kind.clone();
        let rtt = self.rtt.clone();
        let failed_sender = self.rpc_failed_sender.clone();

        task::spawn(async move {
            for _ in 0..3 {
                let rpc_req = TonicRequest::new(UnlinkRequest {
                    head: Some(header.clone()),
                    inode: parent,
                    name: name.to_string(),
                });

                let mut client = client_kind.get_client().await.clone();

                let mut rtt = *rtt.read().await * 2;
                if rtt < MIN_RTT {
                    rtt = MIN_RTT;
                }

                let result = match timeout(rtt, client.unlink(rpc_req)).await {
                    Err(err) => {
                        warn!("unlink rpc timeout {}", err);

                        if let Some(failed_sender) = &failed_sender {
                            select! {
                                _ = failed_sender.send(true).fuse() => (),
                                default => ()
                            };
                        }

                        continue;
                    }

                    Ok(result) => result,
                };

                match result {
                    Err(err) => {
                        if code_can_retry(err.code()) {
                            warn!("unlink rpc has error {}", err);

                            if let Some(failed_sender) = &failed_sender {
                                select! {
                                    _ = failed_sender.send(true).fuse() => (),
                                    default => ()
                                };
                            }

                            task::sleep(Duration::from_secs(1)).await;

                            continue;
                        }

                        error!("unlink rpc has error {}", err);
                        reply.error(libc::EIO);

                        return;
                    }

                    Ok(resp) => {
                        if let Some(failed_sender) = &failed_sender {
                            select! {
                                _ = failed_sender.send(false).fuse() => (),
                                default => ()
                            };
                        }

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
            reply.error(libc::EIO);
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

        let client_kind = self.client_kind.clone();
        let rtt = self.rtt.clone();
        let failed_sender = self.rpc_failed_sender.clone();

        task::spawn(async move {
            for _ in 0..3 {
                let rpc_req = TonicRequest::new(RmDirRequest {
                    head: Some(header.clone()),
                    inode: parent,
                    name: name.to_string(),
                });

                let mut client = client_kind.get_client().await.clone();

                let mut rtt = *rtt.read().await * 2;
                if rtt < MIN_RTT {
                    rtt = MIN_RTT;
                }

                let result = match timeout(rtt, client.rm_dir(rpc_req)).await {
                    Err(err) => {
                        warn!("rmdir rpc timeout {}", err);

                        if let Some(failed_sender) = &failed_sender {
                            select! {
                                _ = failed_sender.send(true).fuse() => (),
                                default => ()
                            };
                        }

                        continue;
                    }

                    Ok(result) => result,
                };

                match result {
                    Err(err) => {
                        if code_can_retry(err.code()) {
                            warn!("rmdir rpc has error {}", err);

                            if let Some(failed_sender) = &failed_sender {
                                select! {
                                    _ = failed_sender.send(true).fuse() => (),
                                    default => ()
                                };
                            }

                            task::sleep(Duration::from_secs(1)).await;

                            continue;
                        }

                        error!("rmdir rpc has error {}", err);
                        reply.error(libc::EIO);

                        return;
                    }

                    Ok(resp) => {
                        if let Some(failed_sender) = &failed_sender {
                            select! {
                                _ = failed_sender.send(false).fuse() => (),
                                default => ()
                            };
                        }

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
            reply.error(libc::EIO);
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

        let client_kind = self.client_kind.clone();
        let rtt = self.rtt.clone();
        let failed_sender = self.rpc_failed_sender.clone();

        task::spawn(async move {
            for _ in 0..3 {
                let rpc_req = TonicRequest::new(RenameRequest {
                    head: Some(header.clone()),
                    old_parent: parent,
                    old_name: name.to_string(),
                    new_parent,
                    new_name: new_name.to_string(),
                });

                let mut client = client_kind.get_client().await.clone();

                let mut rtt = *rtt.read().await * 2;
                if rtt < MIN_RTT {
                    rtt = MIN_RTT;
                }

                let result = match timeout(rtt, client.rename(rpc_req)).await {
                    Err(err) => {
                        warn!("rename rpc timeout {}", err);

                        if let Some(failed_sender) = &failed_sender {
                            select! {
                                _ = failed_sender.send(true).fuse() => (),
                                default => ()
                            };
                        }

                        continue;
                    }

                    Ok(result) => result,
                };

                match result {
                    Err(err) => {
                        if code_can_retry(err.code()) {
                            warn!("rename rpc has error {}", err);

                            if let Some(failed_sender) = &failed_sender {
                                select! {
                                    _ = failed_sender.send(false).fuse() => (),
                                    default => ()
                                };
                            }

                            task::sleep(Duration::from_secs(1)).await;

                            continue;
                        }

                        error!("rename rpc has error {}", err);
                        reply.error(libc::EIO);

                        return;
                    }

                    Ok(resp) => {
                        if let Some(failed_sender) = &failed_sender {
                            select! {
                                _ = failed_sender.send(false).fuse() => (),
                                default => ()
                            };
                        };

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
            reply.error(libc::EIO);
        });
    }

    fn open(&mut self, _req: &Request, inode: u64, flags: u32, reply: ReplyOpen) {
        let header = self.get_rpc_header();

        let client_kind = self.client_kind.clone();
        let rtt = self.rtt.clone();
        let failed_sender = self.rpc_failed_sender.clone();

        debug!("client open inode {} flags {}", inode, flags);

        task::spawn(async move {
            for _ in 0..3 {
                let rpc_req = TonicRequest::new(OpenFileRequest {
                    head: Some(header.clone()),
                    inode,
                    flags,
                });

                let mut client = client_kind.get_client().await.clone();

                let mut rtt = *rtt.read().await * 2;
                if rtt < MIN_RTT {
                    rtt = MIN_RTT;
                }

                let result = match timeout(rtt, client.open_file(rpc_req)).await {
                    Err(err) => {
                        warn!("open file rpc timeout {}", err);

                        if let Some(failed_sender) = &failed_sender {
                            select! {
                                _ = failed_sender.send(true).fuse() => (),
                                default => ()
                            };
                        }

                        continue;
                    }

                    Ok(result) => result,
                };

                let result = match result {
                    Err(err) => {
                        if code_can_retry(err.code()) {
                            warn!("open rpc has error {}", err);

                            if let Some(failed_sender) = &failed_sender {
                                select! {
                                    _ = failed_sender.send(true).fuse() => (),
                                    default => ()
                                };
                            }

                            task::sleep(Duration::from_secs(1)).await;

                            continue;
                        }

                        error!("open rpc has error {}", err);
                        reply.error(libc::EIO);

                        return;
                    }

                    Ok(resp) => {
                        if let Some(failed_sender) = &failed_sender {
                            select! {
                                _ = failed_sender.send(false).fuse() => (),
                                default => ()
                            };
                        };

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
            reply.error(libc::EIO);
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

        let client_kind = self.client_kind.clone();
        let rtt = self.rtt.clone();
        let failed_sender = self.rpc_failed_sender.clone();

        task::spawn(async move {
            for _ in 0..3 {
                let rpc_req = TonicRequest::new(ReadFileRequest {
                    head: Some(header.clone()),
                    file_handle_id: fh,
                    offset,
                    size: size as u64,
                });

                let mut client = client_kind.get_client().await.clone();

                let mut rtt = *rtt.read().await * 2;
                if rtt < MIN_RTT {
                    rtt = MIN_RTT;
                }

                let result = match timeout(rtt, client.read_file(rpc_req)).await {
                    Err(err) => {
                        warn!("read_file rpc timeout {}", err);

                        if let Some(failed_sender) = &failed_sender {
                            select! {
                                _ = failed_sender.send(true).fuse() => (),
                                default => ()
                            };
                        }

                        continue;
                    }

                    Ok(result) => result,
                };

                let result = match result {
                    Err(err) => {
                        if code_can_retry(err.code()) {
                            warn!("read_file rpc has error {}", err);

                            if let Some(failed_sender) = &failed_sender {
                                select! {
                                    _ = failed_sender.send(true).fuse() => (),
                                    default => ()
                                };
                            }

                            task::sleep(Duration::from_secs(1)).await;

                            continue;
                        }

                        error!("read_file rpc has error {}", err);
                        reply.error(libc::EIO);

                        return;
                    }

                    Ok(resp) => {
                        if let Some(failed_sender) = &failed_sender {
                            select! {
                                _ = failed_sender.send(false).fuse() => (),
                                default => ()
                            };
                        };

                        if let Some(result) = resp.into_inner().result {
                            result
                        } else {
                            error!("read_file result is None");
                            reply.error(libc::EIO);

                            return;
                        }
                    }
                };

                match result {
                    read_file_response::Result::Error(err) => reply.error(err.errno as i32),

                    read_file_response::Result::Data(data) => reply.data(&data),
                }

                return;
            }

            error!("read_file failed more than 3 times");
            reply.error(libc::EIO);
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

        let client_kind = self.client_kind.clone();
        let rtt = self.rtt.clone();
        let failed_sender = self.rpc_failed_sender.clone();

        let data = data.to_vec();

        task::spawn(async move {
            for _ in 0..3 {
                let rpc_req = TonicRequest::new(WriteFileRequest {
                    head: Some(header.clone()),
                    file_handle_id: fh,
                    offset,
                    data: data.clone(),
                });

                let mut client = client_kind.get_client().await.clone();

                let mut rtt = *rtt.read().await * 2;
                if rtt < MIN_RTT {
                    rtt = MIN_RTT;
                }

                let result = match timeout(rtt, client.write_file(rpc_req)).await {
                    Err(err) => {
                        warn!("write file rpc timeout {}", err);

                        if let Some(failed_sender) = &failed_sender {
                            select! {
                                _ = failed_sender.send(true).fuse() => (),
                                default => ()
                            };
                        }

                        continue;
                    }

                    Ok(result) => result,
                };

                let result = match result {
                    Err(err) => {
                        if code_can_retry(err.code()) {
                            warn!("write_file rpc has error {}", err);

                            if let Some(failed_sender) = &failed_sender {
                                select! {
                                    _ = failed_sender.send(true).fuse() => (),
                                    default => ()
                                };
                            }

                            task::sleep(Duration::from_secs(1)).await;

                            continue;
                        }

                        error!("write_file rpc has error {}", err);
                        reply.error(libc::EIO);

                        return;
                    }

                    Ok(resp) => {
                        if let Some(failed_sender) = &failed_sender {
                            select! {
                                _ = failed_sender.send(false).fuse() => (),
                                default => ()
                            };
                        };

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
            reply.error(libc::EIO);
        });
    }

    fn flush(&mut self, _req: &Request, _ino: u64, fh: u64, _lock_owner: u64, reply: ReplyEmpty) {
        let header = self.get_rpc_header();

        let client_kind = self.client_kind.clone();
        let rtt = self.rtt.clone();
        let failed_sender = self.rpc_failed_sender.clone();

        task::spawn(async move {
            for _ in 0..3 {
                let rpc_req = TonicRequest::new(FlushRequest {
                    head: Some(header.clone()),
                    file_handle_id: fh,
                });

                let mut client = client_kind.get_client().await.clone();

                let mut rtt = *rtt.read().await * 2;
                if rtt < MIN_RTT {
                    rtt = MIN_RTT;
                }

                let result = match timeout(rtt, client.flush(rpc_req)).await {
                    Err(err) => {
                        warn!("flush rpc timeout {}", err);

                        if let Some(failed_sender) = &failed_sender {
                            select! {
                                _ = failed_sender.send(true).fuse() => (),
                                default => ()
                            };
                        }

                        continue;
                    }

                    Ok(result) => result,
                };

                match result {
                    Err(err) => {
                        if code_can_retry(err.code()) {
                            warn!("flush rpc has error {}", err);

                            if let Some(failed_sender) = &failed_sender {
                                select! {
                                    _ = failed_sender.send(true).fuse() => (),
                                    default => ()
                                };
                            }

                            task::sleep(Duration::from_secs(1)).await;

                            continue;
                        }

                        error!("flush rpc has error {}", err);
                        reply.error(libc::EIO);

                        return;
                    }

                    Ok(resp) => {
                        if let Some(failed_sender) = &failed_sender {
                            select! {
                                _ = failed_sender.send(false).fuse() => (),
                                default => ()
                            };
                        };

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
            reply.error(libc::EIO);
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

        let client_kind = self.client_kind.clone();
        let rtt = self.rtt.clone();
        let failed_sender = self.rpc_failed_sender.clone();

        task::spawn(async move {
            for _ in 0..3 {
                let rpc_req = TonicRequest::new(CloseFileRequest {
                    head: Some(header.clone()),
                    file_handle_id: fh,
                });

                let mut client = client_kind.get_client().await.clone();

                let mut rtt = *rtt.read().await * 2;
                if rtt < MIN_RTT {
                    rtt = MIN_RTT;
                }

                let result = match timeout(rtt, client.close_file(rpc_req)).await {
                    Err(err) => {
                        warn!("close file rpc timeout {}", err);

                        if let Some(failed_sender) = &failed_sender {
                            select! {
                                _ = failed_sender.send(true).fuse() => (),
                                default => ()
                            };
                        }

                        continue;
                    }

                    Ok(result) => result,
                };

                match result {
                    Err(err) => {
                        if code_can_retry(err.code()) {
                            warn!("close_file rpc has error {}", err);

                            if let Some(failed_sender) = &failed_sender {
                                select! {
                                    _ = failed_sender.send(true).fuse() => (),
                                    default => ()
                                };
                            }

                            task::sleep(Duration::from_secs(1)).await;

                            continue;
                        }

                        error!("close_file rpc has error {}", err);
                        reply.error(libc::EIO);

                        return;
                    }

                    Ok(resp) => {
                        if let Some(failed_sender) = &failed_sender {
                            select! {
                                _ = failed_sender.send(false).fuse() => (),
                                default => ()
                            };
                        };

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
            reply.error(libc::EIO);
        });
    }

    fn fsync(&mut self, _req: &Request, _inode: u64, fh: u64, _datasync: bool, reply: ReplyEmpty) {
        let header = self.get_rpc_header();

        let client_kind = self.client_kind.clone();
        let rtt = self.rtt.clone();
        let failed_sender = self.rpc_failed_sender.clone();

        task::spawn(async move {
            for _ in 0..3 {
                let rpc_req = TonicRequest::new(SyncFileRequest {
                    head: Some(header.clone()),
                    file_handle_id: fh,
                });

                let mut client = client_kind.get_client().await.clone();

                let mut rtt = *rtt.read().await * 2;
                if rtt < MIN_RTT {
                    rtt = MIN_RTT;
                }

                let result = match timeout(rtt, client.sync_file(rpc_req)).await {
                    Err(err) => {
                        warn!("sync file rpc timeout {}", err);

                        if let Some(failed_sender) = &failed_sender {
                            select! {
                                _ = failed_sender.send(true).fuse() => (),
                                default => ()
                            };
                        }

                        continue;
                    }

                    Ok(result) => result,
                };

                match result {
                    Err(err) => {
                        if code_can_retry(err.code()) {
                            warn!("sync_file rpc has error {}", err);

                            if let Some(failed_sender) = &failed_sender {
                                select! {
                                    _ = failed_sender.send(true).fuse() => (),
                                    default => ()
                                };
                            }

                            task::sleep(Duration::from_secs(1)).await;

                            continue;
                        }

                        error!("sync_file rpc has error {}", err);
                        reply.error(libc::EIO);

                        return;
                    }

                    Ok(resp) => {
                        if let Some(failed_sender) = &failed_sender {
                            select! {
                                _ = failed_sender.send(false).fuse() => (),
                                default => ()
                            };
                        };

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
            reply.error(libc::EIO);
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

        let client_kind = self.client_kind.clone();
        let rtt = self.rtt.clone();
        let failed_sender = self.rpc_failed_sender.clone();

        task::spawn(async move {
            for _ in 0..3 {
                let rpc_req = TonicRequest::new(ReadDirRequest {
                    head: Some(header.clone()),
                    inode,
                });

                let mut client = client_kind.get_client().await.clone();

                let mut rtt = *rtt.read().await * 2;
                if rtt < MIN_RTT {
                    rtt = MIN_RTT;
                }

                let result = match timeout(rtt, client.read_dir(rpc_req)).await {
                    Err(err) => {
                        warn!("readdir rpc timeout {}", err);

                        if let Some(failed_sender) = &failed_sender {
                            select! {
                                _ = failed_sender.send(true).fuse() => (),
                                default => ()
                            };
                        }

                        continue;
                    }

                    Ok(result) => result,
                };

                let dir_entries: Vec<read_dir_response::DirEntry> = match result {
                    Err(err) => {
                        if code_can_retry(err.code()) {
                            warn!("read_dir rpc has error {}", err);

                            if let Some(failed_sender) = &failed_sender {
                                select! {
                                    _ = failed_sender.send(true).fuse() => (),
                                    default => ()
                                };
                            }

                            task::sleep(Duration::from_secs(1)).await;

                            continue;
                        }

                        error!("read_dir rpc has error {}", err);
                        reply.error(libc::EIO);

                        return;
                    }

                    Ok(resp) => {
                        if let Some(failed_sender) = &failed_sender {
                            select! {
                                _ = failed_sender.send(false).fuse() => (),
                                default => ()
                            };
                        };

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
            reply.error(libc::EIO);
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

        let client_kind = self.client_kind.clone();
        let rtt = self.rtt.clone();
        let failed_sender = self.rpc_failed_sender.clone();

        let uid = req.uid();
        let gid = req.gid();

        task::spawn(async move {
            for _ in 0..3 {
                let rpc_req = TonicRequest::new(CreateFileRequest {
                    head: Some(header.clone()),
                    inode: parent,
                    name: name.to_string(),
                    mode,
                    flags,
                });

                let mut client = client_kind.get_client().await.clone();

                let mut rtt = *rtt.read().await * 2;
                if rtt < MIN_RTT {
                    rtt = MIN_RTT;
                }

                let result = match timeout(rtt, client.create_file(rpc_req)).await {
                    Err(err) => {
                        warn!("create file rpc timeout {}", err);

                        if let Some(failed_sender) = &failed_sender {
                            select! {
                                _ = failed_sender.send(true).fuse() => (),
                                default => ()
                            };
                        }

                        continue;
                    }

                    Ok(result) => result,
                };

                let (fh_id, attr) = match result {
                    Err(err) => {
                        if code_can_retry(err.code()) {
                            warn!("create_file rpc has error {}", err);

                            if let Some(failed_sender) = &failed_sender {
                                select! {
                                    _ = failed_sender.send(true).fuse() => (),
                                    default => ()
                                };
                            }

                            task::sleep(Duration::from_secs(1)).await;

                            continue;
                        }

                        error!("create_file rpc has error {}", err);
                        reply.error(libc::EIO);

                        return;
                    }

                    Ok(resp) => {
                        if let Some(failed_sender) = &failed_sender {
                            select! {
                                _ = failed_sender.send(false).fuse() => (),
                                default => ()
                            };
                        };

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
            reply.error(libc::EIO);
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

        let client_kind = self.client_kind.clone();
        let rtt = self.rtt.clone();
        let failed_sender = self.rpc_failed_sender.clone();

        task::spawn(async move {
            for _ in 0..3 {
                let rpc_req = TonicRequest::new(GetLockRequest {
                    head: Some(header.clone()),
                    file_handle_id: fh,
                });

                let mut client = client_kind.get_client().await.clone();

                let mut rtt = *rtt.read().await * 2;
                if rtt < MIN_RTT {
                    rtt = MIN_RTT;
                }

                let result = match timeout(rtt, client.get_lock(rpc_req)).await {
                    Err(err) => {
                        warn!("getlk rpc timeout {}", err);

                        if let Some(failed_sender) = &failed_sender {
                            select! {
                                _ = failed_sender.send(true).fuse() => (),
                                default => ()
                            };
                        }

                        continue;
                    }

                    Ok(result) => result,
                };

                let result = match result {
                    Err(err) => {
                        if code_can_retry(err.code()) {
                            warn!("get_lock rpc has error {}", err);

                            if let Some(failed_sender) = &failed_sender {
                                select! {
                                    _ = failed_sender.send(true).fuse() => (),
                                    default => ()
                                };
                            }

                            task::sleep(Duration::from_secs(1)).await;

                            continue;
                        }

                        error!("get_lock rpc has error {}", err);
                        reply.error(libc::EIO);

                        return;
                    }

                    Ok(resp) => {
                        if let Some(failed_sender) = &failed_sender {
                            select! {
                                _ = failed_sender.send(false).fuse() => (),
                                default => ()
                            };
                        };

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
            reply.error(libc::EIO);
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

        let client_kind = self.client_kind.clone();
        let failed_sender = self.rpc_failed_sender.clone();

        if typ as i32 == libc::F_UNLCK {
            task::spawn(async move {
                for _ in 0..3 {
                    let rpc_req = TonicRequest::new(ReleaseLockRequest {
                        head: Some(header.clone()),
                        file_handle_id: fh,
                        block: false,
                    });

                    let mut client = client_kind.get_client().await.clone();

                    match client.release_lock(rpc_req).await {
                        Err(err) => {
                            if code_can_retry(err.code()) {
                                warn!("release_lock rpc has error {}", err);

                                if let Some(failed_sender) = &failed_sender {
                                    select! {
                                        _ = failed_sender.send(true).fuse() => (),
                                        default => ()
                                    };
                                }

                                task::sleep(Duration::from_secs(1)).await;

                                continue;
                            }

                            error!("release_lock rpc has error {}", err);
                            reply.error(libc::EIO);

                            return;
                        }

                        Ok(resp) => {
                            if let Some(failed_sender) = &failed_sender {
                                select! {
                                    _ = failed_sender.send(false).fuse() => (),
                                    default => ()
                                };
                            };

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
                reply.error(libc::EIO);
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

                let mut client = client_kind.get_client().await.clone();

                match client.set_lock(rpc_req).await {
                    Err(err) => {
                        if code_can_retry(err.code()) {
                            warn!("set_lock rpc has error {}", err);

                            if let Some(failed_sender) = &failed_sender {
                                select! {
                                    _ = failed_sender.send(true).fuse() => (),
                                    default => ()
                                };
                            }

                            task::sleep(Duration::from_secs(1)).await;

                            continue;
                        }

                        error!("set_lock rpc has error {}", err);
                        reply.error(libc::EIO);

                        return;
                    }

                    Ok(resp) => {
                        if let Some(failed_sender) = &failed_sender {
                            select! {
                                _ = failed_sender.send(false).fuse() => (),
                                default => ()
                            };
                        };

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
            reply.error(libc::EIO);
        });
    }

    fn interrupt(&mut self, _req: &Request, unique: u64, reply: ReplyEmpty) {
        let header = self.get_rpc_header();

        let client_kind = self.client_kind.clone();
        let rtt = self.rtt.clone();
        let failed_sender = self.rpc_failed_sender.clone();

        debug!("interrupt unique {}", unique);

        task::spawn(async move {
            for _ in 0..3 {
                let rpc_req = TonicRequest::new(InterruptRequest {
                    head: Some(header.clone()),
                    unique,
                });

                let mut client = client_kind.get_client().await.clone();

                let mut rtt = *rtt.read().await * 2;
                if rtt < MIN_RTT {
                    rtt = MIN_RTT;
                }

                let result = match timeout(rtt, client.interrupt(rpc_req)).await {
                    Err(err) => {
                        warn!("interrupt rpc timeout {}", err);

                        if let Some(failed_sender) = &failed_sender {
                            select! {
                                _ = failed_sender.send(true).fuse() => (),
                                default => ()
                            };
                        }

                        continue;
                    }

                    Ok(result) => result,
                };

                match result {
                    Err(err) => {
                        if code_can_retry(err.code()) {
                            warn!("interrupt rpc has error {}", err);

                            if let Some(failed_sender) = &failed_sender {
                                select! {
                                    _ = failed_sender.send(true).fuse() => (),
                                    default => ()
                                };
                            }

                            task::sleep(Duration::from_secs(1)).await;

                            continue;
                        }

                        error!("interrupt rpc has error {}", err);
                        reply.error(libc::EIO);

                        return;
                    }

                    Ok(resp) => {
                        if let Some(failed_sender) = &failed_sender {
                            select! {
                                _ = failed_sender.send(false).fuse() => (),
                                default => ()
                            };
                        };

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
            reply.error(libc::EIO);
        });
    }
}

fn code_can_retry(code: Code) -> bool {
    code == Code::Unavailable || code == Code::DeadlineExceeded
}
