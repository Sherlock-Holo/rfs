use std::ffi::{OsStr, OsString};
use std::io::{Read, Write};
use std::os::raw::c_int;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use async_notify::Notify;
use async_signals::Signals;
use async_std::future::timeout;
use async_std::sync::RwLock;
use atomic_value::AtomicValue;
use fuse3::reply::*;
use fuse3::{
    FileType, Filesystem as FuseFilesystem, MountOptions, Request, Result, Session, SetAttr,
};
use futures_util::stream;
use futures_util::StreamExt;
use log::{debug, error, info, warn};
use nix::mount;
use nix::mount::MntFlags;
use smol::{Task, Timer};
use snap::read::FrameDecoder;
use snap::write::FrameEncoder;
use tonic::transport::{Channel, ClientTlsConfig, Uri};
use tonic::{Code, Request as TonicRequest};
use uuid::Uuid;

use async_trait::async_trait;

use crate::helper::proto_attr_into_fuse_attr;
use crate::pb::rfs_client::RfsClient;
use crate::pb::*;

const TTL: Duration = Duration::from_secs(1);
const INITIAL_TIMEOUT: Duration = Duration::from_secs(10);
const MULTIPLIER: f64 = 1.5;
const MIN_COMPRESS_SIZE: usize = 2048;
const PING_INTERVAL: Duration = Duration::from_secs(60);

pub struct Filesystem {
    uuid: RwLock<Option<Uuid>>,
    client: Arc<AtomicValue<RfsClient<Channel>>>,
    failed_notify: Notify,
    compress: RwLock<bool>,
    uri: Uri,
    tls_cfg: ClientTlsConfig,
}

impl Filesystem {
    pub async fn new(uri: Uri, tls_cfg: ClientTlsConfig, compress: bool) -> anyhow::Result<Self> {
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

        let client = Arc::new(AtomicValue::new(RfsClient::new(channel)));

        Ok(Filesystem {
            uuid: RwLock::new(None),
            client,
            failed_notify: Notify::new(),
            compress: RwLock::new(compress),
            uri,
            tls_cfg,
        })
    }

    pub async fn mount<P: AsRef<Path>>(self, mount_point: P) -> anyhow::Result<()> {
        let mount_options = MountOptions::default().fs_name("rfs").nonempty(true);

        let mount_point = mount_point.as_ref();

        let unmount_point = mount_point.to_path_buf();

        let mut stop_signal = Signals::new(vec![libc::SIGINT, libc::SIGTERM])?;

        Task::spawn(async move {
            debug!("waiting stop signal");

            stop_signal.next().await;

            info!("receive stop signal");

            drop(stop_signal); // in case release signal handle

            info!("stopping rfs");

            Task::blocking(async move {
                while let Err(err) = mount::umount2(&unmount_point, MntFlags::MNT_DETACH) {
                    error!("lazy unmount failed {}", err);

                    Timer::after(Duration::from_secs(1)).await;
                }
            })
            .await;
        })
        .detach();

        let session = Session::new(mount_options);

        session.mount_with_unprivileged(self, mount_point).await?;

        Ok(())
    }

    async fn get_rpc_header(&self) -> Header {
        let uuid = self
            .uuid
            .read()
            .await
            .expect("uuid not init")
            .as_bytes()
            .to_vec();

        Header {
            uuid,
            version: VERSION.to_string(),
        }
    }

    async fn reconnect_loop(
        client: Arc<AtomicValue<RfsClient<Channel>>>,
        uri: Uri,
        tls_cfg: ClientTlsConfig,
        failed_notify: Notify,
    ) {
        loop {
            failed_notify.notified().await;

            warn!("rpc failed, need reconnect");

            let channel = loop {
                let uri = uri.clone();
                let tls_cfg = tls_cfg.clone();

                match Task::spawn(async move {
                    Channel::builder(uri)
                        .tls_config(tls_cfg)
                        .tcp_keepalive(Some(Duration::from_secs(5)))
                        .connect()
                        .await
                })
                .await
                {
                    Err(err) => {
                        error!("reconnect failed {}", err);

                        Timer::after(Duration::from_millis(500)).await;

                        continue;
                    }

                    Ok(channel) => break channel,
                };
            };

            client.store(RfsClient::new(channel));

            info!("reconnect success");
        }
    }

    async fn ping_loop(
        client: Arc<AtomicValue<RfsClient<Channel>>>,
        uuid: Uuid,
        failed_notify: Notify,
    ) {
        let mut rpc_timeout = INITIAL_TIMEOUT;

        'outer: loop {
            for _ in 0..3 {
                Timer::after(PING_INTERVAL).await;

                let ping_req = TonicRequest::new(PingRequest {
                    header: Some(Header {
                        uuid: uuid.as_bytes().to_vec(),
                        version: VERSION.to_string(),
                    }),
                });

                let mut client = (*client.load()).clone();

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

#[async_trait]
impl FuseFilesystem for Filesystem {
    async fn init(&self, _req: Request) -> Result<()> {
        let mut client = (*self.client.load()).clone();

        let mut compress_guard = self.compress.write().await;

        for _ in 0..3 {
            let req = TonicRequest::new(RegisterRequest {
                support_compress: *compress_guard,
            });

            return match client.register(req).await {
                Err(err) => {
                    if code_can_retry(err.code()) {
                        warn!("register failed {}", err);

                        Timer::after(Duration::from_secs(1)).await;

                        continue;
                    }

                    error!("register failed {}", err);

                    Err(libc::EIO.into())
                }

                Ok(resp) => {
                    let resp = resp.into_inner();

                    let uuid = if let Ok(uuid) = Uuid::from_slice(&resp.uuid) {
                        uuid
                    } else {
                        return Err(libc::EINVAL.into());
                    };

                    self.uuid.write().await.replace(uuid);

                    // in case server report allow_compress when client disable compress
                    *compress_guard = *compress_guard && resp.allow_compress;

                    if *compress_guard {
                        info!("compress enabled");
                    }

                    let failed_notify = self.failed_notify.clone();

                    let uri = self.uri.clone();
                    let tls_cfg = self.tls_cfg.clone();

                    Task::spawn(Self::ping_loop(
                        self.client.clone(),
                        uuid,
                        failed_notify.clone(),
                    ))
                    .detach();

                    Task::spawn(Self::reconnect_loop(
                        self.client.clone(),
                        uri,
                        tls_cfg,
                        failed_notify,
                    ))
                    .detach();

                    Ok(())
                }
            };
        }

        error!("register fails more than 3 times");

        Err(libc::ETIMEDOUT.into())
    }

    async fn destroy(&self, _req: Request) {
        let req = if let Some(uuid) = *self.uuid.read().await {
            TonicRequest::new(LogoutRequest {
                uuid: uuid.to_hyphenated().to_string(),
            })
        } else {
            warn!("before init, filesystem destroy");

            return;
        };

        info!("sending logout request");

        let mut client = (*self.client.load()).clone();

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

    async fn lookup(&self, req: Request, parent: u64, name: &OsStr) -> Result<ReplyEntry> {
        let name = match name.to_str() {
            None => return Err(libc::EINVAL.into()),
            Some(name) => name.to_string(),
        };

        let header = self.get_rpc_header().await;

        let client = self.client.clone();

        let mut rpc_timeout = INITIAL_TIMEOUT;

        for _ in 0..3 {
            let rpc_req = TonicRequest::new(LookupRequest {
                head: Some(header.clone()),
                inode: parent,
                name: name.to_string(),
            });

            let mut client = (*client.load()).clone();

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

                        Timer::after(Duration::from_secs(1)).await;

                        continue;
                    }

                    error!("lookup rpc has error {}", err);

                    return Err(libc::EIO.into());
                }

                Ok(resp) => {
                    let resp = resp.into_inner();

                    if let Some(result) = resp.result {
                        result
                    } else {
                        error!("lookup result is None");

                        return Err(libc::EIO.into());
                    }
                }
            };

            return match result {
                lookup_response::Result::Error(err) => Err((err.errno as c_int).into()),

                lookup_response::Result::Attr(attr) => Ok(ReplyEntry {
                    ttl: TTL,
                    attr: proto_attr_into_fuse_attr(attr, req.uid, req.gid)?,
                    generation: 0,
                }),
            };
        }

        error!("lookup failed more than 3 times");

        self.failed_notify.notify();

        Err(libc::ETIMEDOUT.into())
    }

    async fn getattr(
        &self,
        req: Request,
        inode: u64,
        _fh: Option<u64>,
        _flags: u32,
    ) -> Result<ReplyAttr> {
        let header = self.get_rpc_header().await;

        let client = self.client.clone();

        let mut rpc_timeout = INITIAL_TIMEOUT;

        for _ in 0..3 {
            let rpc_req = TonicRequest::new(GetAttrRequest {
                head: Some(header.clone()),
                inode,
            });

            let mut client = (*client.load()).clone();

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

                        Timer::after(Duration::from_secs(1)).await;

                        continue;
                    }

                    error!("getattr rpc has error {}", err);

                    return Err(libc::EIO.into());
                }

                Ok(resp) => {
                    let resp = resp.into_inner();

                    if let Some(result) = resp.result {
                        result
                    } else {
                        error!("getattr result is None");

                        return Err(libc::EIO.into());
                    }
                }
            };

            return match result {
                get_attr_response::Result::Error(err) => Err((err.errno as c_int).into()),
                get_attr_response::Result::Attr(attr) => Ok(ReplyAttr {
                    ttl: TTL,
                    attr: proto_attr_into_fuse_attr(attr, req.uid, req.gid)?,
                }),
            };
        }

        error!("getattr failed more than 3 times");

        self.failed_notify.notify();

        Err(libc::ETIMEDOUT.into())
    }

    async fn setattr(
        &self,
        req: Request,
        inode: u64,
        _fh: Option<u64>,
        set_attr: SetAttr,
    ) -> Result<ReplyAttr> {
        let header = self.get_rpc_header().await;

        let client = self.client.clone();

        let mut rpc_timeout = INITIAL_TIMEOUT;

        for _ in 0..3 {
            let rpc_req = TonicRequest::new(SetAttrRequest {
                head: Some(header.clone()),
                inode,
                attr: Some(Attr {
                    inode,
                    name: String::new(),
                    mode: if let Some(mode) = set_attr.mode {
                        mode as i32
                    } else {
                        -1
                    },
                    size: if let Some(size) = set_attr.size {
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

            let mut client = (*client.load()).clone();

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

                        Timer::after(Duration::from_secs(1)).await;

                        continue;
                    }

                    error!("setattr rpc has error {}", err);

                    return Err(libc::EIO.into());
                }

                Ok(resp) => {
                    if let Some(result) = resp.into_inner().result {
                        result
                    } else {
                        error!("setattr result is None");

                        return Err(libc::EIO.into());
                    }
                }
            };

            return match result {
                set_attr_response::Result::Error(err) => {
                    error!("setattr failed errno {}", err.errno);

                    Err((err.errno as c_int).into())
                }

                set_attr_response::Result::Attr(attr) => Ok(ReplyAttr {
                    ttl: TTL,
                    attr: proto_attr_into_fuse_attr(attr, req.uid, req.gid)?,
                }),
            };
        }

        error!("setattr failed more than 3 times");

        self.failed_notify.notify();

        Err(libc::ETIMEDOUT.into())
    }

    async fn mkdir(
        &self,
        req: Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
    ) -> Result<ReplyEntry> {
        let name = match name.to_str() {
            None => return Err(libc::EINVAL.into()),
            Some(name) => name.to_string(),
        };

        let header = self.get_rpc_header().await;

        let client = self.client.clone();

        let mut rpc_timeout = INITIAL_TIMEOUT;

        for _ in 0..3 {
            let rpc_req = TonicRequest::new(MkdirRequest {
                head: Some(header.clone()),
                inode: parent,
                name: name.to_string(),
                mode,
            });

            let mut client = (*client.load()).clone();

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

                        Timer::after(Duration::from_secs(1)).await;

                        continue;
                    }

                    error!("mkdir rpc has error {}", err);

                    return Err(libc::EIO.into());
                }

                Ok(resp) => {
                    if let Some(result) = resp.into_inner().result {
                        result
                    } else {
                        error!("mkdir result is None");

                        return Err(libc::EIO.into());
                    }
                }
            };

            return match result {
                mkdir_response::Result::Error(err) => Err((err.errno as c_int).into()),
                mkdir_response::Result::Attr(attr) => Ok(ReplyEntry {
                    ttl: TTL,
                    attr: proto_attr_into_fuse_attr(attr, req.uid, req.gid)?,
                    generation: 0,
                }),
            };
        }

        error!("mkdir failed more than 3 times");

        self.failed_notify.notify();

        Err(libc::ETIMEDOUT.into())
    }

    async fn unlink(&self, _req: Request, parent: u64, name: &OsStr) -> Result<()> {
        let name = match name.to_str() {
            None => return Err(libc::EINVAL.into()),
            Some(name) => name.to_string(),
        };

        let header = self.get_rpc_header().await;

        let client = self.client.clone();

        let mut rpc_timeout = INITIAL_TIMEOUT;

        for _ in 0..3 {
            let rpc_req = TonicRequest::new(UnlinkRequest {
                head: Some(header.clone()),
                inode: parent,
                name: name.to_string(),
            });

            let mut client = (*client.load()).clone();

            let result = match timeout(rpc_timeout, client.unlink(rpc_req)).await {
                Err(err) => {
                    warn!("unlink rpc timeout {}", err);

                    rpc_timeout = rpc_timeout.mul_f64(MULTIPLIER);

                    continue;
                }

                Ok(result) => result,
            };

            return match result {
                Err(err) => {
                    if code_can_retry(err.code()) {
                        warn!("unlink rpc has error {}", err);

                        Timer::after(Duration::from_secs(1)).await;

                        continue;
                    }

                    error!("unlink rpc has error {}", err);

                    Err(libc::EIO.into())
                }

                Ok(resp) => {
                    if let Some(error) = resp.into_inner().error {
                        Err((error.errno as c_int).into())
                    } else {
                        Ok(())
                    }
                }
            };
        }

        error!("unlink failed more than 3 times");

        self.failed_notify.notify();

        Err(libc::ETIMEDOUT.into())
    }

    async fn rmdir(&self, _req: Request, parent: u64, name: &OsStr) -> Result<()> {
        let name = match name.to_str() {
            None => return Err(libc::EINVAL.into()),
            Some(name) => name.to_string(),
        };

        let header = self.get_rpc_header().await;

        let client = self.client.clone();

        let mut rpc_timeout = INITIAL_TIMEOUT;

        for _ in 0..3 {
            let rpc_req = TonicRequest::new(RmDirRequest {
                head: Some(header.clone()),
                inode: parent,
                name: name.to_string(),
            });

            let mut client = (*client.load()).clone();

            let result = match timeout(rpc_timeout, client.rm_dir(rpc_req)).await {
                Err(err) => {
                    warn!("rmdir rpc timeout {}", err);

                    rpc_timeout = rpc_timeout.mul_f64(MULTIPLIER);

                    continue;
                }

                Ok(result) => result,
            };

            return match result {
                Err(err) => {
                    if code_can_retry(err.code()) {
                        warn!("rmdir rpc has error {}", err);

                        Timer::after(Duration::from_secs(1)).await;

                        continue;
                    }

                    error!("rmdir rpc has error {}", err);

                    Err(libc::EIO.into())
                }

                Ok(resp) => {
                    if let Some(error) = resp.into_inner().error {
                        Err((error.errno as c_int).into())
                    } else {
                        Ok(())
                    }
                }
            };
        }

        error!("rmdir failed more than 3 times");

        self.failed_notify.notify();

        Err(libc::ETIMEDOUT.into())
    }

    async fn rename(
        &self,
        _req: Request,
        parent: u64,
        name: &OsStr,
        new_parent: u64,
        new_name: &OsStr,
    ) -> Result<()> {
        let name = match name.to_str() {
            None => return Err(libc::EINVAL.into()),
            Some(name) => name.to_string(),
        };

        let new_name = match new_name.to_str() {
            None => return Err(libc::EINVAL.into()),
            Some(new_name) => new_name.to_string(),
        };

        let header = self.get_rpc_header().await;

        let client = self.client.clone();

        let mut rpc_timeout = INITIAL_TIMEOUT;

        for _ in 0..3 {
            let rpc_req = TonicRequest::new(RenameRequest {
                head: Some(header.clone()),
                old_parent: parent,
                old_name: name.to_string(),
                new_parent,
                new_name: new_name.to_string(),
            });

            let mut client = (*client.load()).clone();

            let result = match timeout(rpc_timeout, client.rename(rpc_req)).await {
                Err(err) => {
                    warn!("rename rpc timeout {}", err);

                    rpc_timeout = rpc_timeout.mul_f64(MULTIPLIER);

                    continue;
                }

                Ok(result) => result,
            };

            return match result {
                Err(err) => {
                    if code_can_retry(err.code()) {
                        warn!("rename rpc has error {}", err);

                        Timer::after(Duration::from_secs(1)).await;

                        continue;
                    }

                    error!("rename rpc has error {}", err);

                    Err(libc::EIO.into())
                }

                Ok(resp) => {
                    if let Some(error) = resp.into_inner().error {
                        Err((error.errno as c_int).into())
                    } else {
                        Ok(())
                    }
                }
            };
        }

        error!("rename failed more than 3 times");

        self.failed_notify.notify();

        Err(libc::ETIMEDOUT.into())
    }

    async fn open(&self, _req: Request, inode: u64, flags: u32) -> Result<ReplyOpen> {
        let header = self.get_rpc_header().await;

        let client = self.client.clone();

        debug!("client open inode {} flags {}", inode, flags);

        let mut rpc_timeout = INITIAL_TIMEOUT;

        for _ in 0..3 {
            let rpc_req = TonicRequest::new(OpenFileRequest {
                head: Some(header.clone()),
                inode,
                flags,
            });

            let mut client = (*client.load()).clone();

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

                        Timer::after(Duration::from_secs(1)).await;

                        continue;
                    }

                    error!("open rpc has error {}", err);

                    return Err(libc::EIO.into());
                }

                Ok(resp) => {
                    if let Some(result) = resp.into_inner().result {
                        result
                    } else {
                        error!("open result is None");

                        return Err(libc::EIO.into());
                    }
                }
            };

            return match result {
                open_file_response::Result::Error(err) => Err((err.errno as i32).into()),

                open_file_response::Result::FileHandleId(fh_id) => {
                    Ok(ReplyOpen { fh: fh_id, flags })
                }
            };
        }

        error!("open failed more than 3 times");

        self.failed_notify.notify();

        Err(libc::ETIMEDOUT.into())
    }

    async fn read(
        &self,
        _req: Request,
        _inode: u64,
        fh: u64,
        offset: u64,
        size: u32,
    ) -> Result<ReplyData> {
        let header = self.get_rpc_header().await;

        let client = self.client.clone();

        let mut rpc_timeout = INITIAL_TIMEOUT;

        for _ in 0..3 {
            let rpc_req = TonicRequest::new(ReadFileRequest {
                head: Some(header.clone()),
                file_handle_id: fh,
                offset: offset as _,
                size: size as u64,
            });

            let mut client = (*client.load()).clone();

            let result = match timeout(rpc_timeout, client.read_file(rpc_req)).await {
                Err(err) => {
                    warn!("read_file rpc timeout {}", err);

                    rpc_timeout = rpc_timeout.mul_f64(MULTIPLIER);

                    continue;
                }

                Ok(result) => result,
            };

            return match result {
                Err(err) => {
                    if code_can_retry(err.code()) {
                        warn!("read_file rpc has error {}", err);

                        Timer::after(Duration::from_secs(1)).await;

                        continue;
                    }

                    error!("read_file rpc has error {}", err);

                    Err(libc::EIO.into())
                }

                Ok(resp) => {
                    let result = resp.into_inner();

                    if let Some(err) = result.error {
                        return Err((err.errno as i32).into());
                    }

                    if result.compressed {
                        match Task::blocking(async move {
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
                            Err(err) => Err(err.into()),
                            Ok(data) => Ok(ReplyData { data }),
                        }
                    } else {
                        Ok(ReplyData { data: result.data })
                    }
                }
            };
        }

        error!("read_file failed more than 3 times");

        self.failed_notify.notify();

        Err(libc::ETIMEDOUT.into())
    }

    async fn write(
        &self,
        _req: Request,
        _inode: u64,
        fh: u64,
        offset: u64,
        data: &[u8],
        _flags: u32,
    ) -> Result<ReplyWrite> {
        let header = self.get_rpc_header().await;

        let client = self.client.clone();

        let enable_compress = *self.compress.read().await;

        let data = data.to_vec();

        let (data, compressed) = Task::blocking(async move {
            if enable_compress && data.len() > MIN_COMPRESS_SIZE {
                let mut encoder = FrameEncoder::new(Vec::with_capacity(MIN_COMPRESS_SIZE)); // should I choose a better size?

                if let Err(err) = encoder.write_all(&data) {
                    warn!("compress write data failed {}", err);

                    (data, false)
                } else {
                    match encoder.into_inner() {
                        Err(err) => {
                            warn!("get compress data failed {}", err);

                            (data, false)
                        }

                        Ok(compressed_data) => {
                            // sometimes compressed data is bigger than original data, so we should
                            // use original data directly
                            if compressed_data.len() < data.len() {
                                (compressed_data, true)
                            } else {
                                debug!("compressed data is bigger than original data");

                                (data, false)
                            }
                        }
                    }
                }
            } else {
                (data, false)
            }
        })
        .await;

        let mut rpc_timeout = INITIAL_TIMEOUT;

        for _ in 0..3 {
            let rpc_req = TonicRequest::new(WriteFileRequest {
                head: Some(header.clone()),
                file_handle_id: fh,
                offset: offset as _,
                data: data.clone(),
                compressed,
            });

            let mut client = (*client.load()).clone();

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

                        Timer::after(Duration::from_secs(1)).await;

                        continue;
                    }

                    error!("write_file rpc has error {}", err);

                    return Err(libc::EIO.into());
                }

                Ok(resp) => {
                    if let Some(result) = resp.into_inner().result {
                        result
                    } else {
                        error!("write_file result is None");

                        return Err(libc::EIO.into());
                    }
                }
            };

            return match result {
                write_file_response::Result::Error(err) => Err((err.errno as i32).into()),
                write_file_response::Result::Written(written) => Ok(ReplyWrite { written }),
            };
        }

        error!("write_file failed more than 3 times");

        self.failed_notify.notify();

        Err(libc::ETIMEDOUT.into())
    }

    async fn release(
        &self,
        _req: Request,
        _inode: u64,
        fh: u64,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
    ) -> Result<()> {
        let header = self.get_rpc_header().await;

        let client = self.client.clone();

        let mut rpc_timeout = INITIAL_TIMEOUT;

        for _ in 0..3 {
            let rpc_req = TonicRequest::new(CloseFileRequest {
                head: Some(header.clone()),
                file_handle_id: fh,
            });

            let mut client = (*client.load()).clone();

            let result = match timeout(rpc_timeout, client.close_file(rpc_req)).await {
                Err(err) => {
                    warn!("close file rpc timeout {}", err);

                    rpc_timeout = rpc_timeout.mul_f64(MULTIPLIER);

                    continue;
                }

                Ok(result) => result,
            };

            return match result {
                Err(err) => {
                    if code_can_retry(err.code()) {
                        warn!("close_file rpc has error {}", err);

                        Timer::after(Duration::from_secs(1)).await;

                        continue;
                    }

                    error!("close_file rpc has error {}", err);

                    Err(libc::EIO.into())
                }

                Ok(resp) => {
                    if let Some(err) = resp.into_inner().error {
                        Err((err.errno as c_int).into())
                    } else {
                        Ok(())
                    }
                }
            };
        }

        error!("close_file failed more than 3 times");

        self.failed_notify.notify();

        Err(libc::ETIMEDOUT.into())
    }

    async fn fsync(&self, _req: Request, _inode: u64, fh: u64, _datasync: bool) -> Result<()> {
        let header = self.get_rpc_header().await;

        let client = self.client.clone();

        let mut rpc_timeout = INITIAL_TIMEOUT;

        for _ in 0..3 {
            let rpc_req = TonicRequest::new(SyncFileRequest {
                head: Some(header.clone()),
                file_handle_id: fh,
            });

            let mut client = (*client.load()).clone();

            let result = match timeout(rpc_timeout, client.sync_file(rpc_req)).await {
                Err(err) => {
                    warn!("sync file rpc timeout {}", err);

                    rpc_timeout = rpc_timeout.mul_f64(MULTIPLIER);

                    continue;
                }

                Ok(result) => result,
            };

            return match result {
                Err(err) => {
                    if code_can_retry(err.code()) {
                        warn!("sync_file rpc has error {}", err);

                        Timer::after(Duration::from_secs(1)).await;

                        continue;
                    }

                    error!("sync_file rpc has error {}", err);

                    Err(libc::EIO.into())
                }

                Ok(resp) => {
                    if let Some(err) = resp.into_inner().error {
                        Err((err.errno as c_int).into())
                    } else {
                        Ok(())
                    }
                }
            };
        }

        error!("sync_file failed more than 3 times");

        self.failed_notify.notify();

        Err(libc::ETIMEDOUT.into())
    }

    async fn flush(&self, _req: Request, _inode: u64, fh: u64, _lock_owner: u64) -> Result<()> {
        let header = self.get_rpc_header().await;

        let client = self.client.clone();

        let mut rpc_timeout = INITIAL_TIMEOUT;

        for _ in 0..3 {
            let rpc_req = TonicRequest::new(FlushRequest {
                head: Some(header.clone()),
                file_handle_id: fh,
            });

            let mut client = (*client.load()).clone();

            let result = match timeout(rpc_timeout, client.flush(rpc_req)).await {
                Err(err) => {
                    warn!("flush rpc timeout {}", err);

                    rpc_timeout = rpc_timeout.mul_f64(MULTIPLIER);

                    continue;
                }

                Ok(result) => result,
            };

            return match result {
                Err(err) => {
                    if code_can_retry(err.code()) {
                        warn!("flush rpc has error {}", err);

                        Timer::after(Duration::from_secs(1)).await;

                        continue;
                    }

                    error!("flush rpc has error {}", err);

                    Err(libc::EIO.into())
                }

                Ok(resp) => {
                    if let Some(err) = resp.into_inner().error {
                        Err((err.errno as c_int).into())
                    } else {
                        Ok(())
                    }
                }
            };
        }

        error!("flush failed more than 3 times");

        self.failed_notify.notify();

        Err(libc::ETIMEDOUT.into())
    }

    async fn readdir(
        &self,
        _req: Request,
        parent: u64,
        _fh: u64,
        offset: i64,
    ) -> Result<ReplyDirectory> {
        debug!("readdir inode {}, offset {}", parent, offset);

        let header = self.get_rpc_header().await;

        let client = self.client.clone();

        let mut rpc_timeout = INITIAL_TIMEOUT;

        for _ in 0..3 {
            let rpc_req = TonicRequest::new(ReadDirRequest {
                head: Some(header.clone()),
                inode: parent,
                offset: offset as _,
            });

            let mut client = (*client.load()).clone();

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

                        Timer::after(Duration::from_secs(1)).await;

                        continue;
                    }

                    error!("read_dir rpc has error {}", err);

                    return Err(libc::EIO.into());
                }

                Ok(resp) => {
                    let resp = resp.into_inner();

                    if let Some(error) = resp.error {
                        return Err((error.errno as c_int).into());
                    }

                    resp.dir_entries
                }
            };

            debug!("got readdir result");

            let entries =
                dir_entries
                    .into_iter()
                    .enumerate()
                    .filter_map(move |(index, dir_entry)| {
                        let dir = EntryType::Dir as i32;
                        let file = EntryType::File as i32;

                        let kind = if dir_entry.r#type == dir {
                            FileType::Directory
                        } else if dir_entry.r#type == file {
                            FileType::RegularFile
                        } else {
                            warn!("unexpect file type {}", dir_entry.r#type);

                            return None;
                        };

                        Some(DirectoryEntry {
                            inode: dir_entry.inode,
                            index: offset as u64 + index as u64 + 1,
                            kind,
                            name: OsString::from(dir_entry.name),
                        })
                    });

            return Ok(ReplyDirectory {
                entries: Box::pin(stream::iter(entries)),
            });
        }

        error!("read_dir failed more than 3 times");

        self.failed_notify.notify();

        Err(libc::ETIMEDOUT.into())
    }

    async fn getlk(
        &self,
        _req: Request,
        _inode: u64,
        fh: u64,
        _lock_owner: u64,
        start: u64,
        end: u64,
        _type: u32,
        pid: u32,
    ) -> Result<ReplyLock> {
        let header = self.get_rpc_header().await;

        let client = self.client.clone();

        let mut rpc_timeout = INITIAL_TIMEOUT;

        for _ in 0..3 {
            let rpc_req = TonicRequest::new(GetLockRequest {
                head: Some(header.clone()),
                file_handle_id: fh,
            });

            let mut client = (*client.load()).clone();

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

                        Timer::after(Duration::from_secs(1)).await;

                        continue;
                    }

                    error!("get_lock rpc has error {}", err);

                    return Err(libc::EIO.into());
                }

                Ok(resp) => {
                    if let Some(result) = resp.into_inner().result {
                        result
                    } else {
                        error!("get_lock result is None");

                        return Err(libc::EIO.into());
                    }
                }
            };

            return match result {
                get_lock_response::Result::Error(err) => Err((err.errno as i32).into()),

                get_lock_response::Result::LockType(lock_type) => {
                    let lock_type = match lock_type {
                        n if n == LockType::ReadLock as i32 => libc::F_RDLCK,
                        n if n == LockType::WriteLock as i32 => libc::F_WRLCK,
                        n if n == LockType::NoLock as i32 => libc::F_UNLCK, // TODO is it right way?
                        _ => {
                            error!("unknown lock type {}", lock_type);

                            return Err(libc::EIO.into());
                        }
                    };

                    Ok(ReplyLock {
                        start,
                        end,
                        r#type: lock_type as _,
                        pid,
                    })
                }
            };
        }

        error!("get_lock failed more than 3 times");

        self.failed_notify.notify();

        Err(libc::ETIMEDOUT.into())
    }

    async fn setlk(
        &self,
        req: Request,
        _inode: u64,
        fh: u64,
        _lock_owner: u64,
        _start: u64,
        _end: u64,
        r#type: u32,
        _pid: u32,
        block: bool,
    ) -> Result<()> {
        let header = self.get_rpc_header().await;

        let client = self.client.clone();

        if r#type == libc::F_UNLCK as u32 {
            let mut rpc_timeout = INITIAL_TIMEOUT;

            for _ in 0..3 {
                let rpc_req = TonicRequest::new(ReleaseLockRequest {
                    head: Some(header.clone()),
                    file_handle_id: fh,
                    block: false,
                });

                let mut client = (*client.load()).clone();

                let result = match timeout(rpc_timeout, client.release_lock(rpc_req)).await {
                    Err(err) => {
                        warn!("release_lock rpc timeout {}", err);

                        rpc_timeout = rpc_timeout.mul_f64(MULTIPLIER);

                        continue;
                    }

                    Ok(result) => result,
                };

                return match result {
                    Err(err) => {
                        if code_can_retry(err.code()) {
                            warn!("release_lock rpc has error {}", err);

                            Timer::after(Duration::from_secs(1)).await;

                            continue;
                        }

                        error!("release_lock rpc has error {}", err);

                        Err(libc::EIO.into())
                    }

                    Ok(resp) => {
                        if let Some(error) = resp.into_inner().error {
                            Err((error.errno as c_int).into())
                        } else {
                            Ok(())
                        }
                    }
                };
            }

            error!("release_lock failed more than 3 times");

            self.failed_notify.notify();

            return Err(libc::ETIMEDOUT.into());
        }

        let lock_kind = {
            match r#type as i32 {
                libc::F_RDLCK => LockType::ReadLock,
                libc::F_WRLCK => LockType::WriteLock,

                _ => return Err(libc::EINVAL.into()),
            }
        };

        for _ in 0..3 {
            let rpc_req = TonicRequest::new(SetLockRequest {
                head: Some(header.clone()),
                file_handle_id: fh,
                unique: req.unique,
                lock_kind: lock_kind.into(),
                block,
            });

            let mut client = (*client.load()).clone();

            return match client.set_lock(rpc_req).await {
                Err(err) => {
                    if code_can_retry(err.code()) {
                        warn!("set_lock rpc has error {}", err);

                        Timer::after(Duration::from_secs(1)).await;

                        continue;
                    }

                    error!("set_lock rpc has error {}", err);

                    Err(libc::EIO.into())
                }

                Ok(resp) => {
                    if let Some(error) = resp.into_inner().error {
                        warn!(
                            "set lock failed, unique {} errno is {}",
                            req.unique, error.errno
                        );

                        Err((error.errno as c_int).into())
                    } else {
                        Ok(())
                    }
                }
            };
        }

        error!("set_lock failed more than 3 times");

        self.failed_notify.notify();

        return Err(libc::ETIMEDOUT.into());
    }

    #[inline]
    async fn access(&self, _req: Request, _inode: u64, _mask: u32) -> Result<()> {
        Ok(())
    }

    async fn create(
        &self,
        req: Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        flags: u32,
    ) -> Result<ReplyCreated> {
        let name = match name.to_str() {
            None => return Err(libc::EINVAL.into()),
            Some(name) => name.to_string(),
        };

        let header = self.get_rpc_header().await;

        let client = self.client.clone();

        let mut rpc_timeout = INITIAL_TIMEOUT;

        for _ in 0..3 {
            let rpc_req = TonicRequest::new(CreateFileRequest {
                head: Some(header.clone()),
                inode: parent,
                name: name.to_string(),
                mode,
                flags,
            });

            let mut client = (*client.load()).clone();

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

                        Timer::after(Duration::from_secs(1)).await;

                        continue;
                    }

                    error!("create_file rpc has error {}", err);

                    return Err(libc::EIO.into());
                }

                Ok(resp) => {
                    let resp = resp.into_inner();

                    if let Some(error) = resp.error {
                        return Err((error.errno as c_int).into());
                    }

                    if resp.attr.is_none() {
                        error!("create_file attr is None");

                        return Err(libc::EIO.into());
                    }

                    (resp.file_handle_id, resp.attr.unwrap())
                }
            };

            return Ok(ReplyCreated {
                ttl: TTL,
                attr: proto_attr_into_fuse_attr(attr, req.uid, req.gid)?,
                generation: 0,
                fh: fh_id,
                flags,
            });
        }

        error!("create_file failed more than 3 times");

        self.failed_notify.notify();

        return Err(libc::ETIMEDOUT.into());
    }

    async fn interrupt(&self, _req: Request, unique: u64) -> Result<()> {
        debug!("interrupt unique {}", unique);

        let header = self.get_rpc_header().await;

        let client = self.client.clone();

        let mut rpc_timeout = INITIAL_TIMEOUT;

        for _ in 0..3 {
            let rpc_req = TonicRequest::new(InterruptRequest {
                head: Some(header.clone()),
                unique,
            });

            let mut client = (*client.load()).clone();

            let result = match timeout(rpc_timeout, client.interrupt(rpc_req)).await {
                Err(err) => {
                    warn!("interrupt rpc timeout {}", err);

                    rpc_timeout = rpc_timeout.mul_f64(MULTIPLIER);

                    continue;
                }

                Ok(result) => result,
            };

            return match result {
                Err(err) => {
                    if code_can_retry(err.code()) {
                        warn!("interrupt rpc has error {}", err);

                        Timer::after(Duration::from_secs(1)).await;

                        continue;
                    }

                    error!("interrupt rpc has error {}", err);

                    Err(libc::EIO.into())
                }

                Ok(resp) => {
                    if let Some(err) = resp.into_inner().error {
                        Err((err.errno as c_int).into())
                    } else {
                        Ok(())
                    }
                }
            };
        }

        error!("interrupt failed more than 3 times");

        self.failed_notify.notify();

        return Err(libc::ETIMEDOUT.into());
    }

    async fn fallocate(
        &self,
        _req: Request,
        _inode: u64,
        fh: u64,
        offset: u64,
        length: u64,
        mode: u32,
    ) -> Result<()> {
        let header = self.get_rpc_header().await;

        let client = self.client.clone();

        let mut rpc_timeout = INITIAL_TIMEOUT;

        for _ in 0..3 {
            let rpc_req = TonicRequest::new(AllocateRequest {
                head: Some(header.clone()),
                file_handle_id: fh,
                offset,
                size: length,
                mode,
            });

            let mut client = (*client.load()).clone();

            let result = match timeout(rpc_timeout, client.allocate(rpc_req)).await {
                Err(err) => {
                    warn!("allocate rpc timeout {}", err);

                    rpc_timeout = rpc_timeout.mul_f64(MULTIPLIER);

                    continue;
                }

                Ok(result) => result,
            };

            return match result {
                Err(err) => {
                    if code_can_retry(err.code()) {
                        warn!("allocate rpc has error {}", err);

                        Timer::after(Duration::from_secs(1)).await;

                        continue;
                    }

                    error!("allocate rpc has error {}", err);

                    Err(libc::EIO.into())
                }

                Ok(resp) => {
                    if let Some(err) = resp.into_inner().error {
                        Err((err.errno as c_int).into())
                    } else {
                        Ok(())
                    }
                }
            };
        }

        error!("interrupt failed more than 3 times");

        self.failed_notify.notify();

        return Err(libc::ETIMEDOUT.into());
    }

    async fn readdirplus(
        &self,
        req: Request,
        parent: u64,
        _fh: u64,
        offset: u64,
        _lock_owner: u64,
    ) -> Result<ReplyDirectoryPlus> {
        debug!("readdirplus inode {}, offset {}", parent, offset);

        let header = self.get_rpc_header().await;

        let client = self.client.clone();

        let mut rpc_timeout = INITIAL_TIMEOUT;

        for _ in 0..3 {
            let rpc_req = TonicRequest::new(ReadDirRequest {
                head: Some(header.clone()),
                inode: parent,
                offset: offset as _,
            });

            let mut client = (*client.load()).clone();

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

                        Timer::after(Duration::from_secs(1)).await;

                        continue;
                    }

                    error!("read_dir rpc has error {}", err);

                    return Err(libc::EIO.into());
                }

                Ok(resp) => {
                    let resp = resp.into_inner();

                    if let Some(error) = resp.error {
                        return Err((error.errno as c_int).into());
                    }

                    resp.dir_entries
                }
            };

            debug!("got readdirplus result");

            let entries =
                dir_entries
                    .into_iter()
                    .enumerate()
                    .filter_map(move |(index, dir_entry)| {
                        let attr = if let Some(attr) = dir_entry.attr {
                            attr
                        } else {
                            warn!(
                                "dir entry {} in parent {} attr is None",
                                dir_entry.name, parent
                            );

                            return None;
                        };

                        let attr =
                            if let Ok(attr) = proto_attr_into_fuse_attr(attr, req.uid, req.gid) {
                                attr
                            } else {
                                warn!(
                                    "parse dir entry {} in parent {} fuse attr failed",
                                    dir_entry.name, parent
                                );

                                return None;
                            };

                        Some(DirectoryEntryPlus {
                            inode: dir_entry.inode,
                            generation: 0,
                            index: offset as u64 + index as u64 + 1,
                            kind: attr.kind,
                            name: OsString::from(dir_entry.name),
                            attr,
                            entry_ttl: TTL,
                            attr_ttl: TTL,
                        })
                    });

            return Ok(ReplyDirectoryPlus {
                entries: Box::pin(stream::iter(entries)),
            });
        }

        error!("read_dir failed more than 3 times");

        self.failed_notify.notify();

        Err(libc::ETIMEDOUT.into())
    }

    #[inline]
    async fn rename2(
        &self,
        req: Request,
        parent: u64,
        name: &OsStr,
        new_parent: u64,
        new_name: &OsStr,
        _flags: u32,
    ) -> Result<()> {
        self.rename(req, parent, name, new_parent, new_name).await
    }

    /*async fn lseek(&self, _req: Request, _inode: u64, _fh: u64, _offset: u64, _whence: u32) -> Result<ReplyLSeek> {
        unimplemented!()
    }*/

    async fn copy_file_range(
        &self,
        _req: Request,
        _inode: u64,
        fh_in: u64,
        off_in: u64,
        _inode_out: u64,
        fh_out: u64,
        off_out: u64,
        length: u64,
        flags: u64,
    ) -> Result<ReplyCopyFileRange> {
        let header = self.get_rpc_header().await;

        let client = self.client.clone();

        let mut rpc_timeout = INITIAL_TIMEOUT;

        for _ in 0..3 {
            let rpc_req = TonicRequest::new(CopyFileRangeRequest {
                head: Some(header.clone()),
                file_handle_id_in: fh_in,
                offset_in: off_in,
                file_handle_id_out: fh_out,
                offset_out: off_out,
                size: length,
                flags,
            });

            let mut client = (*client.load()).clone();

            let result = match timeout(rpc_timeout, client.copy_file_range(rpc_req)).await {
                Err(err) => {
                    warn!("copy_file_range rpc timeout {}", err);

                    rpc_timeout = rpc_timeout.mul_f64(MULTIPLIER);

                    continue;
                }

                Ok(result) => result,
            };

            let result = match result {
                Err(err) => {
                    if code_can_retry(err.code()) {
                        warn!("copy_file_range rpc has error {}", err);

                        Timer::after(Duration::from_secs(1)).await;

                        continue;
                    }

                    error!("copy_file_range rpc has error {}", err);

                    return Err(libc::EIO.into());
                }

                Ok(resp) => {
                    if let Some(result) = resp.into_inner().result {
                        result
                    } else {
                        error!("copy_file_range result is None");

                        return Err(libc::EIO.into());
                    }
                }
            };

            return match result {
                copy_file_range_response::Result::Error(err) => Err((err.errno as i32).into()),
                copy_file_range_response::Result::Copied(copied) => {
                    Ok(ReplyCopyFileRange { copied })
                }
            };
        }

        error!("copy_file_range failed more than 3 times");

        self.failed_notify.notify();

        Err(libc::ETIMEDOUT.into())
    }
}

fn code_can_retry(code: Code) -> bool {
    code == Code::Unavailable || code == Code::DeadlineExceeded
}
