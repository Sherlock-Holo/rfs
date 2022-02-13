use std::ffi::{OsStr, OsString};
use std::future::Future;
use std::io::{Read, Write};
use std::os::raw::c_int;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_notify::Notify;
use async_signals::Signals;
use async_trait::async_trait;
use atomic_value::AtomicValue;
use fuse3::path::prelude::*;
use fuse3::{Errno, MountOptions, Result};
use futures_util::stream::{self, Empty, Stream};
use futures_util::{StreamExt, TryFutureExt};
use nix::mount;
use nix::mount::MntFlags;
use snap::read::FrameDecoder;
use snap::write::FrameEncoder;
use tokio::sync::RwLock;
use tokio::task;
use tokio::time::{self, timeout};
use tonic::body::BoxBody;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint, Uri};
use tonic::Request as TonicRequest;
use tower::layer::util::{Identity, Stack};
use tower::reconnect::Reconnect;
use tower::{service_fn, Service, ServiceBuilder};
use tracing::{debug, error, info, info_span, warn, Instrument};
use uuid::Uuid;

use super::rpc::middleware::timeout::{PathTimeoutLayer, PathTimeoutService};
use crate::client::rpc::middleware::retry::{RetryClient, RetryHandle};
use crate::client::rpc::middleware::BoxError;
use crate::helper::proto_attr_into_fuse_attr;
use crate::pb::rfs_client::RfsClient;
use crate::pb::*;

const TTL: Duration = Duration::from_secs(1);
const INITIAL_TIMEOUT: Duration = Duration::from_secs(5);
const MULTIPLIER: f64 = 1.5;
const MIN_COMPRESS_SIZE: usize = 2048;
const PING_INTERVAL: Duration = Duration::from_secs(60);
const READDIR_LIMIT: u64 = 10;

type RetryChannel = RetryClient<Channel, SimpleRetryHandle>;

#[derive(Debug, Clone, Default)]
struct SimpleRetryHandle;

impl RetryHandle for SimpleRetryHandle {
    fn should_retry(&self, err: &(dyn std::error::Error + Send + Sync)) -> bool {
        todo!()
    }
}

pub struct Filesystem {
    uuid: RwLock<Option<Uuid>>,
    client: Arc<AtomicValue<RfsClient<PathTimeoutService<RetryChannel>>>>,
    failed_notify: Arc<Notify>,
    compress: RwLock<bool>,
    endpoint: Endpoint,
    client_service_builder: ServiceBuilder<Stack<PathTimeoutLayer, Identity>>,
}

impl Filesystem {
    pub async fn new(uri: Uri, tls_cfg: ClientTlsConfig, compress: bool) -> anyhow::Result<Self> {
        if compress {
            info!("try to enable compress");
        }

        info!("connecting server");

        let endpoint = Endpoint::from(uri)
            .tls_config(tls_cfg)?
            .tcp_nodelay(true)
            .tcp_keepalive(Some(Duration::from_secs(5)));

        let channel = RetryClient::new_with_retry_handle(
            endpoint.connect().await?,
            SimpleRetryHandle::default(),
        );

        /*let reconnect = Reconnect::with_connection(channel, service_fn(|endpoint: Endpoint| async move {
            let channel = endpoint.connect().await?;

            Ok::<_, BoxError>(RetryClient::new_with_retry_handle(
                channel,
                SimpleRetryHandle::default(),
            ))
        }), endpoint);*/

        info!("server connected");

        let layer = PathTimeoutLayer::new(INITIAL_TIMEOUT, None);
        let builder = ServiceBuilder::new().layer(layer);
        let service = builder.service(channel);
        let client = Arc::new(AtomicValue::new(RfsClient::new(service)));

        Ok(Filesystem {
            uuid: RwLock::new(None),
            client,
            failed_notify: Arc::new(Notify::new()),
            compress: RwLock::new(compress),
            endpoint,
            client_service_builder: builder,
        })
    }

    pub async fn mount<P: AsRef<Path>>(self, mount_point: P) -> anyhow::Result<()> {
        let mount_point = mount_point.as_ref();

        let unmount_point = mount_point.to_path_buf();

        let mut stop_signal = Signals::new(vec![libc::SIGINT, libc::SIGTERM])?;

        tokio::spawn(async move {
            debug!("waiting stop signal");

            stop_signal.next().await;

            info!("receive stop signal");

            drop(stop_signal); // in case release signal handle

            info!("stopping rfs");

            task::spawn_blocking(move || {
                while let Err(err) = mount::umount2(&unmount_point, MntFlags::MNT_DETACH) {
                    error!("lazy unmount failed {}", err);

                    std::thread::sleep(Duration::from_secs(1));
                }
            })
            .await
            .unwrap();
        });

        let mount_options = MountOptions::default()
            .fs_name("rfs")
            .nonempty(true)
            .force_readdir_plus(true);

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
        builder: ServiceBuilder<Stack<PathTimeoutLayer, Identity>>,
        client: Arc<AtomicValue<RfsClient<PathTimeoutService<RetryChannel>>>>,
        endpoint: Endpoint,
        failed_notify: Arc<Notify>,
    ) {
        loop {
            failed_notify.notified().await;

            warn!("rpc failed, need reconnect");

            let channel = loop {
                match endpoint.connect().await {
                    Err(err) => {
                        error!("reconnect failed {}", err);

                        time::sleep(Duration::from_millis(500)).await;

                        continue;
                    }

                    Ok(channel) => break channel,
                };
            };

            let retry_client = RetryClient::new_with_retry_handle(channel, SimpleRetryHandle);

            client.store(RfsClient::new(builder.service(retry_client)));

            info!("reconnect success");
        }
    }

    async fn ping_loop<S, Resp>(
        client: Arc<AtomicValue<RfsClient<S>>>,
        uuid: Uuid,
        failed_notify: Arc<Notify>,
    ) where
        Resp: http_body::Body + Send + 'static,
        <Resp as http_body::Body>::Error: Into<BoxError> + Send,
        S: Service<http::Request<BoxBody>, Response = http::Response<Resp>> + Clone,
        S::Error: Into<BoxError>,
        S::Future: Future<Output = std::result::Result<S::Response, S::Error>>,
    {
        let mut rpc_timeout = INITIAL_TIMEOUT;
        let mut failed = false;

        'outer: loop {
            for _ in 0..3 {
                if !failed {
                    time::sleep(PING_INTERVAL).await;
                }

                let ping_req = TonicRequest::new(PingRequest {
                    header: Some(Header {
                        uuid: uuid.as_bytes().to_vec(),
                        version: VERSION.to_string(),
                    }),
                });

                let mut client = (*client.load()).clone();

                if timeout(rpc_timeout, client.ping(ping_req))
                    .instrument(info_span!("send ping"))
                    .await
                    .is_ok()
                {
                    rpc_timeout = INITIAL_TIMEOUT;

                    failed = false;

                    debug!("sent ping message");

                    continue 'outer;
                } else {
                    rpc_timeout = rpc_timeout.mul_f64(MULTIPLIER);
                }
            }

            // ping failed 3 times, reset rpc timeout and notify to reconnect
            rpc_timeout = INITIAL_TIMEOUT;

            failed = true;

            failed_notify.notify();
        }
    }
}

#[async_trait]
impl PathFilesystem for Filesystem {
    type DirEntryStream = Empty<Result<DirectoryEntry>>;
    // type DirEntryPlusStream = impl Stream<Item = Result<DirectoryEntryPlus>> + Send;
    type DirEntryPlusStream = Pin<Box<dyn Stream<Item = Result<DirectoryEntryPlus>> + Send>>;

    async fn init(&self, _req: Request) -> Result<()> {
        let mut client = (*self.client.load()).clone();

        let mut compress_guard = self.compress.write().await;

        let req = TonicRequest::new(RegisterRequest {
            support_compress: *compress_guard,
        });

        let resp = client
            .register(req)
            .instrument(info_span!("register"))
            .map_err(|err| {
                error!("register failed {}", err);

                libc::EIO
            })
            .await?;

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

        tokio::spawn(Self::ping_loop(
            self.client.clone(),
            uuid,
            failed_notify.clone(),
        ));

        tokio::spawn(Self::reconnect_loop(
            self.client_service_builder.clone(),
            self.client.clone(),
            self.endpoint.clone(),
            failed_notify,
        ));

        Ok(())
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

        match timeout(Duration::from_secs(10), client.logout(req))
            .instrument(info_span!("logout"))
            .await
        {
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

    async fn lookup(&self, req: Request, parent: &OsStr, name: &OsStr) -> Result<ReplyEntry> {
        let name = match name.to_str() {
            None => return Err(libc::EINVAL.into()),
            Some(name) => name.to_string(),
        };

        let header = self.get_rpc_header().await;

        let client = self.client.clone();

        let rpc_req = TonicRequest::new(LookupRequest {
            head: Some(header.clone()),
            parent: parent.to_string_lossy().to_string(),
            name: name.to_string(),
        });

        let mut client = (*client.load()).clone();

        let resp = client
            .lookup(rpc_req)
            .map_err(|err| {
                error!("lookup rpc has error {}", err);

                libc::EIO
            })
            .await?;

        let resp = resp.into_inner();

        if let Some(result) = resp.result {
            match result {
                lookup_response::Result::Error(err) => Err((err.errno as c_int).into()),

                lookup_response::Result::Attr(attr) => Ok(ReplyEntry {
                    ttl: TTL,
                    attr: proto_attr_into_fuse_attr(attr, req.uid, req.gid)?,
                }),
            }
        } else {
            error!("lookup result is None");

            Err(libc::EIO.into())
        }
    }

    async fn getattr(
        &self,
        req: Request,
        path: Option<&OsStr>,
        _fh: Option<u64>,
        _flags: u32,
    ) -> Result<ReplyAttr> {
        let path = path
            .ok_or_else(Errno::new_not_exist)?
            .to_str()
            .ok_or_else(|| Errno::from(libc::EINVAL))?
            .to_owned();

        let header = self.get_rpc_header().await;

        let client = self.client.clone();

        let rpc_req = TonicRequest::new(GetAttrRequest {
            head: Some(header.clone()),
            path: path.clone(),
        });

        let mut client = (*client.load()).clone();

        let resp = client
            .get_attr(rpc_req)
            .map_err(|err| {
                error!("getattr rpc has error {}", err);

                libc::EIO
            })
            .await?;

        let resp = resp.into_inner();

        if let Some(result) = resp.result {
            match result {
                get_attr_response::Result::Error(err) => Err((err.errno as c_int).into()),
                get_attr_response::Result::Attr(attr) => Ok(ReplyAttr {
                    ttl: TTL,
                    attr: proto_attr_into_fuse_attr(attr, req.uid, req.gid)?,
                }),
            }
        } else {
            error!("getattr result is None");

            Err(libc::EIO.into())
        }
    }

    async fn setattr(
        &self,
        req: Request,
        path: Option<&OsStr>,
        _fh: Option<u64>,
        set_attr: SetAttr,
    ) -> Result<ReplyAttr> {
        let path = path
            .ok_or_else(Errno::new_not_exist)?
            .to_str()
            .ok_or_else(|| Errno::from(libc::EINVAL))?
            .to_owned();

        let header = self.get_rpc_header().await;

        let client = self.client.clone();

        let rpc_req = TonicRequest::new(SetAttrRequest {
            head: Some(header.clone()),
            path: path.clone(),
            attr: Some(Attr {
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

        let resp = client
            .set_attr(rpc_req)
            .instrument(info_span!("set_attr"))
            .map_err(|err| {
                error!("setattr rpc has error {}", err);

                libc::EIO
            })
            .await?;

        if let Some(result) = resp.into_inner().result {
            match result {
                set_attr_response::Result::Error(err) => {
                    error!("setattr failed errno {}", err.errno);

                    Err((err.errno as c_int).into())
                }

                set_attr_response::Result::Attr(attr) => Ok(ReplyAttr {
                    ttl: TTL,
                    attr: proto_attr_into_fuse_attr(attr, req.uid, req.gid)?,
                }),
            }
        } else {
            error!("setattr result is None");

            Err(libc::EIO.into())
        }
    }

    async fn mkdir(
        &self,
        req: Request,
        parent: &OsStr,
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

        let rpc_req = TonicRequest::new(MkdirRequest {
            head: Some(header.clone()),
            parent: parent.to_string_lossy().to_string(),
            name: name.to_string(),
            mode,
        });

        let mut client = (*client.load()).clone();

        let resp = client
            .mkdir(rpc_req)
            .instrument(info_span!("mkdir"))
            .map_err(|err| {
                error!("mkdir rpc has error {}", err);

                libc::EIO
            })
            .await?;

        if let Some(result) = resp.into_inner().result {
            match result {
                mkdir_response::Result::Error(err) => Err((err.errno as c_int).into()),
                mkdir_response::Result::Attr(attr) => Ok(ReplyEntry {
                    ttl: TTL,
                    attr: proto_attr_into_fuse_attr(attr, req.uid, req.gid)?,
                }),
            }
        } else {
            error!("mkdir result is None");

            Err(libc::EIO.into())
        }
    }

    async fn unlink(&self, _req: Request, parent: &OsStr, name: &OsStr) -> Result<()> {
        let name = match name.to_str() {
            None => return Err(libc::EINVAL.into()),
            Some(name) => name.to_string(),
        };

        let header = self.get_rpc_header().await;

        let client = self.client.clone();

        let rpc_req = TonicRequest::new(UnlinkRequest {
            head: Some(header.clone()),
            parent: parent.to_string_lossy().to_string(),
            name: name.to_string(),
        });

        let mut client = (*client.load()).clone();

        let resp = client
            .unlink(rpc_req)
            .instrument(info_span!("unlink"))
            .map_err(|err| {
                error!("unlink rpc has error {}", err);

                libc::EIO
            })
            .await?;

        if let Some(error) = resp.into_inner().error {
            Err((error.errno as c_int).into())
        } else {
            Ok(())
        }
    }

    async fn rmdir(&self, _req: Request, parent: &OsStr, name: &OsStr) -> Result<()> {
        let name = match name.to_str() {
            None => return Err(libc::EINVAL.into()),
            Some(name) => name.to_string(),
        };

        let header = self.get_rpc_header().await;

        let client = self.client.clone();

        let rpc_req = TonicRequest::new(RmDirRequest {
            head: Some(header.clone()),
            parent: parent.to_string_lossy().to_string(),
            name: name.to_string(),
        });

        let mut client = (*client.load()).clone();

        let resp = client
            .rm_dir(rpc_req)
            .instrument(info_span!("rmdir"))
            .map_err(|err| {
                error!("rmdir rpc has error {}", err);

                libc::EIO
            })
            .await?;

        if let Some(error) = resp.into_inner().error {
            Err((error.errno as c_int).into())
        } else {
            Ok(())
        }
    }

    async fn rename(
        &self,
        _req: Request,
        origin_parent: &OsStr,
        origin_name: &OsStr,
        parent: &OsStr,
        name: &OsStr,
    ) -> Result<()> {
        let origin_name = origin_name
            .to_str()
            .ok_or_else(|| Errno::from(libc::EINVAL))?
            .to_owned();
        let name = name
            .to_str()
            .ok_or_else(|| Errno::from(libc::EINVAL))?
            .to_owned();

        let header = self.get_rpc_header().await;

        let client = self.client.clone();

        let rpc_req = TonicRequest::new(RenameRequest {
            head: Some(header.clone()),
            old_parent: origin_parent.to_string_lossy().to_string(),
            old_name: origin_name.to_string(),
            new_parent: parent.to_string_lossy().to_string(),
            new_name: name.to_string(),
        });

        let mut client = (*client.load()).clone();

        let resp = client
            .rename(rpc_req)
            .instrument(info_span!("rename"))
            .map_err(|err| {
                error!("rename rpc has error {}", err);

                libc::EIO
            })
            .await?;

        if let Some(error) = resp.into_inner().error {
            Err((error.errno as c_int).into())
        } else {
            Ok(())
        }
    }

    async fn open(&self, _req: Request, path: &OsStr, flags: u32) -> Result<ReplyOpen> {
        let header = self.get_rpc_header().await;

        let client = self.client.clone();

        debug!("client open path {:?} flags {}", path, flags);

        let path = path
            .to_str()
            .ok_or_else(|| Errno::from(libc::EINVAL))?
            .to_owned();

        let rpc_req = TonicRequest::new(OpenFileRequest {
            head: Some(header.clone()),
            path: path.to_owned(),
            flags,
        });

        let mut client = (*client.load()).clone();

        let resp = client
            .open_file(rpc_req)
            .instrument(info_span!("open"))
            .map_err(|err| {
                error!("open rpc has error {}", err);

                libc::EIO
            })
            .await?;

        if let Some(result) = resp.into_inner().result {
            match result {
                open_file_response::Result::Error(err) => Err((err.errno as i32).into()),

                open_file_response::Result::FileHandleId(fh_id) => {
                    Ok(ReplyOpen { fh: fh_id, flags })
                }
            }
        } else {
            error!("open result is None");

            Err(libc::EIO.into())
        }
    }

    async fn read(
        &self,
        _req: Request,
        _path: Option<&OsStr>,
        fh: u64,
        offset: u64,
        size: u32,
    ) -> Result<ReplyData> {
        let header = self.get_rpc_header().await;

        let client = self.client.clone();

        let rpc_req = TonicRequest::new(ReadFileRequest {
            head: Some(header.clone()),
            file_handle_id: fh,
            offset: offset as _,
            size: size as u64,
        });

        let mut client = (*client.load()).clone();

        let resp = client
            .read_file(rpc_req)
            .instrument(info_span!("read"))
            .map_err(|err| {
                error!("read_file rpc has error {}", err);

                libc::EIO
            })
            .await?;

        let result = resp.into_inner();

        if let Some(err) = result.error {
            return Err((err.errno as i32).into());
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
            .unwrap()
            {
                Err(err) => Err(err.into()),
                Ok(data) => Ok(ReplyData { data: data.into() }),
            }
        } else {
            Ok(ReplyData {
                data: result.data.into(),
            })
        }
    }

    async fn write(
        &self,
        _req: Request,
        _path: Option<&OsStr>,
        fh: u64,
        offset: u64,
        data: &[u8],
        _flags: u32,
    ) -> Result<ReplyWrite> {
        let header = self.get_rpc_header().await;

        let client = self.client.clone();

        let enable_compress = *self.compress.read().await;

        let data = data.to_vec();

        let (data, compressed) = task::spawn_blocking(move || {
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
        .instrument(info_span!("compress_write_data"))
        .await
        .unwrap();

        let rpc_req = TonicRequest::new(WriteFileRequest {
            head: Some(header.clone()),
            file_handle_id: fh,
            offset: offset as _,
            data: data.clone(),
            compressed,
        });

        let mut client = (*client.load()).clone();

        let resp = client
            .write_file(rpc_req)
            .instrument(info_span!("write"))
            .map_err(|err| {
                error!("write_file rpc has error {}", err);

                libc::EIO
            })
            .await?;

        if let Some(result) = resp.into_inner().result {
            match result {
                write_file_response::Result::Error(err) => Err((err.errno as i32).into()),
                write_file_response::Result::Written(written) => Ok(ReplyWrite {
                    written: written as _,
                }),
            }
        } else {
            error!("write_file result is None");

            Err(libc::EIO.into())
        }
    }

    async fn statfs(&self, _req: Request, _path: &OsStr) -> Result<ReplyStatFs> {
        let header = self.get_rpc_header().await;

        let client = self.client.clone();

        let rpc_req = TonicRequest::new(StatFsRequest {
            head: Some(header.clone()),
        });

        let mut client = (*client.load()).clone();

        let resp = client
            .stat_fs(rpc_req)
            .instrument(info_span!("statsfs"))
            .map_err(|err| {
                error!("statfs rpc has error {}", err);

                libc::EIO
            })
            .await?;

        if let Some(result) = resp.into_inner().result {
            match result {
                stat_fs_response::Result::Error(err) => Err((err.errno as c_int).into()),
                stat_fs_response::Result::Statfs(statfs) => Ok(ReplyStatFs {
                    blocks: statfs.blocks,
                    bfree: statfs.block_free,
                    bavail: statfs.block_available,
                    files: statfs.files,
                    ffree: statfs.file_free,
                    bsize: statfs.block_size,
                    namelen: statfs.max_name_length,
                    frsize: statfs.fragment_size,
                }),
            }
        } else {
            error!("statfs result is None");

            Err(libc::EIO.into())
        }
    }

    async fn release(
        &self,
        _req: Request,
        _path: Option<&OsStr>,
        fh: u64,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
    ) -> Result<()> {
        let header = self.get_rpc_header().await;

        let client = self.client.clone();

        let rpc_req = TonicRequest::new(CloseFileRequest {
            head: Some(header.clone()),
            file_handle_id: fh,
        });

        let mut client = (*client.load()).clone();

        let resp = client
            .close_file(rpc_req)
            .instrument(info_span!("release"))
            .map_err(|err| {
                error!("close_file rpc has error {}", err);

                libc::EIO
            })
            .await?;

        if let Some(err) = resp.into_inner().error {
            Err((err.errno as c_int).into())
        } else {
            Ok(())
        }
    }

    async fn fsync(
        &self,
        _req: Request,
        _path: Option<&OsStr>,
        fh: u64,
        _datasync: bool,
    ) -> Result<()> {
        let header = self.get_rpc_header().await;

        let client = self.client.clone();

        let rpc_req = TonicRequest::new(SyncFileRequest {
            head: Some(header.clone()),
            file_handle_id: fh,
        });

        let mut client = (*client.load()).clone();

        let resp = client
            .sync_file(rpc_req)
            .instrument(info_span!("fsync"))
            .map_err(|err| {
                error!("sync_file rpc has error {}", err);

                libc::EIO
            })
            .await?;

        if let Some(err) = resp.into_inner().error {
            Err((err.errno as c_int).into())
        } else {
            Ok(())
        }
    }

    async fn flush(
        &self,
        _req: Request,
        _path: Option<&OsStr>,
        fh: u64,
        _lock_owner: u64,
    ) -> Result<()> {
        let header = self.get_rpc_header().await;

        let client = self.client.clone();

        let rpc_req = TonicRequest::new(FlushRequest {
            head: Some(header.clone()),
            file_handle_id: fh,
        });

        let mut client = (*client.load()).clone();

        let resp = client
            .flush(rpc_req)
            .instrument(info_span!("flush"))
            .map_err(|err| {
                error!("flush rpc has error {}", err);

                libc::EIO
            })
            .await?;

        if let Some(err) = resp.into_inner().error {
            Err((err.errno as c_int).into())
        } else {
            Ok(())
        }
    }

    async fn getlk(
        &self,
        _req: Request,
        _path: Option<&OsStr>,
        fh: u64,
        _lock_owner: u64,
        start: u64,
        end: u64,
        _type: u32,
        pid: u32,
    ) -> Result<ReplyLock> {
        let header = self.get_rpc_header().await;

        let client = self.client.clone();

        let rpc_req = TonicRequest::new(GetLockRequest {
            head: Some(header.clone()),
            file_handle_id: fh,
        });

        let mut client = (*client.load()).clone();

        let resp = client
            .get_lock(rpc_req)
            .instrument(info_span!("getlk"))
            .map_err(|err| {
                error!("get_lock rpc has error {}", err);

                libc::EIO
            })
            .await?;

        if let Some(result) = resp.into_inner().result {
            match result {
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
            }
        } else {
            error!("get_lock result is None");

            Err(libc::EIO.into())
        }
    }

    async fn setlk(
        &self,
        req: Request,
        _path: Option<&OsStr>,
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
            let rpc_req = TonicRequest::new(ReleaseLockRequest {
                head: Some(header.clone()),
                file_handle_id: fh,
                block: false,
            });

            let mut client = (*client.load()).clone();

            let resp = client
                .release_lock(rpc_req)
                .instrument(info_span!("setlk"))
                .map_err(|err| {
                    error!("release_lock rpc has error {}", err);

                    libc::EIO
                })
                .await?;

            return if let Some(error) = resp.into_inner().error {
                Err((error.errno as c_int).into())
            } else {
                Ok(())
            };
        }

        let lock_kind = {
            match r#type as i32 {
                libc::F_RDLCK => LockType::ReadLock,
                libc::F_WRLCK => LockType::WriteLock,

                _ => return Err(libc::EINVAL.into()),
            }
        };

        let rpc_req = TonicRequest::new(SetLockRequest {
            head: Some(header.clone()),
            file_handle_id: fh,
            unique: req.unique,
            lock_kind: lock_kind.into(),
            block,
        });

        let mut client = (*client.load()).clone();

        match client
            .set_lock(rpc_req)
            .instrument(info_span!("setlk"))
            .await
        {
            Err(err) => {
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
        }
    }

    #[inline]
    async fn access(&self, _req: Request, _path: &OsStr, _mask: u32) -> Result<()> {
        Ok(())
    }

    async fn create(
        &self,
        req: Request,
        parent: &OsStr,
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

        let rpc_req = TonicRequest::new(CreateFileRequest {
            head: Some(header.clone()),
            parent: parent
                .to_str()
                .ok_or_else(|| Errno::from(libc::EINVAL))?
                .to_owned(),
            name: name.to_string(),
            mode,
            flags,
        });

        let mut client = (*client.load()).clone();

        let resp = client
            .create_file(rpc_req)
            .instrument(info_span!("create"))
            .map_err(|err| {
                error!("create_file rpc has error {}", err);

                libc::EIO
            })
            .await?;

        let resp = resp.into_inner();

        if let Some(error) = resp.error {
            return Err((error.errno as c_int).into());
        }

        if resp.attr.is_none() {
            error!("create_file attr is None");

            return Err(libc::EIO.into());
        }

        let (fh_id, attr) = (resp.file_handle_id, resp.attr.unwrap());

        Ok(ReplyCreated {
            ttl: TTL,
            attr: proto_attr_into_fuse_attr(attr, req.uid, req.gid)?,
            generation: 0,
            fh: fh_id,
            flags,
        })
    }

    async fn interrupt(&self, _req: Request, unique: u64) -> Result<()> {
        debug!("interrupt unique {}", unique);

        let header = self.get_rpc_header().await;

        let client = self.client.clone();

        let rpc_req = TonicRequest::new(InterruptRequest {
            head: Some(header.clone()),
            unique,
        });

        let mut client = (*client.load()).clone();

        let resp = client
            .interrupt(rpc_req)
            .instrument(info_span!("interrupt"))
            .map_err(|err| {
                error!("interrupt rpc has error {}", err);

                libc::EIO
            })
            .await?;

        if let Some(err) = resp.into_inner().error {
            Err((err.errno as c_int).into())
        } else {
            Ok(())
        }
    }

    async fn fallocate(
        &self,
        _req: Request,
        _path: Option<&OsStr>,
        fh: u64,
        offset: u64,
        length: u64,
        mode: u32,
    ) -> Result<()> {
        let header = self.get_rpc_header().await;

        let client = self.client.clone();

        let rpc_req = TonicRequest::new(AllocateRequest {
            head: Some(header.clone()),
            file_handle_id: fh,
            offset,
            size: length,
            mode,
        });

        let mut client = (*client.load()).clone();

        let resp = client
            .allocate(rpc_req)
            .instrument(info_span!("fallocate"))
            .map_err(|err| {
                error!("allocate rpc has error {}", err);

                libc::EIO
            })
            .await?;

        if let Some(err) = resp.into_inner().error {
            Err((err.errno as c_int).into())
        } else {
            Ok(())
        }
    }

    async fn readdirplus(
        &self,
        req: Request,
        parent: &OsStr,
        _fh: u64,
        offset: u64,
        _lock_owner: u64,
    ) -> Result<ReplyDirectoryPlus<Self::DirEntryPlusStream>> {
        debug!("readdirplus path {:?}, offset {}", parent, offset);

        let header = self.get_rpc_header().await;

        let client = self.client.clone();

        let parent = parent
            .to_str()
            .ok_or_else(|| Errno::from(libc::EINVAL))?
            .to_owned();

        let rpc_req = TonicRequest::new(ReadDirRequest {
            head: Some(header.clone()),
            parent: parent.clone(),
            offset: offset as _,
            limit: READDIR_LIMIT,
        });

        let mut client = (*client.load()).clone();

        let resp = client
            .read_dir(rpc_req)
            .instrument(info_span!("readdirplus"))
            .map_err(|err| {
                error!("read_dir rpc has error {}", err);

                libc::EIO
            })
            .await?;

        let resp = resp.into_inner();

        if let Some(error) = resp.error {
            return Err((error.errno as c_int).into());
        }

        let dir_entries: Vec<read_dir_response::DirEntry> = resp.dir_entries;

        debug!("got readdirplus result");

        let entries = dir_entries
            .into_iter()
            .enumerate()
            .filter_map(move |(index, dir_entry)| {
                let attr = if let Some(attr) = dir_entry.attr {
                    attr
                } else {
                    warn!(
                        "dir entry {} in parent {:?} attr is None",
                        dir_entry.name, parent
                    );

                    return None;
                };

                let attr = if let Ok(attr) = proto_attr_into_fuse_attr(attr, req.uid, req.gid) {
                    attr
                } else {
                    warn!(
                        "parse dir entry {} in parent {:?} fuse attr failed",
                        dir_entry.name, parent
                    );

                    return None;
                };

                Some(DirectoryEntryPlus {
                    kind: attr.kind,
                    name: OsString::from(dir_entry.name),
                    offset: (index + 1) as _,
                    attr,
                    entry_ttl: TTL,
                    attr_ttl: TTL,
                })
            });

        return Ok(ReplyDirectoryPlus {
            entries: Box::pin(stream::iter(entries).map(Ok)),
        });
    }

    #[inline]
    async fn rename2(
        &self,
        req: Request,
        origin_parent: &OsStr,
        origin_name: &OsStr,
        parent: &OsStr,
        name: &OsStr,
        _flags: u32,
    ) -> Result<()> {
        self.rename(req, origin_parent, origin_name, parent, name)
            .instrument(info_span!("rename2"))
            .await
    }

    /*async fn lseek(&self, _req: Request, _inode: u64, _fh: u64, _offset: u64, _whence: u32) -> Result<ReplyLSeek> {
        unimplemented!()
    }*/

    async fn copy_file_range(
        &self,
        _req: Request,
        _from_path: Option<&OsStr>,
        fh_in: u64,
        off_in: u64,
        _to_path: Option<&OsStr>,
        fh_out: u64,
        off_out: u64,
        length: u64,
        flags: u64,
    ) -> Result<ReplyCopyFileRange> {
        let header = self.get_rpc_header().await;

        let client = self.client.clone();

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

        let resp = client
            .copy_file_range(rpc_req)
            .instrument(info_span!("copy_file_range"))
            .map_err(|err| {
                error!("copy_file_range rpc has error {}", err);

                libc::EIO
            })
            .await?;

        if let Some(result) = resp.into_inner().result {
            match result {
                copy_file_range_response::Result::Error(err) => Err((err.errno as i32).into()),
                copy_file_range_response::Result::Copied(copied) => {
                    Ok(ReplyCopyFileRange { copied })
                }
            }
        } else {
            error!("copy_file_range result is None");

            Err(libc::EIO.into())
        }
    }
}
