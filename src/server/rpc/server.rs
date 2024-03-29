use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use fuse3::Errno;
use fuse3::FileType;
use futures_util::stream::{FuturesUnordered, StreamExt};
use semver_parser::version;
use semver_parser::version::Version;
use snap::read::FrameDecoder;
use snap::write::FrameEncoder;
use tokio::fs;
use tokio::sync::RwLock;
use tokio::{task, time};
use tonic::transport::Server as TonicServer;
use tonic::transport::{Certificate, Identity, ServerTlsConfig};
use tonic::Response;
use tonic::{Code, Request, Status};
use tracing::{debug, error, info, instrument, warn};
use uuid::Uuid;

use super::super::filesystem::Filesystem;
use super::super::filesystem::LockKind;
use super::super::filesystem::SetAttr;
use super::user::User;
use crate::helper::{convert_proto_time_to_system_time, fuse_attr_into_proto_attr};
use crate::pb;
use crate::pb::read_dir_response::DirEntry;
use crate::pb::rfs_server::Rfs;
use crate::pb::rfs_server::RfsServer;
use crate::pb::*;

type Result<T> = std::result::Result<T, Status>;

const MIN_COMPRESS_SIZE: usize = 2048;
const ONLINE_CHECK_INTERVAL: Duration = Duration::from_secs(60);
const MAX_PING_INTERVAL: Duration = Duration::from_secs(5 * 60);

pub struct Server {
    users: Arc<RwLock<BTreeMap<Uuid, Arc<User>>>>,
    compress: bool,
    filesystem: Filesystem,
    version: Version,
}

impl Server {
    /// new a Server will chroot and listen rpc server and uds server
    pub async fn run(
        root_path: impl AsRef<Path>,
        cert_path: impl AsRef<Path>,
        key_path: impl AsRef<Path>,
        client_ca_path: impl AsRef<Path>,
        listen_path: SocketAddr,
        compress: bool,
    ) -> anyhow::Result<()> {
        if compress {
            info!("enable compress support");
        }

        let cert = fs::read(cert_path.as_ref()).await?;
        let key = fs::read(key_path.as_ref()).await?;
        let client_ca = fs::read(client_ca_path.as_ref()).await?;

        let server_identity = Identity::from_pem(cert, key);

        info!("server identity loaded");

        let client_ca = Certificate::from_pem(client_ca);

        info!("client ca loaded");

        let tls_config = ServerTlsConfig::new()
            .identity(server_identity)
            .client_ca_root(client_ca);

        let filesystem = Filesystem::new(root_path.as_ref()).await?;

        let rpc_server = Self {
            users: Arc::new(RwLock::new(BTreeMap::new())),
            compress,
            filesystem,
            version: version::parse(VERSION).unwrap(),
        };

        let users = rpc_server.users.clone();

        tokio::spawn(Self::check_online_users(users));

        Ok(TonicServer::builder()
            .tls_config(tls_config)?
            .add_service(RfsServer::new(rpc_server))
            .serve(listen_path)
            .await?)
    }

    async fn get_user(&self, header: Option<Header>) -> Result<Arc<User>> {
        let uuid: Uuid = if let Some(header) = header {
            let version = if let Ok(version) = version::parse(&header.version) {
                version
            } else {
                return Err(Status::invalid_argument(format!(
                    "version {} is not support",
                    header.version
                )));
            };

            if version.major != self.version.major {
                return Err(Status::invalid_argument(format!(
                    "version {} major is not support",
                    header.version
                )));
            }

            if version.minor != self.version.minor {
                return Err(Status::invalid_argument(format!(
                    "version {} minor is not support",
                    header.version
                )));
            }

            if let Ok(uuid) = Uuid::from_slice(&header.uuid) {
                uuid
            } else {
                debug!("receive invalid uuid");

                return Err(Status::invalid_argument("header uuid is invalid"));
            }
        } else {
            warn!("header not found");
            return Err(Status::invalid_argument("header miss"));
        };

        let users = self.users.read().await;

        let user = users.get(&uuid).ok_or_else(|| {
            warn!("user {} not found", uuid);
            Status::invalid_argument("user not found")
        })?;

        Ok(user.clone())
    }

    async fn check_online_users(user_map: Arc<RwLock<BTreeMap<Uuid, Arc<User>>>>) {
        loop {
            time::sleep(ONLINE_CHECK_INTERVAL).await;

            {
                let mut user_map = user_map.write().await;

                let futures_unordered = FuturesUnordered::new();

                user_map.iter().for_each(|(uuid, user)| {
                    futures_unordered.push(async move {
                        if !user.is_online(MAX_PING_INTERVAL).await {
                            Some(uuid)
                        } else {
                            None
                        }
                    })
                });

                let dead_user_ids = futures_unordered
                    .filter_map(|uuid| async move { uuid.map(Clone::clone) })
                    .collect::<Vec<_>>()
                    .await;

                for dead_user_id in dead_user_ids {
                    warn!(
                        "user {} may be dead, removing",
                        dead_user_id.hyphenated().to_string()
                    );

                    user_map.remove(&dead_user_id);
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl Rfs for Server {
    #[instrument(skip(self))]
    async fn read_dir(
        &self,
        request: Request<ReadDirRequest>,
    ) -> Result<Response<ReadDirResponse>> {
        let request = request.into_inner();

        self.get_user(request.head).await?;

        match self
            .filesystem
            .read_dir(&request.parent, request.offset as _, request.limit as _)
            .await
        {
            Err(errno) => Ok(Response::new(ReadDirResponse {
                dir_entries: vec![],
                error: Some(errno.into()),
            })),

            Ok(entries) => {
                let dir_entries: Vec<_> = entries
                    .into_iter()
                    .map(|(attr, name)| DirEntry {
                        name: name.clone(),
                        r#type: match attr.kind {
                            FileType::Directory => EntryType::Dir.into(),
                            FileType::RegularFile => EntryType::File.into(),
                            _ => unreachable!(),
                        },
                        attr: Some(fuse_attr_into_proto_attr(attr, &name)),
                    })
                    .collect();

                Ok(Response::new(ReadDirResponse {
                    dir_entries,
                    error: None,
                }))
            }
        }
    }

    #[instrument(skip(self))]
    async fn lookup(&self, request: Request<LookupRequest>) -> Result<Response<LookupResponse>> {
        let request = request.into_inner();

        self.get_user(request.head).await?;

        match self.filesystem.lookup(request.parent, &request.name).await {
            Err(errno) => Ok(Response::new(LookupResponse {
                result: Some(lookup_response::Result::Error(errno.into())),
            })),

            Ok(attr) => Ok(Response::new(LookupResponse {
                result: Some(lookup_response::Result::Attr(fuse_attr_into_proto_attr(
                    attr,
                    &request.name,
                ))),
            })),
        }
    }

    #[instrument(skip(self))]
    async fn mkdir(&self, request: Request<MkdirRequest>) -> Result<Response<MkdirResponse>> {
        let request = request.into_inner();

        self.get_user(request.head).await?;

        match self
            .filesystem
            .create_dir(request.parent, &request.name, request.mode)
            .await
        {
            Err(errno) => Ok(Response::new(MkdirResponse {
                result: Some(mkdir_response::Result::Error(errno.into())),
            })),

            Ok(attr) => Ok(Response::new(MkdirResponse {
                result: Some(mkdir_response::Result::Attr(fuse_attr_into_proto_attr(
                    attr,
                    &request.name,
                ))),
            })),
        }
    }

    #[instrument(skip(self))]
    async fn create_file(
        &self,
        request: Request<CreateFileRequest>,
    ) -> Result<Response<CreateFileResponse>> {
        let request = request.into_inner();

        let user = self.get_user(request.head).await?;

        let (file_handle, attr) = match self
            .filesystem
            .create_file(
                request.parent,
                &request.name,
                request.mode,
                request.flags as _,
            )
            .await
        {
            Err(errno) => {
                return Ok(Response::new(CreateFileResponse {
                    file_handle_id: 0,
                    attr: None,
                    error: Some(errno.into()),
                }));
            }

            Ok(file_handle) => file_handle,
        };

        let fh_id = file_handle.get_id();

        user.add_file_handle(file_handle).await;

        Ok(Response::new(CreateFileResponse {
            file_handle_id: fh_id,
            attr: Some(fuse_attr_into_proto_attr(attr, &request.name)),
            error: None,
        }))
    }

    #[instrument(skip(self))]
    async fn unlink(&self, request: Request<UnlinkRequest>) -> Result<Response<UnlinkResponse>> {
        let request = request.into_inner();

        self.get_user(request.head).await?;

        if let Err(errno) = self
            .filesystem
            .remove_entry(request.parent, &request.name, false)
            .await
        {
            Ok(Response::new(UnlinkResponse {
                error: Some(errno.into()),
            }))
        } else {
            Ok(Response::new(UnlinkResponse { error: None }))
        }
    }

    #[instrument(skip(self))]
    async fn rm_dir(&self, request: Request<RmDirRequest>) -> Result<Response<RmDirResponse>> {
        let request = request.into_inner();

        self.get_user(request.head).await?;

        if let Err(errno) = self
            .filesystem
            .remove_entry(request.parent, &request.name, true)
            .await
        {
            Ok(Response::new(RmDirResponse {
                error: Some(errno.into()),
            }))
        } else {
            Ok(Response::new(RmDirResponse { error: None }))
        }
    }

    #[instrument(skip(self))]
    async fn rename(&self, request: Request<RenameRequest>) -> Result<Response<RenameResponse>> {
        let request = request.into_inner();

        self.get_user(request.head).await?;

        if let Err(errno) = self
            .filesystem
            .rename(
                request.old_parent,
                &request.old_name,
                request.new_parent,
                &request.new_name,
            )
            .await
        {
            Ok(Response::new(RenameResponse {
                error: Some(errno.into()),
            }))
        } else {
            Ok(Response::new(RenameResponse { error: None }))
        }
    }

    #[instrument(skip(self))]
    async fn open_file(
        &self,
        request: Request<OpenFileRequest>,
    ) -> Result<Response<OpenFileResponse>> {
        let request = request.into_inner();

        let user = self.get_user(request.head).await?;

        let file_handle = match self
            .filesystem
            .open(&request.path, request.flags as _)
            .await
        {
            Err(errno) => {
                return Ok(Response::new(OpenFileResponse {
                    result: Some(open_file_response::Result::Error(errno.into())),
                }));
            }

            Ok(file_handle) => file_handle,
        };

        let fh_id = file_handle.get_id();

        user.add_file_handle(file_handle).await;

        Ok(Response::new(OpenFileResponse {
            result: Some(open_file_response::Result::FileHandleId(fh_id)),
        }))
    }

    #[instrument(skip(self))]
    async fn allocate(
        &self,
        request: Request<AllocateRequest>,
    ) -> Result<Response<AllocateResponse>> {
        let request = request.into_inner();

        let user = self.get_user(request.head).await?;

        let result = if let Err(errno) = user
            .fallocate(
                request.file_handle_id,
                request.offset,
                request.size,
                request.mode,
            )
            .await
        {
            Some(pb::Error::from(errno))
        } else {
            None
        };

        Ok(Response::new(AllocateResponse { error: result }))
    }

    #[instrument(skip(self))]
    async fn read_file(
        &self,
        request: Request<ReadFileRequest>,
    ) -> Result<Response<ReadFileResponse>> {
        let request = request.into_inner();

        let user = self.get_user(request.head).await?;

        let data = match user
            .read_file(request.file_handle_id, request.offset, request.size)
            .await
        {
            Err(errno) => {
                return Ok(Response::new(ReadFileResponse {
                    error: Some(errno.into()),
                    data: Bytes::new(),
                    compressed: false,
                }));
            }

            Ok(data) => Bytes::from(data),
        };

        let (data, compressed) =
            if self.compress && user.support_compress() && data.len() > MIN_COMPRESS_SIZE {
                let mut encoder =
                    FrameEncoder::new(BytesMut::with_capacity(MIN_COMPRESS_SIZE).writer());

                task::spawn_blocking(|| {
                    if let Err(err) = encoder.write_all(&data) {
                        warn!("compress read data failed {}", err);

                        return (data, false);
                    }

                    match encoder.into_inner() {
                        Err(err) => {
                            warn!("get compressed read data failed {}", err);

                            (data, false)
                        }

                        Ok(data) => (data.into_inner().freeze(), true),
                    }
                })
                .await
                .unwrap()
            } else {
                (data, false)
            };

        Ok(Response::new(ReadFileResponse {
            error: None,
            data,
            compressed,
        }))
    }

    #[instrument(skip(self, request))]
    async fn write_file(
        &self,
        request: Request<WriteFileRequest>,
    ) -> Result<Response<WriteFileResponse>> {
        let request = request.into_inner();

        let user = self.get_user(request.head).await?;

        if request.compressed && !self.compress {
            return Ok(Response::new(WriteFileResponse {
                result: Some(write_file_response::Result::Error(
                    Errno::from(libc::EINVAL).into(),
                )),
            }));
        }

        let result = if request.compressed {
            let mut decoder = FrameDecoder::new(request.data.as_ref());

            let mut data = Vec::with_capacity(MIN_COMPRESS_SIZE);

            if let Err(err) = decoder.read_to_end(&mut data) {
                error!("decompress write data failed {}", err);

                return Ok(Response::new(WriteFileResponse {
                    result: Some(write_file_response::Result::Error(
                        Errno::from(libc::EIO).into(),
                    )),
                }));
            }

            user.write_file(request.file_handle_id, request.offset, &data)
                .await
        } else {
            user.write_file(request.file_handle_id, request.offset, &request.data)
                .await
        };

        let written = match result {
            Err(errno) => {
                return Ok(Response::new(WriteFileResponse {
                    result: Some(write_file_response::Result::Error(errno.into())),
                }));
            }

            Ok(written) => written,
        };

        Ok(Response::new(WriteFileResponse {
            result: Some(write_file_response::Result::Written(written as u64)),
        }))
    }

    #[instrument(skip(self))]
    async fn close_file(
        &self,
        request: Request<CloseFileRequest>,
    ) -> Result<Response<CloseFileResponse>> {
        let request = request.into_inner();

        let user = self.get_user(request.head).await?;

        match user.close_file(request.file_handle_id).await {
            Err(errno) => Ok(Response::new(CloseFileResponse {
                error: Some(errno.into()),
            })),

            Ok(_) => Ok(Response::new(CloseFileResponse { error: None })),
        }
    }

    #[instrument(skip(self))]
    async fn sync_file(
        &self,
        request: Request<SyncFileRequest>,
    ) -> Result<Response<SyncFileResponse>> {
        let request = request.into_inner();

        let user = self.get_user(request.head).await?;

        match user.sync_file(request.file_handle_id).await {
            Err(errno) => Ok(Response::new(SyncFileResponse {
                error: Some(errno.into()),
            })),

            Ok(_) => Ok(Response::new(SyncFileResponse { error: None })),
        }
    }

    #[instrument(skip(self))]
    async fn flush(&self, request: Request<FlushRequest>) -> Result<Response<FlushResponse>> {
        let request = request.into_inner();

        let user = self.get_user(request.head).await?;

        match user.flush(request.file_handle_id).await {
            Err(errno) => Ok(Response::new(FlushResponse {
                error: Some(errno.into()),
            })),

            Ok(_) => Ok(Response::new(FlushResponse { error: None })),
        }
    }

    #[instrument(skip(self))]
    async fn set_lock(
        &self,
        request: Request<SetLockRequest>,
    ) -> Result<Response<SetLockResponse>> {
        let request = request.into_inner();

        let user = self.get_user(request.head).await?;

        let share = {
            let write = LockType::WriteLock as i32;
            let read = LockType::ReadLock as i32;

            if request.lock_kind == read {
                true
            } else if request.lock_kind == write {
                false
            } else {
                warn!("invalid lock kind {}", request.lock_kind);
                return Err(Status::new(Code::InvalidArgument, "invalid lock kind"));
            }
        };

        if request.block {
            let lock_job = match user
                .set_lock(request.file_handle_id, request.unique, share)
                .await
            {
                Err(err) => {
                    return Ok(Response::new(SetLockResponse {
                        error: Some(err.into()),
                    }));
                }

                Ok(lock_job) => lock_job,
            };

            return if let Ok(true) = lock_job.await {
                Ok(Response::new(SetLockResponse { error: None }))
            } else {
                Ok(Response::new(SetLockResponse {
                    error: Some(Errno::from(libc::EINTR).into()),
                }))
            };
        }

        if let Err(err) = user.try_set_lock(request.file_handle_id, share).await {
            Ok(Response::new(SetLockResponse {
                error: Some(err.into()),
            }))
        } else {
            Ok(Response::new(SetLockResponse { error: None }))
        }
    }

    #[instrument(skip(self))]
    async fn release_lock(
        &self,
        request: Request<ReleaseLockRequest>,
    ) -> Result<Response<ReleaseLockResponse>> {
        let request = request.into_inner();

        let user = self.get_user(request.head).await?;

        if let Err(err) = user.release_lock(request.file_handle_id).await {
            Ok(Response::new(ReleaseLockResponse {
                error: Some(err.into()),
            }))
        } else {
            Ok(Response::new(ReleaseLockResponse { error: None }))
        }
    }

    #[instrument(skip(self))]
    async fn interrupt(
        &self,
        request: Request<InterruptRequest>,
    ) -> Result<Response<InterruptResponse>> {
        let request = request.into_inner();

        let user = self.get_user(request.head).await?;

        if let Err(err) = user.interrupt_lock(request.unique).await {
            Ok(Response::new(InterruptResponse {
                error: Some(err.into()),
            }))
        } else {
            Ok(Response::new(InterruptResponse { error: None }))
        }
    }

    #[instrument(skip(self))]
    async fn get_lock(
        &self,
        request: Request<GetLockRequest>,
    ) -> Result<Response<GetLockResponse>> {
        let request = request.into_inner();

        let user = self.get_user(request.head).await?;

        match user.get_lock_kind(request.file_handle_id).await {
            Err(err) => Ok(Response::new(GetLockResponse {
                result: Some(get_lock_response::Result::Error(err.into())),
            })),

            Ok(kind) => {
                let kind = match kind {
                    LockKind::NoLock => LockType::NoLock,
                    LockKind::Share => LockType::ReadLock,
                    LockKind::Exclusive => LockType::WriteLock,
                };

                Ok(Response::new(GetLockResponse {
                    result: Some(get_lock_response::Result::LockType(kind.into())),
                }))
            }
        }
    }

    #[instrument(skip(self))]
    async fn stat_fs(&self, request: Request<StatFsRequest>) -> Result<Response<StatFsResponse>> {
        let request = request.into_inner();

        self.get_user(request.head).await?;

        match self.filesystem.statfs().await {
            Err(errno) => Ok(Response::new(StatFsResponse {
                result: Some(stat_fs_response::Result::Error(errno.into())),
            })),

            Ok(statfs) => Ok(Response::new(StatFsResponse {
                result: Some(stat_fs_response::Result::Statfs(StatFs {
                    blocks: statfs.blocks,
                    block_free: statfs.bfree,
                    block_available: statfs.bavail,
                    files: statfs.files,
                    file_free: statfs.ffree,
                    block_size: statfs.bsize,
                    max_name_length: statfs.namelen,
                    fragment_size: statfs.frsize,
                })),
            })),
        }
    }

    #[instrument(skip(self))]
    async fn get_attr(
        &self,
        request: Request<GetAttrRequest>,
    ) -> Result<Response<GetAttrResponse>> {
        let request = request.into_inner();

        self.get_user(request.head).await?;

        match self.filesystem.get_attr(&request.path).await {
            Err(errno) => Ok(Response::new(GetAttrResponse {
                result: Some(get_attr_response::Result::Error(errno.into())),
            })),

            Ok(attr) => Ok(Response::new(GetAttrResponse {
                result: Some(get_attr_response::Result::Attr(fuse_attr_into_proto_attr(
                    attr, "",
                ))),
            })),
        }
    }

    #[instrument(skip(self))]
    async fn set_attr(
        &self,
        request: Request<SetAttrRequest>,
    ) -> Result<Response<SetAttrResponse>> {
        let request = request.into_inner();

        self.get_user(request.head).await?;

        let attr = if let Some(attr) = request.attr {
            attr
        } else {
            debug!("attr is miss");
            return Err(Status::new(Code::InvalidArgument, "attr is miss"));
        };

        let new_attr = SetAttr {
            uid: None,
            gid: None,
            flags: None,
            mode: if attr.mode > 0 {
                Some(attr.mode as u32)
            } else {
                None
            },
            size: if attr.size >= 0 {
                Some(attr.size as u64)
            } else {
                None
            },
            atime: attr
                .access_time
                .map(|atime| convert_proto_time_to_system_time(Some(atime))),
            mtime: attr
                .modify_time
                .map(|mtime| convert_proto_time_to_system_time(Some(mtime))),
            ctime: attr
                .change_time
                .map(|ctime| convert_proto_time_to_system_time(Some(ctime))),
        };

        match self.filesystem.set_attr(&request.path, new_attr).await {
            Err(errno) => Ok(Response::new(SetAttrResponse {
                result: Some(set_attr_response::Result::Error(errno.into())),
            })),

            Ok(attr) => Ok(Response::new(SetAttrResponse {
                result: Some(set_attr_response::Result::Attr(
                    fuse_attr_into_proto_attr(attr, ""), // here the name should not important
                )),
            })),
        }
    }

    #[instrument(skip(self))]
    async fn copy_file_range(
        &self,
        request: Request<CopyFileRangeRequest>,
    ) -> Result<Response<CopyFileRangeResponse>> {
        let request = request.into_inner();

        let user = self.get_user(request.head).await?;

        match user
            .copy_file_range(
                request.file_handle_id_in,
                request.offset_in,
                request.file_handle_id_out,
                request.offset_out,
                request.size,
                request.flags,
            )
            .await
        {
            Err(errno) => Ok(Response::new(CopyFileRangeResponse {
                result: Some(copy_file_range_response::Result::Error(errno.into())),
            })),

            Ok(copied) => Ok(Response::new(CopyFileRangeResponse {
                result: Some(copy_file_range_response::Result::Copied(copied as _)),
            })),
        }
    }

    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<PingResponse>> {
        let request = request.into_inner();

        let user = self.get_user(request.header).await?;

        user.update_last_alive_time().await;
        let user_id = user.get_id().await.hyphenated().to_string();

        debug!("receive ping message from {}", user_id);

        Ok(Response::new(PingResponse {}))
    }

    async fn register(
        &self,
        request: Request<RegisterRequest>,
    ) -> Result<Response<RegisterResponse>> {
        let uuid = Uuid::new_v4();

        let enable_compress = request.into_inner().support_compress;

        self.users
            .write()
            .await
            .insert(uuid, Arc::new(User::new(uuid, enable_compress)));

        info!("user {} register", uuid);

        Ok(Response::new(RegisterResponse {
            uuid: Bytes::copy_from_slice(uuid.as_bytes()),
            allow_compress: enable_compress && self.compress,
        }))
    }

    async fn logout(&self, request: Request<LogoutRequest>) -> Result<Response<LogoutResponse>> {
        let request = request.into_inner();

        let uuid: Uuid = match request.uuid.parse() {
            Err(_err) => {
                warn!("invalid uuid {}", request.uuid);

                return Err(Status::new(Code::InvalidArgument, "invalid uuid"));
            }

            Ok(uuid) => uuid,
        };

        let mut users = self.users.write().await;

        users.remove(&uuid);

        info!("user {} logout", uuid);

        Ok(Response::new(LogoutResponse {}))
    }
}
