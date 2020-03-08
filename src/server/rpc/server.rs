use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_std::fs;
use async_std::os::unix::net::UnixListener;
use async_std::path::Path;
use async_std::sync::RwLock;
use async_std::task;
use chrono::prelude::*;
use fuse::FileType;
use futures_util::stream::{FuturesUnordered, TryStreamExt};
use futures_util::{try_join, StreamExt};
use log::{debug, info, warn};
use tonic::transport::Server as TonicServer;
use tonic::transport::{Certificate, Identity, ServerTlsConfig};
use tonic::Response;
use tonic::{Code, Request, Status};
use uuid::Uuid;

use crate::errno::Errno;
use crate::helper::{convert_proto_time_to_system_time, fuse_attr_into_proto_attr};
use crate::pb;
use crate::pb::read_dir_response::DirEntry;
use crate::pb::rfs_server::Rfs;
use crate::pb::rfs_server::RfsServer;
use crate::pb::*;
use crate::TokioUnixStream;

use super::super::filesystem::Filesystem;
use super::super::filesystem::LockKind;
use super::super::filesystem::SetAttr;
use super::user::User;

type Result<T> = std::result::Result<T, Status>;

pub struct Server {
    users: Arc<RwLock<BTreeMap<Uuid, Arc<User>>>>,
    filesystem: Arc<Filesystem>,
}

impl Server {
    /// new a Server will chroot and listen rpc server and uds server
    pub async fn run(
        root_path: impl AsRef<Path>,
        cert_path: impl AsRef<Path>,
        key_path: impl AsRef<Path>,
        client_ca_path: impl AsRef<Path>,
        uds_path: impl AsRef<Path>,
        listen_path: SocketAddr,
    ) -> anyhow::Result<()> {
        let cert = fs::read(cert_path).await?;
        let key = fs::read(key_path).await?;
        let client_ca = fs::read(client_ca_path).await?;

        let server_identity = Identity::from_pem(cert, key);

        info!("server identity loaded");

        let client_ca = Certificate::from_pem(client_ca);

        info!("client ca loaded");

        let tls_config = ServerTlsConfig::new()
            .identity(server_identity)
            .client_ca_root(client_ca);

        let unix_listener = UnixListener::bind(uds_path.as_ref()).await?;

        let fs = Arc::new(Filesystem::new(root_path.as_ref()).await?);

        let nds_server = Self {
            users: Arc::new(RwLock::new(BTreeMap::new())),
            filesystem: fs.clone(),
        };

        let uds_serve = TonicServer::builder()
            .add_service(RfsServer::new(nds_server))
            .serve_with_incoming(unix_listener.incoming().map_ok(TokioUnixStream));

        let rpc_server = Self {
            users: Arc::new(RwLock::new(BTreeMap::new())),
            filesystem: fs,
        };

        let users = rpc_server.users.clone();

        task::spawn(async { Self::check_online_users(users) });

        let rpc_serve = TonicServer::builder()
            .tls_config(tls_config)
            .add_service(RfsServer::new(rpc_server))
            .serve(listen_path);

        try_join!(rpc_serve, uds_serve)?;

        Ok(())
    }

    async fn get_user(&self, header: Option<Header>) -> Result<Arc<User>> {
        let uuid: Uuid = if let Some(header) = header {
            if header.version != VERSION {
                return Err(Status::invalid_argument(format!(
                    "version {} is not support",
                    header.version
                )));
            }

            if let Ok(uuid) = header.uuid.parse() {
                uuid
            } else {
                warn!("header uuid {} is invalid", header.uuid);
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
            task::sleep(Duration::from_secs(60)).await;

            {
                let mut user_map = user_map.write().await;

                let futures_unordered = FuturesUnordered::new();

                user_map.iter().for_each(|(uuid, user)| {
                    futures_unordered.push(async move {
                        if !user.is_online(Duration::from_secs(60)).await {
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
                    info!(
                        "user {} may be dead, removing",
                        dead_user_id.to_hyphenated_ref().to_string()
                    );

                    user_map.remove(&dead_user_id);
                }
            }
        }
    }
}

#[tonic::async_trait]
impl Rfs for Server {
    async fn read_dir(
        &self,
        request: Request<ReadDirRequest>,
    ) -> Result<Response<ReadDirResponse>> {
        let request = request.into_inner();

        self.get_user(request.head).await?;

        match self.filesystem.read_dir(request.inode, 0).await {
            Err(errno) => Ok(Response::new(ReadDirResponse {
                dir_entries: vec![],
                error: Some(errno.into()),
            })),

            Ok(entries) => {
                let dir_entries: Vec<_> = entries
                    .into_iter()
                    .map(|(inode, index, kind, name)| DirEntry {
                        index,
                        inode,
                        r#type: match kind {
                            FileType::Directory => EntryType::Dir.into(),
                            FileType::RegularFile => EntryType::File.into(),
                            _ => unreachable!(),
                        },
                        name: name.into_string().expect("entry name should be valid"),
                    })
                    .collect();

                Ok(Response::new(ReadDirResponse {
                    dir_entries,
                    error: None,
                }))
            }
        }
    }

    async fn lookup(&self, request: Request<LookupRequest>) -> Result<Response<LookupResponse>> {
        let request = request.into_inner();

        self.get_user(request.head).await?;

        match self
            .filesystem
            .lookup(request.inode, OsStr::new(&request.name))
            .await
        {
            Err(errno) => Ok(Response::new(LookupResponse {
                result: Some(pb::lookup_response::Result::Error(errno.into())),
            })),

            Ok(attr) => Ok(Response::new(LookupResponse {
                result: Some(pb::lookup_response::Result::Attr(
                    fuse_attr_into_proto_attr(attr, &request.name),
                )),
            })),
        }
    }

    async fn mkdir(&self, request: Request<MkdirRequest>) -> Result<Response<MkdirResponse>> {
        let request = request.into_inner();

        self.get_user(request.head).await?;

        match self
            .filesystem
            .create_dir(request.inode, OsStr::new(&request.name), request.mode)
            .await
        {
            Err(errno) => Ok(Response::new(MkdirResponse {
                result: Some(pb::mkdir_response::Result::Error(errno.into())),
            })),

            Ok(attr) => Ok(Response::new(MkdirResponse {
                result: Some(pb::mkdir_response::Result::Attr(fuse_attr_into_proto_attr(
                    attr,
                    &request.name,
                ))),
            })),
        }
    }

    async fn create_file(
        &self,
        request: Request<CreateFileRequest>,
    ) -> Result<Response<CreateFileResponse>> {
        let request = request.into_inner();

        let user = self.get_user(request.head).await?;

        let (file_handle, attr) = match self
            .filesystem
            .create_file(
                request.inode,
                OsStr::new(&request.name),
                request.mode,
                request.flags,
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

    async fn unlink(&self, request: Request<UnlinkRequest>) -> Result<Response<UnlinkResponse>> {
        let request = request.into_inner();

        self.get_user(request.head).await?;

        if let Err(errno) = self
            .filesystem
            .remove_entry(request.inode, OsStr::new(&request.name), false)
            .await
        {
            Ok(Response::new(UnlinkResponse {
                error: Some(errno.into()),
            }))
        } else {
            Ok(Response::new(UnlinkResponse { error: None }))
        }
    }

    async fn rm_dir(&self, request: Request<RmDirRequest>) -> Result<Response<RmDirResponse>> {
        let request = request.into_inner();

        self.get_user(request.head).await?;

        if let Err(errno) = self
            .filesystem
            .remove_entry(request.inode, OsStr::new(&request.name), true)
            .await
        {
            Ok(Response::new(RmDirResponse {
                error: Some(errno.into()),
            }))
        } else {
            Ok(Response::new(RmDirResponse { error: None }))
        }
    }

    async fn rename(&self, request: Request<RenameRequest>) -> Result<Response<RenameResponse>> {
        let request = request.into_inner();

        self.get_user(request.head).await?;

        if let Err(errno) = self
            .filesystem
            .rename(
                request.old_parent,
                OsStr::new(&request.old_name),
                request.new_parent,
                OsStr::new(&request.new_name),
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

    async fn open_file(
        &self,
        request: Request<OpenFileRequest>,
    ) -> Result<Response<OpenFileResponse>> {
        let request = request.into_inner();

        let user = self.get_user(request.head).await?;

        let file_handle = match self.filesystem.open(request.inode, request.flags).await {
            Err(errno) => {
                return Ok(Response::new(OpenFileResponse {
                    result: Some(pb::open_file_response::Result::Error(errno.into())),
                }));
            }

            Ok(file_handle) => file_handle,
        };

        let fh_id = file_handle.get_id();

        user.add_file_handle(file_handle).await;

        Ok(Response::new(OpenFileResponse {
            result: Some(pb::open_file_response::Result::FileHandleId(fh_id)),
        }))
    }

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
                    result: Some(pb::read_file_response::Result::Error(errno.into())),
                }));
            }

            Ok(data) => data,
        };

        Ok(Response::new(ReadFileResponse {
            result: Some(pb::read_file_response::Result::Data(data)),
        }))
    }

    async fn write_file(
        &self,
        request: Request<WriteFileRequest>,
    ) -> Result<Response<WriteFileResponse>> {
        let request = request.into_inner();

        let user = self.get_user(request.head).await?;

        let written = match user
            .write_file(request.file_handle_id, request.offset, &request.data)
            .await
        {
            Err(errno) => {
                return Ok(Response::new(WriteFileResponse {
                    result: Some(pb::write_file_response::Result::Error(errno.into())),
                }));
            }

            Ok(written) => written,
        };

        Ok(Response::new(WriteFileResponse {
            result: Some(pb::write_file_response::Result::Written(written as u64)),
        }))
    }

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

    async fn get_attr(
        &self,
        request: Request<GetAttrRequest>,
    ) -> Result<Response<GetAttrResponse>> {
        let request = request.into_inner();

        self.get_user(request.head).await?;

        match self.filesystem.get_attr(request.inode).await {
            Err(errno) => Ok(Response::new(GetAttrResponse {
                result: Some(pb::get_attr_response::Result::Error(errno.into())),
            })),

            Ok(attr) => Ok(Response::new(GetAttrResponse {
                result: Some(pb::get_attr_response::Result::Attr(
                    fuse_attr_into_proto_attr(attr, ""),
                )),
            })),
        }
    }

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

        let set_attr = SetAttr {
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
            atime: if let Some(atime) = attr.access_time {
                Some(convert_proto_time_to_system_time(Some(atime)))
            } else {
                None
            },
            mtime: if let Some(mtime) = attr.modify_time {
                Some(convert_proto_time_to_system_time(Some(mtime)))
            } else {
                None
            },
            ctime: if let Some(ctime) = attr.change_time {
                Some(convert_proto_time_to_system_time(Some(ctime)))
            } else {
                None
            },
        };

        match self.filesystem.set_attr(request.inode, set_attr).await {
            Err(errno) => Ok(Response::new(SetAttrResponse {
                result: Some(pb::set_attr_response::Result::Error(errno.into())),
            })),

            Ok(attr) => Ok(Response::new(SetAttrResponse {
                result: Some(pb::set_attr_response::Result::Attr(
                    fuse_attr_into_proto_attr(attr, ""), // here the name should not important
                )),
            })),
        }
    }

    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<PingResponse>> {
        let request = request.into_inner();

        let user = self.get_user(request.header).await?;

        user.update_last_alive_time(Local::now()).await;

        debug!(
            "receive ping message from {}",
            user.get_id().await.to_hyphenated().to_string()
        );

        Ok(Response::new(PingResponse {}))
    }

    async fn register(
        &self,
        _request: Request<RegisterRequest>,
    ) -> Result<Response<RegisterResponse>> {
        let uuid = Uuid::new_v4();

        self.users
            .write()
            .await
            .insert(uuid, Arc::new(User::new(uuid)));

        info!("user {} register", uuid);

        Ok(Response::new(RegisterResponse {
            uuid: uuid.to_hyphenated().to_string(),
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
