use std::collections::BTreeMap;
use std::ffi::OsStr;

use async_std::sync::{Arc, RwLock};
use chrono::prelude::*;
use fuse::FileType;
use log::{debug, error, info, warn};
use tonic::{Code, Request, Status};
use tonic::Response;
use uuid::Uuid;

use pb::*;
use pb::EntryType;
use pb::read_dir_response::DirEntry;
use pb::rfs_server::Rfs;

use crate::errno::Errno;
use crate::helper::{convert_fuse_attr_to_proto_attr, convert_proto_time_to_system_time};

use super::super::filesystem::Filesystem;
use super::super::filesystem::SetAttr;
use super::user::User;

pub mod pb {
    tonic::include_proto!("proto");
}

type Result<T> = std::result::Result<T, Status>;

pub struct Server {
    users: RwLock<BTreeMap<Uuid, Arc<User>>>,
    filesystem: Filesystem,
}

impl Server {
    async fn get_user(&self, header: Option<Header>) -> Result<Arc<User>> {
        let uuid: Uuid = if let Some(header) = header {
            if let Ok(uuid) = header.uuid.parse() {
                uuid
            } else {
                warn!("header uuid {} is invalid", header.uuid);
                return Err(Status::new(Code::InvalidArgument, "header uuid is invalid"));
            }
        } else {
            warn!("header not found");
            return Err(Status::new(Code::InvalidArgument, "header miss"));
        };

        let user = self.users.read().await;

        let user = user.get(&uuid).ok_or_else(|| {
            warn!("user {} not found", uuid);
            Status::new(Code::InvalidArgument, "user not found")
        })?;

        Ok(Arc::clone(user))
    }
}

#[tonic::async_trait]
impl Rfs for Server {
    async fn read_dir(&self, request: Request<ReadDirRequest>) -> Result<Response<ReadDirResponse>> {
        let request = request.into_inner();

        self.get_user(request.head).await?;

        match self.filesystem.read_dir(request.inode, 0).await {
            Err(errno) => Ok(Response::new(ReadDirResponse {
                dir_entries: vec![],
                error: Some(errno.into()),
            })),

            Ok(entries) => {
                let dir_entries: Vec<_> = entries.into_iter()
                    .map(|(inode, index, kind, name)| {
                        DirEntry {
                            index,
                            inode,
                            r#type: match kind {
                                FileType::Directory => EntryType::Dir.into(),
                                FileType::RegularFile => EntryType::File.into(),
                                _ => unreachable!()
                            },
                            name: name.into_string().expect("entry name should be valid"),
                        }
                    }).collect();

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

        match self.filesystem.lookup(request.inode, OsStr::new(&request.name)).await {
            Err(errno) => Ok(Response::new(LookupResponse {
                result: Some(pb::lookup_response::Result::Error(errno.into())),
            })),

            Ok(attr) => Ok(Response::new(LookupResponse {
                result: Some(pb::lookup_response::Result::Attr(
                    convert_fuse_attr_to_proto_attr(attr, &request.name)
                ))
            }))
        }
    }

    async fn mkdir(&self, request: Request<MkdirRequest>) -> Result<Response<MkdirResponse>> {
        let request = request.into_inner();

        self.get_user(request.head).await?;

        match self.filesystem.create_dir(request.inode, OsStr::new(&request.name), request.mode).await {
            Err(errno) => Ok(Response::new(MkdirResponse {
                result: Some(pb::mkdir_response::Result::Error(errno.into())),
            })),

            Ok(attr) => Ok(Response::new(MkdirResponse {
                result: Some(pb::mkdir_response::Result::Attr(
                    convert_fuse_attr_to_proto_attr(attr, &request.name)
                ))
            }))
        }
    }

    async fn create_file(&self, request: Request<CreateFileRequest>) -> Result<Response<CreateFileResponse>> {
        let request = request.into_inner();

        let user = self.get_user(request.head).await?;

        let file_handle = match self.filesystem
            .create_file(
                request.inode,
                OsStr::new(&request.name),
                request.mode,
                request.flags,
            ).await {
            Err(errno) => return Ok(Response::new(CreateFileResponse {
                result: Some(pb::create_file_response::Result::Error(errno.into())),
            })),

            Ok(file_handle) => file_handle
        };

        let fh_id = file_handle.get_id();

        user.add_file_handle(file_handle).await;

        Ok(Response::new(CreateFileResponse {
            result: Some(pb::create_file_response::Result::FileHandleId(fh_id))
        }))
    }

    async fn unlink(&self, request: Request<UnlinkRequest>) -> Result<Response<UnlinkResponse>> {
        let request = request.into_inner();

        self.get_user(request.head).await?;

        if let Err(errno) = self.filesystem.remove_entry(request.inode, OsStr::new(&request.name), false).await {
            Ok(Response::new(UnlinkResponse {
                error: Some(errno.into())
            }))
        } else {
            Ok(Response::new(UnlinkResponse {
                error: None
            }))
        }
    }

    async fn rm_dir(&self, request: Request<RmDirRequest>) -> Result<Response<RmDirResponse>> {
        let request = request.into_inner();

        self.get_user(request.head).await?;

        if let Err(errno) = self.filesystem.remove_entry(request.inode, OsStr::new(&request.name), true).await {
            Ok(Response::new(RmDirResponse {
                error: Some(errno.into())
            }))
        } else {
            Ok(Response::new(RmDirResponse {
                error: None
            }))
        }
    }

    async fn rename(&self, request: Request<RenameRequest>) -> Result<Response<RenameResponse>> {
        let request = request.into_inner();

        self.get_user(request.head).await?;

        if let Err(errno) = self.filesystem
            .rename(
                request.old_parent,
                OsStr::new(&request.old_name),
                request.new_parent,
                OsStr::new(&request.new_name),
            ).await {
            Ok(Response::new(RenameResponse {
                error: Some(errno.into())
            }))
        } else {
            Ok(Response::new(RenameResponse {
                error: None
            }))
        }
    }

    async fn open_file(&self, request: Request<OpenFileRequest>) -> Result<Response<OpenFileResponse>> {
        let request = request.into_inner();

        let user = self.get_user(request.head).await?;

        let file_handle = match self.filesystem.open(request.inode, request.flags).await {
            Err(errno) => return Ok(Response::new(OpenFileResponse {
                result: Some(pb::open_file_response::Result::Error(errno.into())),
            })),

            Ok(file_handle) => file_handle
        };

        let fh_id = file_handle.get_id();

        user.add_file_handle(file_handle).await;

        Ok(Response::new(OpenFileResponse {
            result: Some(pb::open_file_response::Result::FileHandleId(fh_id)),
        }))
    }

    async fn read_file(&self, request: Request<ReadFileRequest>) -> Result<Response<ReadFileResponse>> {
        let request = request.into_inner();

        let user = self.get_user(request.head).await?;

        let data = match user.read_file(request.file_handle_id, request.offset as i64, request.size).await {
            Err(errno) => return Ok(Response::new(ReadFileResponse {
                result: Some(pb::read_file_response::Result::Error(errno.into())),
            })),

            Ok(data) => data
        };

        Ok(Response::new(ReadFileResponse {
            result: Some(pb::read_file_response::Result::Data(data)),
        }))
    }

    async fn write_file(&self, request: Request<WriteFileRequest>) -> Result<Response<WriteFileResponse>> {
        let request = request.into_inner();

        let user = self.get_user(request.head).await?;

        let written = match user.write_file(request.file_handle_id, request.offset, &request.data).await {
            Err(errno) => return Ok(Response::new(WriteFileResponse {
                result: Some(pb::write_file_response::Result::Error(errno.into())),
            })),

            Ok(written) => written
        };

        Ok(Response::new(WriteFileResponse {
            result: Some(pb::write_file_response::Result::Written(written as u64)),
        }))
    }

    async fn close_file(&self, request: Request<CloseFileRequest>) -> Result<Response<CloseFileResponse>> {
        let request = request.into_inner();

        let user = self.get_user(request.head).await?;

        match user.close_file(request.file_handle_id).await {
            Err(errno) => Ok(Response::new(CloseFileResponse {
                error: Some(errno.into()),
            })),

            Ok(_) => Ok(Response::new(CloseFileResponse {
                error: None,
            })),
        }
    }

    async fn sync_file(&self, request: Request<SyncFileRequest>) -> Result<Response<SyncFileResponse>> {
        let request = request.into_inner();

        let user = self.get_user(request.head).await?;

        match user.sync_file(request.file_handle_id).await {
            Err(errno) => Ok(Response::new(SyncFileResponse {
                error: Some(errno.into()),
            })),

            Ok(_) => Ok(Response::new(SyncFileResponse {
                error: None,
            })),
        }
    }

    async fn set_lock(&self, request: Request<SetLockRequest>) -> Result<Response<SetLockResponse>> {
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
            let lock_job = match user.set_lock(request.file_handle_id, request.unique, share).await {
                Err(err) => return Ok(Response::new(SetLockResponse {
                    error: Some(err.into()),
                })),

                Ok(lock_job) => lock_job
            };

            return if lock_job.await {
                Ok(Response::new(SetLockResponse {
                    error: None,
                }))
            } else {
                Ok(Response::new(SetLockResponse {
                    error: Some(Errno::from(libc::EINTR).into()),
                }))
            };
        }

        match user.try_set_lock(request.file_handle_id, share).await {
            Err(err) => return Ok(Response::new(SetLockResponse {
                error: Some(err.into()),
            })),

            Ok(_) => Ok(Response::new(SetLockResponse {
                error: None,
            }))
        }
    }

    async fn release_lock(&self, request: Request<ReleaseLockRequest>) -> Result<Response<ReleaseLockResponse>> {
        let request = request.into_inner();

        let user = self.get_user(request.head).await?;

        match user.release_lock(request.file_handle_id).await {
            Err(err) => Ok(Response::new(ReleaseLockResponse {
                error: Some(err.into())
            })),

            Ok(_) => Ok(Response::new(ReleaseLockResponse {
                error: None
            }))
        }
    }

    async fn interrupt(&self, request: Request<InterruptRequest>) -> Result<Response<InterruptResponse>> {
        let request = request.into_inner();

        let user = self.get_user(request.head).await?;

        match user.interrupt_lock(request.file_handle_id, request.unique).await {
            Err(err) => Ok(Response::new(InterruptResponse {
                error: Some(err.into())
            })),

            Ok(_) => Ok(Response::new(InterruptResponse {
                error: None
            }))
        }
    }

    async fn get_attr(&self, request: Request<GetAttrRequest>) -> Result<Response<GetAttrResponse>> {
        let request = request.into_inner();

        self.get_user(request.head).await?;

        match self.filesystem.get_attr(request.inode).await {
            Err(errno) => Ok(Response::new(GetAttrResponse {
                result: Some(pb::get_attr_response::Result::Error(errno.into())),
            })),

            Ok(attr) => Ok(Response::new(GetAttrResponse {
                result: Some(pb::get_attr_response::Result::Attr(convert_fuse_attr_to_proto_attr(attr, ""))),
            })),
        }
    }

    async fn set_attr(&self, request: Request<SetAttrRequest>) -> Result<Response<SetAttrResponse>> {
        let request = request.into_inner();

        let user = self.get_user(request.head).await?;

        let attr = if let Some(attr) = request.attr {
            attr
        } else {
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
            size: if attr.size > 0 {
                Some(attr.size as u64)
            } else {
                None
            },
            atime: if let Some(atime) = attr.access_time {
                Some(convert_proto_time_to_system_time(atime))
            } else {
                None
            },
            mtime: if let Some(mtime) = attr.modify_time {
                Some(convert_proto_time_to_system_time(mtime))
            } else {
                None
            },
            ctime: if let Some(ctime) = attr.change_time {
                Some(convert_proto_time_to_system_time(ctime))
            } else {
                None
            },
        };

        if request.handle.is_none() {
            return Err(Status::new(Code::InvalidArgument, "miss handle"));
        }

        let handle = request.handle.as_ref().unwrap();

        let result = match handle {
            pb::set_attr_request::Handle::Inode(inode) => {
                let kind = match self.filesystem.get_attr(*inode).await {
                    Err(errno) => return Ok(Response::new(SetAttrResponse {
                        result: Some(pb::set_attr_response::Result::Error(errno.into())),
                    })),

                    Ok(attr) => attr.kind
                };

                if let FileType::RegularFile = kind {
                    return Ok(Response::new(SetAttrResponse {
                        result: Some(pb::set_attr_response::Result::Error(Errno::from(libc::ENOTDIR).into())),
                    }));
                }

                self.filesystem.set_dir_attr(*inode, set_attr).await
            }

            pb::set_attr_request::Handle::FileHandler(fh_id) => {
                user.set_file_attr(*fh_id, set_attr).await
            }
        };

        match result {
            Err(errno) => Ok(Response::new(SetAttrResponse {
                result: Some(pb::set_attr_response::Result::Error(errno.into())),
            })),

            Ok(attr) => Ok(Response::new(SetAttrResponse {
                result: Some(pb::set_attr_response::Result::Attr(convert_fuse_attr_to_proto_attr(attr, ""))),
            })),
        }
    }

    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<PingResponse>> {
        let request = request.into_inner();

        let user = self.get_user(request.header).await?;

        user.update_last_alive_time(Local::now()).await;

        Ok(Response::new(PingResponse {}))
    }

    async fn register(&self, _request: Request<RegisterRequest>) -> Result<Response<RegisterResponse>> {
        let uuid = Uuid::new_v4();

        self.users.write().await.insert(uuid, Arc::new(User::new(uuid)));

        info!("user {} register", uuid);

        Ok(Response::new(RegisterResponse { uuid: uuid.to_hyphenated().to_string() }))
    }

    async fn login(&self, request: Request<LoginRequest>) -> Result<Response<LoginResponse>> {
        let request = request.into_inner();

        let uuid: Uuid = match request.uuid.parse() {
            Err(_err) => {
                warn!("invalid uuid {}", request.uuid);

                return Err(Status::new(Code::InvalidArgument, "invalid uuid"));
            }

            Ok(uuid) => uuid
        };

        let users = self.users.write().await;

        let exist = if let Some(user) = users.get(&uuid) {
            user.update_last_alive_time(Local::now()).await;

            true
        } else {
            false
        };

        info!("user {} login", uuid);

        Ok(Response::new(LoginResponse { exist }))
    }

    async fn logout(&self, request: Request<LogoutRequest>) -> Result<Response<LogoutResponse>> {
        let request = request.into_inner();

        let uuid: Uuid = match request.uuid.parse() {
            Err(_err) => {
                warn!("invalid uuid {}", request.uuid);

                return Err(Status::new(Code::InvalidArgument, "invalid uuid"));
            }

            Ok(uuid) => uuid
        };

        let mut users = self.users.write().await;

        users.remove(&uuid);

        info!("user {} logout", uuid);

        Ok(Response::new(LogoutResponse {}))
    }
}