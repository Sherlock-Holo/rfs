use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_std::io::{Read, Write};
use async_std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use async_std::path::Path;
use async_std::sync::Arc;
use async_std::task;
use async_tls::TlsConnector;
use fuse::{FileAttr, FileType};
use hyper::client::connect::Connection;
use hyper::rt::Executor;
use rustls::ClientConfig;
use tokio::io::{AsyncRead, AsyncWrite};
use tonic::transport::server::Connected;

use crate::errno::Errno;
use crate::pb::{Attr as PbAttr, EntryType as PbEntryType};
use crate::Result;

pub trait Apply: Sized {
    fn apply<F>(mut self, f: F) -> Self
        where
            F: FnOnce(&mut Self),
    {
        f(&mut self);
        self
    }
}

impl<T> Apply for T {}

#[derive(Debug)]
pub struct UnixStream(pub async_std::os::unix::net::UnixStream);

impl UnixStream {
    pub async fn connect(path: impl AsRef<Path>) -> std::io::Result<Self> {
        Ok(Self(async_std::os::unix::net::UnixStream::connect(path).await?))
    }
}

impl Connection for UnixStream {
    fn connected(&self) -> hyper::client::connect::Connected {
        let connected = hyper::client::connect::Connected::new();

        if let Ok(peer_addr) = self.0.peer_addr() {
            connected.extra(peer_addr)
        } else {
            connected
        }
    }
}

impl Connected for UnixStream {}

impl AsyncRead for UnixStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl AsyncWrite for UnixStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_close(cx)
    }
}

pub struct TlsClientStream(pub async_tls::client::TlsStream<TcpStream>);

impl TlsClientStream {
    pub async fn connect<T: ToSocketAddrs>(tls_cfg: Arc<ClientConfig>, addr: T, domain: &str) -> std::io::Result<Self> {
        let stream = TcpStream::connect(addr).await?;

        let stream = TlsConnector::from(tls_cfg).connect(domain, stream).await?;

        Ok(Self(stream))
    }
}

impl Connection for TlsClientStream {
    fn connected(&self) -> hyper::client::connect::Connected {
        let connected = hyper::client::connect::Connected::new();

        if let Ok(peer_addr) = self.0.get_ref().peer_addr() {
            connected.extra(peer_addr)
        } else {
            connected
        }
    }
}

impl Connected for TlsClientStream {
    fn remote_addr(&self) -> Option<SocketAddr> {
        self.0.get_ref().peer_addr().ok()
    }
}

impl AsyncRead for TlsClientStream {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl AsyncWrite for TlsClientStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_close(cx)
    }
}

pub struct HyperExecutor;

impl<F> Executor<F> for HyperExecutor
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
{
    fn execute(&self, fut: F) {
        task::spawn(fut);
    }
}

fn convert_system_time_to_proto_time(sys_time: SystemTime) -> Option<prost_types::Timestamp> {
    sys_time
        .duration_since(UNIX_EPOCH)
        .map_or(None, |duration| {
            Some(prost_types::Timestamp {
                seconds: duration.as_secs() as i64,
                nanos: duration.as_nanos() as i32,
            })
        })
}

#[inline]
pub fn convert_proto_time_to_system_time(proto_time: Option<prost_types::Timestamp>) -> SystemTime {
    if let Some(proto_time) = proto_time {
        UNIX_EPOCH + Duration::new(proto_time.seconds as u64, proto_time.nanos as u32)
    } else {
        UNIX_EPOCH
    }
}

pub fn fuse_attr_into_proto_attr(fuse_attr: FileAttr, name: &str) -> PbAttr {
    PbAttr {
        inode: fuse_attr.ino,
        name: name.to_string(),
        r#type: match fuse_attr.kind {
            FileType::Directory => PbEntryType::Dir.into(),
            FileType::RegularFile => PbEntryType::File.into(),
            _ => unreachable!(),
        },
        access_time: convert_system_time_to_proto_time(fuse_attr.atime),
        modify_time: convert_system_time_to_proto_time(fuse_attr.mtime),
        change_time: convert_system_time_to_proto_time(fuse_attr.ctime),
        mode: fuse_attr.perm as i32,
        size: fuse_attr.size as i64,
    }
}

pub fn proto_attr_into_fuse_attr(proto_attr: PbAttr, uid: u32, gid: u32) -> Result<FileAttr> {
    let kind = {
        let dir = PbEntryType::Dir as i32;
        let file = PbEntryType::File as i32;

        if proto_attr.r#type == dir {
            FileType::Directory
        } else if proto_attr.r#type == file {
            FileType::RegularFile
        } else {
            return Err(Errno::from(libc::EIO));
        }
    };

    Ok(FileAttr {
        uid,
        gid,
        ino: proto_attr.inode,
        size: proto_attr.size as u64,
        blocks: get_blocks(proto_attr.size as u64),
        atime: convert_proto_time_to_system_time(proto_attr.access_time),
        mtime: convert_proto_time_to_system_time(proto_attr.modify_time),
        ctime: convert_proto_time_to_system_time(proto_attr.change_time),
        kind,
        perm: proto_attr.mode as u16,
        rdev: 0,
        nlink: match kind {
            FileType::Directory => 2,
            FileType::RegularFile => 0,
            _ => unreachable!(), // don't support other type
        },
        flags: 0,
    })
}

#[inline]
fn get_blocks(size: u64) -> u64 {
    const BLOCK_SIZE: u64 = 512;

    let blocks = size / BLOCK_SIZE;

    if size % BLOCK_SIZE == 0 {
        blocks
    } else {
        blocks + 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        let x = 100.apply(|myself| {
            *myself += 1;
        });

        assert_eq!(x, 101);
    }
}
