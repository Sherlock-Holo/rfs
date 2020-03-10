use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_std::net::{TcpListener, ToSocketAddrs};
use async_std::os::unix::net::UnixListener;
use async_std::path::Path;
use async_tls::TlsAcceptor;
use futures_util::ready;
use hyper::Request;
use hyper::server::accept::Accept;
use log::debug;
use rustls::ServerConfig;
use tonic::body::BoxBody;
use tonic::codegen::Never;
use tonic::transport::Body;
use tower_service::Service;

use crate::helper::TlsServerStream;
use crate::pb::rfs_server::{Rfs, RfsServer};
use crate::TokioUnixStream;

pub struct CustomUnixListener(async_std::os::unix::net::UnixListener);

impl From<async_std::os::unix::net::UnixListener> for CustomUnixListener {
    fn from(listener: UnixListener) -> Self {
        Self(listener)
    }
}

impl Accept for CustomUnixListener {
    type Conn = TokioUnixStream;
    type Error = io::Error;

    fn poll_accept(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Self::Conn, Self::Error>>> {
        let accept = self.0.accept();

        futures_util::pin_mut!(accept);

        match ready!(accept.poll(cx)) {
            Err(err) => Poll::Ready(Some(Err(err))),
            Ok((stream, _)) => Poll::Ready(Some(Ok(TokioUnixStream(stream))))
        }
    }
}

pub struct CustomUnixServer<T: Rfs> {
    inner_server: RfsServer<T>,
}

impl<T: Rfs> CustomUnixServer<T> {
    pub fn new(t: T) -> Self {
        Self { inner_server: RfsServer::new(t) }
    }
}

impl<T: Rfs> Service<hyper::Request<hyper::Body>> for CustomUnixServer<T> {
    type Response = hyper::Response<BoxBody>;
    type Error = Never;
    type Future = Pin<Box<dyn Future<Output=Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        self.inner_server.call(req)
    }
}

pub struct CustomTlsListener {
    tls_acceptor: TlsAcceptor,
    tcp_listener: TcpListener,
}

impl CustomTlsListener {
    pub async fn new<A: ToSocketAddrs>(tls_cfg: ServerConfig, addr: A) -> io::Result<Self> {
        Ok(Self {
            tls_acceptor: TlsAcceptor::from(Arc::new(tls_cfg)),
            tcp_listener: TcpListener::bind(addr).await?,
        })
    }
}

impl Accept for CustomTlsListener {
    type Conn = TlsServerStream;
    type Error = io::Error;

    fn poll_accept(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Self::Conn, Self::Error>>> {
        let accept = self.tcp_listener.accept();

        futures_util::pin_mut!(accept);

        let stream = match ready!(accept.poll(cx)) {
            Err(err) => return Poll::Ready(Some(Err(err))),
            Ok((stream, _)) => stream,
        };

        self.tls_acceptor.accept(stream).
    }
}