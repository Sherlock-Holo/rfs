use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_std::net::{TcpListener, TcpStream, ToSocketAddrs};
use async_std::os::unix::net::UnixListener;
use async_tls::server::TlsStream as TlsServerStream;
use async_tls::TlsAcceptor;
use futures_util::pin_mut;
use futures_util::ready;
use futures_util::stream::Stream;
use hyper::server::accept::Accept;
use hyper::Request;
use rustls::ServerConfig;
use tonic::body::BoxBody;
use tonic::codegen::Never;
use tonic::transport::Body;
use tower_service::Service;

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

    fn poll_accept(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Self::Conn, Self::Error>>> {
        let accept = self.0.accept();

        futures_util::pin_mut!(accept);

        match ready!(accept.poll(cx)) {
            Err(err) => Poll::Ready(Some(Err(err))),
            Ok((stream, _)) => Poll::Ready(Some(Ok(TokioUnixStream(stream)))),
        }
    }
}

pub struct CustomUnixServer<T: Rfs> {
    inner_server: RfsServer<T>,
}

impl<T: Rfs> CustomUnixServer<T> {
    pub fn new(t: T) -> Self {
        Self {
            inner_server: RfsServer::new(t),
        }
    }
}

impl<T: Rfs> Service<hyper::Request<hyper::Body>> for CustomUnixServer<T> {
    type Response = hyper::Response<BoxBody>;
    type Error = Never;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        self.inner_server.call(req)
    }
}

pub struct TlsListener {
    tls_acceptor: TlsAcceptor,
    tcp_listener: TcpListener,
    wait_queue: Option<Vec<Pin<Box<dyn Future<Output = io::Result<TlsServerStream<TcpStream>>>>>>>,
    success_queue: Vec<io::Result<TlsServerStream<TcpStream>>>,
}

impl TlsListener {
    pub async fn bind<A: ToSocketAddrs>(tls_cfg: ServerConfig, addr: A) -> io::Result<Self> {
        Ok(Self {
            tls_acceptor: TlsAcceptor::from(Arc::new(tls_cfg)),
            tcp_listener: TcpListener::bind(addr).await?,
            wait_queue: None,
            success_queue: vec![],
        })
    }
}

impl Stream for TlsListener {
    type Item = io::Result<TlsServerStream<TcpStream>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let myself = self.get_mut();

        if let Some(stream) = myself.success_queue.pop() {
            return Poll::Ready(Some(stream));
        }

        if let Some(queue) = myself.wait_queue.take() {
            let mut wait_queue = vec![];

            for mut future in queue {
                match future.as_mut().poll(cx) {
                    Poll::Pending => wait_queue.push(future),
                    Poll::Ready(stream) => myself.success_queue.push(stream),
                }
            }

            myself.wait_queue.replace(wait_queue);
        }

        if let Some(stream) = myself.success_queue.pop() {
            return Poll::Ready(Some(stream));
        }

        let accept = myself.tcp_listener.accept();

        pin_mut!(accept);

        let stream = futures_util::ready!(accept.poll(cx));

        let tls_acceptor = myself.tls_acceptor.clone();

        let future = Box::pin(async move {
            let (tcp_stream, _) = stream?;

            tls_acceptor.accept(tcp_stream).await
        });

        match &mut myself.wait_queue {
            Some(wait_queue) => {
                wait_queue.push(future);
            }
            None => {
                myself.wait_queue.replace(vec![future]);
            }
        }

        Poll::Pending
    }
}
