use std::io::ErrorKind;
use std::pin::Pin;
use std::task::{Context, Poll};

use async_std::io;
use async_std::net::{SocketAddr, TcpListener, TcpStream};
use async_std::prelude::*;
use async_std::sync::Arc;
use async_std::task;
use async_tls::{TlsAcceptor, TlsConnector};
use async_tls::client::TlsStream as ClientTlsStream;
use async_tls::server::TlsStream as ServerTlsStream;
use futures_util::future::ready;
use futures_util::io::AsyncWrite;
use hyper::client::connect::{Connect, Connected, Connection, HttpInfo};
use hyper::service::Service;
use rustls::ClientConfig;
use tonic::body::BoxBody;
use tonic::transport::Uri;

#[derive(Clone)]
pub struct HyperExecutor;

impl<F> hyper::rt::Executor<F> for HyperExecutor
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
{
    fn execute(&self, fut: F) {
        task::spawn(fut);
    }
}

pub struct HyperListener {
    tls_acceptor: TlsAcceptor,
    tcp_listener: TcpListener,
}

impl hyper::server::accept::Accept for HyperListener {
    type Conn = HyperServerStream;
    type Error = io::Error;

    fn poll_accept(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<Option<Result<Self::Conn, Self::Error>>> {
        let stream = task::ready!(Pin::new(&mut self.tcp_listener.incoming()).poll_next(cx)).unwrap()?;

        let stream = task::ready!(Pin::new(&mut self.tls_acceptor.accept(stream)).poll(cx));

        match stream {
            Err(err) => Poll::Ready(Some(Err(err))),

            Ok(stream) => Poll::Ready(Some(Ok(HyperServerStream(stream))))
        }
    }
}

pub struct HyperServerStream(pub ServerTlsStream<TcpStream>);

impl tokio::io::AsyncRead for HyperServerStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl tokio::io::AsyncWrite for HyperServerStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_close(cx)
    }
}

pub struct HyperClientStream(pub ClientTlsStream<TcpStream>);

impl tokio::io::AsyncRead for HyperClientStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl tokio::io::AsyncWrite for HyperClientStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_close(cx)
    }
}

impl Connection for HyperClientStream {
    fn connected(&self) -> Connected {
        let connected = Connected::new();

        if let Ok(remote_addr) = self.0.get_ref().peer_addr() {
            connected.extra(HttpInfo { remote_addr })
        } else {
            connected
        }
    }
}

#[derive(Clone)]
pub struct HyperConnector {
    tls_connector: TlsConnector,
}

impl Service<Uri> for HyperConnector {
    type Response = HyperClientStream;
    type Error = dyn Into<Box<dyn std::error::Error + Send + Sync>>;
    type Future = Box<dyn Future<Output=io::Result<HyperClientStream>> + Send + Sync + 'static>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Uri) -> Self::Future {
        match req.authority() {
            None => Box::new(ready(Err(io::Error::new(ErrorKind::AddrNotAvailable, format!("{} is invalid", req))))),

            Some(authority) => {
                let host = authority.host().to_string();
                let authority = authority.to_string();

                let tls_connector = self.tls_connector.clone();

                Box::new(async move {
                    let stream = TcpStream::connect(authority).await?;

                    let tls_stream = tls_connector.connect(host, stream).await?;

                    Ok(HyperClientStream(tls_stream))
                })
            }
        }
    }
}

impl From<ClientConfig> for HyperConnector {
    fn from(cfg: ClientConfig) -> Self {
        Self {
            tls_connector: TlsConnector::from(Arc::new(cfg))
        }
    }
}