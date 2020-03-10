use std::ffi::{OsStr, OsString};
use std::future::Future;
use std::io::Error;
use std::pin::Pin;

use async_std::net::TcpStream;
use async_std::sync::Arc;
use async_tls::TlsConnector;
use futures_util::io::ErrorKind;
use futures_util::task::{Context, Poll};
use hyper::{Client, Request, Response, Uri};
use rustls::ClientConfig;
use tonic::body::BoxBody;
use tonic::client::GrpcService;
use tower_service::Service;

use crate::{TlsClientStream, TokioUnixStream};
use crate::helper::HyperExecutor;

#[derive(Clone)]
pub struct UdsConnector {
    path: OsString,
}

impl UdsConnector {
    pub fn new<P: AsRef<OsStr>>(path: P) -> Self {
        Self { path: path.as_ref().to_os_string() }
    }
}

impl Service<Uri> for UdsConnector {
    type Response = TokioUnixStream;
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output=Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _req: Uri) -> Self::Future {
        Box::pin(TokioUnixStream::connect(self.path.to_os_string()))
    }
}

#[derive(Clone)]
pub struct UdsClient {
    inner_client: Client<UdsConnector, BoxBody>,
}

impl UdsClient {
    pub fn new<P: AsRef<OsStr>>(path: P) -> Self {
        Self {
            inner_client: Client::builder()
                .executor(HyperExecutor {})
                .build(UdsConnector::new(path))
        }
    }
}

impl GrpcService<BoxBody> for UdsClient {
    type ResponseBody = hyper::Body;
    type Error = hyper::Error;
    type Future = Pin<Box<dyn Future<Output=Result<Response<Self::ResponseBody>, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<BoxBody>) -> Self::Future {
        Box::pin(self.inner_client.request(request))
    }
}

#[derive(Clone)]
pub struct RpcConnector {
    tls_connector: TlsConnector,
}

impl RpcConnector {
    pub fn new(tls_cfg: ClientConfig) -> Self {
        Self { tls_connector: TlsConnector::from(Arc::new(tls_cfg)) }
    }
}

impl Service<Uri> for RpcConnector {
    type Response = TlsClientStream;
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output=Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Uri) -> Self::Future {
        let authority = req.authority().
            map(|authority| authority.clone()).
            ok_or(Error::new(ErrorKind::AddrNotAvailable, format!("uri {} is invalid", req)));

        let host = req.host().
            map(|host| host.to_string()).
            ok_or(Error::new(ErrorKind::AddrNotAvailable, format!("uri {} host is invalid", req)));

        let tls_connector = self.tls_connector.clone();

        Box::pin(async move {
            let authority = authority?.to_string();
            let host = host?;

            let stream = TcpStream::connect(authority).await?;

            let stream = tls_connector.connect(host, stream).await?;

            Ok(TlsClientStream(stream))
        })
    }
}

#[derive(Clone)]
pub struct RpcClient {
    inner_client: Arc<Client<RpcConnector, BoxBody>>,
    uri: Uri,
}

impl RpcClient {
    pub fn new(tls_cfg: ClientConfig, uri: Uri) -> Self {
        Self {
            inner_client: Arc::new(Client::builder()
                .executor(HyperExecutor {})
                .build(RpcConnector::new(tls_cfg))),
            uri,
        }
    }
}

impl GrpcService<BoxBody> for RpcClient {
    type ResponseBody = hyper::Body;
    type Error = hyper::Error;
    type Future = Pin<Box<dyn Future<Output=Result<Response<Self::ResponseBody>, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, mut request: Request<BoxBody>) -> Self::Future {
        let uri = Uri::builder()
            .scheme(self.uri.scheme().unwrap().clone())
            .authority(self.uri.authority().unwrap().clone())
            .path_and_query(request.uri().path_and_query().unwrap().clone())
            .build()
            .unwrap();

        *request.uri_mut() = uri;

        Box::pin(self.inner_client.request(request))
    }
}