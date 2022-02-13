use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::{BufMut, Bytes, BytesMut};
use futures_util::ready;
use http::{HeaderMap, Request, Response};
use http_body::Body;
use tonic::body::BoxBody;
use tonic::Status;
use tower::Service;

pub struct BytesBody(Option<Bytes>, Option<HeaderMap>);

impl Body for BytesBody {
    type Data = Bytes;
    type Error = Status;

    fn poll_data(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Self::Data, Self::Error>>> {
        Poll::Ready(Ok(self.0.take()).transpose())
    }

    fn poll_trailers(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<Option<HeaderMap>, Self::Error>> {
        Poll::Ready(Ok(self.1.take()))
    }
}

pub trait RetryHandle {
    fn should_retry(&self, err: &(dyn Error + Send + Sync)) -> bool;
}

impl RetryHandle for fn(&(dyn Error + Send + Sync)) -> bool {
    fn should_retry(&self, err: &(dyn Error + Send + Sync)) -> bool {
        self(err)
    }
}

#[derive(Clone)]
pub struct RetryClient<C, RetryHandle> {
    client: C,
    err_retry: Option<Arc<RetryHandle>>,
}

impl<C> RetryClient<C, fn(&(dyn Error + Send + Sync)) -> bool> {
    pub fn new(client: C) -> Self {
        Self {
            client,
            err_retry: None,
        }
    }
}

impl<C, RetryHandle> RetryClient<C, RetryHandle> {
    pub fn new_with_retry_handle(client: C, retry_handle: RetryHandle) -> Self {
        Self {
            client,
            err_retry: Some(Arc::new(retry_handle)),
        }
    }
}

#[derive(Debug, Copy, Clone)]
enum RetryFutureState {
    Init,
    GetData,
    GetHeader,
    PollReady,
    PollResult,
}

pub struct RetryFuture<C, F, RetryHandle> {
    state: RetryFutureState,
    client: C,
    req: Request<BoxBody>,
    buf: Option<BytesMut>,
    data: Bytes,
    header: Option<HeaderMap>,
    result_fut: Option<F>,
    err_retry: Option<Arc<RetryHandle>>,
}

impl<C, F, RetryHandle> RetryFuture<C, F, RetryHandle> {
    pub fn new(client: C, req: Request<BoxBody>, err_retry: Option<Arc<RetryHandle>>) -> Self {
        Self {
            state: RetryFutureState::Init,
            client,
            req,
            buf: None,
            data: Bytes::new(),
            header: None,
            result_fut: None,
            err_retry,
        }
    }
}

impl<C, F, Retry> Future for RetryFuture<C, F, Retry>
where
    F: Unpin,
    Retry: RetryHandle + Unpin + Clone,
    C: Service<Request<BoxBody>, Response = Response<tonic::transport::Body>, Future = F>,
    C: Clone + Unpin + Send + 'static,
    C::Error: Into<Box<dyn Error + Send + Sync>> + Error + Send + Sync,
    C::Future: Future<Output = Result<C::Response, C::Error>> + Send,
{
    type Output = Result<C::Response, Box<dyn Error + Send + Sync>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.get_mut();

        loop {
            match this.state {
                RetryFutureState::Init => {
                    this.buf.replace(BytesMut::new());

                    this.state = RetryFutureState::GetData;
                }

                RetryFutureState::GetData => {
                    let data = ready!(Pin::new(&mut this.req).poll_data(cx)).transpose()?;
                    if let Some(data) = data {
                        this.buf.as_mut().unwrap().put(data);

                        continue;
                    } else {
                        let data = this.buf.take().unwrap().freeze();
                        this.data = data;

                        this.state = RetryFutureState::GetHeader;
                    }
                }

                RetryFutureState::GetHeader => {
                    let header = ready!(Pin::new(&mut this.req).poll_trailers(cx))?;
                    if let Some(header) = header {
                        this.header.replace(header);
                    }

                    this.state = RetryFutureState::PollReady;
                }

                RetryFutureState::PollReady => {
                    ready!(this.client.poll_ready(cx))?;

                    let bytes_body = BytesBody(Some(this.data.clone()), this.header.clone());

                    let request = Request::new(BoxBody::new(bytes_body));

                    let result_fut = this.client.call(request);

                    this.result_fut.replace(result_fut);

                    this.state = RetryFutureState::PollResult;
                }

                RetryFutureState::PollResult => {
                    let fut = this.result_fut.as_mut().unwrap();
                    let result = ready!(Pin::new(fut).poll(cx));
                    match result {
                        Err(err) => {
                            let err = err.into();

                            if let Some(err_retry) = this.err_retry.as_ref() {
                                if !err_retry.should_retry(err.as_ref()) {
                                    return Poll::Ready(Err(err));
                                }
                            }

                            eprintln!("{:?}", err.source());

                            eprintln!("{}", err);

                            this.state = RetryFutureState::PollReady;

                            continue;
                        }

                        Ok(resp) => return Poll::Ready(Ok(resp)),
                    }
                }
            }
        }
    }
}

impl<C, Retry> Service<Request<BoxBody>> for RetryClient<C, Retry>
where
    C: Service<Request<BoxBody>, Response = Response<tonic::transport::Body>>,
    C::Future: Unpin,
    C: Clone + Send + Unpin + 'static,
    C::Error: Into<Box<dyn Error + Send + Sync>> + Error + Send + Sync,
    C::Future: Future<Output = Result<C::Response, C::Error>> + Send,
    Retry: RetryHandle + Send + Clone + Unpin,
{
    type Response = C::Response;
    type Error = Box<dyn Error + Send + Sync>;
    type Future = RetryFuture<C, C::Future, Retry>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<BoxBody>) -> Self::Future {
        RetryFuture::new(self.client.clone(), req, self.err_retry.clone())
    }
}
