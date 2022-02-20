use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{BufMut, Bytes, BytesMut};
use futures_util::ready;
use http::{HeaderMap, Request, Response};
use http_body::Body;
use tonic::body::BoxBody;
use tonic::Status;
use tower::Service;
use tracing::debug;

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
    fn should_retry(&mut self, err: &(dyn Error + Send + Sync)) -> bool;
}

#[derive(Clone)]
pub struct RetryClient<C, ErrRetry> {
    client: C,
    err_retry: ErrRetry,
}

impl<C, ErrRetry> RetryClient<C, ErrRetry> {
    pub fn new(client: C, err_retry: ErrRetry) -> Self {
        Self { client, err_retry }
    }
}

#[derive(Debug, Copy, Clone)]
enum RetryFutureState {
    Init,
    PollData,
    PollTrailer,
    PollReady,
    PollResult,
}

pub struct RetryFuture<C, F, ErrRetry> {
    state: RetryFutureState,
    client: C,
    req: Request<BoxBody>,
    buf: Option<BytesMut>,
    data: Bytes,
    trailers: Option<HeaderMap>,
    result_fut: Option<F>,
    err_retry: ErrRetry,
}

impl<C, F, ErrRetry> RetryFuture<C, F, ErrRetry> {
    pub fn new(client: C, req: Request<BoxBody>, err_retry: ErrRetry) -> Self {
        Self {
            state: RetryFutureState::Init,
            client,
            req,
            buf: None,
            data: Bytes::new(),
            trailers: None,
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
                    let size_hint = this.req.size_hint();
                    let capacity = size_hint.upper().unwrap_or_else(|| size_hint.lower());

                    this.buf.replace(BytesMut::with_capacity(capacity as _));

                    this.state = RetryFutureState::PollData;
                }

                RetryFutureState::PollData => {
                    let data = ready!(Pin::new(&mut this.req).poll_data(cx)).transpose()?;

                    if let Some(data) = data {
                        this.buf.as_mut().unwrap().put(data);

                        continue;
                    } else {
                        this.data = this.buf.take().unwrap().freeze();

                        this.state = RetryFutureState::PollTrailer;
                    }
                }

                RetryFutureState::PollTrailer => {
                    this.trailers = ready!(Pin::new(&mut this.req).poll_trailers(cx))?;

                    debug!(trailers = ?this.trailers, "poll_trailer");

                    this.state = RetryFutureState::PollReady;
                }

                RetryFutureState::PollReady => {
                    ready!(this.client.poll_ready(cx))?;

                    debug!("poll_ready done");

                    let bytes_body = BytesBody(Some(this.data.clone()), this.trailers.clone());

                    let mut req_builder = Request::builder()
                        .method(this.req.method().clone())
                        .uri(this.req.uri().clone());

                    for (key, value) in this.req.headers().iter() {
                        req_builder = req_builder.header(key, value);
                    }

                    let request = req_builder.body(BoxBody::new(bytes_body))?;

                    let result_fut = this.client.call(request);

                    debug!("client call done");

                    this.result_fut.replace(result_fut);

                    this.state = RetryFutureState::PollResult;
                }

                RetryFutureState::PollResult => {
                    let fut = this.result_fut.as_mut().unwrap();
                    let result = ready!(Pin::new(fut).poll(cx));

                    debug!("future poll done");

                    match result {
                        Err(err) => {
                            let err = err.into();

                            if !this.err_retry.should_retry(err.as_ref()) {
                                debug!(%err, "error should not retry");

                                return Poll::Ready(Err(err));
                            }

                            debug!(%err, "error should retry");

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
