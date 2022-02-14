use std::collections::HashSet;
use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use http::Request;
use pin_project::pin_project;
use tokio::time::{self, Sleep};
use tonic::body::BoxBody;
use tonic::Status;
use tower::{Layer, Service};

pub type BoxError = Box<dyn Error + Send + Sync + 'static>;

#[pin_project(project = MayTimeoutFutureProj)]
#[derive(Debug)]
pub enum MayTimeoutFuture<F> {
    Timeout(#[pin] TimeoutFuture<F>),
    NonTimeout(#[pin] F),
}

impl<F, O, E> Future for MayTimeoutFuture<F>
where
    F: Future<Output = Result<O, E>>,
    E: Into<BoxError>,
{
    type Output = Result<O, BoxError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project() {
            MayTimeoutFutureProj::Timeout(fut) => fut.poll(cx),
            MayTimeoutFutureProj::NonTimeout(fut) => {
                Poll::Ready(futures_util::ready!(fut.poll(cx)).map_err(Into::into))
            }
        }
    }
}

#[pin_project]
#[derive(Debug)]
pub struct TimeoutFuture<F> {
    #[pin]
    future: F,
    duration: Duration,
    #[pin]
    sleep: Sleep,
}

impl<F> TimeoutFuture<F> {
    fn new(duration: Duration, future: F) -> Self {
        Self {
            future,
            duration,
            sleep: time::sleep(duration),
        }
    }
}

impl<F, O, E> Future for TimeoutFuture<F>
where
    F: Future<Output = Result<O, E>>,
    E: Into<BoxError>,
{
    type Output = Result<O, BoxError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        if this.sleep.poll(cx).is_ready() {
            Poll::Ready(Err(Box::new(Status::deadline_exceeded(format!(
                "{:?} timeout",
                this.duration
            )))))
        } else {
            Poll::Ready(futures_util::ready!(this.future.poll(cx)).map_err(Into::into))
        }
    }
}

#[derive(Debug, Clone)]
pub struct PathTimeoutService<S> {
    service: S,
    timeout_duration: Duration,
    no_timeout_paths: Option<Arc<HashSet<String>>>,
}

impl<S> Service<Request<BoxBody>> for PathTimeoutService<S>
where
    S: Service<Request<BoxBody>>,
    S::Error: Into<BoxError>,
{
    type Response = S::Response;
    type Error = BoxError;
    type Future = MayTimeoutFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx).map_err(|err| err.into())
    }

    fn call(&mut self, req: Request<BoxBody>) -> Self::Future {
        let path = req.uri().path();

        if let Some(no_time_paths) = self.no_timeout_paths.as_ref() {
            if no_time_paths.get(path).is_some() {
                return MayTimeoutFuture::NonTimeout(self.service.call(req));
            }
        }

        MayTimeoutFuture::Timeout(TimeoutFuture::new(
            self.timeout_duration,
            self.service.call(req),
        ))
    }
}

#[derive(Debug, Clone)]
pub struct PathTimeoutLayer {
    timeout_duration: Duration,
    no_timeout_paths: Option<Arc<HashSet<String>>>,
}

impl PathTimeoutLayer {
    pub fn new(
        timeout_duration: Duration,
        no_timeout_paths: impl Into<Option<HashSet<String>>>,
    ) -> Self {
        Self {
            timeout_duration,
            no_timeout_paths: no_timeout_paths.into().map(Arc::new),
        }
    }
}

impl<S> Layer<S> for PathTimeoutLayer {
    type Service = PathTimeoutService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        PathTimeoutService {
            service: inner,
            timeout_duration: self.timeout_duration,
            no_timeout_paths: self.no_timeout_paths.clone(),
        }
    }
}
