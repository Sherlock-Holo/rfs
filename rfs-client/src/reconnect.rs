use std::error::Error;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use pin_project::pin_project;
use tower::make::MakeService;
use tower::Service;
use tracing::trace;

type BoxError = Box<dyn Error + Send + Sync>;

/// Future that resolves to the response or failure to connect.
#[derive(Debug)]
#[pin_project]
pub struct ResponseFuture<F, E> {
    #[pin]
    inner: Inner<F, E>,
}

#[derive(Debug)]
#[pin_project(project = InnerProj)]
enum Inner<F, E> {
    Future {
        #[pin]
        fut: F,
    },
    Error {
        error: Option<E>,
    },
}

impl<F, E> Inner<F, E> {
    fn future(fut: F) -> Self {
        Self::Future { fut }
    }

    fn error(error: Option<E>) -> Self {
        Self::Error { error }
    }
}

impl<F, E> ResponseFuture<F, E> {
    pub(crate) fn new(inner: F) -> Self {
        ResponseFuture {
            inner: Inner::future(inner),
        }
    }

    pub(crate) fn error(error: E) -> Self {
        ResponseFuture {
            inner: Inner::error(Some(error)),
        }
    }
}

impl<F, T, E, ME> Future for ResponseFuture<F, ME>
where
    F: Future<Output = Result<T, E>>,
    E: Into<BoxError>,
    ME: Into<BoxError>,
{
    type Output = Result<T, BoxError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = self.project();
        match me.inner.project() {
            InnerProj::Future { fut } => fut.poll(cx).map_err(Into::into),
            InnerProj::Error { error } => {
                let e = error.take().expect("Polled after ready.").into();
                Poll::Ready(Err(e))
            }
        }
    }
}

/// Reconnect to failed services.
pub struct Reconnect<M, Target>
where
    M: Service<Target>,
{
    mk_service: M,
    state: State<M::Future, M::Response>,
    target: Target,
    error: Option<M::Error>,
}

impl<M, Target> Clone for Reconnect<M, Target>
where
    M: Clone + Service<Target>,
    Target: Clone,
    M::Response: Clone,
{
    fn clone(&self) -> Self {
        Self {
            mk_service: self.mk_service.clone(),
            state: self.state.clone(),
            target: self.target.clone(),
            error: None,
        }
    }
}

#[derive(Debug)]
enum State<F, S> {
    Idle,
    Connecting(F),
    Connected(S),
}

impl<F, S: Clone> Clone for State<F, S> {
    fn clone(&self) -> Self {
        match self {
            State::Idle => State::Idle,
            State::Connecting(_) => State::Idle,
            State::Connected(connected) => State::Connected(connected.clone()),
        }
    }
}

impl<M, Target> Reconnect<M, Target>
where
    M: Service<Target>,
{
    /// Reconnect to a already connected [`Service`].
    pub fn with_connection(init_conn: M::Response, mk_service: M, target: Target) -> Self {
        Reconnect {
            mk_service,
            state: State::Connected(init_conn),
            target,
            error: None,
        }
    }
}

impl<M, Target, S, Request> Service<Request> for Reconnect<M, Target>
where
    M: Service<Target, Response = S> + Send + Sync,
    S: Service<Request> + Send + Sync,
    M::Future: Unpin + Send + Sync,
    Box<dyn Error + Send + Sync>: From<M::Error> + From<S::Error>,
    Target: Clone,
{
    type Response = S::Response;
    type Error = Box<dyn Error + Send + Sync>;
    type Future = ResponseFuture<S::Future, M::Error>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        loop {
            match &mut self.state {
                State::Idle => {
                    trace!("poll_ready; idle");
                    match self.mk_service.poll_ready(cx) {
                        Poll::Ready(r) => r?,
                        Poll::Pending => {
                            trace!("poll_ready; MakeService not ready");
                            return Poll::Pending;
                        }
                    }

                    let fut = self.mk_service.make_service(self.target.clone());
                    self.state = State::Connecting(fut);
                    continue;
                }
                State::Connecting(ref mut f) => {
                    trace!("poll_ready; connecting");
                    match Pin::new(f).poll(cx) {
                        Poll::Ready(Ok(service)) => {
                            self.state = State::Connected(service);
                        }
                        Poll::Pending => {
                            trace!("poll_ready; not ready");
                            return Poll::Pending;
                        }
                        Poll::Ready(Err(e)) => {
                            trace!("poll_ready; error");
                            self.state = State::Idle;
                            self.error = Some(e);
                            break;
                        }
                    }
                }
                State::Connected(ref mut inner) => {
                    trace!("poll_ready; connected");
                    match inner.poll_ready(cx) {
                        Poll::Ready(Ok(())) => {
                            trace!("poll_ready; ready");
                            return Poll::Ready(Ok(()));
                        }
                        Poll::Pending => {
                            trace!("poll_ready; not ready");
                            return Poll::Pending;
                        }
                        Poll::Ready(Err(_)) => {
                            trace!("poll_ready; error");
                            self.state = State::Idle;
                        }
                    }
                }
            }
        }

        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request) -> Self::Future {
        if let Some(error) = self.error.take() {
            return ResponseFuture::error(error);
        }

        let service = match self.state {
            State::Connected(ref mut service) => service,
            _ => panic!("service not ready; poll_ready must be called first"),
        };

        let fut = service.call(request);
        ResponseFuture::new(fut)
    }
}
