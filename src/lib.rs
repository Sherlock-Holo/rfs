pub use client::Filesystem;
pub use helper::Apply;
use opentelemetry::global;
use opentelemetry::global::shutdown_tracer_provider;
use opentelemetry_jaeger::Propagator;
pub use server::rpc::Server;
use tracing_subscriber::filter::{LevelFilter, Targets};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Registry;

mod client;
mod helper;
mod path;

mod pb {
    tonic::include_proto!("proto");

    pub const VERSION: &str = "0.3.0";

    impl From<fuse3::Errno> for Error {
        fn from(err: fuse3::Errno) -> Self {
            Error {
                errno: -libc::c_int::from(err) as u32,
            }
        }
    }
}

mod server;

pub(crate) const BLOCK_SIZE: u32 = 4096;

pub fn log_init(server_name: String, debug: bool) -> LogShutdownGuard {
    global::set_text_map_propagator(Propagator::new());

    let tracer = opentelemetry_jaeger::new_pipeline()
        .with_service_name(server_name)
        .install_simple()
        .unwrap();

    let stdout_subscriber = tracing_subscriber::fmt::layer().json();

    let trace = tracing_opentelemetry::layer().with_tracer(tracer);

    let level_filter = if debug {
        LevelFilter::DEBUG
    } else {
        LevelFilter::INFO
    };

    let filter_target = Targets::new().with_default(level_filter).with_targets([
        ("h2", LevelFilter::OFF),
        ("tower", LevelFilter::OFF),
        ("hyper", LevelFilter::OFF),
    ]);

    let subscriber = Registry::default()
        .with(filter_target)
        .with(trace)
        .with(stdout_subscriber);

    tracing::subscriber::set_global_default(subscriber).unwrap();

    LogShutdownGuard { _priv: () }
}

#[must_use]
pub struct LogShutdownGuard {
    _priv: (),
}

impl Drop for LogShutdownGuard {
    fn drop(&mut self) {
        shutdown_tracer_provider()
    }
}
