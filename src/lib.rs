#![feature(type_alias_impl_trait)]
#![feature(generic_associated_types)]

use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Registry;

pub use client::Filesystem;
pub use helper::Apply;
pub use server::rpc::Server;

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

pub fn log_init(server_name: String, debug: bool) {
    let (tracer, _uninstall) = opentelemetry_jaeger::new_pipeline()
        .with_service_name(server_name)
        .install()
        .unwrap();

    let stdout_subscriber = tracing_subscriber::fmt::layer().json();

    let trace = tracing_opentelemetry::layer().with_tracer(tracer);

    let level_filter = if debug {
        tracing_subscriber::filter::LevelFilter::DEBUG
    } else {
        tracing_subscriber::filter::LevelFilter::INFO
    };

    let subscriber = Registry::default()
        .with(level_filter)
        .with(trace)
        .with(stdout_subscriber);

    tracing::subscriber::set_global_default(subscriber).unwrap();
}
