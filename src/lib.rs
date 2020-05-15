#![feature(backtrace)]

use std::thread;

use log::LevelFilter;

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
                errno: err.0 as u32,
            }
        }
    }
}

mod server;

pub(crate) const BLOCK_SIZE: u32 = 4096;

pub fn log_init(debug: bool) {
    let mut builder = pretty_env_logger::formatted_timed_builder();

    builder
        .filter(Some("h2"), LevelFilter::Info)
        .filter(Some("tower_buffer"), LevelFilter::Info)
        .filter(Some("hyper"), LevelFilter::Info)
        .filter(Some("rustls"), LevelFilter::Info);

    if debug {
        builder.filter_level(LevelFilter::Debug);
    } else {
        builder.filter_level(LevelFilter::Info);
    }

    builder.init();
}

pub fn init_smol_runtime() {
    for _ in 0..num_cpus::get().min(1) {
        thread::spawn(|| smol::run(futures_util::future::pending::<()>()));
    }
}
