#![feature(backtrace)]

use log::LevelFilter;

pub use client::Filesystem;
use errno::Errno;
pub use helper::Apply;
pub use helper::UnixStream as TokioUnixStream;
pub use server::rpc::Server;

mod client;
mod errno;
mod helper;
mod path;
mod pb;
mod server;

pub type Result<T> = std::result::Result<T, Errno>;

pub fn log_init(debug: bool) {
    let mut builder = pretty_env_logger::formatted_timed_builder();

    builder
        .filter(Some("h2"), LevelFilter::Info)
        .filter(Some("tower_buffer"), LevelFilter::Info)
        .filter(Some("rustls"), LevelFilter::Info);

    if debug {
        builder.filter_level(LevelFilter::Debug);
    } else {
        builder.filter_level(LevelFilter::Info);
    }

    builder.init();
}
