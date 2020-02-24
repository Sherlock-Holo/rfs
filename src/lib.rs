#![feature(backtrace)]

use log::LevelFilter;

use errno::Errno;

mod client;
mod errno;
mod helper;
mod path;
pub mod pb;
mod server;

pub type Result<T> = std::result::Result<T, Errno>;

pub fn log_init(debug: bool) {
    let mut builder = pretty_env_logger::formatted_timed_builder();

    if debug {
        builder.filter_level(LevelFilter::Debug);
    } else {
        builder.filter_level(LevelFilter::Info);
    }

    builder.init();
}
