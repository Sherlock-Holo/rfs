use std::path::PathBuf;

use anyhow::Result;
use async_signals::Signals;
use futures_util::future::FutureExt;
use futures_util::select;
use futures_util::stream::StreamExt;
use nix::libc;
use serde::Deserialize;
use structopt::clap::AppSettings::*;
use structopt::StructOpt;
use tokio::fs;
use tracing::info;

use rfs::log_init;
use rfs::Server;

#[derive(Debug, Deserialize)]
pub struct Config {
    root_path: PathBuf,
    listen_addr: String,
    cert_path: PathBuf,
    key_path: PathBuf,
    ca_path: PathBuf,
    debug: Option<bool>,
    compress: Option<bool>,
}

#[derive(Debug, StructOpt)]
#[structopt(about = "rfs server.", settings(& [ColorAuto, ColoredHelp]))]
pub struct Argument {
    #[structopt(short, long, default_value = "/etc/rfs/server.yml", parse(from_os_str))]
    config: PathBuf,
}

pub async fn run() -> Result<()> {
    let args = Argument::from_args();

    let cfg_data = fs::read(&args.config).await?;

    let cfg: Config = serde_yaml::from_slice(&cfg_data)?;

    let debug = if let Some(debug) = cfg.debug {
        debug
    } else {
        false
    };

    log_init("rfs-server".to_owned(), debug);

    info!("starting rfs server");

    let compress = if let Some(compress) = cfg.compress {
        compress
    } else {
        false
    };

    let serve = Server::run(
        cfg.root_path,
        cfg.cert_path,
        cfg.key_path,
        cfg.ca_path,
        cfg.listen_addr.parse()?,
        compress,
    );

    let mut stop_signal = Signals::new(vec![libc::SIGINT, libc::SIGTERM])?;

    select! {
        result = serve.fuse() => result,

        _ = stop_signal.next().fuse() => {
            info!("receive stop signal");

            Ok(())
        }
    }
}
