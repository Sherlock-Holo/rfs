use std::env;
use std::io::ErrorKind;
use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

use anyhow::{Context, format_err, Result};
use log::{debug, info};
use nix::mount;
use nix::mount::MntFlags;
use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;
use rand::distributions::Alphanumeric;
use rand::Rng;
use scopeguard::defer;
use serde::{Deserialize, Serialize};
use structopt::StructOpt;
use tokio::fs;
use tokio::select;
use tokio::signal::unix::{self, SignalKind};
use tokio::task;
use tokio::time;

use rfs::Apply;
use rfs::Filesystem;
use rfs::log_init;
use rfs::Server;

const UDS_DIR: &str = "/run/rfs";
const UDS_CLIENT_ENV: &str = "RFS_UDS_CLIENT";

#[derive(Debug, Deserialize)]
pub struct Config {
    root_path: PathBuf,
    listen_addr: String,
    cert_path: PathBuf,
    key_path: PathBuf,
    ca_path: PathBuf,
    debug: Option<bool>,
}

#[derive(Debug, StructOpt)]
#[structopt(name = "rfs-server", about = "rfs server.")]
pub struct Argument {
    #[structopt(short, long, default_value = "/etc/rfs/server.yml", parse(from_os_str))]
    config: PathBuf,
}

#[derive(Debug, Serialize, Deserialize)]
struct UdsConfig {
    uds_path: PathBuf,
    config_path: PathBuf,
    debug: bool,
}

pub async fn run() -> Result<()> {
    if let Some(uds_client_cfg) = env::var_os(UDS_CLIENT_ENV) {
        let uds_cfg: UdsConfig = serde_json::from_slice(uds_client_cfg.as_bytes())?;

        log_init(uds_cfg.debug);

        debug!("uds config loaded");

        let cfg_data = fs::read(&uds_cfg.config_path).await?;

        let cfg: Config = serde_yaml::from_slice(&cfg_data)?;

        let mut signal_stream = unix::signal(SignalKind::hangup())?;

        if let None = signal_stream.recv().await {
            return Err(format_err!("should receive a SIGHUP signal"));
        }

        debug!("receive signal, start uds client");

        let filesystem = Filesystem::new_uds(uds_cfg.uds_path).await?;

        info!("start uds client");

        defer! {
            let _ = mount::umount2(&cfg.root_path, MntFlags::MNT_DETACH);
        }

        filesystem.mount(&cfg.root_path)?;

        return Ok(());
    }

    let args = Argument::from_args();

    let cfg_data = fs::read(&args.config).await?;

    let cfg: Config = serde_yaml::from_slice(&cfg_data)?;

    let debug = if let Some(debug) = cfg.debug {
        debug
    } else {
        false
    };

    log_init(debug);

    debug!("config loaded");

    if let Err(err) = fs::create_dir(UDS_DIR).await {
        if ErrorKind::AlreadyExists != err.kind() {
            Err(err)?;
        }
    }

    debug!("uds dir created");

    let uds_name = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(10)
        .collect::<String>()
        + ".sock";

    let uds_path = PathBuf::from(UDS_DIR).apply(|path| path.push(uds_name));

    debug!("uds path is {}", uds_path.display());

    let defer_remove_path = uds_path.to_path_buf();

    defer! {
        let _ = std::fs::remove_file(defer_remove_path);
    }

    let uds_cfg = UdsConfig {
        debug,
        uds_path: uds_path.to_path_buf(),
        config_path: args.config.to_path_buf(),
    };

    let uds_json_cfg = serde_json::to_string(&uds_cfg).context("serialize uds config failed")?;

    let mut uds_client = Command::new("/proc/self/exe")
        .env(UDS_CLIENT_ENV, uds_json_cfg)
        .spawn()?;

    let uds_client_id = uds_client.id();

    info!("uds client starting");

    info!("starting rfs server");

    task::spawn(async move {
        time::delay_for(Duration::from_secs(1)).await;

        task::spawn_blocking(move || {
            signal::kill(Pid::from_raw(uds_client_id as i32), Signal::SIGHUP)
        })
            .await
    });

    let serve = Server::run(
        cfg.root_path,
        cfg.cert_path,
        cfg.key_path,
        cfg.ca_path,
        uds_path,
        cfg.listen_addr.parse()?,
    );

    let uds_client_job =
        task::spawn_blocking(move || uds_client.wait().context("uds client quit unexpected"));

    select! {
        result = serve => {
            result?;
            Ok(())
        },
        result = uds_client_job => {
            result??;
            Ok(())
        }
    }
}
