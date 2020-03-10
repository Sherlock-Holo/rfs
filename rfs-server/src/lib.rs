use std::env;
use std::io::ErrorKind;
use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

use anyhow::{format_err, Context, Result};
use async_signals::Signals;
use async_std::fs;
use async_std::task;
use futures_util::future::FutureExt;
use futures_util::{select, StreamExt};
use log::{debug, info};
use nix::libc;
use nix::mount;
use nix::mount::MntFlags;
use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;
use rand::distributions::Alphanumeric;
use rand::prelude::*;
use rand::rngs::OsRng;
use scopeguard::defer;
use serde::{Deserialize, Serialize};
use structopt::StructOpt;

use rfs::log_init;
use rfs::Apply;
use rfs::Filesystem;
use rfs::Server;
pub use tokio_runtime::enter_tokio;

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

        let mut initialize_signal = Signals::new(vec![libc::SIGHUP])?;

        if let None = initialize_signal.next().await {
            return Err(format_err!("should receive a SIGHUP signal"));
        }

        debug!("receive signal, start uds client");

        let filesystem = Filesystem::new_uds(uds_cfg.uds_path).await?;

        info!("start uds client");

        defer! {
            let _ = mount::umount2(&cfg.root_path, MntFlags::MNT_DETACH);
        }

        filesystem.mount(&cfg.root_path).await?;

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

    let uds_name = OsRng {}
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
        task::sleep(Duration::from_secs(1)).await;

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

    let mut stop_signal = Signals::new(vec![libc::SIGINT])?;

    debug!("stop signal initialize");

    select! {
        result = serve.fuse() => {
            signal::kill(Pid::from_raw(uds_client_id as i32), Signal::SIGINT)?;

            result?;
            Ok(())
        },
        result = uds_client_job.fuse() => {
            result?;
            Ok(())
        }
        _ = stop_signal.next().fuse() => {
            signal::kill(Pid::from_raw(uds_client_id as i32), Signal::SIGINT)?;

            Ok(())
        }
    }
}

mod tokio_runtime {
    use std::future::Future;
    use std::pin::Pin;
    use std::thread;

    use futures_util::future::{pending, poll_fn};
    use tokio::runtime::{Handle, Runtime};

    use lazy_static::lazy_static;

    lazy_static! {
        static ref HANDLE: Handle = {
            let mut rt = Runtime::new().unwrap();
            let handle = rt.handle().clone();
            thread::spawn(move || rt.block_on(pending::<()>()));
            handle
        };
    }

    pub async fn enter_tokio<T>(mut f: Pin<Box<dyn Future<Output = T> + 'static + Send>>) -> T {
        poll_fn(|context| {
            HANDLE.enter(|| {
                // Safety: pinned on stack, and we are in an async fn
                // WARN: DO NOT use f in other places
                // let f = unsafe { Pin::new_unchecked(&mut f) };

                f.as_mut().poll(context)
            })
        })
        .await
    }
}
