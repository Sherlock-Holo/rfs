use std::path::PathBuf;

use anyhow::Result;
use async_signals::Signals;
use async_std::fs;
use futures_util::future::FutureExt;
use futures_util::{select, StreamExt};
use log::{debug, info};
use nix::libc;
use serde::Deserialize;
use structopt::StructOpt;

use rfs::log_init;
use rfs::Server;
pub use tokio_runtime::enter_tokio;

/*const UDS_DIR: &str = "/run/rfs";
const UDS_CLIENT_ENV: &str = "RFS_UDS_CLIENT";*/

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
#[structopt(name = "rfs-server", about = "rfs server.")]
pub struct Argument {
    #[structopt(short, long, default_value = "/etc/rfs/server.yml", parse(from_os_str))]
    config: PathBuf,
}

/*#[derive(Debug, Serialize, Deserialize)]
struct UdsConfig {
    uds_path: PathBuf,
    config_path: PathBuf,
    debug: bool,
}*/

pub async fn run() -> Result<()> {
    /*if let Some(uds_client_cfg) = env::var_os(UDS_CLIENT_ENV) {
        let uds_cfg: UdsConfig = serde_json::from_slice(uds_client_cfg.as_bytes())?;

        log_init(uds_cfg.debug);

        debug!("uds config loaded");

        let cfg_data = fs::read(&uds_cfg.config_path).await?;

        let cfg: Config = serde_yaml::from_slice(&cfg_data)?;

        let mut initialize_signal = Signals::new(vec![libc::SIGHUP])?;

        if let None = initialize_signal.next().await {
            return Err(format_err!("should receive a SIGHUP signal"));
        }

        drop(initialize_signal);

        debug!("receive signal, start uds client");

        let filesystem = Filesystem::new_uds(uds_cfg.uds_path).await?;

        info!("start uds client");

        filesystem.mount(&cfg.root_path).await?;

        return Ok(());
    }*/

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

    /*if let Err(err) = fs::create_dir(UDS_DIR).await {
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

    info!("uds client starting");*/

    info!("starting rfs server");

    /*task::spawn(async move {
        task::sleep(Duration::from_secs(1)).await;

        task::spawn_blocking(move || {
            signal::kill(Pid::from_raw(uds_client_id as i32), Signal::SIGHUP)
        })
        .await
    });*/

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

    /*let mut uds_client_job =
    task::spawn_blocking(move || uds_client.wait().context("uds client quit unexpected"))
        .fuse();*/

    let mut stop_signal = Signals::new(vec![libc::SIGINT, libc::SIGTERM])?;

    select! {
        result = serve.fuse() => result,

        _ = stop_signal.next().fuse() => {
            debug!("receive stop signal");

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
        poll_fn(|context| HANDLE.enter(|| f.as_mut().poll(context))).await
    }
}
