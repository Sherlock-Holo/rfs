use std::path::PathBuf;
use std::str::FromStr;

use anyhow::{Context, Result};
use async_std::fs;
use log::info;
use serde::Deserialize;
use structopt::StructOpt;
use tonic::transport::{Certificate, ClientTlsConfig, Identity, Uri};

use rfs::{log_init, Filesystem};
pub use tokio_runtime::enter_tokio;

#[derive(Debug, Deserialize)]
pub struct Config {
    mount_path: PathBuf,
    server_addr: String,
    cert_path: PathBuf,
    key_path: PathBuf,
    debug_ca_path: Option<PathBuf>,
    debug: Option<bool>,
}

#[derive(Debug, StructOpt)]
#[structopt(name = "rfs-client", about = "rfs client.")]
pub struct Argument {
    #[structopt(short, long, default_value = "/etc/rfs/client.yml", parse(from_os_str))]
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

    log_init(debug);

    let key = fs::read(&cfg.key_path).await.context("read key failed")?;

    let cert = fs::read(&cfg.cert_path).await.context("read cert failed")?;

    let client_identity = Identity::from_pem(cert, key);

    let uri = Uri::from_str(&cfg.server_addr)?;

    info!("server uri is {}", uri);

    let mut tls_config = ClientTlsConfig::new().identity(client_identity);

    if let Some(ca_path) = &cfg.debug_ca_path {
        let ca_data = fs::read(ca_path).await?;

        let ca = Certificate::from_pem(ca_data);

        tls_config = tls_config.ca_certificate(ca);
    }

    let filesystem = Filesystem::new(uri, tls_config).await?;

    // let filesystem = Filesystem::new(uri, ClientTlsConfig::new().rustls_client_config(client_cfg)).await?;

    filesystem.mount(&cfg.mount_path)?;

    Ok(())
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

    pub async fn enter_tokio<T>(mut f: impl Future<Output = T>) -> T {
        poll_fn(|context| {
            HANDLE.enter(|| {
                // Safety: pinned on stack, and we are in an async fn
                // WARN: DO NOT use f in other places
                let f = unsafe { Pin::new_unchecked(&mut f) };
                f.poll(context)
            })
        })
        .await
    }
}
