use std::convert::TryInto;
use std::env::args;
use std::ffi::OsString;
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;

use anyhow::{Context, Result};
use async_std::fs;
use log::info;
use nix::unistd;
use nix::unistd::ForkResult;
use serde::Deserialize;
use smol::Task;
use structopt::clap::AppSettings::*;
use structopt::StructOpt;
use tonic::transport::{Certificate, ClientTlsConfig, Identity, Uri};

use rfs::{init_smol_runtime, log_init, Filesystem};

#[derive(Debug, Deserialize)]
struct Config {
    mount_path: PathBuf,
    server_addr: String,
    cert_path: PathBuf,
    key_path: PathBuf,
    debug_ca_path: Option<PathBuf>,
    debug: Option<bool>,
    compress: Option<bool>,
}

#[derive(Debug, StructOpt)]
#[structopt(about = "mount a rfs filesystem", settings(& [ColorAuto, ColoredHelp]))]
struct Argument {
    #[structopt(short, long, default_value = "/etc/rfs/client.yml", parse(from_os_str))]
    config: PathBuf,
}

#[derive(Debug, Eq, PartialEq)]
enum RunMode {
    Foreground,
    Background,
}

#[derive(Debug, StructOpt)]
#[structopt(about = "mount a rfs filesystem", settings(& [ColorAuto, ColoredHelp]))]
struct MountArgument {
    #[structopt(help = "server addr, such as https://example.com")]
    server_addr: String,

    #[structopt(help = "mount point")]
    mount_point: String,

    #[structopt(
        short,
        long,
        help = "cert=<path> and key=<path> are required, debug, compress, debug_ca=<path> and background is optional"
    )]
    options: String,
}

impl TryInto<(Config, RunMode)> for MountArgument {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<(Config, RunMode), Self::Error> {
        let server_addr = self.server_addr;
        let mount_path = PathBuf::from(self.mount_point);

        let mut cert = None;
        let mut key = None;
        let mut debug = None;
        let mut compress = None;
        let mut debug_ca = None;
        let mut mode = RunMode::Background;

        for opt in self.options.split(',') {
            if opt.starts_with("cert") {
                cert = Some(opt.replace("cert=", ""));
            } else if opt.starts_with("key") {
                key = Some(opt.replace("key=", ""));
            } else if opt == "debug" {
                debug = Some(true)
            } else if opt == "compress" {
                compress = Some(true);
            } else if opt == "debug_ca" {
                debug_ca = Some(PathBuf::from(opt.replace("debug_ca=", "")));
            } else if opt == "foreground" {
                mode = RunMode::Foreground;
            }
        }

        let cert = cert.ok_or_else(|| anyhow::anyhow!("cert is miss"))?.into();
        let key = key.ok_or_else(|| anyhow::anyhow!("key is miss"))?.into();

        Ok((
            Config {
                server_addr,
                mount_path,
                cert_path: cert,
                key_path: key,
                debug,
                compress,
                debug_ca_path: debug_ca,
            },
            mode,
        ))
    }
}

pub fn run() -> Result<()> {
    let program_name = args().next().map_or(String::from(""), |name| name);

    let program_name = Path::new(&program_name)
        .file_name()
        .map_or(OsString::new(), |filename| filename.to_os_string());

    let cfg = if program_name == "mount.rfs" {
        let args = MountArgument::from_args();

        let (cfg, mode): (Config, RunMode) = args.try_into()?;

        if mode == RunMode::Background {
            if let ForkResult::Child = unistd::fork()? {
                cfg
            } else {
                return Ok(());
            }
        } else {
            cfg
        }
    } else {
        let args = Argument::from_args();

        let cfg_data = std::fs::read(&args.config)?;

        serde_yaml::from_slice(&cfg_data)?
    };

    init_smol_runtime();

    smol::block_on(Task::spawn(inner_run(cfg)))
}

async fn inner_run(cfg: Config) -> Result<()> {
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

    let compress = if let Some(compress) = cfg.compress {
        compress
    } else {
        false
    };

    let filesystem = Filesystem::new(uri, tls_config, compress).await?;

    filesystem.mount(&cfg.mount_path).await
}
