use std::path::PathBuf;

use clap::Parser as _;
use shared::{AppConfig, AppInfo, AppState};
use tracing::info;
use tracing_subscriber::EnvFilter;

mod error;
mod interface;
mod prelude;
mod shared;

mod result {
    #[allow(unused)]
    pub type Result<T> = std::result::Result<T, crate::error::Error>;
}

pub use error::*;
pub use result::*;

const APP_NAME: &str = "axus-daemon";

#[derive(clap::Parser)]
struct Opts {
    #[clap(short = 'c', long = "config", default_value = "config.toml")]
    config_path: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    if cfg!(debug_assertions) {
        let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,sqlx=off"));
        tracing_subscriber::fmt().with_env_filter(filter).with_target(false).init();
    } else {
        let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,sqlx=off"));
        tracing_subscriber::fmt().with_env_filter(filter).with_target(false).json().init();
    }

    info!("----- start -----");

    let info = AppInfo::new(APP_NAME)?;
    info!("info: {}", info);

    let opts = Opts::parse();
    if !opts.config_path.is_file() {
        anyhow::bail!("config file not found: {}", opts.config_path.to_string_lossy());
    }

    let conf = AppConfig::load(opts.config_path).await?;

    let state = AppState::new(info, conf).await?;
    interface::RpcServer::serve(state).await?;
    Ok(())
}
