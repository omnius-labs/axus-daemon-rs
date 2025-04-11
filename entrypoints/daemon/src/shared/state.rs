use super::{info::AppInfo, AppConfig};

pub struct AppState {
    pub info: AppInfo,
    pub conf: AppConfig,
}

impl AppState {
    pub async fn new(info: AppInfo, conf: AppConfig) -> anyhow::Result<Self> {
        Ok(Self { info, conf })
    }
}
