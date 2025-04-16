use super::{AppConfig, info::AppInfo};

use crate::prelude::*;

pub struct AppState {
    pub info: AppInfo,
    pub conf: AppConfig,
}

impl AppState {
    pub async fn new(info: AppInfo, conf: AppConfig) -> Result<Self> {
        Ok(Self { info, conf })
    }
}
