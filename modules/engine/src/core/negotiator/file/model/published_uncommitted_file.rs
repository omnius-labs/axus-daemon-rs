use chrono::{DateTime, Utc};

use omnius_core_omnikit::model::OmniHash;

pub struct PublishedUncommittedFile {
    pub id: String,
    pub file_name: String,
    pub block_size: u32,
    pub attrs: Option<String>,
    pub priority: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone)]
pub struct PublishedUncommittedBlock {
    pub file_id: String,
    pub block_hash: OmniHash,
    pub rank: u32,
    pub index: u32,
}
