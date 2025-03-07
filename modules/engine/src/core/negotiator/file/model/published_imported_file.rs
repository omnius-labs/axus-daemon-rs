use chrono::{DateTime, Utc};

use omnius_core_omnikit::model::OmniHash;

pub struct PublishedImportedFile {
    pub root_hash: OmniHash,
    pub file_name: String,
    pub block_size: i64,
    pub attrs: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
