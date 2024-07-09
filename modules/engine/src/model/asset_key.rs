use serde::{Deserialize, Serialize};

use omnius_core_omnikit::OmniHash;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AssetKey {
    pub typ: String,
    pub hash: OmniHash,
}
