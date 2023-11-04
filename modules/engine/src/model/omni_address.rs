use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct OmniAddress {
    value: String,
}
