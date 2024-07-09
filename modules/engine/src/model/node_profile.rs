use std::fmt;

use serde::{Deserialize, Serialize};

use omnius_core_omnikit::OmniAddr;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, sqlx::FromRow)]
pub struct NodeProfile {
    pub id: Vec<u8>,
    pub addrs: Vec<OmniAddr>,
}

impl fmt::Display for NodeProfile {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let addrs: Vec<String> = self.addrs.iter().map(|n| n.to_string()).collect();
        write!(f, "id: {}, addrs: [{}]", hex::encode(&self.id), addrs.join(", "))
    }
}
