use std::fmt;

use serde::{Deserialize, Serialize};

use super::OmniAddress;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeProfile {
    pub addrs: Vec<OmniAddress>,
}

impl fmt::Display for NodeProfile {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let addrs: Vec<String> = self.addrs.iter().map(|n| n.to_string()).collect();
        write!(f, "{}", addrs.join(", "))
    }
}
