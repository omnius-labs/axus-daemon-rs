#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeProfile {
    addrs: Vec<String>,
}

impl fmt::Display for NodeProfile {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.addrs.join(", "))
    }
}
