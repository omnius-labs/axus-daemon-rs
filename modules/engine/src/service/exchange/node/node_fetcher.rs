use async_trait::async_trait;

use crate::{model::NodeProfile, service::util::UriConverter};

#[async_trait]
pub trait NodeFetcher {
    async fn fetch(&self) -> anyhow::Result<Vec<NodeProfile>>;
}

pub struct NodeFetcherImpl {
    urls: Vec<String>,
}

impl NodeFetcherImpl {
    pub fn new(urls: &[&str]) -> Self {
        Self {
            urls: urls.iter().map(|&n| n.to_string()).collect(),
        }
    }
}

#[async_trait]
impl NodeFetcher for NodeFetcherImpl {
    async fn fetch(&self) -> anyhow::Result<Vec<NodeProfile>> {
        let mut vs: Vec<NodeProfile> = vec![];
        let client = reqwest::Client::new();

        for u in self.urls.iter() {
            let res = client.get(u).send().await?;
            let res = res.text().await?;

            for line in res.split_whitespace() {
                if let Ok(node_profile) = UriConverter::decode_node_profile(line) {
                    vs.push(node_profile);
                }
            }
        }

        Ok(vs)
    }
}
