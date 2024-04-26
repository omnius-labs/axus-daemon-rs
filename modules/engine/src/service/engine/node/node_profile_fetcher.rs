use async_trait::async_trait;

use crate::{model::NodeProfile, service::util::UriConverter};

#[async_trait]
pub trait NodeProfileFetcher {
    async fn fetch(&self) -> anyhow::Result<Vec<NodeProfile>>;
}

pub struct NodeProfileFetcherImpl {
    urls: Vec<String>,
}

impl NodeProfileFetcherImpl {
    pub fn new(urls: &[&str]) -> Self {
        Self {
            urls: urls.iter().map(|&n| n.to_string()).collect(),
        }
    }
}

#[async_trait]
impl NodeProfileFetcher for NodeProfileFetcherImpl {
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

pub struct NodeProfileFetcherMock {
    pub node_profiles: Vec<NodeProfile>,
}

#[async_trait]
impl NodeProfileFetcher for NodeProfileFetcherMock {
    async fn fetch(&self) -> anyhow::Result<Vec<NodeProfile>> {
        Ok(self.node_profiles.clone())
    }
}
