use async_trait::async_trait;

use crate::{model::NodeRef, service::util::UriConverter};

#[async_trait]
pub trait NodeRefFetcher {
    async fn fetch(&self) -> anyhow::Result<Vec<NodeRef>>;
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
impl NodeRefFetcher for NodeFetcherImpl {
    async fn fetch(&self) -> anyhow::Result<Vec<NodeRef>> {
        let mut vs: Vec<NodeRef> = vec![];
        let client = reqwest::Client::new();

        for u in self.urls.iter() {
            let res = client.get(u).send().await?;
            let res = res.text().await?;

            for line in res.split_whitespace() {
                if let Ok(node_ref) = UriConverter::decode_node_ref(line) {
                    vs.push(node_ref);
                }
            }
        }

        Ok(vs)
    }
}
