use crate::{model::NodeProfile, service::util::UriConverter};

pub struct NodeFetcher {
    urls: Vec<String>,
}

impl NodeFetcher {
    pub fn new(urls: &[&str]) -> Self {
        Self {
            urls: urls.iter().map(|&n| n.to_string()).collect(),
        }
    }

    pub async fn fetch(&self) -> anyhow::Result<Vec<NodeProfile>> {
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
