use crate::model::NodeProfile;
use reqwest;

pub struct InitialNodeFetcher {
    urls: Vec<String>,
}

impl InitialNodeFetcher {
    pub fn new(urls: [&str]) {
        Self { urls: urls.to_vec() }
    }

    pub async fn fetch(&self) -> Vec<NodeProfile> {
        let client = reqwest::Client::new();

        for u in self.urls {
            let res = client.get(u).send().await?;
            let res = res.text().await?;

            for line in res.split_whitespace() {}
        }
    }
}
