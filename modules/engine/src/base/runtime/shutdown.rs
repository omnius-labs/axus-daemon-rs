use async_trait::async_trait;

#[async_trait]
pub trait Shutdown {
    async fn shutdown(&self);
}
