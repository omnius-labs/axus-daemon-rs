use async_trait::async_trait;

#[async_trait]
pub trait Terminable {
    async fn terminate(&self);
}
