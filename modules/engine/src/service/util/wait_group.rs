use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

pub struct WaitGroup {
    counter: Arc<AtomicUsize>,
    notify: Arc<Notify>,
}

impl WaitGroup {
    pub fn new() -> Self {
        WaitGroup {
            counter: Arc::new(AtomicUsize::new(0)),
            notify: Arc::new(Notify::new()),
        }
    }

    pub fn add(&self, count: usize) {
        self.counter.fetch_add(count, Ordering::SeqCst);
    }

    pub async fn done(&self) {
        if self.counter.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.notify.notify_one();
        }
    }

    pub async fn wait(&self) {
        while self.counter.load(Ordering::SeqCst) > 0 {
            self.notify.notified().await;
        }
    }
}

impl Default for WaitGroup {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore]
    async fn test_wait_group() {
        let wg = Arc::new(WaitGroup::new());
        let mut handles = Vec::new();

        for _ in 0..10 {
            let wg_clone = wg.clone();
            wg.add(1);
            let handle = tokio::spawn(async move {
                // ここで何かの処理を行う
                wg_clone.done().await;
            });
            handles.push(handle);
        }

        wg.wait().await;

        // 全てのタスクが終了していることを確認
        for handle in handles {
            assert!(handle.await.is_ok());
        }
    }
}
