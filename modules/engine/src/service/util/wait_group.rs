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

    pub fn worker(&self) -> WaitGroupWorker {
        WaitGroupWorker::new(self.counter.clone(), self.notify.clone())
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

pub struct WaitGroupWorker {
    counter: Arc<AtomicUsize>,
    notify: Arc<Notify>,
}

impl WaitGroupWorker {
    fn new(counter: Arc<AtomicUsize>, notify: Arc<Notify>) -> Self {
        counter.fetch_add(1, Ordering::SeqCst);
        Self { counter, notify }
    }
}

impl Drop for WaitGroupWorker {
    fn drop(&mut self) {
        if self.counter.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.notify.notify_one();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore]
    async fn test_wait_group() {
        let wg = WaitGroup::new();
        let mut handles = Vec::new();

        for _ in 0..10 {
            let w = wg.worker();
            let handle = tokio::spawn(async move {
                // ここで何かの処理を行う
                drop(w);
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
