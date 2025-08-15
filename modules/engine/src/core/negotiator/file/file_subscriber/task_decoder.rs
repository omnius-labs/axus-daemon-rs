use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use futures::FutureExt as _;
use omnius_core_omnikit::model::OmniHash;
use parking_lot::Mutex;
use tokio::{
    io::{AsyncWrite, AsyncWriteExt},
    sync::Mutex as TokioMutex,
    task::JoinHandle,
};

use omnius_core_base::{clock::Clock, sleeper::Sleeper, tsid::TsidProvider};

use crate::{
    core::{
        negotiator::file::model::SubscribedFile,
        storage::KeyValueFileStorage,
        util::{EventListener, Terminable},
    },
    prelude::*,
};

use super::*;

#[allow(unused)]
pub struct TaskDecoder {
    file_subscriber_repo: Arc<FileSubscriberRepo>,
    blocks_storage: Arc<KeyValueFileStorage>,

    tsid_provider: Arc<Mutex<dyn TsidProvider + Send + Sync>>,
    clock: Arc<dyn Clock<Utc> + Send + Sync>,
    sleeper: Arc<dyn Sleeper + Send + Sync>,

    current_decoding_file_id: Arc<Mutex<Option<String>>>,
    enqueue_event_listener: Arc<EventListener>,
    cancel_event_listener: Arc<EventListener>,

    join_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
}

#[async_trait]
impl Terminable for TaskDecoder {
    async fn terminate(&self) {
        if let Some(join_handle) = self.join_handle.lock().await.take() {
            join_handle.abort();
            let _ = join_handle.fuse().await;
        }
    }
}

#[allow(unused)]
impl TaskDecoder {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        file_subscriber_repo: Arc<FileSubscriberRepo>,
        blocks_storage: Arc<KeyValueFileStorage>,

        tsid_provider: Arc<Mutex<dyn TsidProvider + Send + Sync>>,
        clock: Arc<dyn Clock<Utc> + Send + Sync>,
        sleeper: Arc<dyn Sleeper + Send + Sync>,
    ) -> Result<Arc<Self>> {
        let v = Arc::new(Self {
            file_subscriber_repo,
            blocks_storage,

            tsid_provider,
            clock,
            sleeper,

            current_decoding_file_id: Arc::new(Mutex::new(None)),
            enqueue_event_listener: Arc::new(EventListener::new()),
            cancel_event_listener: Arc::new(EventListener::new()),

            join_handle: Arc::new(TokioMutex::new(None)),
        });
        v.clone().start().await?;

        Ok(v)
    }

    pub async fn export(&self, _file_id: &str) -> Result<()> {
        self.enqueue_event_listener.notify();

        Ok(())
    }

    pub async fn cancel(&self, file_id: &str) -> Result<()> {
        self.file_subscriber_repo
            .update_file_status(file_id, &SubscribedFileStatus::Canceled)
            .await?;

        let guard = self.current_decoding_file_id.lock();

        let Some(current_decoding_file_id) = guard.as_ref() else {
            return Ok(());
        };

        if current_decoding_file_id != file_id {
            return Ok(());
        }

        self.cancel_event_listener.notify();

        Ok(())
    }

    async fn start(self: Arc<Self>) -> Result<()> {
        let cancel_event_listener = self.cancel_event_listener.clone();

        let join_handle = self.join_handle.clone();
        *join_handle.lock().await = Some(tokio::spawn(async move {
            loop {
                tokio::select! {
                    next = self.decode() => {
                        if next {
                            continue;
                        }
                    }
                    _ = cancel_event_listener.wait() => {
                        info!("import task canceled");
                    }
                };

                self.enqueue_event_listener.wait().await;
            }
        }));

        Ok(())
    }

    async fn decode(&self) -> bool {
        let Some(file) = self.pickup().await else {
            return false;
        };

        if let Err(e) = self.decode_file(file).await {
            warn!(error = ?e, "encode file error");
        }

        *self.current_decoding_file_id.lock() = None;

        true
    }

    async fn pickup(&self) -> Option<SubscribedFile> {
        let file = match self.file_subscriber_repo.find_file_by_decoding_next().await {
            Ok(file) => file,
            Err(e) => {
                info!(error_message = e.to_string(), "file not found",);
                return None;
            }
        };
        let Some(file) = file else {
            *self.current_decoding_file_id.lock() = None;
            return None;
        };

        *self.current_decoding_file_id.lock() = Some(file.id.clone());

        // current_import_file_idがセットされる前に状態が変わる可能性があるため、再度取得する
        let file = match self.file_subscriber_repo.find_file_by_id(&file.id).await {
            Ok(file) => file,
            Err(e) => {
                warn!(error_message = e.to_string(), "file did lost",);
                return None;
            }
        };
        let Some(file) = file else {
            *self.current_decoding_file_id.lock() = None;
            return None;
        };

        Some(file)
    }

    async fn decode_file(&self, file: SubscribedFile) -> Result<()> {
        if file.status == SubscribedFileStatus::Canceled {
            self.file_subscriber_repo.delete_file(&file.id).await?;
            return Ok(());
        }

        if file.rank == 0 {
            // ファイルをエクスポートする
        } else {
            let blocks = self
                .file_subscriber_repo
                .find_blocks_by_root_hash_and_rank(&file.root_hash, file.rank)
                .await?;
        }

        Ok(())
    }

    async fn decode_bytes<W>(&self, writer: &mut W, root_hash: &OmniHash, block_hashes: &[OmniHash]) -> Result<()>
    where
        W: AsyncWrite + Unpin,
    {
        for block_hash in block_hashes {
            let key = gen_block_path(root_hash, block_hash);
            let Some(block) = self.blocks_storage.get_value(&key).await? else {
                return Err(Error::builder()
                    .kind(ErrorKind::IoError)
                    .message("decoding error: block is not found")
                    .build());
            };
            writer.write_all(&block).await?;
        }

        writer.flush().await?;

        Ok(())
    }
}
