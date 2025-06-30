use std::{path::Path, str::FromStr as _, sync::Arc};

use chrono::{DateTime, NaiveDateTime, Utc};
use sqlx::{QueryBuilder, Sqlite, migrate::MigrateDatabase, sqlite::SqlitePool};
use tokio::sync::Mutex;

use omnius_core_base::clock::Clock;
use omnius_core_migration::sqlite::{MigrationRequest, SqliteMigrator};
use omnius_core_omnikit::model::OmniHash;

use crate::{core::negotiator::file::model::PublishedUncommittedFileStatus, prelude::*};

use super::*;

#[allow(unused)]
pub struct FileSubscriberRepo {
    db: Arc<SqlitePool>,
    clock: Arc<dyn Clock<Utc> + Send + Sync>,
}

#[allow(unused)]
impl FileSubscriberRepo {
    pub async fn new<P: AsRef<Path>>(dir_path: P, clock: Arc<dyn Clock<Utc> + Send + Sync>) -> Result<Self> {
        let path = dir_path.as_ref().join("sqlite.db");
        let path = path
            .to_str()
            .ok_or_else(|| Error::new(ErrorKind::UnexpectedError).message("Invalid path"))?;

        let options = sqlx::sqlite::SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .busy_timeout(std::time::Duration::from_secs(5));

        let db = Arc::new(SqlitePool::connect_with(options).await?);
        Self::migrate(&db).await?;

        Ok(Self { db, clock })
    }

    async fn migrate(db: &SqlitePool) -> Result<()> {
        let requests = vec![MigrationRequest {
            name: "2025-06-08_init".to_string(),
            queries: r#"
CREATE TABLE IF NOT EXISTS files (
    root_hash TEXT NOT NULL,
    file_path TEXT NOT NULL,
    depth INTEGER NOT NULL,
    block_count_downloaded INTEGER NOT NULL,
    block_count_total INTEGER NOT NULL,
    attrs TEXT,
    priority INTEGER NOT NULL,
    status TEXT NOT NULL,
    failed_reason TEXT,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    PRIMARY KEY (root_hash, file_name)
);
CREATE TABLE IF NOT EXISTS blocks (
    root_hash TEXT NOT NULL,
    block_hash TEXT NOT NULL,
    rank INTEGER NOT NULL,
    `index` INTEGER NOT NULL,
    downloaded INTEGER NOT NULL,
    PRIMARY KEY (root_hash, block_hash, rank, `index`)
);
CREATE INDEX IF NOT EXISTS index_root_hash_rank_index_for_committed_blocks ON committed_blocks (root_hash, rank ASC, `index` ASC);
"#
            .to_string(),
        }];

        SqliteMigrator::migrate(db, requests).await?;

        Ok(())
    }

    pub async fn fetch_blocks(&self, root_hash: &OmniHash, block_hash: &OmniHash) -> Result<Vec<SubscribedBlock>> {
        let res: Vec<SubscribedBlockRow> = sqlx::query_as(
            r#"
SELECT root_hash, file_name, block_size, property, created_at, updated_at
    FROM committed_files
"#,
        )
        .fetch_all(self.db.as_ref())
        .await?;

        let res: Vec<SubscribedBlock> = res.into_iter().filter_map(|r| r.into().ok()).collect();

        Ok(res)
    }

    pub async fn upsert_blocks(&self, blocks: &[SubscribedBlock]) -> Result<()> {
        let mut tx = self.db.begin().await?;

        const CHUNK_SIZE: i64 = 100;

        for chunk in blocks.chunks(CHUNK_SIZE as usize) {
            let mut query_builder: QueryBuilder<sqlx::Sqlite> = QueryBuilder::new(
                r#"
INSERT OR IGNORE INTO committed_blocks (root_hash, block_hash, rank, `index`)
"#,
            );

            let rows: Vec<PublishedCommittedBlockRow> = chunk.iter().filter_map(|item| PublishedCommittedBlockRow::from(item).ok()).collect();

            query_builder.push_values(rows, |mut b, row| {
                b.push_bind(row.root_hash);
                b.push_bind(row.block_hash);
                b.push_bind(row.rank);
                b.push_bind(row.index);
            });
            query_builder.build().execute(&mut *tx).await?;
        }

        tx.commit().await?;

        Ok(())
    }
}

#[derive(sqlx::FromRow)]
struct SubscribedFileRow {
    root_hash: String,
    file_path: String,
    depth: i64,
    block_count_downloaded: i64,
    block_count_total: i64,
    attrs: Option<String>,
    priority: i64,
    status: SubscribedFileStatus,
    failed_reason: Option<String>,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
}

impl SubscribedFileRow {
    pub fn into(self) -> Result<SubscribedFile> {
        Ok(SubscribedFile {
            root_hash: OmniHash::from_str(self.root_hash.as_str()).unwrap(),
            file_path: self.file_path,
            depth: self.depth,
            block_count_downloaded: self.block_count_downloaded,
            block_count_total: self.block_count_total,
            attrs: self.attrs,
            priority: self.priority,
            status: self.status,
            failed_reason: self.failed_reason,
            created_at: DateTime::from_naive_utc_and_offset(self.created_at, Utc),
            updated_at: DateTime::from_naive_utc_and_offset(self.updated_at, Utc),
        })
    }

    #[allow(unused)]
    pub fn from(item: &SubscribedFile) -> Result<Self> {
        Ok(Self {
            root_hash: item.root_hash.to_string(),
            file_path: item.file_path.clone(),
            depth: item.depth,
            block_count_downloaded: item.block_count_downloaded,
            block_count_total: item.block_count_downloaded,
            attrs: item.attrs.as_ref().map(|n| n.to_string()),
            priority: item.priority,
            status: item.status.clone(),
            failed_reason: item.failed_reason.clone(),
            created_at: item.created_at.naive_utc(),
            updated_at: item.updated_at.naive_utc(),
        })
    }
}

#[derive(sqlx::FromRow)]
pub struct SubscribedBlockRow {
    pub root_hash: String,
    pub block_hash: String,
    pub rank: u32,
    pub index: u32,
    pub downloaded: bool,
}

impl SubscribedBlockRow {
    pub fn into(self) -> Result<SubscribedBlock> {
        Ok(SubscribedBlock {
            root_hash: OmniHash::from_str(self.root_hash.as_str()).unwrap(),
            block_hash: OmniHash::from_str(self.block_hash.as_str()).unwrap(),
            rank: self.rank,
            index: self.index,
            downloaded: self.downloaded,
        })
    }

    #[allow(unused)]
    pub fn from(item: &SubscribedBlock) -> Result<Self> {
        Ok(Self {
            root_hash: item.root_hash.to_string(),
            block_hash: item.block_hash.to_string(),
            rank: item.rank,
            index: item.index,
            downloaded: item.downloaded,
        })
    }
}
