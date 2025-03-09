use std::{path::Path, str::FromStr as _, sync::Arc};

use chrono::{DateTime, NaiveDateTime, Utc};
use sqlx::{Sqlite, migrate::MigrateDatabase, sqlite::SqlitePool};

use omnius_core_base::clock::Clock;
use omnius_core_migration::sqlite::{MigrationRequest, SqliteMigrator};
use omnius_core_omnikit::model::OmniHash;

use super::{PublishedCommittedFile, PublishedUncommittedFile};

#[allow(unused)]
pub struct FilePublisherRepo {
    db: Arc<SqlitePool>,
    clock: Arc<dyn Clock<Utc> + Send + Sync>,
}

#[allow(unused)]
impl FilePublisherRepo {
    pub async fn new(dir_path: &str, clock: Arc<dyn Clock<Utc> + Send + Sync>) -> anyhow::Result<Self> {
        let path = Path::new(dir_path).join("sqlite.db");
        let path = path.to_str().ok_or(anyhow::anyhow!("Invalid path"))?;
        let url = format!("sqlite:{}", path);

        if !Sqlite::database_exists(url.as_str()).await.unwrap_or(false) {
            Sqlite::create_database(url.as_str()).await?;
        }

        let db = Arc::new(SqlitePool::connect(&url).await?);
        Self::migrate(&db).await?;

        Ok(Self { db, clock })
    }

    async fn migrate(db: &SqlitePool) -> anyhow::Result<()> {
        let requests = vec![MigrationRequest {
            name: "2024-06-23_init".to_string(),
            queries: r#"
-- committed
CREATE TABLE IF NOT EXISTS committed_files (
    root_hash TEXT NOT NULL,
    file_name TEXT NOT NULL,
    block_size INTEGER NOT NULL,
    attrs TEXT,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    PRIMARY KEY (root_hash, file_path)
);
CREATE TABLE IF NOT EXISTS committed_blocks (
    root_hash TEXT NOT NULL,
    block_hash TEXT NOT NULL,
    depth INTEGER NOT NULL,
    `index` INTEGER NOT NULL,
    PRIMARY KEY (root_hash, block_hash, depth, `index`)
);
CREATE INDEX IF NOT EXISTS index_root_hash_depth_index_for_committed_blocks ON committed_blocks (root_hash, depth ASC, `index` ASC);

-- uncommitted
CREATE TABLE IF NOT EXISTS uncommitted_files (
    id TEXT NOT NULL,
    file_name TEXT NOT NULL,
    block_size INTEGER NOT NULL,
    attrs TEXT,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    PRIMARY KEY (root_hash, file_path)
);
CREATE TABLE IF NOT EXISTS uncommitted_blocks (
    file_id TEXT NOT NULL,
    block_hash TEXT NOT NULL,
    depth INTEGER NOT NULL,
    `index` INTEGER NOT NULL,
    PRIMARY KEY (root_hash, block_hash, depth, `index`)
);
CREATE INDEX IF NOT EXISTS index_root_hash_depth_index_for_committed_blocks ON committed_blocks (root_hash, depth ASC, `index` ASC);
"#
            .to_string(),
        }];

        SqliteMigrator::migrate(db, requests).await?;

        Ok(())
    }

    pub async fn contains_committed_file(&self, root_hash: &OmniHash) -> anyhow::Result<bool> {
        let (res,): (i64,) = sqlx::query_as(
            r#"
SELECT COUNT(1)
    FROM committed_files
    WHERE root_hash = ?
    LIMIT 1
"#,
        )
        .bind(root_hash.to_string())
        .fetch_one(self.db.as_ref())
        .await?;

        Ok(res > 0)
    }

    pub async fn get_committed_files(&self) -> anyhow::Result<Vec<PublishedCommittedFile>> {
        let res: Vec<PublishedCommittedFileRow> = sqlx::query_as(
            r#"
SELECT root_hash, file_name, block_size, property, created_at, updated_at
    FROM committed_files
"#,
        )
        .fetch_all(self.db.as_ref())
        .await?;

        let res: Vec<PublishedCommittedFile> = res.into_iter().filter_map(|r| r.into().ok()).collect();
        Ok(res)
    }

    pub async fn put_committed_file(&self, item: &PublishedCommittedFile) -> anyhow::Result<()> {
        let row = PublishedCommittedFileRow::from(item)?;
        sqlx::query(
            r#"
INSERT INTO committed_files (root_hash, file_name, block_size, attrs, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?)
"#,
        )
        .bind(row.root_hash)
        .bind(row.file_name)
        .bind(row.block_size)
        .bind(row.attrs)
        .bind(row.created_at)
        .bind(row.updated_at)
        .execute(self.db.as_ref())
        .await?;

        Ok(())
    }

    pub async fn contains_committed_block(&self, root_hash: &OmniHash, block_hash: &OmniHash) -> anyhow::Result<bool> {
        let (res,): (i64,) = sqlx::query_as(
            r#"
SELECT COUNT(1)
    FROM committed_blocks
    WHERE root_hash = ? AND block_hash = ?
    LIMIT 1
"#,
        )
        .bind(root_hash.to_string())
        .bind(block_hash.to_string())
        .fetch_one(self.db.as_ref())
        .await?;

        Ok(res > 0)
    }

    pub async fn put_committed_block(&self, root_hash: &OmniHash, block_hash: &OmniHash, depth: i32, index: i32) -> anyhow::Result<()> {
        sqlx::query(
            r#"
INSERT INTO committed_blocks (root_hash, block_hash, depth, `index`)
    VALUES (?, ?, ?, ?)
"#,
        )
        .bind(root_hash.to_string())
        .bind(block_hash.to_string())
        .bind(depth)
        .bind(index)
        .execute(self.db.as_ref())
        .await?;

        Ok(())
    }

    pub async fn contains_uncommitted_file(&self, id: &str) -> anyhow::Result<bool> {
        let (res,): (i64,) = sqlx::query_as(
            r#"
SELECT COUNT(1)
    FROM uncommitted_files
    WHERE id = ?
    LIMIT 1
"#,
        )
        .bind(id)
        .fetch_one(self.db.as_ref())
        .await?;

        Ok(res > 0)
    }

    pub async fn get_uncommitted_files(&self) -> anyhow::Result<Vec<PublishedUncommittedFile>> {
        let res: Vec<PublishedUncommittedFileRow> = sqlx::query_as(
            r#"
SELECT id, file_name, block_size, property, created_at, updated_at
    FROM uncommitted_files
"#,
        )
        .fetch_all(self.db.as_ref())
        .await?;

        let res: Vec<PublishedUncommittedFile> = res.into_iter().filter_map(|r| r.into().ok()).collect();
        Ok(res)
    }

    pub async fn put_uncommitted_file(&self, item: &PublishedUncommittedFile) -> anyhow::Result<()> {
        let row = PublishedUncommittedFileRow::from(item)?;
        sqlx::query(
            r#"
INSERT INTO uncommitted_files (id, file_name, block_size, attrs, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?)
"#,
        )
        .bind(row.id)
        .bind(row.file_name)
        .bind(row.block_size)
        .bind(row.attrs)
        .bind(row.created_at)
        .bind(row.updated_at)
        .execute(self.db.as_ref())
        .await?;

        Ok(())
    }

    pub async fn contains_uncommitted_block(&self, file_id: &str, block_hash: &OmniHash) -> anyhow::Result<bool> {
        let (res,): (i64,) = sqlx::query_as(
            r#"
SELECT COUNT(1)
    FROM uncommitted_blocks
    WHERE file_id = ? AND block_hash = ?
    LIMIT 1
"#,
        )
        .bind(file_id)
        .bind(block_hash.to_string())
        .fetch_one(self.db.as_ref())
        .await?;

        Ok(res > 0)
    }

    pub async fn put_uncommitted_block(&self, file_id: &str, block_hash: &OmniHash, depth: u32, index: u32) -> anyhow::Result<()> {
        sqlx::query(
            r#"
INSERT INTO uncommitted_blocks (file_id, block_hash, depth, `index`)
    VALUES (?, ?, ?, ?)
"#,
        )
        .bind(file_id)
        .bind(block_hash.to_string())
        .bind(depth)
        .bind(index)
        .execute(self.db.as_ref())
        .await?;

        Ok(())
    }
}

#[derive(sqlx::FromRow)]
struct PublishedCommittedFileRow {
    root_hash: String,
    file_name: String,
    block_size: i64,
    attrs: Option<String>,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
}

impl PublishedCommittedFileRow {
    pub fn into(self) -> anyhow::Result<PublishedCommittedFile> {
        Ok(PublishedCommittedFile {
            root_hash: OmniHash::from_str(self.root_hash.as_str()).unwrap(),
            file_name: self.file_name,
            block_size: self.block_size,
            attrs: self.attrs,
            created_at: DateTime::from_naive_utc_and_offset(self.created_at, Utc),
            updated_at: DateTime::from_naive_utc_and_offset(self.updated_at, Utc),
        })
    }

    #[allow(unused)]
    pub fn from(item: &PublishedCommittedFile) -> anyhow::Result<Self> {
        Ok(Self {
            root_hash: item.root_hash.to_string(),
            file_name: item.file_name.to_string(),
            block_size: item.block_size,
            attrs: item.attrs.as_ref().map(|n| n.to_string()),
            created_at: item.created_at.naive_utc(),
            updated_at: item.updated_at.naive_utc(),
        })
    }
}

#[derive(sqlx::FromRow)]
struct PublishedUncommittedFileRow {
    id: String,
    file_name: String,
    block_size: i64,
    attrs: Option<String>,
    priority: i64,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
}

impl PublishedUncommittedFileRow {
    pub fn into(self) -> anyhow::Result<PublishedUncommittedFile> {
        Ok(PublishedUncommittedFile {
            id: self.id,
            file_name: self.file_name,
            block_size: self.block_size,
            attrs: self.attrs,
            priority: self.priority,
            created_at: DateTime::from_naive_utc_and_offset(self.created_at, Utc),
            updated_at: DateTime::from_naive_utc_and_offset(self.updated_at, Utc),
        })
    }

    #[allow(unused)]
    pub fn from(item: &PublishedUncommittedFile) -> anyhow::Result<Self> {
        Ok(Self {
            id: item.id.to_string(),
            file_name: item.file_name.to_string(),
            block_size: item.block_size,
            attrs: item.attrs.as_ref().map(|n| n.to_string()),
            priority: item.priority,
            created_at: item.created_at.naive_utc(),
            updated_at: item.updated_at.naive_utc(),
        })
    }
}

#[cfg(test)]
mod tests {
    use testresult::TestResult;

    #[tokio::test]
    pub async fn simple_test() -> TestResult {
        Ok(())
    }
}
