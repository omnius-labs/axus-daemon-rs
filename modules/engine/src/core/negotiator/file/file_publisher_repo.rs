use std::{path::Path, str::FromStr as _, sync::Arc};

use chrono::{DateTime, NaiveDateTime, Utc};
use sqlx::{migrate::MigrateDatabase, sqlite::SqlitePool, Sqlite};

use omnius_core_base::clock::Clock;
use omnius_core_omnikit::model::OmniHash;

use crate::core::util::{MigrationRequest, SqliteMigrator};

use super::PublishedFile;

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
        let res = Self { db, clock };

        res.migrate().await?;

        Ok(res)
    }

    async fn migrate(&self) -> anyhow::Result<()> {
        let migrator = SqliteMigrator::new(self.db.clone());

        let requests = vec![MigrationRequest {
            name: "2024-06-23_init".to_string(),
            queries: r#"
CREATE TABLE IF NOT EXISTS files (
    root_hash TEXT NOT NULL,
    file_name TEXT NOT NULL,
    block_size INTEGER NOT NULL,
    property TEXT,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    PRIMARY KEY (root_hash, file_path)
);
CREATE TABLE IF NOT EXISTS blocks (
    root_hash TEXT NOT NULL,
    block_hash TEXT NOT NULL,
    depth INTEGER NOT NULL,
    `index` INTEGER NOT NULL,
    UNIQUE(root_hash, block_hash, depth, `index`)
);
CREATE INDEX IF NOT EXISTS index_root_hash_depth_index_for_blocks ON blocks (root_hash, depth ASC, `index` ASC);
"#
            .to_string(),
        }];

        migrator.migrate(requests).await?;

        Ok(())
    }

    pub async fn file_exists(&self, root_hash: OmniHash) -> anyhow::Result<bool> {
        let (res,): (i64,) = sqlx::query_as(
            r#"
SELECT COUNT(1)
    FROM files
    WHERE root_hash = ?
    LIMIT 1
"#,
        )
        .bind(root_hash.to_string())
        .fetch_one(self.db.as_ref())
        .await?;

        Ok(res > 0)
    }

    pub async fn get_published_files(&self) -> anyhow::Result<Vec<PublishedFile>> {
        let res: Vec<PublishedFileRow> = sqlx::query_as(
            r#"
SELECT root_hash, file_name, block_size, property, created_at, updated_at
    FROM files
"#,
        )
        .fetch_all(self.db.as_ref())
        .await?;

        let res: Vec<PublishedFile> = res.into_iter().filter_map(|r| r.into().ok()).collect();
        Ok(res)
    }

    pub async fn block_exists(&self, root_hash: OmniHash, block_hash: OmniHash) -> anyhow::Result<bool> {
        let (res,): (i64,) = sqlx::query_as(
            r#"
SELECT COUNT(1)
    FROM blocks
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
}

#[derive(sqlx::FromRow)]
struct PublishedFileRow {
    root_hash: String,
    file_name: String,
    block_size: i64,
    property: Option<String>,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
}

impl PublishedFileRow {
    pub fn into(self) -> anyhow::Result<PublishedFile> {
        Ok(PublishedFile {
            root_hash: OmniHash::from_str(self.root_hash.as_str()).unwrap(),
            file_name: self.file_name,
            block_size: self.block_size,
            property: self.property,
            created_at: DateTime::from_naive_utc_and_offset(self.created_at, Utc),
            updated_at: DateTime::from_naive_utc_and_offset(self.updated_at, Utc),
        })
    }

    #[allow(unused)]
    pub fn from(item: PublishedFile) -> anyhow::Result<Self> {
        Ok(Self {
            root_hash: item.root_hash.to_string(),
            file_name: item.file_name,
            block_size: item.block_size,
            property: item.property,
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
