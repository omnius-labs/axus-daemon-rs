use std::{path::Path, sync::Arc};

use chrono::Utc;
use core_base::clock::Clock;
use core_omnius::OmniHash;
use sqlx::migrate::MigrateDatabase;
use sqlx::QueryBuilder;
use sqlx::{sqlite::SqlitePool, Sqlite};

use crate::service::util::{MigrationRequest, SqliteMigrator};
use crate::{model::NodeProfile, service::util::UriConverter};

pub struct FilePublisherRepo {
    db: Arc<SqlitePool>,
    clock: Arc<dyn Clock<Utc> + Send + Sync>,
}

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
    created_time INTEGER NOT NULL,
    updated_time INTEGER NOT NULL,
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

    pub async fn get_node_profiles(&self) -> anyhow::Result<Vec<NodeProfile>> {
        let res: Vec<(String,)> = sqlx::query_as(
            r#"
SELECT value FROM node_profiles
ORDER BY weight DESC, updated_time DESC
"#,
        )
        .fetch_all(self.db.as_ref())
        .await?;

        let res: Vec<NodeProfile> = res
            .into_iter()
            .filter_map(|(v,)| UriConverter::decode_node_profile(v.as_str()).ok())
            .collect();
        Ok(res)
    }

    pub async fn insert_bulk_node_profile(&self, vs: &[&NodeProfile], weight: i64) -> anyhow::Result<()> {
        let mut query_builder: QueryBuilder<sqlx::Sqlite> = QueryBuilder::new(
            r#"
INSERT OR IGNORE INTO node_profiles (value, weight, created_time, updated_time)
"#,
        );

        let now = self.clock.now().timestamp();
        let vs: Vec<String> = vs.iter().filter_map(|v| UriConverter::encode_node_profile(v).ok()).collect();

        query_builder.push_values(vs, |mut b, v| {
            b.push_bind(v);
            b.push_bind(weight);
            b.push_bind(now);
            b.push_bind(now);
        });
        query_builder.build().execute(self.db.as_ref()).await?;

        Ok(())
    }

    pub async fn shrink(&self, limit: usize) -> anyhow::Result<()> {
        let total: i64 = sqlx::query_scalar(
            r#"
SELECT COUNT(*) FROM node_profiles
"#,
        )
        .fetch_one(self.db.as_ref())
        .await?;

        let count_to_delete = total - limit as i64;

        if count_to_delete > 0 {
            sqlx::query(
                r#"
DELETE FROM node_profiles
WHERE rowid IN (
    SELECT rowid FROM node_profiles
    ORDER BY updated_time ASC, rowid ASC
    LIMIT ?
)
"#,
            )
            .bind(count_to_delete)
            .execute(self.db.as_ref())
            .await?;
        }

        Ok(())
    }
}

pub struct PublishedFile {
    pub root_hash: OmniHash,
    pub file_name: String,
    pub block_size: i64,
    pub property: Option<String>,
    pub created_time: i64,
    pub updated_time: i64,
}

#[cfg(test)]
mod tests {

    #[tokio::test]
    pub async fn simple_test() -> TestResult {
        Ok(())
    }
}
