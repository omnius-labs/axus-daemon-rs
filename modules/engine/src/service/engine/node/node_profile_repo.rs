use std::{path::Path, sync::Arc};

use chrono::Utc;
use core_base::clock::SystemClock;
use sqlx::migrate::MigrateDatabase;
use sqlx::QueryBuilder;
use sqlx::{sqlite::SqlitePool, Sqlite};

use crate::service::util::{MigrationRequest, SqliteMigrator};
use crate::{model::NodeProfile, service::util::UriConverter};

pub struct NodeRefRepo {
    db: Arc<SqlitePool>,
    system_clock: Arc<dyn SystemClock<Utc> + Send + Sync>,
}

impl NodeRefRepo {
    pub async fn new(dir_path: &str, system_clock: Arc<dyn SystemClock<Utc> + Send + Sync>) -> anyhow::Result<Self> {
        let path = Path::new(dir_path).join("sqlite.db");
        let path = path.to_str().ok_or(anyhow::anyhow!("Invalid path"))?;
        let url = format!("sqlite:{}", path);

        if !Sqlite::database_exists(url.as_str()).await.unwrap_or(false) {
            Sqlite::create_database(url.as_str()).await?;
        }

        let db = Arc::new(SqlitePool::connect(&url).await?);
        let res = Self { db, system_clock };

        res.migrate().await?;

        Ok(res)
    }

    async fn migrate(&self) -> anyhow::Result<()> {
        let migrator = SqliteMigrator::new(self.db.clone());

        let requests = vec![MigrationRequest {
            name: "2024-03-19_init".to_string(),
            queries: r#"
CREATE TABLE IF NOT EXISTS node_profiles (
    value TEXT NOT NULL PRIMARY KEY,
    weight INTEGER NOT NULL,
    created_time INTEGER NOT NULL,
    updated_time INTEGER NOT NULL
);
"#
            .to_string(),
        }];

        migrator.migrate(requests).await?;

        Ok(())
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

    pub async fn insert_bulk_node_profile(&self, vs: &[NodeProfile], weight: i64) -> anyhow::Result<()> {
        let mut query_builder: QueryBuilder<sqlx::Sqlite> = QueryBuilder::new(
            r#"
INSERT OR IGNORE INTO node_profiles (value, weight, created_time, updated_time)
"#,
        );

        let now = self.system_clock.now().timestamp();
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
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::{DateTime, Utc};
    use core_base::clock::SystemClockUtcMock;
    use testresult::TestResult;

    use crate::model::{NodeProfile, OmniAddress};

    use super::NodeRefRepo;

    #[tokio::test]
    pub async fn simple_test() -> TestResult {
        let dir = tempfile::tempdir()?;
        let path = dir.path().as_os_str().to_str().unwrap();

        let vs: Vec<DateTime<Utc>> = vec![
            DateTime::parse_from_rfc3339("2000-01-01T00:00:00Z").unwrap().into(),
            DateTime::parse_from_rfc3339("2000-01-01T00:00:00Z").unwrap().into(),
        ];
        let clock = Arc::new(SystemClockUtcMock::new(vs));
        let repo = NodeRefRepo::new(path, clock).await?;

        let vs: Vec<NodeProfile> = vec![NodeProfile {
            id: vec![0],
            addrs: vec![OmniAddress::new("test")],
        }];
        repo.insert_bulk_node_profile(&vs, 1).await?;

        let res = repo.get_node_profiles().await?;
        assert_eq!(res, vs);

        Ok(())
    }
}
