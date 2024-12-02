use std::{collections::HashSet, sync::Arc};

use chrono::NaiveDateTime;
use sqlx::SqlitePool;

pub struct SqliteMigrator {
    db: Arc<SqlitePool>,
}

impl SqliteMigrator {
    pub fn new(db: Arc<SqlitePool>) -> Self {
        Self { db }
    }

    pub async fn migrate(&self, requests: Vec<MigrationRequest>) -> anyhow::Result<()> {
        self.init().await?;

        let histories = self.fetch_migration_histories().await?;
        let ignore_set: HashSet<String> = histories.iter().map(|n| n.name.clone()).collect();

        let requests: Vec<MigrationRequest> = requests
            .into_iter()
            .filter(|x| !ignore_set.contains(x.name.as_str()))
            .collect();

        if requests.is_empty() {
            return Ok(());
        }

        self.execute_migration_queries(requests).await?;

        Ok(())
    }

    async fn init(&self) -> anyhow::Result<()> {
        sqlx::query(
            r#"
CREATE TABLE IF NOT EXISTS _migrations (
    name TEXT NOT NULL,
    queries TEXT NOT NULL,
    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    PRIMARY KEY (name)
);
"#,
        )
        .execute(self.db.as_ref())
        .await?;

        Ok(())
    }

    async fn fetch_migration_histories(&self) -> anyhow::Result<Vec<MigrationHistory>> {
        let res: Vec<MigrationHistory> = sqlx::query_as(
            r#"
SELECT name, executed_at FROM _migrations
"#,
        )
        .fetch_all(self.db.as_ref())
        .await?;

        Ok(res)
    }

    async fn execute_migration_queries(
        &self,
        requests: Vec<MigrationRequest>,
    ) -> anyhow::Result<()> {
        for r in requests {
            for query in r.queries.split(';') {
                if query.trim().is_empty() {
                    continue;
                }
                sqlx::query(query).execute(self.db.as_ref()).await?;
            }

            self.insert_migration_history(r.name.as_str(), r.queries.as_str())
                .await?;
        }

        Ok(())
    }

    async fn insert_migration_history(&self, name: &str, queries: &str) -> anyhow::Result<()> {
        sqlx::query(
            r#"
INSERT INTO _migrations (name, queries) VALUES ($1, $2)
"#,
        )
        .bind(name)
        .bind(queries)
        .execute(self.db.as_ref())
        .await?;

        Ok(())
    }
}

#[derive(Clone)]
pub struct MigrationRequest {
    pub name: String,
    pub queries: String,
}

#[derive(sqlx::FromRow)]
struct MigrationHistory {
    pub name: String,
    #[allow(unused)]
    pub executed_at: NaiveDateTime,
}

#[cfg(test)]
mod tests {
    use std::{path::Path, sync::Arc};

    use sqlx::{migrate::MigrateDatabase, Sqlite, SqlitePool};

    use super::SqliteMigrator;

    #[tokio::test]
    pub async fn success_test() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().as_os_str().to_str().unwrap();

        let path = Path::new(dir_path).join("sqlite.db");
        let path = path.to_str().unwrap();
        let url = format!("sqlite:{}", path);

        if !Sqlite::database_exists(url.as_str()).await.unwrap_or(false) {
            Sqlite::create_database(url.as_str()).await.unwrap();
        }

        let db = Arc::new(SqlitePool::connect(&url).await.unwrap());
        let migrator = SqliteMigrator::new(db);

        let requests = vec![super::MigrationRequest {
            name: "test".to_string(),
            queries: r#"
CREATE TABLE test (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);
"#
            .to_string(),
        }];

        // Migrate
        migrator.migrate(requests.clone()).await.unwrap();

        // Migrate again
        migrator.migrate(requests).await.unwrap();
    }

    #[tokio::test]
    pub async fn error_test() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().as_os_str().to_str().unwrap();

        let path = Path::new(dir_path).join("sqlite.db");
        let path = path.to_str().unwrap();
        let url = format!("sqlite:{}", path);

        if !Sqlite::database_exists(url.as_str()).await.unwrap_or(false) {
            Sqlite::create_database(url.as_str()).await.unwrap();
        }

        let db = Arc::new(SqlitePool::connect(&url).await.unwrap());
        let migrator = SqliteMigrator::new(db);

        let requests = vec![super::MigrationRequest {
            name: "test".to_string(),
            queries: r#"
CREATE TABLE test (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,,,,
);
"#
            .to_string(),
        }];

        assert!(migrator.migrate(requests).await.is_err());
    }
}
