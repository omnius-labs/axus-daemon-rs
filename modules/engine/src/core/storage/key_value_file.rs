use std::{
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
};

use futures::Stream;
use sqlx::{QueryBuilder, Sqlite, SqlitePool, migrate::MigrateDatabase as _};
use tokio::{fs::create_dir_all, sync::Mutex};

use omnius_core_migration::sqlite::{MigrationRequest, SqliteMigrator};

pub struct KeyValueFileStorage {
    dir_path: PathBuf,
    db: Arc<SqlitePool>,
    lock: Mutex<()>,
}

impl KeyValueFileStorage {
    pub async fn new<P: AsRef<Path>>(dir_path: P) -> anyhow::Result<Self> {
        let dir_path = dir_path.as_ref().to_path_buf();
        let sqlite_path = dir_path.join("sqlite.db");
        let sqlite_url = format!("sqlite:{}", sqlite_path.to_str().ok_or_else(|| anyhow::anyhow!("Invalid path"))?);

        if !Sqlite::database_exists(sqlite_url.as_str()).await.unwrap_or(false) {
            Sqlite::create_database(sqlite_url.as_str()).await?;
        }

        let db = Arc::new(SqlitePool::connect(&sqlite_url).await?);
        Self::migrate(&db).await?;

        Ok(Self {
            dir_path,
            db,
            lock: Mutex::new(()),
        })
    }

    async fn migrate(db: &SqlitePool) -> anyhow::Result<()> {
        let requests = vec![MigrationRequest {
            name: "2025-03-05_init".to_string(),
            queries: r#"
CREATE TABLE IF NOT EXISTS keys (
    id INTEGER NOT NULL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);
"#
            .to_string(),
        }];

        SqliteMigrator::migrate(db, requests).await?;

        Ok(())
    }

    pub async fn rename_key(&self, old_key: &str, new_key: &str) -> anyhow::Result<bool> {
        let _guard = self.lock.lock().await;

        let result = sqlx::query("UPDATE keys SET name = ? WHERE name = ?")
            .bind(new_key)
            .bind(old_key)
            .execute(self.db.as_ref())
            .await?;

        Ok(result.rows_affected() > 0)
    }

    pub async fn contains_key(&self, key: &str) -> anyhow::Result<bool> {
        let _guard = self.lock.lock().await;

        let (count,): (i64,) = sqlx::query_as("SELECT COUNT(1) FROM keys WHERE name = ? LIMIT 1")
            .bind(key)
            .fetch_one(self.db.as_ref())
            .await?;

        Ok(count > 0)
    }

    pub async fn get_keys(&self) -> anyhow::Result<Pin<Box<impl Stream<Item = Result<String, anyhow::Error>>>>> {
        const CHUNK_SIZE: i64 = 500;

        let _guard = self.lock.lock().await;

        let mut offset = 0;
        let db = self.db.clone();

        Ok(Box::pin(async_stream::try_stream! {
            loop {
                let keys: Vec<String> = sqlx::query_as::<_, (String,)>("SELECT name FROM keys LIMIT ? OFFSET ?")
                    .bind(CHUNK_SIZE)
                    .bind(offset)
                    .fetch_all(db.as_ref())
                    .await?
                    .into_iter()
                    .map(|row| row.0)
                    .collect();

                if keys.is_empty() {
                    break;
                }

                for key in keys {
                    yield key;
                }

                offset += CHUNK_SIZE;
            }
        }))
    }

    pub async fn get_value(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        let _guard = self.lock.lock().await;

        let id = self.get_id(key).await?;
        if id.is_none() {
            return Ok(None);
        }
        let id = id.unwrap();

        let file_path = self.gen_file_path(id).await?;
        let bytes = tokio::fs::read(file_path).await?;

        Ok(Some(bytes))
    }

    pub async fn put_value(&self, key: &str, value: &[u8]) -> anyhow::Result<()> {
        let _guard = self.lock.lock().await;

        let id = self.put_id(key).await?;
        let file_path = self.gen_file_path(id).await?;
        tokio::fs::write(file_path, value).await?;

        Ok(())
    }

    pub async fn delete_key(&self, key: &str) -> anyhow::Result<bool> {
        let _guard = self.lock.lock().await;

        let id = self.get_id(key).await?;
        if id.is_none() {
            return Ok(false);
        }
        let id = id.unwrap();

        let result = sqlx::query("DELETE FROM keys WHERE name = ?").bind(key).execute(self.db.as_ref()).await?;

        if result.rows_affected() == 0 {
            return Ok(false);
        }

        let file_path = self.gen_file_path(id).await?;
        tokio::fs::remove_file(file_path).await?;

        Ok(true)
    }

    pub async fn shrink<T>(&self, exclude_key: T) -> anyhow::Result<()>
    where
        T: Fn(&str) -> bool,
    {
        const CHUNK_SIZE: i64 = 100;

        let _guard = self.lock.lock().await;

        let mut tx = self.db.begin().await?;
        sqlx::query(
            r#"
CREATE TEMP TABLE unused_keys (
    id INTEGER NOT NULL
)"#,
        )
        .execute(&mut *tx)
        .await?;

        let mut offset = 0;

        loop {
            let keys: Vec<Key> = sqlx::query_as("SELECT * FROM keys LIMIT ? OFFSET ?")
                .bind(CHUNK_SIZE)
                .bind(offset)
                .fetch_all(&mut *tx)
                .await?
                .into_iter()
                .collect();

            if keys.is_empty() {
                break;
            }

            let unused_ids: Vec<i64> = keys.into_iter().filter(|key| !exclude_key(&key.name)).map(|key| key.id).collect();

            let mut query_builder: QueryBuilder<sqlx::Sqlite> = QueryBuilder::new("INSERT INTO unused_keys (id)");

            query_builder.push_values(unused_ids, |mut b, id| {
                b.push_bind(id);
            });
            query_builder.build().execute(&mut *tx).await?;

            offset += CHUNK_SIZE;
        }

        let mut offset = 0;

        loop {
            let unused_ids: Vec<i64> = sqlx::query_as::<_, (i64,)>("SELECT id FROM unused_keys LIMIT ? OFFSET ?")
                .bind(CHUNK_SIZE)
                .bind(offset)
                .fetch_all(&mut *tx)
                .await?
                .into_iter()
                .map(|row| row.0)
                .collect();

            if unused_ids.is_empty() {
                break;
            }

            for id in unused_ids {
                let file_path = self.gen_file_path(id).await?;
                if let Err(e) = tokio::fs::remove_file(&file_path).await {
                    if e.kind() != std::io::ErrorKind::NotFound {
                        return Err(e.into());
                    }
                }
            }

            offset += CHUNK_SIZE;
        }

        sqlx::query("DELETE FROM keys WHERE id IN (SELECT id FROM unused_keys)")
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;

        sqlx::query("VACUUM").execute(self.db.as_ref()).await?;

        Ok(())
    }

    async fn get_id(&self, key: &str) -> anyhow::Result<Option<i64>> {
        let result: Option<(i64,)> = sqlx::query_as("SELECT id FROM keys WHERE name = ? LIMIT 1")
            .bind(key)
            .fetch_optional(self.db.as_ref())
            .await?;
        Ok(result.map(|(id,)| id))
    }

    async fn put_id(&self, key: &str) -> anyhow::Result<i64> {
        let (id,): (i64,) = sqlx::query_as("INSERT INTO keys (name) VALUES (?) RETURNING id")
            .bind(key)
            .fetch_one(self.db.as_ref())
            .await?;
        Ok(id)
    }

    async fn gen_file_path(&self, id: i64) -> anyhow::Result<PathBuf> {
        let relative_path = Self::gen_relative_file_path(id);
        let file_path = self.dir_path.join("blocks").join(relative_path);
        create_dir_all(file_path.parent().unwrap()).await?;
        Ok(file_path)
    }

    fn gen_relative_file_path(id: i64) -> String {
        let mut res = [0; 6];
        for i in 0..6 {
            let v = ((id >> (i * 11)) & 0x7FF) as usize;
            res[5 - i] = v;
        }
        res.iter().map(|v| format!("{:03x}", v)).collect::<Vec<_>>().join("/")
    }
}

#[derive(Debug, sqlx::FromRow)]
struct Key {
    pub id: i64,
    pub name: String,
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;
    use testresult::TestResult;
    use tokio_stream::StreamExt as _;
    use tokio_util::bytes::Bytes;

    use super::*;

    #[tokio::test]
    async fn test_basic_operations() -> TestResult<()> {
        let temp_dir = tempdir()?;
        let storage = KeyValueFileStorage::new(temp_dir.path()).await?;

        // テストデータ
        let key = "test_key";
        let value = b"test_value";

        // 初期状態の確認
        assert!(!storage.contains_key(key).await?);
        assert!(storage.get_value(key).await?.is_none());

        // 値の保存
        storage.put_value(key, value).await?;
        assert!(storage.contains_key(key).await?);

        // 値の取得
        let retrieved_value = storage.get_value(key).await?.unwrap();
        assert_eq!(retrieved_value, value);

        // 値の削除
        assert!(storage.delete_key(key).await?);
        assert!(!storage.contains_key(key).await?);
        assert!(storage.get_value(key).await?.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_key_rename() -> TestResult<()> {
        let temp_dir = tempdir()?;
        let storage = KeyValueFileStorage::new(temp_dir.path()).await?;

        let old_key = "old_key";
        let new_key = "new_key";
        let value = b"test_value";

        // 値を保存
        storage.put_value(old_key, value).await?;

        // キーをリネーム
        assert!(storage.rename_key(old_key, new_key).await?);

        // 古いキーが存在しないことを確認
        assert!(!storage.contains_key(old_key).await?);

        // 新しいキーで値が取得できることを確認
        let retrieved_value = storage.get_value(new_key).await?.unwrap();
        assert_eq!(retrieved_value, value);

        Ok(())
    }

    #[tokio::test]
    async fn test_streaming_keys() -> TestResult<()> {
        let temp_dir = tempdir()?;
        let storage = KeyValueFileStorage::new(temp_dir.path()).await?;

        // 複数のキーを保存
        let test_keys = vec!["key1", "key2", "key3"];
        for key in &test_keys {
            storage.put_value(key, b"value").await?;
        }

        // ストリーミングで取得したキーを収集
        let mut retrieved_keys = Vec::new();
        let mut stream = storage.get_keys().await?;
        while let Some(key) = stream.next().await {
            retrieved_keys.push(key?);
        }

        // 取得したキーをソート（順序は保証されないため）
        retrieved_keys.sort();

        let mut expected_keys = test_keys.iter().map(|&k| k.to_string()).collect::<Vec<_>>();
        expected_keys.sort();

        assert_eq!(retrieved_keys, expected_keys);

        Ok(())
    }

    #[tokio::test]
    async fn test_shrink_storage() -> TestResult<()> {
        let temp_dir = tempdir()?;
        let storage = KeyValueFileStorage::new(temp_dir.path()).await?;

        // テストデータを準備
        let keep_keys = vec!["keep1", "keep2"];
        let delete_keys = vec!["delete1", "delete2"];

        // すべてのキーに値を保存
        for key in keep_keys.iter().chain(delete_keys.iter()) {
            storage.put_value(key, b"value").await?;
        }

        // shrink実行（keep_keysのみを保持）
        storage.shrink(|key| keep_keys.contains(&key)).await?;

        // 期待される結果を確認
        for key in &keep_keys {
            assert!(storage.contains_key(key).await?);
        }
        for key in &delete_keys {
            assert!(!storage.contains_key(key).await?);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_error_cases() -> TestResult<()> {
        // 無効なパスでの初期化
        let invalid_path = PathBuf::from("\0");
        assert!(KeyValueFileStorage::new(invalid_path).await.is_err());

        let temp_dir = tempdir()?;
        let storage = KeyValueFileStorage::new(temp_dir.path()).await?;

        // 存在しないキーの削除
        assert!(!storage.delete_key("non_existent").await?);

        // 存在しないキーのリネーム
        assert!(!storage.rename_key("non_existent", "new_key").await?);

        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn test_concurrent_operations() -> TestResult<()> {
        let temp_dir = tempdir()?;
        let storage = KeyValueFileStorage::new(temp_dir.path()).await?;
        let storage = std::sync::Arc::new(storage);

        let mut handles = Vec::new();

        // 複数の同時書き込み
        for i in 0..10 {
            let storage = storage.clone();
            let handle = tokio::spawn(async move {
                let key = format!("key{}", i);
                let value = format!("value{}", i);
                storage.put_value(&key, value.as_bytes()).await
            });
            handles.push(handle);
        }

        // すべての操作が完了するのを待つ
        for handle in handles {
            handle.await??;
        }

        // すべてのキーが正しく保存されているか確認
        for i in 0..10 {
            let key = format!("key{}", i);
            let expected_value = format!("value{}", i);
            let value = storage.get_value(&key).await?.unwrap();
            assert_eq!(value, Bytes::from(expected_value));
        }

        Ok(())
    }
}
