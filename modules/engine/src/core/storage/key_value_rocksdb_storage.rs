use std::{path::Path, sync::Arc};

use tokio_util::bytes::Bytes;

use omnius_core_base::tsid::TsidProvider;

use crate::prelude::*;

pub struct KeyValueRocksdbStorage {
    db: Arc<rocksdb::TransactionDB<rocksdb::MultiThreaded>>,
    tsid_provider: Arc<Mutex<dyn TsidProvider + Send + Sync>>,
}

impl KeyValueRocksdbStorage {
    #[allow(unused)]
    pub async fn new<P: AsRef<Path>>(dir_path: P, tsid_provider: Arc<Mutex<dyn TsidProvider + Send + Sync>>) -> Result<Self> {
        tokio::fs::create_dir_all(&dir_path).await?;

        let mut db_opts = rocksdb::Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        db_opts.set_atomic_flush(true);

        let names_opts = rocksdb::Options::default();

        let mut blocks_opts = rocksdb::Options::default();
        blocks_opts.set_enable_blob_files(true);
        blocks_opts.set_enable_blob_gc(true);
        blocks_opts.set_blob_compression_type(rocksdb::DBCompressionType::None);

        let cfs = vec![
            rocksdb::ColumnFamilyDescriptor::new(rocksdb::DEFAULT_COLUMN_FAMILY_NAME, rocksdb::Options::default()),
            rocksdb::ColumnFamilyDescriptor::new("names", names_opts),
            rocksdb::ColumnFamilyDescriptor::new("blocks", blocks_opts),
        ];

        let txn_db_opts = rocksdb::TransactionDBOptions::default();

        let db = Arc::new(rocksdb::TransactionDB::open_cf_descriptors(&db_opts, &txn_db_opts, dir_path, cfs)?);

        Ok(Self { db, tsid_provider })
    }

    #[allow(unused)]
    pub async fn rename<K>(&self, old_name: K, new_name: K, overwrite: bool) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        let old_name = old_name.as_ref().to_vec();
        let new_name = new_name.as_ref().to_vec();

        let db = self.db.clone();

        tokio::task::spawn_blocking(move || {
            let txn = db.transaction();

            let cf_names = db.cf_handle("names").expect("missing CF");
            let cf_blocks = db.cf_handle("blocks").expect("missing CF");

            let (old_id, new_id) = if old_name <= new_name {
                let old_id = txn.get_for_update_cf(&cf_names, &old_name, true)?;
                let new_id = txn.get_for_update_cf(&cf_names, &new_name, true)?;
                (old_id, new_id)
            } else {
                let new_id = txn.get_for_update_cf(&cf_names, &new_name, true)?;
                let old_id = txn.get_for_update_cf(&cf_names, &old_name, true)?;
                (old_id, new_id)
            };

            if let Some(old_id) = old_id {
                if let Some(new_id) = new_id {
                    if overwrite {
                        txn.delete_cf(&cf_blocks, &new_id)?;
                        txn.put_cf(&cf_names, &new_name, &old_id)?;
                        txn.delete_cf(&cf_names, &old_name)?;
                    } else {
                        return Err(Error::builder().kind(ErrorKind::AlreadyExists).build());
                    }
                } else {
                    txn.put_cf(&cf_names, new_name, &old_id)?;
                    txn.delete_cf(&cf_names, old_name)?;
                }
            } else {
                return Err(Error::builder().kind(ErrorKind::NotFound).build());
            }

            txn.commit()?;

            Ok(())
        })
        .await?
    }

    #[allow(unused)]
    pub async fn contains<K>(&self, name: K) -> Result<bool>
    where
        K: AsRef<[u8]>,
    {
        let name = name.as_ref().to_vec();
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || -> Result<bool> {
            let cf_names = db.cf_handle("names").expect("missing CF");
            let res = db.get_cf(&cf_names, &name)?;
            Ok(res.is_some())
        })
        .await?
    }

    #[allow(unused)]
    pub fn get_names(&self) -> Result<BlobStorageKeyIterator> {
        let cf_names = self.db.cf_handle("names").expect("missing CF");
        let mut iter = self.db.raw_iterator_cf(&cf_names);
        iter.seek_to_first();
        let iter = BlobStorageKeyIterator::new(iter);
        Ok(iter)
    }

    #[allow(unused)]
    pub async fn put_value<K>(&self, name: K, value: Bytes, overwrite: bool) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        let name = name.as_ref().to_vec();
        let value = value.clone();
        let db = self.db.clone();
        let tsid_provider = self.tsid_provider.clone();

        tokio::task::spawn_blocking(move || {
            let cf_names = db.cf_handle("names").expect("missing CF");
            let cf_blocks = db.cf_handle("blocks").expect("missing CF");

            let txn = db.transaction();

            if !overwrite {
                let id = match txn.get_cf(&cf_names, &name)? {
                    Some(id) => id,
                    None => return Err(Error::builder().kind(ErrorKind::AlreadyExists).build()),
                };

                txn.put_cf(&cf_blocks, &id, &value)?;
            } else {
                let id = match txn.get_cf(&cf_names, &name)? {
                    Some(id) => id,
                    None => {
                        let mut tsid_provider = tsid_provider.lock();
                        let tsid = tsid_provider.create();
                        let id = tsid.to_string().into_bytes();
                        txn.put_cf(&cf_names, &name, &id)?;
                        id
                    }
                };

                txn.put_cf(&cf_blocks, &id, &value)?;
            }

            txn.commit()?;

            Ok(())
        })
        .await?
    }

    #[allow(unused)]
    pub async fn get_value<K>(&self, name: K) -> Result<Option<Vec<u8>>>
    where
        K: AsRef<[u8]>,
    {
        let name = name.as_ref().to_vec();
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || -> Result<Option<Vec<u8>>> {
            let cf_names = db.cf_handle("names").expect("missing CF");
            let cf_blocks = db.cf_handle("blocks").expect("missing CF");

            let id = match db.get_cf(&cf_names, &name)? {
                Some(id) => id,
                None => return Ok(None),
            };

            let value = db.get_cf(&cf_blocks, &id)?;

            Ok(value)
        })
        .await?
    }

    #[allow(unused)]
    pub async fn delete<K>(&self, name: K) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        let name = name.as_ref().to_vec();
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || -> Result<()> {
            let cf_names = db.cf_handle("names").expect("missing CF");
            let cf_blocks = db.cf_handle("blocks").expect("missing CF");

            let txn = db.transaction();

            let id = match txn.get_cf(&cf_names, &name)? {
                Some(id) => id,
                None => return Ok(()),
            };

            txn.delete_cf(&cf_blocks, &id)?;
            txn.delete_cf(&cf_names, name)?;

            txn.commit()?;

            Ok(())
        })
        .await?
    }

    #[allow(unused)]
    pub async fn delete_bulk<K>(&self, names: &[K]) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        let names_owned: Vec<Vec<u8>> = names.iter().map(|n| n.as_ref().to_vec()).collect();
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || -> Result<()> {
            let cf_names = db.cf_handle("names").expect("missing CF");
            let cf_blocks = db.cf_handle("blocks").expect("missing CF");

            let mut batch = rocksdb::WriteBatchWithTransaction::default();

            for name in &names_owned {
                let id = match db.get_cf(&cf_names, name)? {
                    Some(id) => id,
                    None => return Ok(()),
                };

                batch.delete_cf(&cf_blocks, &id);
                batch.delete_cf(&cf_names, name);
            }

            db.write(batch)?;

            Ok(())
        })
        .await?
    }

    #[allow(unused)]
    pub async fn shrink<T>(&self, exclude_key_fn: T) -> Result<()>
    where
        T: Fn(&[u8]) -> bool + Send + Sync + 'static,
    {
        let db = self.db.clone();
        #[allow(clippy::type_complexity)]
        let func: Arc<dyn Fn(&[u8]) -> bool + Send + Sync> = Arc::new(exclude_key_fn);

        tokio::task::spawn_blocking(move || -> Result<()> {
            let cf_names = db.cf_handle("names").expect("missing CF");
            let cf_blocks = db.cf_handle("blocks").expect("missing CF");

            let mut iter = db.raw_iterator_cf(&cf_names);
            iter.seek_to_first();

            let mut batch = rocksdb::WriteBatchWithTransaction::default();

            while let Some(name) = iter.key() {
                if let Some(id) = iter.value() {
                    if !(func)(name) {
                        batch.delete_cf(&cf_names, name);
                        batch.delete_cf(&cf_blocks, id);
                    }
                }
                iter.next();
            }

            db.write(batch)?;

            Ok(())
        })
        .await?
    }
}

pub struct BlobStorageKeyIterator<'a> {
    iter: rocksdb::DBRawIteratorWithThreadMode<'a, rocksdb::TransactionDB<rocksdb::MultiThreaded>>,
}

impl<'a> BlobStorageKeyIterator<'a> {
    fn new(iter: rocksdb::DBRawIteratorWithThreadMode<'a, rocksdb::TransactionDB<rocksdb::MultiThreaded>>) -> Self {
        Self { iter }
    }
}

impl Iterator for BlobStorageKeyIterator<'_> {
    type Item = Box<[u8]>;

    fn next(&mut self) -> Option<Box<[u8]>> {
        let key = self.iter.key();
        if let Some(key) = key {
            let key: Box<[u8]> = Box::from(key);
            self.iter.next();
            Some(key)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use tempfile::tempdir;
    use testresult::TestResult;
    use tokio_util::bytes::Bytes;

    use omnius_core_base::{clock::FakeClockUtc, random_bytes::FakeRandomBytesProvider, tsid::TsidProviderImpl};

    use super::*;

    #[tokio::test]
    async fn test_basic_operations() -> TestResult<()> {
        // setup
        let temp_dir = tempdir()?;
        let clock = FakeClockUtc::new(DateTime::parse_from_rfc3339("2000-01-01T00:00:00Z")?.into());
        let tsid_provider: Arc<Mutex<dyn TsidProvider + Send + Sync>> =
            Arc::new(Mutex::new(TsidProviderImpl::new(clock, FakeRandomBytesProvider::new(), 8)));
        let storage = KeyValueRocksdbStorage::new(temp_dir.path(), tsid_provider.clone()).await?;

        // contains -> false
        let exists = storage.contains("key_not_exist").await?;
        assert!(!exists);

        // put_value (create) with overwrite = true
        storage.put_value("name1", Bytes::from_static(b"value1"), true).await?;
        let v = storage.get_value("name1").await?;
        assert_eq!(v, Some(b"value1".to_vec()));

        // put_value with overwrite = false on missing key should return AlreadyExists
        let res = storage.put_value("missing", Bytes::from_static(b"x"), false).await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().kind(), &ErrorKind::AlreadyExists);

        // put_value with overwrite = false on existing key should update the block (same id)
        storage.put_value("name1", Bytes::from_static(b"value2"), false).await?;
        let v = storage.get_value("name1").await?;
        assert_eq!(v, Some(b"value2".to_vec()));

        // create another key
        storage.put_value("name2", Bytes::from_static(b"vv2"), true).await?;
        assert_eq!(storage.get_value("name2").await?, Some(b"vv2".to_vec()));

        // get_names iterator should include inserted keys
        let names: Vec<Box<[u8]>> = storage.get_names()?.collect();
        // convert to Vec<Vec<u8>> for easier assertions
        let names_vec: Vec<Vec<u8>> = names.into_iter().map(|b| b.into_vec()).collect();
        assert!(names_vec.iter().any(|n| n == b"name1"));
        assert!(names_vec.iter().any(|n| n == b"name2"));

        // rename: name1 -> name3 (non-existing)
        storage.rename("name1", "name3", false).await?;
        assert_eq!(storage.get_value("name1").await?, None);
        assert_eq!(storage.get_value("name3").await?, Some(b"value2".to_vec()));

        // rename: when dest exists and overwrite = false -> AlreadyExists
        // prepare: name4 exists
        storage.put_value("name4", Bytes::from_static(b"v4"), true).await?;
        let res = storage.rename("name3", "name4", false).await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().kind(), &ErrorKind::AlreadyExists);

        // rename: when dest exists and overwrite = true -> dest replaced
        storage.rename("name3", "name4", true).await?;
        // after overwrite, name3 should be gone, name4 should contain old name3's data
        assert_eq!(storage.get_value("name3").await?, None);
        assert_eq!(storage.get_value("name4").await?, Some(b"value2".to_vec()));

        // delete single
        storage.put_value("to_delete", Bytes::from_static(b"del"), true).await?;
        assert_eq!(storage.get_value("to_delete").await?, Some(b"del".to_vec()));
        storage.delete("to_delete").await?;
        assert_eq!(storage.get_value("to_delete").await?, None);

        // delete_bulk
        storage.put_value("bulk1", Bytes::from_static(b"b1"), true).await?;
        storage.put_value("bulk2", Bytes::from_static(b"b2"), true).await?;
        assert_eq!(storage.get_value("bulk1").await?, Some(b"b1".to_vec()));
        assert_eq!(storage.get_value("bulk2").await?, Some(b"b2".to_vec()));
        storage.delete_bulk(&["bulk1", "bulk2"]).await?;
        assert_eq!(storage.get_value("bulk1").await?, None);
        assert_eq!(storage.get_value("bulk2").await?, None);

        // shrink: keep only keys that match predicate
        // prepare keys
        storage.put_value("keep1", Bytes::from_static(b"k1"), true).await?;
        storage.put_value("keep2", Bytes::from_static(b"k2"), true).await?;
        storage.put_value("drop1", Bytes::from_static(b"d1"), true).await?;
        // shrink keep only keys starting with "keep"
        storage.shrink(|k| k.starts_with(b"keep")).await?;
        // confirm
        let names_after: Vec<Box<[u8]>> = storage.get_names()?.collect();
        let names_after_vec: Vec<Vec<u8>> = names_after.into_iter().map(|b| b.into_vec()).collect();
        assert!(names_after_vec.iter().any(|n| n == b"keep1"));
        assert!(names_after_vec.iter().any(|n| n == b"keep2"));
        assert!(!names_after_vec.iter().any(|n| n == b"drop1"));

        Ok(())
    }
}
