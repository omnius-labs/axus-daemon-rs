// https://rocksdb.org/blog/2021/05/26/integrated-blob-db.html

use std::path::Path;

#[allow(dead_code)]
pub struct BlobStorage {
    rocksdb: rocksdb::DBWithThreadMode<rocksdb::MultiThreaded>,
}

#[allow(dead_code)]
impl BlobStorage {
    pub fn new<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.set_blob_compression_type(rocksdb::DBCompressionType::None);
        opts.set_enable_blob_files(true);
        opts.set_enable_blob_gc(true);
        let db = rocksdb::DBWithThreadMode::<rocksdb::MultiThreaded>::open(&opts, path)?;
        Ok(Self { rocksdb: db })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        self.rocksdb.put(key, value)?;
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        let value = self.rocksdb.get(key)?;
        Ok(value)
    }

    pub fn delete(&self, key: &[u8]) -> anyhow::Result<()> {
        self.rocksdb.delete(key)?;
        Ok(())
    }

    pub fn keys(&self) -> anyhow::Result<BlobStorageKeyIterator> {
        let mut iter = self.rocksdb.raw_iterator();
        iter.seek_to_first();
        let iter = BlobStorageKeyIterator::new(iter);
        Ok(iter)
    }

    pub fn flush(&self) -> anyhow::Result<()> {
        self.rocksdb.flush()?;
        Ok(())
    }

    pub fn destroy<P: AsRef<Path>>(path: P) -> anyhow::Result<()> {
        let opts = rocksdb::Options::default();
        rocksdb::DB::destroy(&opts, path)?;
        Ok(())
    }
}

pub struct BlobStorageKeyIterator<'a> {
    iter: rocksdb::DBRawIteratorWithThreadMode<'a, rocksdb::DBWithThreadMode<rocksdb::MultiThreaded>>,
}

impl<'a> BlobStorageKeyIterator<'a> {
    fn new(iter: rocksdb::DBRawIteratorWithThreadMode<'a, rocksdb::DBWithThreadMode<rocksdb::MultiThreaded>>) -> Self {
        Self { iter }
    }
}

impl<'a> Iterator for BlobStorageKeyIterator<'a> {
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
    use super::BlobStorage;

    #[test]
    pub fn simple_test() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().as_os_str().to_str().unwrap();
        let storage = BlobStorage::new(path).unwrap();

        let key1: Vec<u8> = vec![0x00, 0x00];
        let key2: Vec<u8> = vec![0x00, 0x01];
        let value1: Vec<u8> = vec![0x01, 0x00];
        let value2: Vec<u8> = vec![0x01, 0x01];

        storage.put(key1.as_ref(), value1.as_ref()).unwrap();
        assert_eq!(storage.get(key1.as_ref()).unwrap().unwrap(), value1);
        assert_ne!(storage.get(key1.as_ref()).unwrap().unwrap(), value2);
        assert!(storage.get(key2.as_ref()).unwrap().is_none());
        storage.flush().unwrap();
        assert_eq!(storage.keys().unwrap().map(|n| n.to_vec()).collect::<Vec<_>>(), vec![key1.clone()]);
        assert!(storage.delete(key1.as_ref()).is_ok());
        assert_eq!(storage.keys().unwrap().count(), 0);
        assert!(storage.get(key1.as_ref()).unwrap().is_none());
    }
}
