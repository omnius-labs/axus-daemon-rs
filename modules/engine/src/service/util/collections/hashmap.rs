use std::hash::Hash;
use std::{collections::HashMap, sync::Arc};

use chrono::{DateTime, Duration, Utc};
use core_base::clock::SystemClock;

struct ValueEntry<T> {
    pub value: T,
    pub created_time: DateTime<Utc>,
}

pub struct VolatileHashMap<K, V> {
    map: HashMap<K, ValueEntry<V>>,
    expired_time: Duration,
    system_clock: Arc<dyn SystemClock<Utc> + Send + Sync>,
}

impl<K, V> VolatileHashMap<K, V>
where
    K: Hash + Eq,
{
    pub fn new(expired_time: Duration, system_clock: Arc<dyn SystemClock<Utc> + Send + Sync>) -> Self {
        Self {
            map: HashMap::new(),
            expired_time,
            system_clock: system_clock.clone(),
        }
    }

    pub fn refresh(&mut self) {
        let now = self.system_clock.now();
        let expired_time = self.expired_time;
        self.map.retain(|_, v| now - v.created_time < expired_time);
    }

    pub fn insert(&mut self, key: K, value: V) {
        self.map.insert(
            key,
            ValueEntry {
                value,
                created_time: self.system_clock.now(),
            },
        );
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.map.contains_key(key)
    }

    pub fn remove(&mut self, key: &K) {
        self.map.remove(key);
    }

    pub fn clear(&mut self) {
        self.map.clear();
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.map.iter().map(|(k, v)| (k, &v.value))
    }
}
