use std::hash::Hash;
use std::{collections::HashMap, sync::Arc};

use chrono::{DateTime, Duration, Utc};
use core_base::clock::Clock;

pub struct VolatileHashSet<T> {
    map: HashMap<T, DateTime<Utc>>,
    expired_time: Duration,
    clock: Arc<dyn Clock<Utc> + Send + Sync>,
}

impl<T> VolatileHashSet<T>
where
    T: Hash + Eq,
{
    pub fn new(expired_time: Duration, clock: Arc<dyn Clock<Utc> + Send + Sync>) -> Self {
        Self {
            map: HashMap::new(),
            expired_time,
            clock: clock.clone(),
        }
    }

    pub fn refresh(&mut self) {
        let now = self.clock.now();
        let expired_time = self.expired_time;
        self.map.retain(|_, v| now - *v < expired_time);
    }

    pub fn insert(&mut self, value: T) {
        self.map.insert(value, self.clock.now());
    }

    pub fn contains(&self, value: &T) -> bool {
        self.map.contains_key(value)
    }

    pub fn remove(&mut self, value: &T) {
        self.map.remove(value);
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

    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.map.keys()
    }
}
