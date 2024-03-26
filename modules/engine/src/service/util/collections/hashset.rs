use std::hash::Hash;
use std::{collections::HashMap, sync::Arc};

use chrono::{DateTime, Duration, Utc};
use core_base::clock::SystemClock;

pub struct VolatileHashSet<T> {
    map: HashMap<T, DateTime<Utc>>,
    expired_time: Duration,
    refresh_interval: Duration,
    system_clock: Arc<dyn SystemClock<Utc> + Send + Sync>,
    refreshed_time: DateTime<Utc>,
}

impl<T> VolatileHashSet<T>
where
    T: Hash + Eq,
{
    pub fn new(expired_time: Duration, refresh_interval: Duration, system_clock: Arc<dyn SystemClock<Utc> + Send + Sync>) -> Self {
        Self {
            map: HashMap::new(),
            expired_time,
            refresh_interval,
            system_clock: system_clock.clone(),
            refreshed_time: system_clock.now(),
        }
    }

    pub fn refresh(&mut self) {
        let now = self.system_clock.now();
        if now - self.refreshed_time < self.refresh_interval {
            return;
        }

        let expired_time = self.expired_time;
        self.map.retain(|_, v| now - *v < expired_time);
        self.refreshed_time = now;
    }

    pub fn insert(&mut self, value: T) {
        self.map.insert(value, self.system_clock.now());
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
