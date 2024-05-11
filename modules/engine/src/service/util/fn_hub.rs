use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

type FnBox<T, Args> = Box<dyn Fn(&Args) -> T + Send + Sync>;

#[allow(unused)]
pub struct FnHub<T, Args> {
    tasks: Arc<Mutex<HashMap<u32, FnBox<T, Args>>>>,
    next_id: Arc<Mutex<u32>>,
}

#[allow(unused)]
impl<T, Args> FnHub<T, Args> {
    pub fn new() -> Self {
        Self {
            tasks: Arc::new(Mutex::new(HashMap::new())),
            next_id: Arc::new(Mutex::new(0)),
        }
    }

    pub fn executor(&self) -> FnExecutor<T, Args> {
        FnExecutor {
            tasks: Arc::clone(&self.tasks),
        }
    }

    pub fn registrar(&self) -> FnRegistrar<T, Args> {
        FnRegistrar {
            tasks: Arc::clone(&self.tasks),
            next_id: Arc::clone(&self.next_id),
        }
    }
}

impl<T, Args> Default for FnHub<T, Args> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
pub struct FnExecutor<T, Args> {
    tasks: Arc<Mutex<HashMap<u32, FnBox<T, Args>>>>,
}

impl<T, Args> FnExecutor<T, Args> {
    pub fn execute(&self, args: &Args) -> Vec<T> {
        let tasks = self.tasks.lock().unwrap();
        tasks.values().map(|f| f(args)).collect()
    }
}

#[allow(unused)]
#[derive(Clone)]
pub struct FnRegistrar<T, Args> {
    tasks: Arc<Mutex<HashMap<u32, FnBox<T, Args>>>>,
    next_id: Arc<Mutex<u32>>,
}

#[allow(unused)]
impl<T, Args> FnRegistrar<T, Args> {
    pub fn register<F>(&self, f: F) -> FnHandle<T, Args>
    where
        F: Fn(&Args) -> T + Send + Sync + 'static,
    {
        let mut tasks = self.tasks.lock().unwrap();
        let mut id = self.next_id.lock().unwrap();
        let current_id = *id;
        tasks.insert(current_id, Box::new(f));
        *id += 1;
        FnHandle {
            tasks: Arc::clone(&self.tasks),
            id: current_id,
        }
    }
}

pub struct FnHandle<T, Args> {
    tasks: Arc<Mutex<HashMap<u32, FnBox<T, Args>>>>,
    id: u32,
}

impl<T, Args> Drop for FnHandle<T, Args> {
    fn drop(&mut self) {
        let mut tasks = self.tasks.lock().unwrap();
        tasks.remove(&self.id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn call_test() {
        let hub = FnHub::<i32, ()>::new();
        let registrar = hub.registrar();
        let _cookie = registrar.register(|_| 42);
        assert_eq!(registrar.tasks.lock().unwrap().len(), 1);
        let executor = hub.executor();
        assert_eq!(executor.execute(&()), vec![42]);
    }

    #[test]
    fn cookie_drop_test() {
        let hub = FnHub::<i32, ()>::new();
        let registrar = hub.registrar();
        {
            let _cookie = registrar.register(|_| 42);
        } // _cookie is dropped here
        assert!(registrar.tasks.lock().unwrap().is_empty());
    }
}
