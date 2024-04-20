use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

type FnBox<T> = Box<dyn Fn() -> T + Send + Sync>;

pub struct FnPipe<T> {
    fs: Arc<Mutex<HashMap<u32, FnBox<T>>>>,
    index: Arc<Mutex<u32>>,
}

impl<T> FnPipe<T> {
    pub fn new() -> Self {
        Self {
            fs: Arc::new(Mutex::new(HashMap::new())),
            index: Arc::new(Mutex::new(0)),
        }
    }

    pub fn caller(&self) -> FnCaller<T> {
        FnCaller { fs: Arc::clone(&self.fs) }
    }

    pub fn listener(&self) -> FuListener<T> {
        FuListener {
            fs: Arc::clone(&self.fs),
            index: Arc::clone(&self.index),
        }
    }
}

impl<T> Default for FnPipe<T> {
    fn default() -> Self {
        Self::new()
    }
}

pub struct FnCaller<T> {
    fs: Arc<Mutex<HashMap<u32, FnBox<T>>>>,
}

impl<T> FnCaller<T> {
    pub fn call(&self) -> Vec<T> {
        let fs = self.fs.lock().unwrap();
        fs.iter().map(|(_, f)| f()).collect()
    }
}

pub struct FuListener<T> {
    fs: Arc<Mutex<HashMap<u32, FnBox<T>>>>,
    index: Arc<Mutex<u32>>,
}

impl<T> FuListener<T> {
    pub fn listen<F>(&self, f: F) -> Cookie<T>
    where
        F: Fn() -> T + Send + Sync + 'static,
    {
        let mut index = self.index.lock().unwrap();
        let pos = *index;
        *index += 1;

        let mut fs = self.fs.lock().unwrap();
        fs.insert(pos, Box::new(f));

        Cookie {
            fs: Arc::clone(&self.fs),
            pos,
        }
    }
}

pub struct Cookie<T> {
    fs: Arc<Mutex<HashMap<u32, FnBox<T>>>>,
    pos: u32,
}

impl<T> Drop for Cookie<T> {
    fn drop(&mut self) {
        let mut fs = self.fs.lock().unwrap();
        let _ = fs.remove(&self.pos);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn call_test() {
        let pipe = FnPipe::<i32>::new();
        let listener = pipe.listener();
        let _cookie = listener.listen(|| 42);
        assert_eq!(listener.fs.lock().unwrap().len(), 1);
        let caller = pipe.caller();
        assert_eq!(caller.call(), vec![42]);
    }

    #[test]
    fn cookie_drop_test() {
        let pipe = FnPipe::<i32>::new();
        let listener = pipe.listener();
        {
            let _cookie = listener.listen(|| 42);
        } // _cookie is dropped here
        assert!(listener.fs.lock().unwrap().is_empty());
    }
}
