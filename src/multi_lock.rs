use std::sync::{Arc, Condvar, Mutex};

pub struct MultiLock<T> {
    inner: Arc<(Condvar, Mutex<(usize, T)>)>,
}

impl<T> MultiLock<T> {
    pub fn new(empty_data: T) -> Self {
        Self {
            inner: Arc::new((Condvar::new(), Mutex::new((0, empty_data)))),
        }
    }

    pub fn set_map<R>(self, setter: impl FnOnce(&mut T), mapper: impl FnOnce(&T) -> R) -> R {
        let (condis, mutis) = &*self.inner;
        let mut guard = mutis.lock().unwrap();
        guard.0 += 1;
        setter(&mut guard.1);
        condis.notify_one();
        let guard = condis
            .wait_while(guard, |(done, _)| *done < Arc::strong_count(&self.inner))
            .unwrap();
        let ret = mapper(&guard.1);
        condis.notify_one();
        ret
    }
}

impl<T> Clone for MultiLock<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    struct Data {
        data1: i32,
        data2: i32,
    }

    #[test]
    fn test_multilock() {
        std::thread::scope(|s| {
            let multi = MultiLock::new(Data { data1: 0, data2: 0 });
            let multi2 = multi.clone();
            s.spawn(move || {
                let data2 = multi2.set_map(|data| data.data1 = 5, |data| data.data2);
                assert_eq!(data2, 7);
            });
            s.spawn(|| {
                let data1 = multi.set_map(|data| data.data2 = 7, |data| data.data1);
                assert_eq!(data1, 5);
            });
        });
    }
}
