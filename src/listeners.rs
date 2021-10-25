use snowflake::ProcessUniqueId;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// A unique identifier returned by `ZooKeeper::add_listener`.
///
/// It can be used to remove a listener with `ZooKeeper::remove_listener`.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct Subscription(ProcessUniqueId);

impl Subscription {
    fn new() -> Subscription {
        Subscription(ProcessUniqueId::new())
    }
}

type ListenerMap<T> = HashMap<Subscription, Box<dyn Fn(T) + Send + 'static>>;

#[derive(Clone)]
pub struct ListenerSet<T>
where
    T: Send,
{
    listeners: Arc<Mutex<ListenerMap<T>>>,
}

impl<T> ListenerSet<T>
where
    T: Send + Clone,
{
    pub fn new() -> Self {
        ListenerSet {
            listeners: Arc::new(Mutex::new(ListenerMap::new())),
        }
    }

    pub fn subscribe<Listener: Fn(T) + Send + 'static>(&self, listener: Listener) -> Subscription {
        let mut acquired_listeners = self.listeners.lock().unwrap();

        let subscription = Subscription::new();
        acquired_listeners.insert(subscription, Box::new(listener));

        subscription
    }

    pub fn unsubscribe(&self, sub: Subscription) {
        let mut acquired_listeners = self.listeners.lock().unwrap();

        // channel will be close here automatically
        acquired_listeners.remove(&sub);
    }

    pub fn notify(&self, payload: &T) {
        let listeners = self.listeners.lock().unwrap();

        for listener in listeners.values() {
            listener(payload.clone())
        }
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.listeners.lock().unwrap().len()
    }
}

#[cfg(test)]
mod tests {
    use super::ListenerSet;
    use std::sync::mpsc;

    #[test]
    fn test_new_listener_set() {
        let ls = ListenerSet::<()>::new();
        assert_eq!(ls.len(), 0);
    }

    #[test]
    fn test_new_listener_for_chan() {
        let ls = ListenerSet::<bool>::new();

        ls.subscribe(|_e| {});
        assert_eq!(ls.len(), 1);
    }

    #[test]
    fn test_add_listener_to_set() {
        let (tx, rx) = mpsc::channel();
        let ls = ListenerSet::<bool>::new();

        ls.subscribe(move |e| tx.send(e).unwrap());
        assert_eq!(ls.len(), 1);

        ls.notify(&true);
        assert!(rx.recv().is_ok());
    }

    #[test]
    fn test_remove_listener_from_set() {
        let (tx, rx) = mpsc::channel();
        let ls = ListenerSet::<bool>::new();

        let sub = ls.subscribe(move |e| tx.send(e).unwrap());
        ls.unsubscribe(sub);
        assert_eq!(ls.len(), 0);

        ls.notify(&true);
        assert!(rx.recv().is_err());
    }
}
