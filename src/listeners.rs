use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::mpsc::Sender;
use snowflake::ProcessUniqueId;

#[derive(Debug,Clone,Copy,Eq,PartialEq,Hash)]
pub struct Subscription(ProcessUniqueId);

impl Subscription {
    fn new() -> Subscription {
        Subscription(ProcessUniqueId::new())
    }
}

type ListenerMap<T> = HashMap<Subscription, Sender<T>>;

#[derive(Clone)]
pub struct ListenerSet<T> where T: Send {
    listeners: Arc<Mutex<ListenerMap<T>>>,
}

impl<T> ListenerSet<T> where T: Send + Clone {
    pub fn new() -> Self {
        ListenerSet{
            listeners: Arc::new(Mutex::new(ListenerMap::new())),
        }
    }
    
    pub fn subscribe(&self, listener: Sender<T>) -> Subscription {
        let mut acquired_listeners = self.listeners.lock().unwrap();

        let subscription = Subscription::new();
        acquired_listeners.insert(subscription, listener);

        subscription
    }

    pub fn unsubscribe(&self, sub: Subscription) {
        let mut acquired_listeners = self.listeners.lock().unwrap();

        // channel will be close here automatically
        acquired_listeners.remove(&sub);
    }
    
    pub fn notify(&self, payload: &T) {
        let mut acquired_listeners = self.listeners.lock().unwrap();

        let failures = self.notify_listeners(&*acquired_listeners, payload);

        for failure in failures {
            acquired_listeners.remove(&failure);
        }
    }

    fn notify_listeners(&self, listeners: &ListenerMap<T>, payload: &T) -> Vec<Subscription> {
        let mut failures = vec![];
        for listener in listeners.iter() {
            if !self.notify_listener(&listener.1, payload) {
                failures.push(*listener.0);
            }
        }
        failures
    }

    fn notify_listener(&self, chan: &Sender<T>, payload: &T) -> bool {
        match chan.send(payload.clone()) {
            Ok(_) => true,
            _ => false
        }
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.listeners.lock().unwrap().len()
    }
}

#[cfg(test)]
mod tests {
    use super::{ListenerSet};
    use std::sync::mpsc;

    #[test]
    fn test_new_listener_set() {
        let ls = ListenerSet::<()>::new();
        assert_eq!(ls.len(), 0);
    }

    #[test]
    fn test_new_listener_for_chan() {
        let (tx, _rx) = mpsc::channel();
        let ls = ListenerSet::<bool>::new();

        ls.subscribe(tx);
        assert_eq!(ls.len(), 1);
    }
    
    #[test]
    fn test_add_listener_to_set() {        
        let (tx, rx) = mpsc::channel();
        let ls = ListenerSet::<bool>::new();

        ls.subscribe(tx);
        assert_eq!(ls.len(), 1);
        
        ls.notify(&true);
        assert_eq!(rx.recv().is_ok(), true);
    }

    #[test]
    fn test_remove_listener_from_set() {
        let (tx, rx) = mpsc::channel();
        let ls = ListenerSet::<bool>::new();

        let sub = ls.subscribe(tx);
        ls.unsubscribe(sub);
        assert_eq!(ls.len(), 0);
        
        ls.notify(&true);
        assert_eq!(rx.recv().is_err(), true);
    }

    #[test]
    fn test_scoped_unsubscribe() {
        let ls = ListenerSet::<bool>::new();

        {
            let (tx, _rx) = mpsc::channel();
            let _sub = ls.subscribe(tx);
            assert_eq!(ls.len(), 1);
        }

        assert_eq!(ls.len(), 1);
        
        ls.notify(&true);
        assert_eq!(ls.len(), 0);
    }
}
