
use std::collections::HashMap;
use std::sync::{Arc,Mutex};
use std::sync::mpsc::Sender;
use snowflake::ProcessUniqueId;

#[derive(Debug,Clone,Eq,PartialEq,Hash)]
pub struct Subscription(ProcessUniqueId);

impl Subscription {
    fn new() -> Subscription {
        Subscription(ProcessUniqueId::new())
    }
}

#[derive(Clone)]
pub struct ListenerSet<T> where T: Send {
    listeners: Arc<Mutex<HashMap<Subscription, Sender<T>>>>,
}

impl<T> ListenerSet<T> where T: Send + Clone {
    pub fn new() -> Self {
        ListenerSet{
            listeners: Arc::new(Mutex::new(HashMap::new())),
        }
    }
    
    pub fn subscribe(&self, listener: Sender<T>) -> Subscription {
        let mut acquired_listeners = self.listeners.lock().unwrap();

        let subscription = Subscription::new();
        acquired_listeners.insert(subscription.clone(), listener);

        subscription
    }

    pub fn unsubscribe(&self, sub: Subscription) {
        let mut acquired_listeners = self.listeners.lock().unwrap();

        // channel will be close here automatically
        acquired_listeners.remove(&sub);
    }
    
    pub fn notify(&self, payload: &T) {
        let acquired_listeners = self.listeners.lock().unwrap();
        
        for listener in acquired_listeners.iter() {
            Self::notify_listener(listener.1, payload);
        }
    }

    fn notify_listener(chan: &Sender<T>, payload: &T) {
        chan.send(payload.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::{ListenerSet,Subscription};
    use std::sync::mpsc;

    #[test]
    fn test_new_listener_set() {
        let ls = ListenerSet::<()>::new();
    }

    #[test]
    fn test_new_listener_for_chan() {
        let (tx, _rx) = mpsc::channel();
        let mut ls = ListenerSet::<bool>::new();

        ls.subscribe(tx);
    }
    
    #[test]
    fn test_add_listener_to_set() {        
        let (tx, rx) = mpsc::channel();
        let mut ls = ListenerSet::<bool>::new();

        ls.subscribe(tx);
        ls.notify(&true);

        assert_eq!(rx.recv().is_ok(), true);
    }

    #[test]
    fn test_remove_listener_from_set() {
        let (tx, rx) = mpsc::channel();
        let mut ls = ListenerSet::<bool>::new();

        let sub = ls.subscribe(tx);
        ls.unsubscribe(sub);
        
        ls.notify(&true);
        
        assert_eq!(rx.recv().is_err(), true);
    }
}
