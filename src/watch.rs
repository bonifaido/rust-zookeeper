use consts::{KeeperState, WatchedEventType};
use consts::{
    WatchedEventType::{NodeChildrenChanged, NodeCreated, NodeDataChanged, NodeDeleted},
    WatcherType,
};
use proto::ReadFrom;
use std::collections::HashMap;
use std::io;
use std::sync::mpsc::{self, Receiver, Sender};
use zookeeper::RawResponse;

const PERSISTENT_WATCH_TRIGGERS: [WatchedEventType; 4] = [
    NodeChildrenChanged,
    NodeCreated,
    NodeDataChanged,
    NodeDeleted,
];

/// Represents a change on the ZooKeeper that a `Watcher` is able to respond to.
///
/// The `WatchedEvent` includes exactly what happened, the current state of the ZooKeeper, and the
/// path of the znode that was involved in the event.
#[derive(Clone, Debug)]
pub struct WatchedEvent {
    /// The trigger that caused the watch to hit.
    pub event_type: WatchedEventType,
    /// The current state of ZooKeeper (and the client's connection to it).
    pub keeper_state: KeeperState,
    /// The path of the znode that was involved. This will be `None` for session-related triggers.
    pub path: Option<String>,
}

/// An object watching a path for certain changes.
pub struct Watch {
    /// The path to the znode this is watching.
    pub path: String,
    /// The type of changes this watch is looking for.
    pub watcher_type: WatcherType,
    /// The handler for this watch, to call when it is triggered.
    pub watcher: Box<dyn Watcher>,
}

/// The interface for handling events when a `Watch` triggers.
pub trait Watcher: Send {
    /// Receive the triggered event.
    fn handle(&self, event: WatchedEvent);
}

impl<F> Watcher for F
where
    F: Fn(WatchedEvent) + Send,
{
    fn handle(&self, event: WatchedEvent) {
        self(event)
    }
}

pub enum WatchMessage {
    Event(RawResponse),
    Watch(Watch),
    RemoveWatch(String, WatcherType),
}

pub struct ZkWatch<W: Watcher> {
    watcher: W,
    watches: HashMap<String, Vec<Watch>>,
    // Storing peristent watches separately since they may require splitting the event path
    // which will have a performance impact. This replicates how they are stored in Zookeeper as well
    // and allows us to skip removing them from the hashmap and re-adding them.
    persistent_watches: HashMap<String, Vec<Watch>>,
    chroot: Option<String>,
    rx: Receiver<WatchMessage>,
}

impl<W: Watcher> ZkWatch<W> {
    pub fn new(watcher: W, chroot: Option<String>) -> (Self, Sender<WatchMessage>) {
        trace!("ZkWatch::new");
        let (tx, rx) = mpsc::channel();

        let watch = ZkWatch {
            watches: HashMap::new(),
            persistent_watches: HashMap::new(),
            watcher: watcher,
            chroot: chroot,
            rx,
        };
        (watch, tx)
    }

    pub fn run(mut self) -> io::Result<()> {
        while let Ok(msg) = self.rx.recv() {
            self.process_message(msg);
        }

        Ok(())
    }

    fn process_message(&mut self, message: WatchMessage) {
        match message {
            WatchMessage::Event(response) => {
                info!("Event thread got response {:?}", response.header);
                let mut data = response.data;
                match response.header.err {
                    0 => match WatchedEvent::read_from(&mut data) {
                        Ok(mut event) => {
                            self.cut_chroot(&mut event);
                            self.dispatch(&event);
                        }
                        Err(e) => error!("Failed to parse WatchedEvent {:?}", e),
                    },
                    e => error!("WatchedEvent.error {:?}", e),
                }
            }
            WatchMessage::Watch(watch) => {
                let group = if watch.watcher_type.is_persistent() {
                    &mut self.persistent_watches
                } else {
                    &mut self.watches
                };
                group
                    .entry(watch.path.clone())
                    .or_insert_with(|| vec![])
                    .push(watch);
            }
            WatchMessage::RemoveWatch(path, watcher_type) => {
                remove_matching_watches(&path, watcher_type, &mut self.watches);
                remove_matching_watches(&path, watcher_type, &mut self.persistent_watches);
            }
        }
    }

    fn cut_chroot(&self, event: &mut WatchedEvent) {
        if let Some(ref chroot) = self.chroot {
            if event.path.is_some() {
                event.path = Some(event.path.as_ref().unwrap()[chroot.len()..].to_owned());
            }
        }
    }

    fn dispatch(&mut self, event: &WatchedEvent) {
        debug!("{:?}", event);
        if !self.trigger_watches(&event) {
            self.watcher.handle(event.clone())
        }
    }

    /// Triggers all the watches that we have registered, removing the ones that are not persistent.
    /// Returns whether or not any of the watches fired.
    fn trigger_watches(&mut self, event: &WatchedEvent) -> bool {
        if let Some(ref path) = event.path {
            // We execute this in two steps. Once for the one-off watches, and once for the persistent ones.
            let triggered_watch = match self.watches.remove(path) {
                Some(watches) => {
                    let (matching, left): (_, Vec<Watch>) =
                        watches.into_iter().partition(|w| {
                            match (event.event_type, w.watcher_type) {
                                (NodeChildrenChanged, WatcherType::Children) => true,
                                (NodeCreated | NodeDataChanged, WatcherType::Data) => true,
                                (NodeDeleted, _) => true,
                                _ => false,
                            }
                        });
                    // put back the remaining watches
                    if !left.is_empty() {
                        self.watches.insert(path.to_owned(), left);
                    }
                    // Trigger all matching watches.
                    matching
                        .iter()
                        .for_each(|w| w.watcher.handle(event.clone()));
                    !matching.is_empty()
                }
                None => false,
            };

            let triggered_peristent_watch = if PERSISTENT_WATCH_TRIGGERS.contains(&event.event_type)
                && !self.persistent_watches.is_empty()
            {
                let mut watch_path = String::from("");
                let mut parts = path.split("/").skip(1);
                let mut triggered = false;
                while let Some(part) = parts.next() {
                    watch_path = watch_path + "/" + part;
                    if let Some(watches) = self.persistent_watches.get(&watch_path) {
                        for w in watches {
                            if match w.watcher_type {
                                WatcherType::Persistent => path == &watch_path,
                                WatcherType::PersistentRecursive => true,
                                _ => false,
                            } {
                                w.watcher.handle(event.clone());
                                triggered = true;
                            }
                        }
                    }
                }
                triggered
            } else {
                false
            };
            triggered_watch || triggered_peristent_watch
        } else {
            false
        }
    }
}

fn remove_matching_watches(
    path: &str,
    watcher_type: WatcherType,
    watches: &mut HashMap<String, Vec<Watch>>,
) {
    let remaining_watches: Option<Vec<_>> = watches.remove(path).map(|watches| {
        watches
            .into_iter()
            .filter(|w| w.watcher_type == watcher_type || watcher_type == WatcherType::Any)
            .collect()
    });
    if let Some(w) = remaining_watches {
        if !w.is_empty() {
            watches.insert(path.into(), w);
        }
    }
}
