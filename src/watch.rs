use consts::WatchedEventType::{NodeCreated, NodeDataChanged, NodeDeleted, NodeChildrenChanged};
use proto::{ReadFrom, WatchedEvent};
use zookeeper::RawResponse;
use mio::{EventLoop, Handler, Sender};
use std::boxed::FnBox;
use std::collections::HashMap;
use std::io;

#[derive(PartialEq)]
pub enum WatchType {
    Child,
    Data,
    Exist
}

pub struct Watch {
    pub path: String,
    pub watch_type: WatchType,
    pub watcher: Box<FnBox(&WatchedEvent) + Send>
}

pub trait Watcher: Send {
    fn handle(&mut self, &WatchedEvent);
}

impl<F> Watcher for F where F: FnMut(&WatchedEvent) + Send {
    fn handle(&mut self, event: &WatchedEvent) {
        self(event)
    }
}

pub enum WatchMessage {
    Event(RawResponse),
    Watch(Watch)
}

pub struct ZkWatch<W: Watcher> {
    handler: ZkWatchHandler<W>,
    event_loop: EventLoop<ZkWatchHandler<W>>
}

impl<W: Watcher> ZkWatch<W> {
    pub fn new(watcher: W, chroot: Option<String>) -> Self {
        let event_loop = EventLoop::new().unwrap();
        let handler = ZkWatchHandler{watcher: watcher, watches: HashMap::new(), chroot: chroot};
        ZkWatch{event_loop: event_loop, handler: handler}
    }

    pub fn sender(&self) -> Sender<WatchMessage> {
        self.event_loop.channel()
    }

    pub fn run(mut self) -> io::Result<()> {
        self.event_loop.run(&mut self.handler)
    }
}

struct ZkWatchHandler<W: Watcher> {
    watcher: W,
    watches: HashMap<String, Vec<Watch>>,
    chroot: Option<String>
}

impl<W: Watcher> Handler for ZkWatchHandler<W> {
    type Message = WatchMessage;
    type Timeout = ();

    fn notify(&mut self, _: &mut EventLoop<Self>, message: Self::Message) {
        match message {
            WatchMessage::Event(response) => {
                info!("Event thread got response {:?}", response.header);
                let mut data = response.data;
                match response.header.err {
                    0 => match WatchedEvent::read_from(&mut data) {
                        Ok(mut event) => {
                            self.cut_chroot(&mut event);
                            self.dispatch(&event);
                        },
                        Err(e) => error!("Failed to parse WatchedEvent {:?}", e)
                    },
                    e => error!("WatchedEvent.error {:?}", e)
                }
            },
            WatchMessage::Watch(watch) => {
                self.watches.entry(watch.path.clone()).or_insert(vec![]).push(watch);
            }
        }
    }
}

impl<W: Watcher> ZkWatchHandler<W> {

    fn cut_chroot(&self, event: &mut WatchedEvent) {
        if let Some(ref chroot) = self.chroot {
            if event.path.is_some() {
                event.path = Some(event.path.as_ref().unwrap()[chroot.len()..].to_owned());
            }
        }
    }

    fn dispatch(&mut self, event: &WatchedEvent) {
        debug!("{:?}", event);
        if let Some(watches) = self.find_watches(&event) {
            for watch in watches.into_iter() {
                watch.watcher.call_box((event,))
            }
        } else {
            self.watcher.handle(event)
        }
    }

    fn find_watches(&mut self, event: &WatchedEvent) -> Option<Vec<Watch>> {
        if let Some(ref path) = event.path {
            match self.watches.remove(path) {
                Some(watches) => {

                    let (matching, left): (_, Vec<Watch>) = watches.into_iter().partition(
                        |w|
                        match event.event_type {
                            NodeChildrenChanged => w.watch_type == WatchType::Child,
                            NodeCreated | NodeDataChanged => w.watch_type == WatchType::Data
                                                          || w.watch_type == WatchType::Exist,
                            NodeDeleted => true,
                            _ => false
                        });

                    // put back the remaining watches
                    if !left.is_empty() {
                        self.watches.insert(path.to_owned(), left);
                    }
                    if matching.is_empty() {
                        None
                    } else {
                        Some(matching)
                    }
                },
                None => None
            }
        } else {
            None
        }
    }
}
