use consts::WatchedEventType::{NodeCreated, NodeDataChanged, NodeDeleted, NodeChildrenChanged};
use proto::{ReadFrom, WatchedEvent};
use zookeeper::RawResponse;
use mio::{Events, Poll, PollOpt, Ready, Token};
use mio::channel::Receiver;
use std::collections::HashMap;
use std::io;

const CHANNEL: Token = Token(3);

#[derive(PartialEq)]
pub enum WatchType {
    Child,
    Data,
    Exist,
}

pub struct Watch {
    pub path: String,
    pub watch_type: WatchType,
    pub watcher: Box<Watcher>,
}

pub trait Watcher: Send {
    fn handle(&self, WatchedEvent);
}

impl<F> Watcher for F where F: Fn(WatchedEvent) + Send
{
    fn handle(&self, event: WatchedEvent) {
        self(event)
    }
}

pub enum WatchMessage {
    Event(RawResponse),
    Watch(Watch),
}

pub struct ZkWatch<W: Watcher> {
    poll: Poll,
    receiver: Receiver<WatchMessage>,
    watcher: W,
    watches: HashMap<String, Vec<Watch>>,
    chroot: Option<String>
}

impl<W: Watcher> ZkWatch<W> {
    pub fn new(watcher: W, receiver: Receiver<WatchMessage>, chroot: Option<String>) -> Self {
        let poll = Poll::new().unwrap();
        ZkWatch {
            poll: poll,
            receiver: receiver,
            watcher: watcher,
            watches: HashMap::new(),
            chroot: chroot
        }
    }

    pub fn run(&mut self) -> io::Result<()> {
        let mut events = Events::with_capacity(1024);
        self.poll.register(&self.receiver, CHANNEL, Ready::readable(), PollOpt::edge()).unwrap();
        loop {
            self.poll.poll(&mut events, None).unwrap();
            match self.receiver.try_recv().unwrap() {
                WatchMessage::Event(response) => {
                    info!("Event thread got response {:?}", response.header);
                    let mut data = response.data;
                    match response.header.err {
                        0 => {
                            match WatchedEvent::read_from(&mut data) {
                                Ok(mut event) => {
                                    self.cut_chroot(&mut event);
                                    self.dispatch(&event);
                                }
                                Err(e) => error!("Failed to parse WatchedEvent {:?}", e),
                            }
                        }
                        e => error!("WatchedEvent.error {:?}", e),
                    }
                }
                WatchMessage::Watch(watch) => {
                    self.watches.entry(watch.path.clone()).or_insert(vec![]).push(watch);
                }
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
        trace!("dispatch event {:?}", event);
        if let Some(watches) = self.find_watches(&event) {
            for watch in watches.into_iter() {
                watch.watcher.handle(event.clone())
            }
        } else {
            self.watcher.handle(event.clone())
        }
    }

    fn find_watches(&mut self, event: &WatchedEvent) -> Option<Vec<Watch>> {
        if let Some(ref path) = event.path {
            match self.watches.remove(path) {
                Some(watches) => {

                    let (matching, left): (_, Vec<Watch>) = watches.into_iter().partition(|w| {
                        match event.event_type {
                            NodeChildrenChanged => w.watch_type == WatchType::Child,
                            NodeCreated | NodeDataChanged => {
                                w.watch_type == WatchType::Data || w.watch_type == WatchType::Exist
                            }
                            NodeDeleted => true,
                            _ => false,
                        }
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
                }
                None => None,
            }
        } else {
            None
        }
    }
}
