use io::RawResponse;
use proto::{ReadFrom, WatchedEvent};
use mio::{EventLoop, Handler, Sender};
use std::io;

pub trait Watcher: Send {
    fn handle(&mut self, &WatchedEvent);
}

impl<F> Watcher for F where F: FnMut(&WatchedEvent) + Send {
    fn handle(&mut self, event: &WatchedEvent) {
        self(event)
    }
}

pub struct ZkWatch<W: Watcher> {
    handler: ZkWatchHandler<W>,
    event_loop: EventLoop<ZkWatchHandler<W>>
}

impl<W: Watcher> ZkWatch<W> {
    pub fn new(watcher: W) -> Self {
        let event_loop = EventLoop::new().unwrap();
        let handler = ZkWatchHandler{watcher: watcher};
        ZkWatch{event_loop: event_loop, handler: handler}
    }

    pub fn sender(&self) -> Sender<RawResponse> {
        self.event_loop.channel()
    }

    pub fn run(mut self) -> io::Result<()> {
        self.event_loop.run(&mut self.handler)
    }

    // This should be in IO
    // fn send_watched_event(keeper_state: KeeperState) {
    //     match sender.send(WatchedEvent{event_type: WatchedEventType::None,
    //                                    keeper_state: keeper_state,
    //                                    path: None}) {
    //         Ok(()) => (),
    //         Err(e) => panic!("Reader/Writer: Event died {}", e)
    //     }
    // }
}

struct ZkWatchHandler<W: Watcher> {
    watcher: W
}

impl<W: Watcher> Handler for ZkWatchHandler<W> {
    type Message = RawResponse;
    type Timeout = ();

    fn notify(&mut self, _: &mut EventLoop<Self>, response: Self::Message) {
        info!("Event thread got response {:?}", response.header);
        let mut response = response;
        match response.header.err {
            0 => match WatchedEvent::read_from(&mut response.data) {
                Ok(event) => {
                    info!("{:?}", event);
                    self.watcher.handle(&event);
                },
                Err(e) => error!("Failed to parse WatchedEvent {:?}", e)
            },
            e => error!("WatchedEvent.error {:?}", e)
        }
    }
}
