extern crate env_logger;
extern crate uuid;
extern crate zookeeper;

use std::{sync::Arc, thread, time::Duration};
use uuid::Uuid;
use zookeeper::{recipes::leader::LeaderLatch, WatchedEvent, Watcher, ZooKeeper};

const LATCH_PATH: &str = "/latch-ex";

struct NoopWatcher;

impl Watcher for NoopWatcher {
    fn handle(&self, _ev: WatchedEvent) {}
}

fn main() {
    env_logger::init();
    let zk =
        ZooKeeper::connect("localhost:2181", Duration::from_millis(2500), NoopWatcher).unwrap();

    let id = Uuid::new_v4().to_string();
    log::info!("starting host with id: {:?}", id);

    let latch = LeaderLatch::new(Arc::new(zk), id.clone(), LATCH_PATH.into());
    latch.start().unwrap();

    loop {
        if latch.has_leadership() {
            log::info!("{:?} is the leader", id);
        } else {
            log::info!("{:?} is a follower", id);
        }
        thread::sleep(Duration::from_millis(1000));
    }
}
