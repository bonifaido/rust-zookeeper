extern crate env_logger;
extern crate uuid;
extern crate zookeeper;

use std::{env, sync::Arc, thread, time::Duration};
use uuid::Uuid;
use zookeeper::{recipes::leader::LeaderLatch, WatchedEvent, Watcher, ZooKeeper};

const LATCH_PATH: &str = "/latch-ex";

struct NoopWatcher;

impl Watcher for NoopWatcher {
    fn handle(&self, _ev: WatchedEvent) {}
}

fn zk_server_urls() -> String {
    let key = "ZOOKEEPER_SERVERS";
    match env::var(key) {
        Ok(val) => val,
        Err(_) => "localhost:2181".to_string(),
    }
}

fn main() {
    env_logger::init();

    let zk_urls = zk_server_urls();
    log::info!("connecting to {}", zk_urls);

    let zk = ZooKeeper::connect(&*zk_urls, Duration::from_millis(2500), NoopWatcher).unwrap();

    let id = Uuid::new_v4().to_string();
    log::info!("starting host with id: {:?}", id);

    let latch = LeaderLatch::new(Arc::new(zk), id.clone(), LATCH_PATH.into());
    latch.start().unwrap();

    loop {
        if latch.has_leadership().unwrap() {
            log::info!("{:?} is the leader", id);
        } else {
            log::info!("{:?} is a follower", id);
        }
        thread::sleep(Duration::from_millis(1000));
    }
}
