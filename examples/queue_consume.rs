extern crate env_logger;
extern crate uuid;
extern crate zookeeper;

use std::{env, sync::Arc, thread, time::Duration};

use zookeeper::{recipes::queue::ZkQueue, WatchedEvent, Watcher, ZooKeeper};

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

    let queue = ZkQueue::new(Arc::new(zk), "/testing2".to_string()).unwrap();

    println!("waiting for a message");
    let msg = queue.take();
    if msg.is_err() {
        eprint!("unable to listen for message. error: {}", msg.err().unwrap().to_string())
    } else {
        println!("got {:?}", String::from_utf8(msg.unwrap()).unwrap());

    }

}