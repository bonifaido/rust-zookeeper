use std::{env, sync::Arc, thread, time::Duration};
use uuid::Uuid;
use zookeeper_async::{recipes::leader_latch::LeaderLatch, WatchedEvent, Watcher, ZooKeeper};

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

#[tokio::main]
async fn main() {
    let zk_urls = zk_server_urls();
    println!("connecting to {}", zk_urls);

    let zk = ZooKeeper::connect(&*zk_urls, Duration::from_millis(2500), NoopWatcher)
        .await
        .unwrap();

    let id = Uuid::new_v4().to_string();
    println!("starting host with id: {:?}", id);

    let latch = LeaderLatch::new(Arc::new(zk), id.clone(), LATCH_PATH.into());
    latch.start().await.unwrap();

    loop {
        if latch.has_leadership() {
            println!("{:?} is the leader", id);
        } else {
            println!("{:?} is a follower", id);
        }
        thread::sleep(Duration::from_millis(1000));
    }
}
