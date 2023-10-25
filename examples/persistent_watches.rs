#![deny(unused_mut)]
extern crate zookeeper;
#[macro_use]
extern crate log;
extern crate env_logger;

use std::env;
use std::io;
use std::io::BufRead;
use std::time::Duration;
use zookeeper::AddWatchMode;
use zookeeper::WatcherType;
use zookeeper::ZooKeeperExt;
use zookeeper::{Acl, CreateMode, WatchedEvent, Watcher, ZooKeeper};

struct LoggingWatcher;
impl Watcher for LoggingWatcher {
    fn handle(&self, e: WatchedEvent) {
        info!("{:?}", e)
    }
}

fn zk_server_urls() -> String {
    let key = "ZOOKEEPER_SERVERS";
    match env::var(key) {
        Ok(val) => val,
        Err(_) => "localhost:2181".to_string(),
    }
}

fn zk_example() {
    let zk_urls = zk_server_urls();
    println!("connecting to {}", zk_urls);

    let root = format!("/example-{}", uuid::Uuid::new_v4());
    let modifying_zk =
        ZooKeeper::connect(&*zk_urls, Duration::from_secs(15), LoggingWatcher, None).unwrap();
    let recursive_watch_zk =
        ZooKeeper::connect(&*zk_urls, Duration::from_secs(15), LoggingWatcher, None).unwrap();
    let persistent_watch_zk =
        ZooKeeper::connect(&*zk_urls, Duration::from_secs(15), LoggingWatcher, None).unwrap();

    // Creating separate clients to show the example where modifications to the nodes
    // take place in a different session than our own.
    modifying_zk.add_listener(|zk_state| println!("New modifying ZkState is {:?}", zk_state));

    // Also separate clients per type of watch as there is a bug when creating multiple type watchers in the same
    // path in the same session
    // https://issues.apache.org/jira/browse/ZOOKEEPER-4466
    recursive_watch_zk
        .add_listener(|zk_state| println!("New recursive watch ZkState is {:?}", zk_state));
    persistent_watch_zk
        .add_listener(|zk_state| println!("New peristent watch ZkState is {:?}", zk_state));

    modifying_zk.ensure_path(&root).unwrap();

    recursive_watch_zk
        .add_watch(&root, AddWatchMode::PersistentRecursive, |event| {
            println!("received persistent recursive watch event {event:?}");
        })
        .unwrap();

    persistent_watch_zk
        .add_watch(&root, AddWatchMode::Persistent, |event| {
            println!("received persistent watch event {event:?}");
        })
        .unwrap();

    println!(
        "press c to add and modify child, e to edit the watched node, anything else to proceed"
    );
    let stdin = io::stdin();
    let inputs = stdin.lock().lines();
    let mut incr = 0;
    for input in inputs {
        incr += 1;
        match input.unwrap().as_str() {
            "c" => {
                let child_path = format!("{root}/child-{incr}");
                modifying_zk
                    .create(
                        &child_path,
                        b"".to_vec(),
                        Acl::open_unsafe().clone(),
                        CreateMode::Ephemeral,
                    )
                    .unwrap();
                modifying_zk
                    .set_data(&child_path, b"new-data".to_vec(), None)
                    .unwrap();
                modifying_zk.delete(&child_path, None).unwrap();
            }
            "e" => {
                modifying_zk
                    .set_data(&root, format!("new-data-{incr}").into_bytes(), None)
                    .unwrap();
            }
            other => {
                println!("received {other}");
                break;
            }
        }
    }

    println!("removing watch");
    recursive_watch_zk
        .remove_watches(&root, WatcherType::Any)
        .unwrap();
    persistent_watch_zk
        .remove_watches(&root, WatcherType::Any)
        .unwrap();

    println!("creating child node. This shouldn't have a new event");
    modifying_zk
        .create(
            &format!("{root}/child-no-notification"),
            b"".to_vec(),
            Acl::open_unsafe().clone(),
            CreateMode::Ephemeral,
        )
        .unwrap();

    modifying_zk.delete_recursive(&root).unwrap();
}

fn main() {
    env_logger::init();
    zk_example();
}
