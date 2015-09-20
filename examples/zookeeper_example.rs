#![deny(unused_mut)]
extern crate zookeeper;
#[macro_use]
extern crate log;
extern crate env_logger;

use std::io;
use std::sync::Arc;
use std::time::Duration;
use std::thread;
use std::env;
use std::sync::mpsc;
use zookeeper::{CreateMode, Watcher, WatchedEvent, ZooKeeper};
use zookeeper::acls;
use zookeeper::recipes::cache::{PathChildrenCache};

struct LoggingWatcher;
impl Watcher for LoggingWatcher {
    fn handle(&mut self, e: &WatchedEvent) {
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
    
    let zk = ZooKeeper::connect(&*zk_urls, Duration::from_secs(5), LoggingWatcher).unwrap();

    let mut tmp = String::new();

    let auth = zk.add_auth("digest", vec![1,2,3,4]);

    println!("authenticated -> {:?}", auth);

    let path = zk.create("/test", vec![1,2], acls::OPEN_ACL_UNSAFE.clone(), CreateMode::Ephemeral);

    println!("created -> {:?}", path);

    let exists = zk.exists("/test", true);

    println!("exists -> {:?}", exists);

    let doesnt_exist = zk.exists("/blabla", true);

    println!("don't exists path -> {:?}", doesnt_exist);

    let get_acl = zk.get_acl("/test");

    println!("get_acl -> {:?}", get_acl);

    let set_acl = zk.set_acl("/test", acls::OPEN_ACL_UNSAFE.clone(), -1);

    println!("set_acl -> {:?}", set_acl);

    let children = zk.get_children("/", true);

    println!("children of / -> {:?}", children);

    let set_data = zk.set_data("/test", vec![6,5,4,3], -1);

    println!("set_data -> {:?}", set_data);

    let get_data = zk.get_data("/test", true);

    println!("get_data -> {:?}", get_data);

    // let delete = zk.delete("/test", -1);

    // println!("deleted /test -> {:?}", delete);

    let watch_children = zk.get_children_w("/", |event: &WatchedEvent| {
        println!("watched event {:?}", event);
    });
    
    println!("watch children -> {:?}", watch_children);

    let zk_arc = Arc::new(zk);
    
    let mut pcc = PathChildrenCache::new(zk_arc.clone(), "/").unwrap();
    match pcc.start() {
        Err(err) => {
            println!("error starting cache: {:?}", err);
            return;
        },
        _ => {
            println!("cache started");
        }
    }

    let (ev_tx, ev_rx) = mpsc::channel();
    pcc.add_listener(ev_tx);
    thread::spawn(move || {
        for ev in ev_rx {
            println!("received event {:?}", ev);
        };
    });

    println!("press enter to close client");
    io::stdin().read_line(&mut tmp).unwrap();

    // The client can be shared between tasks
    let zk_arc_captured = zk_arc.clone();
    thread::spawn(move || {
        zk_arc_captured.close().unwrap();

        // And operations return error after closed
        match zk_arc_captured.exists("/test", false) {
            Err(err) => println!("Usage after closed should end up with error: {:?}", err),
            Ok(_) => panic!("Shouldn't happen")
        }
    });

    println!("press enter to exit example");
    io::stdin().read_line(&mut tmp).unwrap();
}

fn main() {
    env_logger::init().unwrap();
    zk_example();
}
