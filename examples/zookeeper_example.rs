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
use zookeeper::{CreateMode, Watcher, WatchedEvent, ZooKeeper};
use zookeeper::acls;

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
    let zk = ZooKeeper::connect(&*zk_server_urls(), Duration::from_secs(5), LoggingWatcher).unwrap();

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

    let watch_children = zk.get_children_w("/", LoggingWatcher);
    println!("watch children -> {:?}", watch_children);
    
    println!("press enter to close client");
    io::stdin().read_line(&mut tmp).unwrap();

    // The client can be shared between tasks
    let zk = Arc::new(zk);
    thread::spawn(move || {
        zk.close().unwrap();

        // And operations return error after closed
        match zk.exists("/test", false) {
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
