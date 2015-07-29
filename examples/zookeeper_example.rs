#![feature(duration)]
extern crate zookeeper;
#[macro_use]
extern crate log;
extern crate log4rs;

use std::time::Duration;
use std::thread;
use std::io;
use zookeeper::{CreateMode, Watcher, WatchedEvent, ZooKeeper};
use zookeeper::acls;

struct LoggingWatcher;
impl Watcher for LoggingWatcher {
    fn handle(&self, e: &WatchedEvent) {
        info!("{:?}", e)
    }
}

fn zk_example() {
    let zk = ZooKeeper::connect("localhost:2181/", Duration::from_secs(5), LoggingWatcher).unwrap();

    let mut tmp = String::new();
    println!("connecting... press any to continue");
    io::stdin().read_line(&mut tmp).unwrap();

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

    println!("press enter to close client");
    io::stdin().read_line(&mut tmp).unwrap();

    // The client can be shared between tasks
    let zk2 = zk.clone();
    thread::spawn(move || {
        zk2.close();

        // And operations return error after closed
        match zk.exists("/test", false) {
            Err(err) => println!("Usage after closed should error: {:?}", err),
            Ok(_) => panic!("Shouldn't happen")
        }
    });

    println!("press enter to exit example");
    io::stdin().read_line(&mut tmp).unwrap();
}

fn init_logging() {
    let root = log4rs::config::Root::builder(log::LogLevelFilter::Debug)
               .appender("stdout".to_string());
    let console = Box::new(log4rs::appender::ConsoleAppender::builder().build());
    let config = log4rs::config::Config::builder(root.build())
                 .appender(log4rs::config::Appender::builder("stdout".to_string(), console).build());
    log4rs::init_config(config.build().unwrap()).unwrap();
}

fn main() {
    init_logging();
    zk_example();
}
