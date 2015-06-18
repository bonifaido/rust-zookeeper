#![feature(duration)]
extern crate zookeeper;

use std::time::Duration;
use std::thread;
use std::io;
use zookeeper::{Acl, CreateMode, Watcher, WatchedEvent, ZkResult, ZooKeeper};
use zookeeper::perms;

struct LoggingWatcher;
impl Watcher for LoggingWatcher {
    fn handle(&self, e: &WatchedEvent) {
        println!("{:?}", e)
    }
}

fn zk_example() -> ZkResult<()> {
    let zk = try!(ZooKeeper::connect("localhost:2181", Duration::from_secs(5), LoggingWatcher));

    let mut tmp = String::new();
    println!("connecting... press any to continue");
    io::stdin().read_line(&mut tmp).unwrap();

    let acl1 = vec![Acl{perms: perms::ALL, scheme: "world".to_string(), id: "anyone".to_string()}];
    let acl2 = vec![Acl{perms: perms::ALL, scheme: "world".to_string(), id: "anyone".to_string()}];

    let auth = zk.add_auth("digest", vec![1,2,3,4]);

    println!("authenticated -> {:?}", auth);

    let path = zk.create("/test", vec![1,2], acl1, CreateMode::Ephemeral);

    println!("created -> {:?}", path);

    let exists = zk.exists("/test", true);

    println!("exists -> {:?}", exists);

    let dont_exists = zk.exists("/blabla", true);

    println!("don't exists path -> {:?}", dont_exists);

    let get_acl = zk.get_acl("/test");

    println!("get_acl -> {:?}", get_acl);

    let set_acl = zk.set_acl("/test", acl2, -1);

    println!("set_acl -> {:?}", set_acl);

    let children = zk.get_children("/", true);

    println!("children of / -> {:?}", children);

    let set_data = zk.set_data("/test", vec![6,5,4,3], -1);

    println!("set_data -> {:?}", set_data);

    let get_data = zk.get_data("/test", true);

    println!("get_data -> {:?}", get_data);

    let delete = zk.delete("/test", -1);

    println!("deleted /test -> {:?}", delete);

    println!("press enter to close client");
    io::stdin().read_line(&mut tmp).unwrap();

    // The client can be shared between tasks
    let zk2 = zk.clone();
    thread::spawn(move || {
        zk2.close();

        // And operations return error after closed
        match zk.exists("/test", false) {
            Err(err) => println!("Usage after closed: {:?}", err),
            Ok(_) => panic!("Shouldn't happen")
        }
    });

    println!("press enter to exit example");
    io::stdin().read_line(&mut tmp).unwrap();

    Ok(())
}

fn main() {
    zk_example().unwrap();
}
