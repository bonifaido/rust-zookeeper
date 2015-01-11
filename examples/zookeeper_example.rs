extern crate zookeeper;

use std::time::Duration;
use std::thread::Thread;
use zookeeper::{Acl, CreateMode, Watcher, WatchedEvent, ZkResult, ZooKeeper};
use zookeeper::perms;

struct LoggingWatcher;
impl Watcher for LoggingWatcher {
    fn handle(&self, e: &WatchedEvent) {
        println!("{:?}", e)
    }
}

fn zk_example() -> ZkResult<()> {
    let zk = try!(ZooKeeper::connect("127.0.0.1:2181", Duration::seconds(5), LoggingWatcher));

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
    std::io::stdin().read_line();

    // The client can be shared between tasks
    let zk2 = zk.clone();
    Thread::spawn(move || {
        zk2.close();
    });

    println!("press enter to exit example");
    std::io::stdin().read_line();

    Ok(())
}

fn main() {
    zk_example().unwrap();
}
