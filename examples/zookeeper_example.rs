extern crate zookeeper;

use std::time::Duration;
use zookeeper::{Acl, Ephemeral, Watcher, WatchedEvent, ZooKeeper};
use zookeeper::perms;

struct LoggingWatcher;
impl Watcher for LoggingWatcher {
    fn handle(&self, e: &WatchedEvent) {
        println!("{}", e)
    }
}

fn main() {
    match ZooKeeper::connect("127.0.0.1:2181", Duration::seconds(5), LoggingWatcher) {
        Ok(zk) => {
            let acl1 = vec![Acl{perms: perms::ALL, scheme: "world".to_string(), id: "anyone".to_string()}];
            let acl2 = vec![Acl{perms: perms::ALL, scheme: "world".to_string(), id: "anyone".to_string()}];

            let auth = zk.add_auth("digest", vec![1,2,3,4]);

            println!("authenticated -> {}", auth);

            let path = zk.create("/test", vec![1,2], acl1, Ephemeral);

            println!("created -> {}", path);

            let exists = zk.exists("/test", true);

            println!("exists -> {}", exists);

            let dont_exists = zk.exists("/blabla", true);

            println!("don't exists path -> {}", dont_exists);

            let get_acl = zk.get_acl("/test");

            println!("get_acl -> {}", get_acl);

            let set_acl = zk.set_acl("/test", acl2, -1);

            println!("set_acl -> {}", set_acl);

            let children = zk.get_children("/", true);

            println!("children of / -> {}", children);

            let set_data = zk.set_data("/test", vec![6,5,4,3], -1);

            println!("set_data -> {}", set_data);

            let get_data = zk.get_data("/test", true);

            println!("get_data -> {}", get_data);

            let delete = zk.delete("/test", -1);

            println!("deleted /test -> {}", delete);

            std::io::stdin().read_line();

            // Showing thet this client can be shared between tasks
            let zk2 = zk.clone();
            spawn(proc() {
                zk2.close();                
            })
        },
        Err(error) => {
            println!("Error connecting to ZooKeeper: {}", error)
        }
    }
}
