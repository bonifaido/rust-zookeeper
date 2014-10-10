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
    match ZooKeeper::new("127.0.0.1:2181", Duration::seconds(5), LoggingWatcher) {
        Ok(zk) => {
            let zk2 = zk.clone();

            let path = zk.create("/test".to_string(), vec![], vec![Acl{perms: perms::ALL, scheme: "world".to_string(), id: "anyone".to_string()}], Ephemeral);

            println!("created path -> {}", path);

            let children = zk.get_children("/".to_string(), true);

            println!("children of / -> {}", children);

            let ok = zk.delete("/test".to_string(), -1);

            println!("deleted path /test {}", ok);

            std::io::stdin().read_line();

            // Showing thet this client can be shared between tasks
            spawn(proc() {
                zk2.close();                
            })
        },
        Err(error) => {
            println!("Error connecting to ZooKeeper: {}", error)
        }
    }
}
