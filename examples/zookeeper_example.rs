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
            let acl1 = vec![Acl{perms: perms::ALL, scheme: "world".to_string(), id: "anyone".to_string()}];
            let acl2 = vec![Acl{perms: perms::ALL, scheme: "world".to_string(), id: "anyone".to_string()}];

            let auth = zk.add_auth("digest".to_string(), vec![1,2,3,4]);

            println!("authenticated -> {}", auth);

            let path = zk.create("/test".to_string(), vec![1,2], acl1, Ephemeral);

            println!("created -> {}", path);

            let exists = zk.exists("/test".to_string(), true);

            println!("exists -> {}", exists);

            let dont_exists = zk.exists("/blabla".to_string(), true);

            println!("don't exists path -> {}", dont_exists);

            let get_acl = zk.get_acl("/test".to_string());

            println!("get_acl -> {}", get_acl);

            let set_acl = zk.set_acl("/test".to_string(), acl2, -1);

            println!("set_acl -> {}", set_acl);

            let children = zk.get_children("/".to_string(), true);

            println!("children of / -> {}", children);

            let data = zk.get_data("/test".to_string(), true);

            println!("data -> {}", data);

            let delete = zk.delete("/test".to_string(), -1);

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
