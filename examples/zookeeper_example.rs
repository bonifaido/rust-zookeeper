#![deny(unused_mut)]

use log::*;
use std::env;
use std::io;
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use zookeeper_async::recipes::cache::PathChildrenCache;
use zookeeper_async::{Acl, CreateMode, WatchedEvent, Watcher, ZooKeeper};

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

async fn zk_example() {
    let zk_urls = zk_server_urls();
    println!("connecting to {}", zk_urls);

    let zk = ZooKeeper::connect(&*zk_urls, Duration::from_secs(15), LoggingWatcher)
        .await
        .unwrap();

    zk.add_listener(|zk_state| println!("New ZkState is {:?}", zk_state));

    let mut tmp = String::new();

    let auth = zk.add_auth("digest", vec![1, 2, 3, 4]).await;

    println!("authenticated -> {:?}", auth);

    let path = zk
        .create(
            "/test",
            vec![1, 2],
            Acl::open_unsafe().clone(),
            CreateMode::Ephemeral,
        )
        .await;

    println!("created -> {:?}", path);

    let exists = zk.exists("/test", true).await;

    println!("exists -> {:?}", exists);

    let doesnt_exist = zk.exists("/blabla", true).await;

    println!("don't exists path -> {:?}", doesnt_exist);

    let get_acl = zk.get_acl("/test").await;

    println!("get_acl -> {:?}", get_acl);

    let set_acl = zk.set_acl("/test", Acl::open_unsafe().clone(), None).await;

    println!("set_acl -> {:?}", set_acl);

    let children = zk.get_children("/", true).await;

    println!("children of / -> {:?}", children);

    let set_data = zk.set_data("/test", vec![6, 5, 4, 3], None).await;

    println!("set_data -> {:?}", set_data);

    let get_data = zk.get_data("/test", true).await;

    println!("get_data -> {:?}", get_data);

    // let delete = zk.delete("/test", -1);

    // println!("deleted /test -> {:?}", delete);

    let watch_children = zk
        .get_children_w("/", |event: WatchedEvent| {
            println!("watched event {:?}", event);
        })
        .await;

    println!("watch children -> {:?}", watch_children);

    let zk_arc = Arc::new(zk);

    let mut pcc = PathChildrenCache::new(zk_arc.clone(), "/").await.unwrap();
    match pcc.start() {
        Err(err) => {
            println!("error starting cache: {:?}", err);
            return;
        }
        _ => {
            println!("cache started");
        }
    }

    let (ev_tx, ev_rx) = mpsc::channel();
    pcc.add_listener(move |e| ev_tx.send(e).unwrap());
    thread::spawn(move || {
        for ev in ev_rx {
            println!("received event {:?}", ev);
        }
    });

    println!("press enter to close client");
    io::stdin().read_line(&mut tmp).unwrap();

    // The client can be shared between tasks
    let zk_arc_captured = zk_arc.clone();
    tokio::spawn(async move {
        zk_arc_captured.close().await.unwrap();

        // And operations return error after closed
        match zk_arc_captured.exists("/test", false).await {
            Err(err) => println!("Usage after closed should end up with error: {:?}", err),
            Ok(_) => panic!("Shouldn't happen"),
        }
    });

    println!("press enter to exit example");
    io::stdin().read_line(&mut tmp).unwrap();

    zk_arc.close().await.unwrap();
}

#[tokio::main]
async fn main() {
    env_logger::init();
    zk_example().await;
}
