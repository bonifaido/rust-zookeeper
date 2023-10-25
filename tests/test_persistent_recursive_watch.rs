use zookeeper::{Acl, AddWatchMode, CreateMode, WatcherType};
use zookeeper::{WatchedEvent, ZooKeeper, ZooKeeperExt};

use ZkCluster;

use env_logger;
use std::sync::mpsc;
use std::time::Duration;

#[test]
fn persistent_watch_receives_more_than_one_message_on_modifications() {
    let _ = env_logger::try_init();

    // Create a test cluster
    let cluster = ZkCluster::start(1);

    let zk_watcher = ZooKeeper::connect(
        &cluster.connect_string,
        Duration::from_secs(30),
        |event: WatchedEvent| info!("{:?}", event),
        None,
    )
    .unwrap();

    let zk_modifier = ZooKeeper::connect(
        &cluster.connect_string,
        Duration::from_secs(30),
        |event: WatchedEvent| info!("{:?}", event),
        None,
    )
    .unwrap();

    zk_modifier.ensure_path("/base").unwrap();

    let (snd, rcv) = mpsc::channel::<()>();
    zk_watcher
        .add_watch("/base", AddWatchMode::Persistent, move |_| {
            snd.send(()).unwrap();
        })
        .unwrap();
    zk_modifier
        .set_data("/base", b"hello1".to_vec(), None)
        .unwrap();
    rcv.recv_timeout(Duration::from_millis(100)).unwrap();
    zk_modifier
        .set_data("/base", b"hello2".to_vec(), None)
        .unwrap();
    rcv.recv_timeout(Duration::from_millis(100)).unwrap();
}

#[test]
fn persistent_watch_does_not_receive_children_changes() {
    let _ = env_logger::try_init();

    // Create a test cluster
    let cluster = ZkCluster::start(1);

    let zk_watcher = ZooKeeper::connect(
        &cluster.connect_string,
        Duration::from_secs(30),
        |event: WatchedEvent| info!("{:?}", event),
        None,
    )
    .unwrap();

    let zk_modifier = ZooKeeper::connect(
        &cluster.connect_string,
        Duration::from_secs(30),
        |event: WatchedEvent| info!("{:?}", event),
        None,
    )
    .unwrap();

    zk_modifier.ensure_path("/base").unwrap();
    zk_modifier.ensure_path("/base/child").unwrap();

    let (snd, rcv) = mpsc::channel::<()>();
    zk_watcher
        .add_watch("/base", AddWatchMode::Persistent, move |_| {
            snd.send(()).unwrap();
        })
        .unwrap();
    zk_modifier
        .set_data("/base", b"hello1".to_vec(), None)
        .unwrap();
    rcv.recv_timeout(Duration::from_millis(100)).unwrap();
    zk_modifier
        .set_data("/base/child", b"hello2".to_vec(), None)
        .unwrap();
    if rcv.recv_timeout(Duration::from_millis(100)).is_ok() {
        panic!("received unexpected event for child");
    }
}

#[test]
fn persistent_recursive_watch_stops_receiving_updates_when_removed() {
    let _ = env_logger::try_init();

    // Create a test cluster
    let cluster = ZkCluster::start(1);

    let zk_watcher = ZooKeeper::connect(
        &cluster.connect_string,
        Duration::from_secs(30),
        |event: WatchedEvent| info!("{:?}", event),
        None,
    )
    .unwrap();

    let zk_modifier = ZooKeeper::connect(
        &cluster.connect_string,
        Duration::from_secs(30),
        |event: WatchedEvent| info!("{:?}", event),
        None,
    )
    .unwrap();

    zk_modifier.ensure_path("/base").unwrap();

    let (snd, rcv) = mpsc::channel::<()>();
    zk_watcher
        .add_watch("/base", AddWatchMode::PersistentRecursive, move |_| {
            snd.send(()).unwrap();
        })
        .unwrap();
    zk_modifier
        .create(
            "/base/child1",
            b"hello2".to_vec(),
            Acl::open_unsafe().clone(),
            CreateMode::Persistent,
        )
        .unwrap();
    rcv.recv_timeout(Duration::from_millis(100)).unwrap();
    zk_watcher
        .remove_watches("/base", WatcherType::Any)
        .unwrap();
    zk_modifier
        .create(
            "/base/child2",
            b"hello2".to_vec(),
            Acl::open_unsafe().clone(),
            CreateMode::Persistent,
        )
        .unwrap();
    if rcv.recv_timeout(Duration::from_millis(100)).is_ok() {
        panic!("received unexpected event for child");
    }
}

#[test]
fn persistent_recursive_watch_receive_children_changes() {
    let _ = env_logger::try_init();

    // Create a test cluster
    let cluster = ZkCluster::start(1);

    let zk_watcher = ZooKeeper::connect(
        &cluster.connect_string,
        Duration::from_secs(30),
        |event: WatchedEvent| info!("{:?}", event),
        None,
    )
    .unwrap();

    let zk_modifier = ZooKeeper::connect(
        &cluster.connect_string,
        Duration::from_secs(30),
        |event: WatchedEvent| info!("{:?}", event),
        None,
    )
    .unwrap();

    zk_modifier.ensure_path("/base").unwrap();

    let (snd, rcv) = mpsc::channel::<()>();
    zk_watcher
        .add_watch("/base", AddWatchMode::PersistentRecursive, move |_| {
            snd.send(()).unwrap();
        })
        .unwrap();
    zk_modifier
        .create(
            "/base/child1",
            b"hello2".to_vec(),
            Acl::open_unsafe().clone(),
            CreateMode::Persistent,
        )
        .unwrap();
    rcv.recv_timeout(Duration::from_millis(100)).unwrap();
    zk_modifier
        .create(
            "/base/child2",
            b"hello2".to_vec(),
            Acl::open_unsafe().clone(),
            CreateMode::Persistent,
        )
        .unwrap();
    rcv.recv_timeout(Duration::from_millis(100)).unwrap();
}
