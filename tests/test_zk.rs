use zookeeper::{CreateMode, WatchedEvent, ZooKeeper};
use zookeeper::acls;
use zookeeper::KeeperState;

use ZkCluster;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use std::thread;
use env_logger;


#[test]
fn zk_test() {
    let _ = env_logger::init();

    // Create a test cluster
    let mut cluster = ZkCluster::start(3);

    let disconnects = Arc::new(AtomicUsize::new(0));
    let disconnects_watcher = disconnects.clone();

    // Connect to the test cluster
    let zk = ZooKeeper::connect(&cluster.connect_string,
                                    Duration::from_secs(5),
                                    move |event: &WatchedEvent| {
                                        info!("{:?}", event);
                                        if event.keeper_state == KeeperState::Disconnected {
                                            disconnects_watcher.fetch_add(1, Ordering::Relaxed);
                                        }
                                    }).unwrap();


    // Do the tests
    let create = zk.create("/test", vec![8,8], acls::OPEN_ACL_UNSAFE.clone(), CreateMode::Ephemeral);
    assert_eq!(create.ok(), Some("/test".to_owned()));


    let exists = zk.exists("/test", true);
    assert!(exists.is_ok());


    // Check that during inactivity, pinging keeps alive the connection
    thread::sleep_ms(8000);


    // Set/Get Big-Data(tm)
    let data = vec![7; 1024 * 1000];
    let set_data = zk.set_data("/test", data.clone(), -1);
    assert!(set_data.is_ok());
    let get_data = zk.get_data("/test", false);
    assert!(get_data.is_ok());
    assert_eq!(get_data.unwrap().0, data);

    let children = zk.get_children("/", true);
    assert!(children.is_ok());

    let children = zk.get_children_w("/", |event: &WatchedEvent| println!("Custom {:?}", event));
    assert!(children.is_ok());
    let delete = zk.delete("/test", -1);
    assert!(delete.is_ok());


    let mut sorted_children = children.unwrap();
    sorted_children.sort();
    assert_eq!(sorted_children, vec!["test".to_owned(), "zookeeper".to_owned()]);


    // Let's see what happens when the connected server goes down
    assert_eq!(disconnects.load(Ordering::Relaxed), 0);

    cluster.kill_an_instance();

    thread::sleep_ms(1000);

    // TODO once `manual` events are possible
    // assert_eq!(disconnects.load(Ordering::Relaxed), 1);


    // After closing the client all operations return Err
    zk.close().unwrap();

    let exists = zk.exists("/test", true);
    assert!(exists.is_err());


    // Close the whole cluster
    cluster.shutdown();
}
