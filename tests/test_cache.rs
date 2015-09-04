use zookeeper::CreateMode::*;
use zookeeper::{WatchedEvent, ZooKeeper};
use zookeeper::acls::*;
use zookeeper::recipes::cache::PathChildrenCache;

use ZkCluster;

use std::sync::Arc;
use std::time::Duration;
use std::thread;
use env_logger;


#[test]
fn path_children_cache_test() {
    let _ = env_logger::init();

    // Create a test cluster
    //let cluster = ZkCluster::start(3);

    // Connect to the test cluster
    let zk = Arc::new(ZooKeeper::connect("localhost:2181", //&cluster.connect_string,
                                         Duration::from_secs(30),
                                         |event: &WatchedEvent| info!("{:?}", event)).unwrap());

    zk.create("/cache/a", vec![1,4], OPEN_ACL_UNSAFE.clone(), Ephemeral).unwrap();
    zk.create("/cache/b", vec![2,4], OPEN_ACL_UNSAFE.clone(), Ephemeral).unwrap();
    zk.create("/cache/c", vec![3,4], OPEN_ACL_UNSAFE.clone(), Ephemeral).unwrap();

    let path_children_cache = Arc::new(PathChildrenCache::new(zk, "/cache").unwrap());

    let data = path_children_cache.get_current_data();

    info!("Data: {:?}", data);

    thread::sleep_ms(9999999);
}