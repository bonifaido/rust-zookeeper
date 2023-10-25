use zookeeper::recipes::cache::PathChildrenCache;
use zookeeper::CreateMode::*;
use zookeeper::{Acl, WatchedEvent, ZooKeeper, ZooKeeperExt};

use ZkCluster;

use env_logger;
use std::sync::Arc;
use std::time::Duration;

#[test]
fn path_children_cache_test() {
    let _ = env_logger::try_init();

    // Create a test cluster
    let cluster = ZkCluster::start(1);

    // Connect to the test cluster
    let zk = Arc::new(
        ZooKeeper::connect(
            &cluster.connect_string,
            Duration::from_secs(30),
            |event: WatchedEvent| info!("{:?}", event),
            None,
        )
        .unwrap(),
    );

    zk.ensure_path("/cache").unwrap();
    zk.create(
        "/cache/a",
        vec![1, 4],
        Acl::open_unsafe().clone(),
        Ephemeral,
    )
    .unwrap();
    zk.create(
        "/cache/b",
        vec![2, 4],
        Acl::open_unsafe().clone(),
        Ephemeral,
    )
    .unwrap();
    zk.create(
        "/cache/c",
        vec![3, 4],
        Acl::open_unsafe().clone(),
        Ephemeral,
    )
    .unwrap();

    let path_children_cache = Arc::new(PathChildrenCache::new(zk, "/cache").unwrap());

    let data = path_children_cache.get_current_data();

    info!("Data: {:?}", data);
}
