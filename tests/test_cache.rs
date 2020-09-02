mod test;

use test::ZkCluster;
use zookeeper_async::recipes::cache::PathChildrenCache;
use zookeeper_async::CreateMode::*;
use zookeeper_async::{Acl, WatchedEvent, ZooKeeper, ZooKeeperExt};

use env_logger;
use std::sync::Arc;
use std::time::Duration;
use tracing::*;

#[tokio::test]
async fn path_children_cache_test() {
    let _ = env_logger::try_init();

    // Create a test cluster
    let cluster = ZkCluster::start(1);

    // Connect to the test cluster
    let zk = Arc::new(
        ZooKeeper::connect(
            &cluster.connect_string,
            Duration::from_secs(30),
            |event: WatchedEvent| info!("{:?}", event),
        )
        .await
        .unwrap(),
    );

    zk.ensure_path("/cache").await.unwrap();
    zk.create(
        "/cache/a",
        vec![1, 4],
        Acl::open_unsafe().clone(),
        Ephemeral,
    )
    .await
    .unwrap();
    zk.create(
        "/cache/b",
        vec![2, 4],
        Acl::open_unsafe().clone(),
        Ephemeral,
    )
    .await
    .unwrap();
    zk.create(
        "/cache/c",
        vec![3, 4],
        Acl::open_unsafe().clone(),
        Ephemeral,
    )
    .await
    .unwrap();

    let path_children_cache = Arc::new(PathChildrenCache::new(zk.clone(), "/cache").await.unwrap());

    let data = path_children_cache.get_current_data().await;

    info!("Data: {:?}", data);

    zk.close().await.unwrap();
}
