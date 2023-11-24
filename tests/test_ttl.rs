use std::time::Duration;
use tracing::info;
use zookeeper_async::{Acl, CreateMode, WatchedEvent, Watcher, ZooKeeper};
use crate::test::ZkCluster;

mod test;

struct LogWatcher;

impl Watcher for LogWatcher {
    fn handle(&self, event: WatchedEvent) {
        info!("{:?}", event);
    }
}

async fn create_zk(connection_string: &str) -> ZooKeeper {
    ZooKeeper::connect(
        connection_string,
        Duration::from_secs(10),
        LogWatcher,
    )
        .await
        .unwrap()
}

#[tokio::test]
async fn zk_ttl_test() {
    // Create a test cluster
    let mut cluster = ZkCluster::start(3);

    // Connect to the test cluster
    let zk = create_zk(&cluster.connect_string).await;

    // Do the tests
    let create = zk
        .create_ttl(
            "/test",
            vec![8, 8],
            Acl::open_unsafe().clone(),
            CreateMode::PersistentWithTTL,
            Duration::from_millis(100),
        )
        .await;
    assert_eq!(create, Ok("/test".to_owned()), "create failed: {:?}", create);

    // We drop connection here to make sure that the znode is deleted by the zookeeper server
    zk.close().await.unwrap();
    drop(zk);

    tokio::time::sleep(Duration::from_secs(30)).await;

    // Reconnect to the test cluster
    let zk = create_zk(&cluster.connect_string).await;

    let exists = zk.exists("/test", false).await;
    assert!(exists.is_ok(), "exists failed: {:?}", exists);
    assert!(exists.unwrap().is_some(), "value should not be exist"); // << should fail here

    let create2 = zk.create2(
        "/test2",
        vec![8, 8],
        Acl::open_unsafe().clone(),
        CreateMode::Persistent,
    ).await;

    println!("create2: {:?}", create2);

    let create2_ttl = zk.create2_ttl(
        "/test3",
        vec![8, 8],
        Acl::open_unsafe().clone(),
        CreateMode::PersistentWithTTL,
        Duration::from_millis(100),
    ).await;
    println!("create2_ttl: {:?}", create2_ttl);

    cluster.kill_an_instance();

    // After closing the client all operations return Err
    zk.close().await.unwrap();
}