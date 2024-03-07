use crate::test::ZkCluster;
use std::time::Duration;
use tracing::info;
use zookeeper_async::{Acl, CreateMode, WatchedEvent, Watcher, ZooKeeper};

mod test;

struct LogWatcher;

impl Watcher for LogWatcher {
    fn handle(&self, event: WatchedEvent) {
        info!("{:?}", event);
    }
}

async fn create_zk(connection_string: &str) -> ZooKeeper {
    ZooKeeper::connect(connection_string, Duration::from_secs(10), LogWatcher)
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
    assert_eq!(
        create,
        Ok("/test".to_owned()),
        "create failed: {:?}",
        create
    );

    // We drop connection here to make sure that the znode is deleted by the zookeeper server
    zk.close().await.unwrap();
    drop(zk);

    // Wait for the znode to be deleted
    tokio::time::sleep(Duration::from_secs(60)).await;

    // Reconnect to the test cluster
    let zk = create_zk(&cluster.connect_string).await;

    let exists = zk.exists("/test", false).await;
    assert!(exists.is_ok(), "exists failed: {:?}", exists);
    assert!(exists.unwrap().is_none(), "value should not be exist");

    cluster.kill_an_instance();

    // After closing the client all operations return Err
    zk.close().await.unwrap();
}

#[tokio::test]
async fn zk_create2_test() {
    // Create a test cluster
    let mut cluster = ZkCluster::start(3);

    // Connect to the test cluster
    let zk = create_zk(&cluster.connect_string).await;

    // Do the tests
    let create = zk
        .create2(
            "/create2_test",
            vec![8, 8],
            Acl::open_unsafe().clone(),
            CreateMode::Persistent,
        )
        .await;
    match create {
        Ok((path, _stat)) if path == "/create2_test" => {}
        Ok((path, _stat)) => {
            println!("create2 failed: {:?}", path);
            assert!(false);
        }
        Err(e) => {
            println!("create2 failed: {:?}", e);
            assert!(false);
        }
    }

    let exists = zk.exists("/create2_test", false).await;
    assert!(exists.is_ok(), "exists failed: {:?}", exists);
    assert!(exists.unwrap().is_some(), "value should exist");

    cluster.kill_an_instance();

    // After closing the client all operations return Err
    zk.close().await.unwrap();
}

#[tokio::test]
async fn zk_create2_sequential_test() {
    // Create a test cluster
    let mut cluster = ZkCluster::start(3);

    // Connect to the test cluster
    let zk = create_zk(&cluster.connect_string).await;

    // Do the tests
    let create = zk
        .create2(
            "/create2_test",
            vec![8, 8],
            Acl::open_unsafe().clone(),
            CreateMode::PersistentSequential,
        )
        .await;
    match create {
        Ok((path, _stat)) if path == "/create2_test0000000000" => {}
        Ok((path, _stat)) => {
            println!("create2 failed: {:?}", path);
            assert!(false);
        }
        Err(e) => {
            println!("create2 failed: {:?}", e);
            assert!(false);
        }
    }

    let exists = zk.exists("/create2_test0000000000", false).await;
    assert!(exists.is_ok(), "exists failed: {:?}", exists);
    assert!(exists.unwrap().is_some(), "value should exist");

    cluster.kill_an_instance();

    // After closing the client all operations return Err
    zk.close().await.unwrap();
}

#[tokio::test]
async fn zk_create2_with_ttl_test() {
    // Create a test cluster
    let mut cluster = ZkCluster::start(3);

    // Connect to the test cluster
    let zk = create_zk(&cluster.connect_string).await;

    // Do the tests
    let create = zk
        .create2_ttl(
            "/create2_test",
            vec![8, 8],
            Acl::open_unsafe().clone(),
            CreateMode::PersistentWithTTL,
            Duration::from_millis(100),
        )
        .await;
    match create {
        Ok((path, _stat)) if path == "/create2_test" => {}
        Ok((path, _stat)) => {
            println!("create2 failed: {:?}", path);
            assert!(false);
        }
        Err(e) => {
            println!("create2 failed: {:?}", e);
            assert!(false);
        }
    }

    // We drop connection here to make sure that the znode is deleted by the zookeeper server
    zk.close().await.unwrap();
    drop(zk);

    // Wait for the znode to be deleted
    tokio::time::sleep(Duration::from_secs(60)).await;

    // Reconnect to the test cluster
    let zk = create_zk(&cluster.connect_string).await;

    let exists = zk.exists("/create2_test", false).await;
    assert!(exists.is_ok(), "exists failed: {:?}", exists);
    assert!(exists.unwrap().is_none(), "value should not be exist");

    cluster.kill_an_instance();

    // After closing the client all operations return Err
    zk.close().await.unwrap();
}
