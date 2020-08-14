mod test;

use std::iter::once;
use std::time::Duration;
use test::ZkCluster;
use zookeeper::{WatchedEvent, ZkError, ZooKeeper, ZooKeeperExt};

#[tokio::test]
async fn get_children_recursive_test() {
    // Create a test cluster
    let cluster = ZkCluster::start(1);

    // Connect to the test cluster
    let zk = ZooKeeper::connect(
        &cluster.connect_string,
        Duration::from_secs(30),
        |_: WatchedEvent| {},
    )
    .await
    .unwrap();

    let tree = vec![
        "/root/a/1",
        "/root/a/2",
        "/root/a/3",
        "/root/b/1",
        "/root/b/2",
        "/root/b/3",
        "/root/c/1",
        "/root/c/2",
        "/root/c/3",
    ];

    for path in tree.iter() {
        zk.ensure_path(path).await.unwrap();
    }

    let children = zk.get_children_recursive("/root").await.unwrap();
    for path in tree {
        for (i, _) in path
            .chars()
            .chain(once('/'))
            .enumerate()
            .skip(1)
            .filter(|c| c.1 == '/')
        {
            assert!(children.contains(&path[..i].to_string()));
        }
    }
}

#[tokio::test]
async fn get_children_recursive_invalid_path_test() {
    // Create a test cluster
    let cluster = ZkCluster::start(1);

    // Connect to the test cluster
    let zk = ZooKeeper::connect(
        &cluster.connect_string,
        Duration::from_secs(30),
        |_: WatchedEvent| {},
    )
    .await
    .unwrap();

    let result = zk.get_children_recursive("/bad").await;
    assert_eq!(result, Err(ZkError::NoNode))
}

#[tokio::test]
async fn get_children_recursive_only_root_test() {
    // Create a test cluster
    let cluster = ZkCluster::start(1);

    // Connect to the test cluster
    let zk = ZooKeeper::connect(
        &cluster.connect_string,
        Duration::from_secs(30),
        |_: WatchedEvent| {},
    )
    .await
    .unwrap();

    let root = "/root";
    zk.ensure_path(root).await.unwrap();
    let result = zk.get_children_recursive(root).await.unwrap();
    assert_eq!(result, vec![root]);
}

#[tokio::test]
async fn delete_recursive_test() {
    // Create a test cluster
    let cluster = ZkCluster::start(1);

    // Connect to the test cluster
    let zk = ZooKeeper::connect(
        &cluster.connect_string,
        Duration::from_secs(30),
        |_: WatchedEvent| {},
    )
    .await
    .unwrap();

    let tree = vec![
        "/root/a/1",
        "/root/a/2",
        "/root/a/3",
        "/root/b/1",
        "/root/b/2",
        "/root/b/3",
        "/root/c/1",
        "/root/c/2",
        "/root/c/3",
    ];

    for path in tree.iter() {
        zk.ensure_path(path).await.unwrap();
    }

    zk.delete_recursive("/root/a").await.unwrap();
    assert!(zk.exists("/root/a", false).await.unwrap().is_none());
    assert!(zk.exists("/root/b", false).await.unwrap().is_some());

    zk.close().await.unwrap();
}

#[tokio::test]
async fn delete_recursive_invalid_path_test() {
    // Create a test cluster
    let cluster = ZkCluster::start(1);

    // Connect to the test cluster
    let zk = ZooKeeper::connect(
        &cluster.connect_string,
        Duration::from_secs(30),
        |_: WatchedEvent| {},
    )
    .await
    .unwrap();

    let result = zk.delete_recursive("/bad").await;
    assert_eq!(result, Err(ZkError::NoNode));

    zk.close().await.unwrap();
}
