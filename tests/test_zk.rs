mod test;

use zookeeper_async::KeeperState;
use zookeeper_async::{Acl, CreateMode, WatchedEvent, ZooKeeper};

use test::ZkCluster;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tracing::*;

#[tokio::test]
async fn zk_test() {
    // Create a test cluster
    let mut cluster = ZkCluster::start(3);

    let disconnects = Arc::new(AtomicUsize::new(0));
    let disconnects_watcher = disconnects.clone();

    // Connect to the test cluster
    let zk = ZooKeeper::connect(
        &cluster.connect_string,
        Duration::from_secs(10),
        move |event: WatchedEvent| {
            info!("{:?}", event);
            if event.keeper_state == KeeperState::Disconnected {
                disconnects_watcher.fetch_add(1, Ordering::Relaxed);
            }
        },
    )
    .await
    .unwrap();

    // Do the tests
    let create = zk
        .create(
            "/test",
            vec![8, 8],
            Acl::open_unsafe().clone(),
            CreateMode::Ephemeral,
        )
        .await;
    assert_eq!(create.ok(), Some("/test".to_owned()));

    let exists = zk.exists("/test", true).await;
    assert!(exists.is_ok());

    // Check that during inactivity, pinging keeps alive the connection
    thread::sleep(Duration::from_secs(8));

    // Set/Get Big-Data(tm)
    let data = vec![7; 1024 * 1000];
    let set_data = zk.set_data("/test", data.clone(), None).await;
    assert!(set_data.is_ok());
    let get_data = zk.get_data("/test", false).await;
    assert!(get_data.is_ok());
    assert_eq!(get_data.unwrap().0, data);

    let children = zk.get_children("/", true).await;
    assert!(children.is_ok());

    let children = zk
        .get_children_w("/", |event: WatchedEvent| println!("Custom {:?}", event))
        .await;
    assert!(children.is_ok());
    let delete = zk.delete("/test", None).await;
    assert!(delete.is_ok());

    let mut sorted_children = children.unwrap();
    sorted_children.sort();
    assert_eq!(
        sorted_children,
        vec!["test".to_owned(), "zookeeper".to_owned()]
    );

    // Let's see what happens when the connected server goes down
    assert_eq!(disconnects.load(Ordering::Relaxed), 0);

    cluster.kill_an_instance();

    thread::sleep(Duration::from_secs(1));

    // TODO once `manual` events are possible
    // assert_eq!(disconnects.load(Ordering::Relaxed), 1);

    // After closing the client all operations return Err
    zk.close().await.unwrap();

    let exists = zk.exists("/test", true).await;
    assert!(exists.is_err());

    // Close the whole cluster
    cluster.shutdown();
}
