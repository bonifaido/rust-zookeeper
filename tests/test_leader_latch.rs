mod test;

use std::{sync::Arc, thread, time::Duration};
use test::ZkCluster;
use uuid::Uuid;
use zookeeper_async::{recipes::leader_latch::LeaderLatch, ZkResult, ZooKeeper};

#[tokio::test]
async fn leader_latch_test() -> ZkResult<()> {
    let latch_path = "/latch-test";

    let cluster = ZkCluster::start(1);
    let zk = Arc::new(
        ZooKeeper::connect(&cluster.connect_string, Duration::from_secs(30), |_ev| {}).await?,
    );

    let id1 = Uuid::new_v4().to_string();
    let latch1 = LeaderLatch::new(Arc::clone(&zk), id1, latch_path.into());

    let id2 = Uuid::new_v4().to_string();
    let latch2 = LeaderLatch::new(Arc::clone(&zk), id2, latch_path.into());

    latch1.start().await.unwrap();
    assert!(latch1.has_leadership());

    latch2.start().await.unwrap();
    assert!(!latch2.has_leadership());

    let latch1_path = latch1.path().await.unwrap();
    let latch2_path = latch2.path().await.unwrap();

    latch1.stop().await?;
    assert!(zk.exists(&latch1_path, false).await?.is_none());
    assert!(zk.exists(&latch2_path, false).await?.is_some());

    assert!(!latch1.has_leadership());

    // Need to wait for leadership to propogate to latch2.
    thread::sleep(Duration::from_secs(1));
    assert!(latch2.has_leadership());

    latch2.stop().await?;
    assert!(!latch2.has_leadership());

    zk.delete(latch_path, None).await?;
    zk.close().await
}

#[tokio::test]
async fn leader_latch_test_disconnect() -> ZkResult<()> {
    let latch_path = "/latch-test-disconnect";

    let cluster = ZkCluster::start(1);
    let zk = Arc::new(
        ZooKeeper::connect(&cluster.connect_string, Duration::from_secs(30), |_ev| {}).await?,
    );

    let id = Uuid::new_v4().to_string();
    let latch = LeaderLatch::new(Arc::clone(&zk), id, latch_path.into());

    latch.start().await.unwrap();
    assert!(latch.has_leadership());

    zk.close().await?;
    assert!(!latch.has_leadership());
    Ok(())
}
