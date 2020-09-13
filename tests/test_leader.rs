use uuid::Uuid;
use zookeeper::{recipes::leader::LeaderLatch, ZkResult, ZooKeeper};

use env_logger;
use std::{sync::Arc, time::Duration};
use ZkCluster;

const LATCH_PATH: &str = "/latch-test";

#[test]
fn leader_latch_test() -> ZkResult<()> {
    let _ = env_logger::try_init();

    let cluster = ZkCluster::start(1);
    let zk = Arc::new(ZooKeeper::connect(
        &cluster.connect_string,
        Duration::from_secs(30),
        |_ev| {},
    )?);

    let id1 = Uuid::new_v4().to_string();
    let latch1 = LeaderLatch::new(Arc::clone(&zk), id1, LATCH_PATH.into());

    let id2 = Uuid::new_v4().to_string();
    let latch2 = LeaderLatch::new(Arc::clone(&zk), id2, LATCH_PATH.into());

    latch1.start().unwrap();
    assert!(latch1.has_leadership());

    latch2.start().unwrap();
    assert!(!latch2.has_leadership());

    let latch1_path = latch1.path().unwrap();
    let latch2_path = latch2.path().unwrap();

    latch1.stop()?;
    assert!(zk.exists(&latch1_path, false)?.is_none());
    assert!(zk.exists(&latch2_path, false)?.is_some());

    assert!(!latch1.has_leadership());
    assert!(latch2.has_leadership());

    latch2.stop()?;
    assert!(!latch2.has_leadership());

    zk.delete(LATCH_PATH, None)?;
    Ok(())
}

#[test]
fn leader_latch_test_disconnect() -> ZkResult<()> {
    let _ = env_logger::try_init();

    let cluster = ZkCluster::start(1);
    let zk = Arc::new(ZooKeeper::connect(
        &cluster.connect_string,
        Duration::from_secs(30),
        |_ev| {},
    )?);

    let id = Uuid::new_v4().to_string();
    let latch = LeaderLatch::new(Arc::clone(&zk), id, LATCH_PATH.into());

    latch.start().unwrap();
    assert!(latch.has_leadership());

    zk.close()?;
    assert!(!latch.has_leadership());
    Ok(())
}
