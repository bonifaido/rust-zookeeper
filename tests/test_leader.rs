use uuid::Uuid;
use zookeeper::{recipes::leader::LeaderLatch, ZkResult, ZooKeeper};
use env_logger;
use std::{sync::Arc, time::Duration, thread};
use ZkCluster;

#[test]
fn leader_latch_test() -> ZkResult<()> {
    let _ = env_logger::try_init();
    let latch_path = "/latch-test";

    let cluster = ZkCluster::start(1);
    let zk = Arc::new(ZooKeeper::connect(
        &cluster.connect_string,
        Duration::from_secs(30),
        |_ev| {},
    )?);

    let id1 = Uuid::new_v4().to_string();
    let latch1 = LeaderLatch::new(Arc::clone(&zk), id1, latch_path.into());

    let id2 = Uuid::new_v4().to_string();
    let latch2 = LeaderLatch::new(Arc::clone(&zk), id2, latch_path.into());

    latch1.start().unwrap();
    assert!(latch1.has_leadership().unwrap());

    latch2.start().unwrap();
    assert!(!latch2.has_leadership().unwrap());

    let latch1_path = latch1.path().unwrap();
    let latch2_path = latch2.path().unwrap();

    latch1.stop()?;
    assert!(zk.exists(&latch1_path, false)?.is_none());
    assert!(zk.exists(&latch2_path, false)?.is_some());

    assert!(!latch1.has_leadership().unwrap());

    // Need to wait for leadership to propogate to latch2.
    thread::sleep(Duration::from_secs(1));
    assert!(latch2.has_leadership().unwrap());

    latch2.stop()?;
    assert!(!latch2.has_leadership().unwrap());

    zk.delete(latch_path, None)?;
    Ok(())
}

#[test]
fn leader_latch_test_disconnect() -> ZkResult<()> {
    let _ = env_logger::try_init();
    let latch_path = "/latch-test-disconnect";

    let cluster = ZkCluster::start(1);
    let zk = Arc::new(ZooKeeper::connect(
        &cluster.connect_string,
        Duration::from_secs(30),
        |_ev| {},
    )?);

    let id = Uuid::new_v4().to_string();
    let latch = LeaderLatch::new(Arc::clone(&zk), id, latch_path.into());

    latch.start().unwrap();
    assert!(latch.has_leadership().unwrap());

    zk.close()?;
    assert!(!latch.has_leadership().unwrap());
    Ok(())
}
