use std::sync::Arc;
use zookeeper::ZooKeeper;

pub struct PathChildrenCache {
    zk: Arc<ZooKeeper>
}

impl PathChildrenCache {
    fn new(zk: Arc<ZooKeeper>, path: &str) -> PathChildrenCache {

        //zk.get_children(path, );

        PathChildrenCache{zk: zk}
    }
}