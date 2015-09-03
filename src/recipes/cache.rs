use std::sync::Arc;
use std::collections::HashMap;
use zookeeper::ZooKeeper;

pub struct PathChildrenCache {
    zk: Arc<ZooKeeper>,
    data: HashMap<String, Vec<u8>>
}

impl PathChildrenCache {
    fn new(zk: Arc<ZooKeeper>, path: &str) -> PathChildrenCache {

        //zk.get_children(path, );

        PathChildrenCache{zk: zk, data: HashMap::new()}
    }

    fn get_current_data(&self) -> &HashMap<String, Vec<u8>> {
        &self.data
    }
}