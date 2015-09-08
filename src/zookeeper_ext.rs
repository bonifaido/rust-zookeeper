use acls::*;
use consts::{CreateMode, ZkError};
use zookeeper::{ZkResult, ZooKeeper};
use std::iter::once;

pub trait ZooKeeperExt {
    fn ensure_path(&self, path: &str) -> ZkResult<()>;
}

impl ZooKeeperExt for ZooKeeper {
    fn ensure_path(&self, path: &str) -> ZkResult<()> {
        for (i, _) in path.chars().chain(once('/')).enumerate().skip(1).filter(|c| c.1 == '/') {
            match self.create(&path[..i], vec![], OPEN_ACL_UNSAFE.clone(), CreateMode::Persistent) {
                Ok(_) | Err(ZkError::NodeExists) => {},
                Err(e) => return Err(e)
            }
        }
        Ok(())
    }
}
