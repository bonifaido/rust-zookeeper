use acl::*;
use consts::{CreateMode, ZkError};
use zookeeper::{ZkResult, ZooKeeper};
use std::iter::once;

/// Extended ZooKeeper operations that are not needed for the "core."
pub trait ZooKeeperExt {
    /// Ensure that `path` exists and create all potential paths leading up to it if it does not.
    /// This operates in a manner similar to `mkdir -p`.
    fn ensure_path(&self, path: &str) -> ZkResult<()>;
}

impl ZooKeeperExt for ZooKeeper {
    fn ensure_path(&self, path: &str) -> ZkResult<()> {
        for (i, _) in path.chars()
                          .chain(once('/'))
                          .enumerate()
                          .skip(1)
                          .filter(|c| c.1 == '/') {
            match self.create(&path[..i],
                              vec![],
                              Acl::open_unsafe().clone(),
                              CreateMode::Persistent) {
                Ok(_) | Err(ZkError::NodeExists) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
}
