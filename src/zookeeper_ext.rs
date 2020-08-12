use std::iter::once;
use std::collections::VecDeque;
use log::*;

use crate::{ZkResult, ZooKeeper, Acl, CreateMode, ZkError};

/// Extended ZooKeeper operations that are not needed for the "core."
pub trait ZooKeeperExt {
    /// Ensure that `path` exists and create all potential paths leading up to it if it does not.
    /// This operates in a manner similar to `mkdir -p`.
    fn ensure_path(&self, path: &str) -> ZkResult<()>;

    /// Performs a breadth-first tree traversal of the tree starting at `path`,
    /// returning a list of fully prefixed child nodes.
    /// *NOTE*: This is not an atomic operation.
    fn get_children_recursive(&self, path: &str) -> ZkResult<Vec<String>>;

    /// Deletes the node at `path` and all its children.
    /// *NOTE*: This is not an atomic operation.
    fn delete_recursive(&self, path: &str) -> ZkResult<()>;
}

impl ZooKeeperExt for ZooKeeper {
    fn ensure_path(&self, path: &str) -> ZkResult<()> {
        trace!("ensure_path {}", path);
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

    fn get_children_recursive(&self, path: &str) -> ZkResult<Vec<String>> {
        trace!("get_children_recursive {}", path);
        let mut queue: VecDeque<String> = VecDeque::new();
        let mut result = vec![path.to_string()];
        queue.push_front(path.to_string());

        while let Some(current) = queue.pop_front() {
            let children = self.get_children(&current, false)?;
            children
                .into_iter()
                .map(|child| format!("{}/{}", current, child))
                .for_each(|full_path| {
                    result.push(full_path.clone());
                    queue.push_back(full_path);
                });
        }

        Ok(result)
    }

    fn delete_recursive(&self, path: &str) -> ZkResult<()> {
        trace!("delete_recursive {}", path);
        let children = self.get_children_recursive(path)?;
        for child in children.iter().rev() {
            self.delete(child, None)?;
        }

        Ok(())
    }
}
