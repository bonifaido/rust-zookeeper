use async_trait::async_trait;
use log::*;
use std::collections::VecDeque;
use std::iter::once;

use crate::{Acl, CreateMode, ZkError, ZkResult, ZooKeeper};

/// Extended ZooKeeper operations that are not needed for the "core."
#[async_trait]
pub trait ZooKeeperExt {
    /// Ensure that `path` exists and create all potential paths leading up to it if it does not.
    /// This operates in a manner similar to `mkdir -p`.
    async fn ensure_path(&self, path: &str) -> ZkResult<()>;

    /// Ensures path like `ensure_path`, but with the last node created with `mode`.
    async fn ensure_path_with_leaf_mode(&self, path: &str, mode: CreateMode) -> ZkResult<()>;

    /// Performs a breadth-first tree traversal of the tree starting at `path`,
    /// returning a list of fully prefixed child nodes.
    /// *NOTE*: This is not an atomic operation.
    async fn get_children_recursive(&self, path: &str) -> ZkResult<Vec<String>>;

    /// Deletes the node at `path` and all its children.
    /// *NOTE*: This is not an atomic operation.
    async fn delete_recursive(&self, path: &str) -> ZkResult<()>;
}

#[async_trait]
impl ZooKeeperExt for ZooKeeper {
    async fn ensure_path(&self, path: &str) -> ZkResult<()> {
        trace!("ensure_path {}", path);
        for (i, _) in path
            .chars()
            .chain(once('/'))
            .enumerate()
            .skip(1)
            .filter(|c| c.1 == '/')
        {
            match self
                .create(
                    &path[..i],
                    vec![],
                    Acl::open_unsafe().clone(),
                    CreateMode::Persistent,
                )
                .await
            {
                Ok(_) | Err(ZkError::NodeExists) => {}
                Err(e) => return Err(e),
            }
        }

        Ok(())
    }

    async fn ensure_path_with_leaf_mode(&self, path: &str, mode: CreateMode) -> ZkResult<()> {
        trace!("ensure_path_with_leaf_mode {}", path);
        let path_len = path.len();
        for (i, _) in path
            .chars()
            .chain(once('/'))
            .enumerate()
            .skip(1)
            .filter(|c| c.1 == '/')
        {
            match self
                .create(
                    &path[..i],
                    vec![],
                    Acl::open_unsafe().clone(),
                    if i == path_len {
                        mode
                    } else {
                        CreateMode::Persistent
                    },
                )
                .await
            {
                Ok(_) | Err(ZkError::NodeExists) => {}
                Err(e) => return Err(e),
            }
        }

        Ok(())
    }

    async fn get_children_recursive(&self, path: &str) -> ZkResult<Vec<String>> {
        trace!("get_children_recursive {}", path);
        let mut queue: VecDeque<String> = VecDeque::new();
        let mut result = vec![path.to_string()];
        queue.push_front(path.to_string());

        while let Some(current) = queue.pop_front() {
            let children = self.get_children(&current, false).await?;
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

    async fn delete_recursive(&self, path: &str) -> ZkResult<()> {
        trace!("delete_recursive {}", path);
        let children = self.get_children_recursive(path).await?;
        for child in children.iter().rev() {
            self.delete(child, None).await?;
        }

        Ok(())
    }
}
