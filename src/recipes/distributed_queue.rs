use std::sync::Arc;
use tracing::*;

use crate::{Acl, CreateMode, ZkError, ZkResult, ZooKeeper};

const ZK_DISTRIBUTED_QUEUE_PREFIX: &str = "qn-";

/// An implementation of the Distributed Queue ZK recipe. Items put into the queue are guaranteed to
/// be ordered (by means of ZK's PERSISTENTSEQUENTIAL node). If a single consumer takes items out of
/// the queue, they will be ordered FIFO. If ordering is important, use a LeaderSelector to nominate
/// a single consumer.
pub struct DistributedQueue {
    path: String,
    zk: Arc<ZooKeeper>,
}

impl DistributedQueue {
    pub fn new(path: String, zk: Arc<ZooKeeper>) -> Self {
        DistributedQueue { path, zk }
    }

    /// Inserts data into the queue.
    pub async fn put(&self, data: Vec<u8>) -> ZkResult<String> {
        self.zk
            .create(
                &format!("{}/{}", self.path, ZK_DISTRIBUTED_QUEUE_PREFIX),
                data,
                Acl::open_unsafe().clone(),
                CreateMode::PersistentSequential,
            )
            .await
    }

    async fn claim(&self, key: &str) -> ZkResult<Vec<u8>> {
        let data = self.zk.get_data(key, false).await?;
        self.zk.delete(key, None).await.map(move |()| data.0)
    }

    async fn ordered_children(&self) -> ZkResult<Vec<String>> {
        let mut children: Vec<(u64, String)> = Vec::new();
        self.zk
            .get_children(&self.path, false)
            .await?
            .into_iter()
            .for_each(|child| {
                if !child.starts_with(ZK_DISTRIBUTED_QUEUE_PREFIX) {
                    warn!("Found child with improper name: {}. Ignoring", child);
                    return;
                }

                // the child names will be like qn-0000001. chop off the prefix, and try and convert the
                // rest to a u64. if it fails, let's ignore it and move on
                if let Ok(index) = child[ZK_DISTRIBUTED_QUEUE_PREFIX.len()..].parse::<u64>() {
                    children.push((index, child))
                } else {
                    warn!("Found child with improper index: {}. Ignoring", child);
                }
            });
        children.sort_by(|a, b| a.0.cmp(&b.0));

        Ok(children.into_iter().map(|i| i.1).collect())
    }

    /// Try to take the first item, if available.
    pub async fn try_take(&self) -> ZkResult<Option<Vec<u8>>> {
        let children = self.ordered_children().await?;

        if let Some(child) = children.get(0) {
            match self.claim(&format!("{}/{}", self.path, child)).await {
                // if the claim fails because the requested znode has been deleted, assume
                // someone else claimed it and try again
                Err(e) if e == ZkError::NoNode => Ok(None),
                // any other error should be passed up
                Err(e) => Err(e),
                Ok(claim) => Ok(Some(claim)),
            }
        } else {
            Ok(None)
        }
    }

    /// Returns the data at the first element of the queue, or Ok(None) if the queue is empty.
    /// Note: peeking does not claim ownership of the element in the queue.
    pub async fn peek(&self) -> ZkResult<Option<Vec<u8>>> {
        let children = self.ordered_children().await?;
        Ok(match children.get(0) {
            Some(child) => Some(
                self.zk
                    .get_data(&format!("{}/{}", self.path, child), false)
                    .await?
                    .0,
            ),
            None => None,
        })
    }
}
