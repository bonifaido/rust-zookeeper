/// https://github.com/apache/zookeeper/blob/master/zookeeper-recipes/zookeeper-recipes-queue/src/main/java/org/apache/zookeeper/recipes/queue/DistributedQueue.java
///
use crate::{
    Acl, CreateMode, WatchedEvent, WatchedEventType, ZkError, ZkResult, ZooKeeper, watch::Watcher
};
use std::sync::mpsc::{SyncSender, Receiver, sync_channel};
use std::sync::Arc;

/// The default prefix to use for all children in the znode
/// should be the same as the example recipe: https://github.com/apache/zookeeper/blob/245ff759b0e9fe0a1815e03433306ac805bf5e95/zookeeper-recipes/zookeeper-recipes-queue/src/main/java/org/apache/zookeeper/recipes/queue/DistributedQueue.java#L48
const ZK_DISTRIBUTEDQUEUE_PREFIX: &str = "qn-";

pub struct ZkQueue {
    dir: String,
    zk: Arc<ZooKeeper>,
}
impl ZkQueue {
    pub fn new(zk: Arc<ZooKeeper>, dir: String) -> ZkResult<Self> {
        if zk.exists(&dir, false)?.is_none() {
            let _ = zk.create(&dir, vec![0], Acl::open_unsafe().clone(), CreateMode::Container)?;
        }
        Ok(Self {
            zk,
            dir
        })
    }

    /// Inserts data into the queue
    pub fn offer(&self, data: Vec<u8>) -> ZkResult<String> {
        self.zk.create(
            &*format!("{}/{}", self.dir, ZK_DISTRIBUTEDQUEUE_PREFIX),
            data,
            Acl::open_unsafe().clone(),
            CreateMode::PersistentSequential)
    }

    /// Claim a item from the queue. gets the contents of the znode, and then delete it.
    ///
    /// NOTE. There is a small chance that another client could execute getData before this client
    /// deletes the znode. If this is an issue, a LeaderLatch would need to be implemented
    fn claim(&self, key: String) -> ZkResult<Vec<u8>> {
        let data = self.zk.get_data(&key, false)?;
        self.zk.delete(&key, None)?;
        Ok(data.0)
    }

    /// Returns a Vec of the children, in order, of the task znode
    fn ordered_children<W: Watcher + 'static>(&self, watcher: Option<W>) -> ZkResult<Vec<String>> {
        let mut children: Vec<(u64, String)> = Vec::new();
        match watcher {
            Some(w) => self.zk.get_children_w(&self.dir, w),
            None => self.zk.get_children(&self.dir, false) // false I think?
        }?.iter().for_each(|child| {
            // the child names will be like qn-0000001. chop off the prefix, and try and convert the
            // rest to a u64. if it fails, let's ignore it and move on
            if let Ok(index) = child.replace(ZK_DISTRIBUTEDQUEUE_PREFIX, "").parse::<u64>() {
                children.push((index, child.clone()))
            } else {
                warn!("found child with improper name: {}. ignoring", child);
            }
        });
        children.sort_by(|a, b| a.0.cmp(&b.0));

        Ok(children.iter().map(|i| i.1.clone()).collect())
    }

    /// Removes the head of the queue and returns it, blocking until it succeeds or throws an error
    pub fn take(&self) -> ZkResult<Vec<u8>> {
        // create a channel with a capacity of 1 to act as a latch if there are not messages in the
        // queue.
        let latch: (SyncSender<bool>, Receiver<bool>) = sync_channel(1);
        loop {
            let tx = latch.0.clone();
            let op = self.ordered_children(Some(move |ev| {
                handle_znode_change(&tx, ev)
            }))?;

            // if self.ordered_children returned something, let's try and claim it
            if !op.is_empty() {
                return match self.claim(format!("{}/{}", self.dir, op[0])) {
                    // if the claim fails because the requested znode has been deleted, assume
                    // someone else claimed it and try again
                    Err(e) if e == ZkError::NoNode => continue,
                    // any other error should be passed up
                    Err(e) => Err(e),

                    Ok(claim) => Ok(claim)
                };
            }

            // otherwise, wait until the handler is called and try this again
            let _ = latch.1.recv().unwrap();
        }
    }

    /// Returns the data at the first element of the queue, or Ok(None) if the queue is empty.
    pub fn peek(&self) -> ZkResult<Option<Vec<u8>>> {
        let op = self.ordered_children(Some(|_|{}))?;
        Ok(match op.is_empty() {
            false => Some(self.zk.get_data(&*format!("{}/{}", self.dir, op[0]), false)?.0),
            true => None
        })
    }

    /// Attempts to remove the head of the queue and return it. Returns Ok(None) if the queue is empty.
    pub fn poll(&self) -> ZkResult<Option<Vec<u8>>> {
        let op = self.ordered_children(Some(|_|{}))?;
        if !op.is_empty() {
            return match self.claim(format!("{}/{}", self.dir, op[0])) {
                Err(e) if e == ZkError::NoNode => Ok(None),
                Err(e) => Err(e),
                Ok(claim) => Ok(Some(claim))
            };
        }
        Ok(None)
    }

}

fn handle_znode_change(chan: &SyncSender<bool>, ev: WatchedEvent) {
    if let WatchedEventType::NodeChildrenChanged = ev.event_type {
        let _ = chan.send(true);
    }
}