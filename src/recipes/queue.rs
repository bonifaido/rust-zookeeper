/// https://github.com/apache/zookeeper/blob/master/zookeeper-recipes/zookeeper-recipes-queue/src/main/java/org/apache/zookeeper/recipes/queue/DistributedQueue.java
///
use crate::{
    paths, Acl, CreateMode, Subscription, WatchedEvent, WatchedEventType, ZkError, ZkResult,
    ZkState, ZooKeeper,
    watch::Watcher
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
    pub fn new(zk: Arc<ZooKeeper>, dir: String) -> Self {
        if zk.exists(&dir, false).unwrap().is_none() {
            println!("does mot exist");
            zk.create(&dir, vec![0], Acl::open_unsafe().clone(), CreateMode::Container).unwrap();
        }
        Self {
            zk,
            dir
        }
    }

    /// Inserts data into the queue
    pub fn offer(&self, data: Vec<u8>) -> ZkResult<String> {
        self.zk.create(
            &*format!("{}/{}", self.dir, ZK_DISTRIBUTEDQUEUE_PREFIX),
            data,
            Acl::open_unsafe().clone(),
            CreateMode::PersistentSequential)
    }

    fn claim(&self, key: String) -> ZkResult<Vec<u8>> {
        debug!("claiming {}", &key);
        let data = self.zk.get_data(&key, false)?;
        self.zk.delete(&key, None)?;
        Ok(data.0)
    }

    /// Returns a Vec of the children, in
    fn ordered_children<W: Watcher + 'static>(&self, watcher: Option<W>) -> ZkResult<Vec<String>> {
        let mut children: Vec<(u64, String)> = Vec::new();
        match watcher {
            Some(W) => self.zk.get_children_w(&self.dir, W),
            None => self.zk.get_children(&self.dir, false) // false I think?
        }?.iter().for_each(|child| {
            if let Ok(index) = child.replace(ZK_DISTRIBUTEDQUEUE_PREFIX, "").parse::<u64>() {
                children.push((index, child.clone()))
            } else {
                warn!("found child with improper name: {}", child);
            }
        });

        children.sort_by(|a, b| a.0.cmp(&b.0));

        Ok(children.iter().map(|i| i.1.clone()).collect())
    }

    /// Removes the head of the queue and returns it, blocking until it succeeds or throws an error
    pub fn take(&self) -> ZkResult<Vec<u8>> {
        // call self.ordered_children
        let latch: (SyncSender<bool>, Receiver<bool>) = sync_channel(1);
        loop {
            let tx = latch.0.clone();
            let op = self.ordered_children(Some(move |ev| {
                handle_znode_change(&tx, ev)
            }))?;

            // if we get keys from self.ordered_children, return the top key after claiming it
            if op.len() > 0 {
                let claim = self.claim(format!("{}/{}", self.dir, op[0]));
                if claim.is_err() {
                    let error = claim.err().unwrap();
                    // if there is no node, we want to try this whole process again
                    // if there is an error, abort because something is broken
                    if ZkError::NoNode != error { return Err(error); }
                    continue;
                }
                return Ok(claim.unwrap());
            }

            // otherwise, wait until we get something back from the Reciver
            let _ = latch.1.recv().unwrap();
        }
        // infinite recursion for the win
    }

    /// Returns the data at the first element of the queue, or Ok(None) if the queue is empty.
    pub fn peek(&self) -> ZkResult<Option<Vec<u8>>> {
        let op = self.ordered_children(Some(|ev|{}))?;
        Ok(match op.len() > 0 {
            true => Some(self.zk.get_data(&*format!("{}/{}", self.dir, op[0]), false)?.0),
            false => None
        })
    }

    /// Attempts to remove the head of the queue and return it. Returns Ok(None) if the queue is empty.
    pub fn poll(&self) -> ZkResult<Option<Vec<u8>>> {
        let op = self.ordered_children(Some(|ev|{}))?;

        if op.len() > 0 {
            return match self.claim(format!("{}/{}", self.dir, op[0])) {
                Ok(r) => Ok(Some(r)),
                Err(e) => {
                    if e != ZkError::NoNode { return Err(e); }
                    return Ok(None);
                }
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