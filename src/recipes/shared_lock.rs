use std::sync::{Arc, Mutex, MutexGuard};

use tokio::sync::oneshot;
use tracing::*;
use uuid::Uuid;

use crate::{Acl, CreateMode, ZkError, ZkResult, ZooKeeper, ZooKeeperExt};

/// An RAII implementation of a "scoped lock" for a ZooKeeper distributed lock. When this structure
/// is dropped (falls out of scope), the lock will be unlocked.
pub struct LockGuard {
    // we need a sync mutex, because we don't have async drop yet
    state: Mutex<LockGuardState>,
}

struct LockGuardState {
    id: Option<String>,
    prefix: Option<String>,
    path: String,
    zookeeper: Arc<ZooKeeper>,
    acquired: bool,
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        let mut state = self.state();
        if !state.acquired {
            return;
        }

        if let Some(id) = state.id.take() {
            let zookeeper = state.zookeeper.clone();
            let path = state.path.clone();
            tokio::spawn(async move {
                debug!("Removing lock: {}/{}", path, id);

                if let Err(error) = zookeeper.delete(&format!("{}/{}", path, id), None).await {
                    panic!("Couldn't remove lock {}/{}: {}", path, id, error);
                }
            });
        }
    }
}

impl LockGuard {
    fn new(path: String, zookeeper: Arc<ZooKeeper>) -> Self {
        LockGuard {
            state: Mutex::new(LockGuardState {
                id: None,
                prefix: None,
                path,
                zookeeper,
                acquired: false,
            }),
        }
    }

    fn state(&self) -> MutexGuard<LockGuardState> {
        self.state.lock().expect("Error acquiring state mutex")
    }

    async fn try_lock(self: Arc<LockGuard>) -> ZkResult<()> {
        loop {
            let no_id = self.state().id.is_none();
            if no_id {
                let prefix = Uuid::new_v4().to_string();

                let (path, zookeeper) = {
                    let state = self.state();
                    debug!("Creating a lock in {} with prefix {}.", state.path, prefix);

                    (
                        format!("{}/{}_", state.path, prefix),
                        state.zookeeper.clone(),
                    )
                };

                let id = zookeeper
                    .create(
                        &path,
                        Vec::new(),
                        Acl::read_unsafe().clone(),
                        CreateMode::EphemeralSequential,
                    )
                    .await?;

                debug!("Resulting path: {}", id);

                let id = &id[id.rfind('/').expect("Missing last path separator!") + 1..];

                let mut state = self.state();
                state.id = Some(id.into());
                state.prefix = Some(prefix);
            }

            let (path, prefix, zookeeper) = {
                let state = self.state();
                (
                    state.path.clone(),
                    state.prefix.as_ref().cloned().unwrap(),
                    state.zookeeper.clone(),
                )
            };

            let nodes = zookeeper.get_children(&path, false).await?;

            if nodes.is_empty() {
                warn!("No lock node after creation - recreating.");

                let mut state = self.state();
                state.id = None;
                state.prefix = None;
                continue;
            }

            let mut nodes = nodes
                .into_iter()
                .map(|node| (node.clone(), node.rfind('_')))
                .filter_map(|node| {
                    let (left, right) = node.0.split_at(node.1?);
                    Some((String::from(left), String::from(right)))
                })
                .collect::<Vec<_>>();

            if nodes.is_empty() {
                warn!("Couldn't find lock nodes - recreating.");

                let mut state = self.state();
                state.id = None;
                state.prefix = None;
                continue;
            }

            nodes.sort_unstable_by(|a, b| a.1.cmp(&b.1));

            if nodes[0].0 == *prefix {
                break;
            }

            let id_position = nodes.binary_search_by(|node| node.0.cmp(&prefix)).unwrap();
            let previous = &nodes[id_position - 1];

            let path = {
                let state = self.state();
                debug!(
                    "Watching previous node to {}: {}{}",
                    state.id.as_ref().unwrap(),
                    previous.0,
                    previous.1
                );

                state.path.clone()
            };

            let previous_path = format!("{}/{}{}", path, previous.0, previous.1);

            let (tx, rx) = oneshot::channel();
            let stat = zookeeper
                .exists_w(&previous_path, move |_| {
                    if tx.send(()).is_err() {
                        panic!("Error sending lock notification!");
                    }
                })
                .await?;

            if stat.is_some() {
                if rx.await.is_err() {
                    return Err(ZkError::ConnectionLoss);
                }
            }
        }

        let mut state = self.state();
        state.acquired = true;

        Ok(())
    }
}

/// Fully distributed [locks](https://curator.apache.org/curator-recipes/shared-lock.html) that are
/// globally synchronous, meaning at any snapshot in time no two clients think they hold the same
/// lock.
pub async fn lock(zookeeper: Arc<ZooKeeper>, path: String) -> ZkResult<Arc<LockGuard>> {
    let path_copy = path.clone();

    zookeeper
        .ensure_path_with_leaf_mode(&path_copy, CreateMode::Container)
        .await?;

    let guard = Arc::new(LockGuard::new(path, zookeeper));
    guard.clone().try_lock().await?;

    Ok(guard)
}
