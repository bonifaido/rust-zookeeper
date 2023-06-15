use crate::{
    paths, Acl, CreateMode, Subscription, WatchedEvent, WatchedEventType, ZkError, ZkResult,
    ZkState, ZooKeeper,
};
use std::{
    cmp, iter,
    sync::{
        atomic::{self, AtomicBool, AtomicU8},
        Arc, Mutex,
    },
};

const LATCH_PREFIX: &str = "latch";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Latent = 0,
    Started = 1,
    Closed = 2,
    Failed = 3,
}

impl PartialEq<u8> for State {
    fn eq(&self, other: &u8) -> bool {
        *self == State::from(*other)
    }
}

impl From<u8> for State {
    fn from(value: u8) -> Self {
        match value {
            0 => State::Latent,
            1 => State::Started,
            2 => State::Closed,
            3 => State::Failed,
            _ => unreachable!(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ZNode {
    path: String,
    seqn: usize,
}

impl ZNode {
    pub fn with_parent(parent_path: &str, path: &str) -> Option<Self> {
        let seqn = path.split('-').last()?.parse().ok()?;
        Some(Self {
            path: paths::make_path(parent_path, path),
            seqn,
        })
    }

    pub fn creation_path(parent_path: &str, id: &str) -> String {
        paths::make_path(parent_path, &format!("{}-{}-", LATCH_PREFIX, id))
    }
}

impl Ord for ZNode {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.seqn.cmp(&other.seqn)
    }
}

impl PartialOrd for ZNode {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone)]
pub struct LeaderLatch {
    zk: Arc<ZooKeeper>,
    id: String,
    parent_path: String,
    path: Arc<Mutex<Option<String>>>,
    state: Arc<AtomicU8>,
    subscription: Arc<Mutex<Option<Subscription>>>,
    has_leadership: Arc<AtomicBool>,
    error: Arc<Mutex<Option<ZkError>>>,
}

impl LeaderLatch {
    pub fn new(zk: Arc<ZooKeeper>, id: String, parent_path: String) -> Self {
        Self {
            zk,
            id,
            parent_path,
            path: Arc::default(),
            state: Arc::new(AtomicU8::new(State::Latent as u8)),
            subscription: Arc::default(),
            has_leadership: Arc::default(),
            error: Arc::default(),
        }
    }

    pub fn start(&self) -> ZkResult<()> {
        let prev_state = self.set_state(State::Latent, State::Started);
        if prev_state != State::Latent {
            panic!("cannot start leader latch in state: {:?}", prev_state);
        }

        let latch = self.clone();
        let subscription = self
            .zk
            .add_listener(move |x| handle_state_change(&latch, x));
        *self.subscription.lock().unwrap() = Some(subscription);
        self.reset()
    }

    fn reset(&self) -> ZkResult<()> {
        self.set_leadership(false);
        self.set_path(None)?;

        let path = create_latch_znode(self, &self.parent_path, &self.id)?;
        self.set_path(Some(path))?;

        self.check_leadership()
    }

    fn check_leadership(&self) -> ZkResult<()> {
        let znodes = get_latch_znodes(&self.zk, &self.parent_path)?;
        if let Some(path) = &*self.path.lock().unwrap() {
            match znodes.iter().position(|znode| &znode.path == path) {
                Some(0) => {
                    log::info!("become the leader");
                    self.set_leadership(true);
                }
                Some(index) => {
                    self.set_leadership(false);
                    let latch = self.clone();
                    self.zk.exists_w(&znodes[index - 1].path, move |ev| {
                        handle_znode_change(&latch, ev)
                    })?;
                }
                None => {
                    // the path doesn't exist in the path of dir
                    log::error!("cannot find znode: {:?}", path);
                    // can't call `reset`, the path will be deadlocked.
                    // self.reset()?;

                    // return error, and should be handled by callerre
                    return Err(ZkError::NoNode);
                }
            }
        }
        Ok(())
    }

    pub fn stop(&self) -> ZkResult<()> {
        let mut prev_state = self.set_state(State::Started, State::Closed);
        if prev_state != State::Started {
            prev_state = self.set_state(State::Failed, State::Closed);
            if prev_state != State::Failed {
                panic!("cannot close leader latch in state: {:?}", self.state);
            }
        }

        self.set_path(None)?;
        self.set_leadership(false);

        let subscription = &mut *self.subscription.lock().unwrap();
        if let Some(sub) = subscription.take() {
            self.zk.remove_listener(sub);
        }
        Ok(())
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn path(&self) -> Option<String> {
        self.path.lock().unwrap().clone()
    }

    pub fn has_leadership(&self) -> ZkResult<bool> {
        match State::from(self.state.load(atomic::Ordering::SeqCst)) {
            State::Started => {
                Ok(self.has_leadership.load(atomic::Ordering::SeqCst))
            }
            State::Failed => {
                let zk_error = &*self.error.lock().unwrap();
                match zk_error {
                    None => {
                        Ok(false)
                    }
                    Some(e) => {
                        Err(e.clone())
                    }
                }
            }
            _ => Ok(false)
        }
    }

    fn set_leadership(&self, value: bool) {
        self.has_leadership.store(value, atomic::Ordering::SeqCst);
    }

    fn set_path(&self, value: Option<String>) -> ZkResult<()> {
        let path = &mut *self.path.lock().unwrap();
        if let Some(old_path) = path {
            match self.zk.delete(old_path, None) {
                Ok(()) | Err(ZkError::NoNode) => Ok(()),
                Err(e) => Err(e),
            }?;
        }
        *path = value;
        Ok(())
    }

    fn set_state(&self, cur: State, new: State) -> State {
        State::from(
            self.state
                .compare_and_swap(cur as u8, new as u8, atomic::Ordering::SeqCst),
        )
    }

    fn become_failed(&self, zk_error: ZkError) {
        log::warn!("set state to Failed, the error is: {:?}", zk_error);
        let prev_state = self.set_state(State::Started, State::Failed);
        if prev_state != State::Started {
            panic!("cannot start leader latch in state: {:?}", prev_state);
        }
        // set the error message to the latch
        let error = &mut *self.error.lock().unwrap();
        *error = Some(zk_error);
    }
}

fn create_latch_znode(ll: &LeaderLatch, parent_path: &str, id: &str) -> ZkResult<String> {
    ensure_path(&ll.zk, parent_path)?;
    let zrsp = ll.zk.create(
        &ZNode::creation_path(parent_path, id),
        vec![],
        Acl::open_unsafe().clone(),
        CreateMode::EphemeralSequential,
    )?;
    // add the handle_znode_change to the freshly created znode
    let latch = ll.clone();
    ll.zk.exists_w(&zrsp, move |ev| {
        handle_znode_change(&latch, ev)
    })?;
    Ok(zrsp)
}

fn get_latch_znodes(zk: &ZooKeeper, parent_path: &str) -> ZkResult<Vec<ZNode>> {
    let znodes = zk.get_children(&parent_path, false)?;
    let mut latch_znodes: Vec<_> = znodes
        .into_iter()
        .filter_map(|path| ZNode::with_parent(&parent_path, &path))
        .collect();
    latch_znodes.sort();
    Ok(latch_znodes)
}

fn ensure_path(zk: &ZooKeeper, path: &str) -> ZkResult<()> {
    for (i, _) in path
        .chars()
        .chain(iter::once('/'))
        .enumerate()
        .skip(1)
        .filter(|c| c.1 == '/')
    {
        match zk.create(
            &path[..i],
            vec![],
            Acl::open_unsafe().clone(),
            CreateMode::Container,
        ) {
            Ok(_) | Err(ZkError::NodeExists) => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

fn handle_znode_change(latch: &LeaderLatch, ev: WatchedEvent) {
    if let WatchedEventType::NodeDeleted = ev.event_type {
        log::info!("receive {:?}, the path {:?}", ev.event_type, ev.path);
        if let Err(e) = latch.check_leadership() {
            // TODO: how to do if got error when check leader ship:
            // 1. this latch has created the znode in the zk server
            // 2. when `check_leadership`, meet some issues about network or other issues
            // 3. this latch will lost the role of leader, and other latch can't become the leader
            log::error!("failed to check for leadership: {:?}", e);
            latch.set_leadership(false);
            // change the latch state to failed
            // this is should be handled by user
            latch.become_failed(e);
        }
    }
}

fn handle_state_change(latch: &LeaderLatch, zk_state: ZkState) {
    if let ZkState::Closed = zk_state {
        log::warn!("got the {:?} with zookeeper, and need to set LeaderLatch to Failed", zk_state);
        latch.set_leadership(false);
        let error = ZkError::ConnectionLoss;
        latch.become_failed(error);
    }
}
