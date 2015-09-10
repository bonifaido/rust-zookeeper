use std::thread;
use std::sync::{Arc, Mutex};
use std::sync::mpsc;
use std::sync::mpsc::{Sender};
use std::collections::HashMap;
use consts::{WatchedEventType, ZkError, ZkState};
use proto::WatchedEvent;
use zookeeper::{ZkResult, ZooKeeper};
use zookeeper_ext::ZooKeeperExt;
use listeners::{Subscription};

pub type Data = HashMap<String, Arc<Vec<u8>>>;

#[derive(Debug)]
pub struct ChildData;

#[derive(Debug)]
pub enum PathChildrenCacheEvent {
    Initialized,
    ConnectionSuspended,
    ConnectionLost,
    ConnectionReconnected,
    ChildRemoved(ChildData),
    ChildAdded(ChildData),
    ChildUpdated(ChildData),
}

#[derive(Debug)]
pub enum RefreshMode {
    Standard,
    ForceGetDataAndStat,
}

pub enum Operation {
    Shutdown(Option<Sender<ZkResult<bool>>>),
    Refresh(RefreshMode, Option<Sender<ZkResult<()>>>),
    Event(PathChildrenCacheEvent, Option<Sender<ZkResult<bool>>>),
    GetData(String /* path */, Option<Sender<ZkResult<Vec<u8>>>>),
}

use std::fmt;
impl fmt::Debug for Operation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Operation::Shutdown(ref _sender) => {
                write!(f, "Operation::Shutdown")
            },
            Operation::Refresh(ref mode, ref _sender) => {
                write!(f, "Operation::Refresh({:?})", mode)
            },
            Operation::Event(ref ev, ref _sender) => {
                write!(f, "Operation::Event({:?})", ev)
            },
            Operation::GetData(ref path, ref _sender) => {
                write!(f, "Operation::GetData({})", path)
            }
        }
    }
}

pub struct PathChildrenCache {
    path: Arc<String>,
    zk: Arc<ZooKeeper>,
    data: Arc<Mutex<Data>>,
    worker_thread: Option<thread::JoinHandle<()>>,
    channel: Option<Sender<Operation>>,
    listener_subscription: Option<Subscription>,
}

impl PathChildrenCache {

    fn get_children(zk: Arc<ZooKeeper>, path: &str, data: Arc<Mutex<Data>>, ops_chan: Sender<Operation>) -> ZkResult<()> {
        let ops_chan1 = ops_chan.clone();

        let watcher = move |event: &WatchedEvent| {
            match event.event_type {
                WatchedEventType::NodeChildrenChanged => {
                    let path = event.path.as_ref().expect("Path absent");

                    // Subscribe to new changes recursively
                    match ops_chan1.send(Operation::Refresh(RefreshMode::Standard, None)) {
                        Err(err) => {
                            warn!("error sending Refresh operation to ops channel: {:?}", err);
                        },
                        _ => {}
                    };
                },
                _ => error!("Unexpected: {:?}", event)
            };
            // Send change event
        };

        let children = try!(zk.get_children_w(&path, watcher));

        let mut data_locked = data.lock().unwrap();

        for child in children.iter() {
            let child_path = join_path(path, child);

            if !data_locked.contains_key(&child_path) {

                let child_data = try!(Self::get_data(zk.clone(), &child_path, data.clone(), ops_chan.clone()));

                data_locked.insert(child_path, Arc::new(child_data));
            }
        }

        debug!("New data: {:?}", *data_locked);

        Ok(())
    }

    fn get_data(zk: Arc<ZooKeeper>, path: &str, data: Arc<Mutex<Data>>, ops_chan: Sender<Operation>) -> ZkResult<Vec<u8>> {
        let path1 = path.to_owned();

        let data_watcher = move |event: &WatchedEvent| {
            let mut data_locked = data.lock().unwrap();
            match event.event_type {
                WatchedEventType::NodeDeleted => {
                    data_locked.remove(&path1);
                },
                WatchedEventType::NodeDataChanged => {
                    // Subscribe to new changes recursively
                    match ops_chan.send(Operation::GetData(path1, None)) {
                        Err(err) => {
                            warn!("error sending GetData to op channel: {:?}", err);
                        },
                        _ => {}
                    }
                },
                _ => error!("Unexpected: {:?}", event)
            };

            debug!("New data: {:?}", *data_locked);

            //TODO Send change event
        };

        zk.get_data_w(path, data_watcher).map(|stuff| { stuff.0 })
    }

    pub fn new(zk: Arc<ZooKeeper>, path: &str) -> ZkResult<PathChildrenCache> {

        let data = Arc::new(Mutex::new(HashMap::new()));

        try!(zk.ensure_path(path));

        Ok(PathChildrenCache{
            path: Arc::new(path.to_string()),
            zk: zk,
            data: data,
            worker_thread: None,
            channel: None,
            listener_subscription: None,
        })
    }

    pub fn get_current_data(&self) -> Data {
        self.data.lock().unwrap().clone()
    }

    pub fn clear(&self) {
        self.data.lock().unwrap().clear()
    }

    pub fn start(&mut self) -> ZkResult<()> {
        let (ops_chan_tx, ops_chan_rx) = mpsc::channel();
        let (listener_chan_tx, listener_chan_rx) = mpsc::channel();
        
        let sub = self.zk.add_listener(listener_chan_tx.clone());
        self.listener_subscription = Some(sub);
        
        let zk = self.zk.clone();
        let path = self.path.clone();
        let data = self.data.clone();
        self.channel = Some(ops_chan_tx.clone());

        self.worker_thread = Some(thread::spawn(move || {
            let mut done = false;

            while !done {
                select! (
                    state = listener_chan_rx.recv() => {
                        match state {
                            Ok(state) => {
                                info!("zk state change {:?}", state);
                                match state {
                                    ZkState::Connected => {
                                        match ops_chan_tx.send(Operation::Refresh(RefreshMode::ForceGetDataAndStat, None)) {
                                            Err(err) => {
                                                warn!("error sending Refresh to op channel: {:?}", err);
                                                done = true;
                                            },
                                            _ => {}
                                        }
                                    },
                                    _ => {
                                    }
                                }
                            },
                            Err(err) => {
                                info!("zk state chan err. shutting down. {:?}", err);
                                done = true;
                            }
                        }
                    },
                    op = ops_chan_rx.recv() => {

                        debug!("handling op {:?}", op);
                        match op {
                            Ok(Operation::Shutdown(_maybe_chan)) => {
                                info!("shutting down worker thread");
                                done = true;
                            },
                            Ok(Operation::Refresh(_mode, maybe_chan)) => {
                                debug!("getting children");
                                let result = Self::get_children(zk.clone(), &*path, data.clone(), ops_chan_tx.clone());
                                info!("got children {:?}", result);
                                
                                maybe_chan.map_or((), |chan| {
                                    match chan.send(result) {
                                        Err(err) => {
                                            warn!("error returning Refresh result to channel: {:?}", err);
                                        },
                                        _ => {}
                                    }
                                });
                            },
                            Ok(Operation::GetData(path, maybe_chan)) => {
                                debug!("getting data");
                                let result = Self::get_data(zk.clone(), &*path, data.clone(), ops_chan_tx.clone());
                                info!("got data {:?}", result);
                                
                                maybe_chan.map_or((), |chan| {
                                    match chan.send(result) {
                                        Err(err) => {
                                            warn!("error returning GetData result to channel: {:?}", err);
                                        },
                                        _ => {}
                                    }
                                });
                            },
                            Ok(Operation::Event(event, _maybe_chan)) => {
                                info!("received event {:?}", event);
                            },
                            Err(err) => {
                                info!("error receiving from operations channel ({:?}). shutting down worker thread", err);
                                done = true;
                            }
                        }
                        
                    });
            }
        }));
        
        self.offer_operation(Operation::Refresh(RefreshMode::ForceGetDataAndStat, None))
    }

    fn offer_operation(&self, op: Operation) -> ZkResult<()> {
        match self.channel {
            Some(ref chan) => {
                chan.send(op).map_err(|err| {
                    warn!("error submitting op to channel: {:?}", err);
                    ZkError::APIError
                })
            },
            None => Err(ZkError::APIError)
        }
    }
      
}

pub fn join_path(dir: &str, child: &str) -> String {
    let dir_bytes = dir.as_bytes();
    let mut result = dir.to_string();
    if dir_bytes[dir_bytes.len() - 1] != ('/' as u8) {
        result.push_str("/");
    }
    result.push_str(child);
    result
}
