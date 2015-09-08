use std::thread;
use std::sync::{Arc, Mutex};
use std::sync::mpsc;
use std::sync::mpsc::{Sender,Receiver};
use std::collections::HashMap;
use consts::{WatchedEventType, ZkError};
use proto::WatchedEvent;
use zookeeper::{ZkResult, ZooKeeper};
use zookeeper_ext::ZooKeeperExt;
use watch;

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
    GetData(String /* path */, Option<Sender<ZkResult<bool>>>),
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
}

impl PathChildrenCache {

    fn get_children(zk: Arc<ZooKeeper>, path: &str, data: Arc<Mutex<Data>>) -> ZkResult<()> {

        let zk1 = zk.clone();
        let data1 = data.clone();

        let watcher = move |event: &WatchedEvent| {
            match event.event_type {
                WatchedEventType::NodeChildrenChanged => {
                    let path = event.path.as_ref().expect("Path absent");
                    // Subscribe to new changes recursively
                    let _ = Self::get_children(zk1, path, data1); // ignore errors
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

                let child_data = Self::get_data(zk.clone(), &child_path, data.clone());

                data_locked.insert(child_path, Arc::new(child_data));
            }
        }

        debug!("New data: {:?}", *data_locked);

        Ok(())
    }

    fn get_data(zk: Arc<ZooKeeper>, path: &str, data: Arc<Mutex<Data>>) -> Vec<u8> {
        let zk1 = zk.clone();
        let path1 = path.to_owned();

        let data_watcher = move |event: &WatchedEvent| {
            let mut data_locked = data.lock().unwrap();
            match event.event_type {
                WatchedEventType::NodeDeleted => {
                    data_locked.remove(&path1);
                },
                WatchedEventType::NodeDataChanged => {
                    // Subscribe to new changes recursively
                    let child_data = Self::get_data(zk1, &path1, data.clone());
                    data_locked.insert(path1, Arc::new(child_data));
                },
                _ => error!("Unexpected: {:?}", event)
            };

            debug!("New data: {:?}", *data_locked);

            //TODO Send change event
        };

        zk.get_data_w(path, data_watcher).unwrap().0
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
        })
    }

    pub fn get_current_data(&self) -> Data {
        self.data.lock().unwrap().clone()
    }

    pub fn clear(&self) {
        self.data.lock().unwrap().clear()
    }

    pub fn start(&mut self) {
        let (tx, rx) = mpsc::channel();
        self.channel = Some(tx);

        let zk = self.zk.clone();
        let path = self.path.clone();
        let data = self.data.clone();
        
        self.worker_thread = Some(thread::spawn(move || {
            for op in rx.iter() {
                println!("handling op {:?}", op);
                match op {
                    Operation::Shutdown(maybe_chan) => {
                        println!("shutting down worker thread");
                        break;
                    },
                    Operation::Refresh(mode, maybe_chan) => {
                        println!("getting children");
                        let result = Self::get_children(zk.clone(), &*path, data.clone());
                        println!("got children {:?}", result);
                        
                        maybe_chan.map_or((), |chan| {
                            chan.send(result);
                        });
                    },
                    Operation::GetData(path, maybe_chan) => {
                    },
                    Operation::Event(event, maybe_chan) => {
                    }
                }
            }
        }));

        self.offer_operation(Operation::Refresh(RefreshMode::ForceGetDataAndStat, None));
    }

    fn offer_operation(&self, op: Operation) -> ZkResult<()> {
        match self.channel {
            Some(ref chan) => {
                chan.send(op);
                Ok(())
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
