#![deny(unused_mut)]
extern crate zookeeper;
extern crate env_logger;

use zookeeper::{CreateMode, WatchedEvent, ZooKeeper};
use zookeeper::acls;
use zookeeper::KeeperState;

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use std::thread;

struct ZkCluster {
    process: Child,
    connect_string: String,
    closed: bool
}

impl ZkCluster {

    fn start(instances: usize) -> ZkCluster {
        let mut process = match Command::new("java")
                .arg("-jar")
                .arg("zk-test-cluster/target/main.jar")
                .arg(instances.to_string())
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .spawn() {
            Ok(p) => p,
            Err(e) => panic!("failed to start ZkCluster: {}", e),
        };
        let connect_string = Self::read_connect_string(&mut process);
        ZkCluster{process: process, connect_string: connect_string, closed: false}
    }

    fn read_connect_string(process: &mut Child) -> String {
        let mut reader = BufReader::new(process.stdout.as_mut().unwrap());
        let mut connect_string = String::new();
        if reader.read_line(&mut connect_string).is_err() {
            panic!("Couldn't read ZK connect_string")
        }
        connect_string.pop(); // remove '\n'
        connect_string
    }

    fn kill_an_instance(&mut self) {
        self.process.stdin.as_mut().unwrap().write(b"k").unwrap();
    }

    fn shutdown(&mut self) {
        if !self.closed {
            self.process.stdin.as_mut().unwrap().write(b"q").unwrap();
            assert!(self.process.wait().unwrap().success());
            self.closed = true
        }
    }
}

impl Drop for ZkCluster {
    fn drop(&mut self) {
        self.shutdown()
    }
}

#[test]
fn simple_integration_test() {
    env_logger::init().unwrap();

    // Create a test cluster
    let mut cluster = ZkCluster::start(3);

    let disconnects = Arc::new(AtomicUsize::new(0));
    let disconnects_watcher = disconnects.clone();

    // Connect to the test cluster
    let client = ZooKeeper::connect(&cluster.connect_string,
                                    Duration::from_secs(5),
                                    move |event: &WatchedEvent| {
                                        if event.keeper_state == KeeperState::Disconnected {
                                            disconnects_watcher.fetch_add(1, Ordering::Relaxed);
                                        }
                                    }).unwrap();


    // Do the tests
    let create = client.create("/test", vec![8,8], acls::OPEN_ACL_UNSAFE.clone(), CreateMode::Ephemeral);
    assert_eq!(create.ok(), Some("/test".to_owned()));


    let exists = client.exists("/test", true);
    assert!(exists.is_ok());


    // Check that during inactivity, pinging keeps alive the connection
    thread::sleep_ms(8000);


    // Set/Get Big-Data(tm)
    let data = vec![7; 1024 * 1000];
    let set_data = client.set_data("/test", data.clone(), -1);
    assert!(set_data.is_ok());
    let get_data = client.get_data("/test", false);
    assert!(get_data.is_ok());
    assert_eq!(get_data.unwrap().0, data);

    let children = client.get_children("/", true);
    assert!(children.is_ok());

    let mut sorted_children = children.unwrap();
    sorted_children.sort();
    assert_eq!(sorted_children, vec!["test".to_owned(), "zookeeper".to_owned()]);


    // Let's see what happens when the connected server goes down
    assert_eq!(disconnects.load(Ordering::Relaxed), 0);

    cluster.kill_an_instance();

    thread::sleep_ms(1000);

    // TODO once `manual` events are possible
    // assert_eq!(disconnects.load(Ordering::Relaxed), 1);


    // After closing the client all operations return Err
    client.close().unwrap();

    let exists = client.exists("/test", true);
    assert!(exists.is_err());


    // Close the whole cluster
    cluster.shutdown();
}
