#![feature(core, old_io, std_misc)]
#![deny(unused_mut)]
extern crate zookeeper;

use zookeeper::{Acl, CreateMode, Watcher, WatchedEvent, ZooKeeper};
use zookeeper::perms;

use std::old_io::{BufferedReader, Command, Process};
use std::slice::SliceExt;
use std::time::Duration;

struct LoggingWatcher;
impl Watcher for LoggingWatcher {
    fn handle(&self, e: &WatchedEvent) {
        println!("{:?}", e)
    }
}

fn start_zk() -> Process {
    match Command::new("java")
            .arg("-jar")
            .arg("zk-test-cluster/target/main.jar")
            .spawn() {
        Ok(p) => p,
        Err(e) => panic!("failed to execute process: {}", e),
    }
}

fn get_connect_string(server: &mut Process) -> String {
    let reader = server.stdout.take().unwrap();
    let mut reader = BufferedReader::new(reader);

    let mut connect_string = reader.read_line().unwrap();
    connect_string.pop(); // remove '\n'
    connect_string
}

#[test]
fn simple_integration_test() {

    // Create a test cluster and obtain its connection string
    let mut server = start_zk();
    let connect_string = get_connect_string(&mut server);

    // Connect to the test cluster
    let client = ZooKeeper::connect(connect_string.as_slice(), Duration::seconds(5), LoggingWatcher).unwrap();


    // Do the tests
    let acl1 = vec![Acl{perms: perms::ALL, scheme: "world".to_string(), id: "anyone".to_string()}];
    let create = client.create("/test", vec![8,8], acl1, CreateMode::Ephemeral);

    assert_eq!(create.ok(), Some("/test".to_string()));


    let exists = client.exists("/test", true);

    assert!(exists.is_ok());


    let children = client.get_children("/", true);

    assert!(children.is_ok());

    let mut sorted_children = children.unwrap();
    sorted_children.sort();

    assert_eq!(sorted_children, vec!["test".to_string(), "zookeeper".to_string()]);


    // After closing the client all operations return Err
    client.close();

    let exists = client.exists("/test", true);

    assert!(exists.is_err());


    // Close the server
    server.signal_exit().unwrap();
}
