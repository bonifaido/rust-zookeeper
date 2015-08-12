#![deny(unused_mut)]
extern crate zookeeper;
extern crate env_logger;

use zookeeper::{CreateMode, Watcher, WatchedEvent, ZooKeeper};
use zookeeper::acls;

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

struct LoggingWatcher;
impl Watcher for LoggingWatcher {
    fn handle(&self, e: &WatchedEvent) {
        println!("{:?}", e)
    }
}

fn start_zk() -> Child {
    match Command::new("java")
            .arg("-jar")
            .arg("zk-test-cluster/target/main.jar")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn() {
        Ok(p) => p,
        Err(e) => panic!("failed to execute process: {}", e),
    }
}

fn get_connect_string(server: &mut Child) -> String {
    let mut reader = BufReader::new(server.stdout.as_mut().unwrap());

    let mut connect_string = String::new();
    if reader.read_line(&mut connect_string).is_err() {
        panic!("Couldn't read ZK connect_string")
    }
    connect_string.pop(); // remove '\n'
    connect_string
}

fn shutdown(server: &mut Child) {
    server.stdin.as_mut().unwrap().write(b"q").unwrap();
    assert!(server.wait().unwrap().success());
}

#[test]
fn simple_integration_test() {
    env_logger::init().unwrap();

    // Create a test cluster and obtain its connection string
    let mut server = start_zk();
    let connect_string = get_connect_string(&mut server);

    // Connect to the test cluster
    let client = ZooKeeper::connect(connect_string.as_ref(), Duration::from_secs(5), LoggingWatcher).unwrap();


    // Do the tests
    let create = client.create("/test", vec![8,8], acls::OPEN_ACL_UNSAFE.clone(), CreateMode::Ephemeral);

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
    shutdown(&mut server);
}
