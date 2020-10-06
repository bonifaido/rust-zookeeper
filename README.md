# zookeeper-async

[![Version](https://img.shields.io/crates/v/zookeeper-async.svg)](https://crates.io/crates/zookeeper-async)

Async Zookeeper client written 100% in Rust, based on tokio.

This library is intended to be equivalent with the official (low-level) [ZooKeeper][javadoc] client which ships with the official ZK distribution.

Some [Curator][curator] recipes are available in the [recipes][recipes] directory. 

## Examples
Check the [examples][examples] directory

## Features and Bugs
If you find a bug or would like to see a feature implemented please raise an issue or send a pull-request.

[examples]: https://github.com/krojew/rust-zookeeper/tree/master/examples
[recipes]: https://github.com/krojew/rust-zookeeper/tree/master/src/recipes
[javadoc]: https://zookeeper.apache.org/doc/r3.4.6/api/org/apache/zookeeper/ZooKeeper.html
[curator]: http://curator.apache.org/

## Running tests
```shell
cd zk-test-cluster
mvn clean package
cd ..
cargo test
```

## Contributing
All contributions are welcome! If you need some inspiration, please take a look at the currently open [issues](https://github.com/krojew/rust-zookeeper/issues).
