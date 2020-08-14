[rust-zookeeper][doc]
=====================
[![Version](https://img.shields.io/crates/v/zookeeper-async.svg)](https://crates.io/crates/zookeeper-async)

Async Zookeeper client written 100% in Rust, based on tokio.

This library is intended to be equivalent with the official (low-level) [ZooKeeper][javadoc] client which ships with the official ZK distribution.

I have plans to implement recipes and more complex [Curator][curator] like logic as well, but that takes a lot of time, so pull requests are more than welcome! At the moment only PathChildrenCache is implemented.

## Usage

Put this in your Cargo.toml:

```ini
[dependencies]
zookeeper-async = "1.0"
```

## Examples
Check the [examples][examples] directory

## Feature and Bug Handling
Also if you find a bug or would like to see a feature implemented please raise an issue or send a pull-request.

## Documentation
Documentation is available on the [gh-pages][doc] branch.

[examples]: https://github.com/krojew/rust-zookeeper/tree/master/examples
[javadoc]: https://zookeeper.apache.org/doc/r3.4.6/api/org/apache/zookeeper/ZooKeeper.html
[curator]: http://curator.apache.org/

## Build and develop
```shell
cd zk-test-cluster
mvn clean package
cd ..
cargo test
```
## Contributing
All contributions are welcome! If you need some inspiration, please take a look at the currently open [issues](https://github.com/krojew/rust-zookeeper/issues).
