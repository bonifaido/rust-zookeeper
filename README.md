[rust-zookeeper][doc]
=====================
[![Version](https://img.shields.io/crates/v/zookeeper.svg)](https://crates.io/crates/zookeeper)
[![Build Status](https://travis-ci.org/bonifaido/rust-zookeeper.svg?branch=master)](https://travis-ci.org/bonifaido/rust-zookeeper)
[![License](https://img.shields.io/crates/l/zookeeper.svg)](http://opensource.org/licenses/MIT)

Zookeeper client written 100% in Rust - Work in Progress

This library is intended to be the equivalent of the official (low-level) [ZooKeeper][javadoc] client which ships with the ZK distribution.

I have plans to implement recipes and more complex [Curator][curator] like logic as well, but that takes a lot of work, so pull requests are more than welcome!

## Feature and Bug Handling
Also if you find a bug or would like to see a feature implemented please raise an issue or send a pull-request.

## Documentation
Documentation is available at [rust-ci.org][doc]

## Usage

Put this in your Cargo.toml:

```ini
[dependencies.zookeeper]
git = "0.1.0"
```

And this in your crate root:

```rust
extern crate zookeeper;
```

## Examples
Check the [examples][examples] directory

[doc]: http://bonifaido.github.io/rust-zookeeper
[examples]: https://github.com/bonifaido/rust-zookeeper/tree/master/examples
[javadoc]: https://zookeeper.apache.org/doc/r3.4.6/api/org/apache/zookeeper/ZooKeeper.html
[curator]: http://curator.apache.org/

## Build and develop
```shell
cd zk-test-cluster
mvn clean package
cd ..
cargo clean
cargo test
```
