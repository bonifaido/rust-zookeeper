[rust-zookeeper][doc]
=====================

[![Build Status](https://travis-ci.org/bonifaido/rust-zookeeper.svg?branch=master)](https://travis-ci.org/bonifaido/rust-zookeeper)

Zookeeper client written 100% in Rust - Work in Progress

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

[doc]: http://www.rust-ci.org/bonifaido/rust-zookeeper/doc/zookeeper
[examples]: https://github.com/bonifaido/rust-zookeeper/tree/master/examples

## Build and develop
```shell
cd zk-test-cluster
mvn clean package
cd ..
cargo clean
cargo test
```
