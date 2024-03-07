#!/usr/bin/env just --justfile

compile-zk:
  cd zk-test-cluster && mvn clean package

test: compile-zk
  cargo test

lint:
  cargo clippy
