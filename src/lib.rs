#![feature(fnbox)]
#![feature(filling_drop)]
#![feature(mpsc_select)]
#![deny(unused_mut)]
extern crate byteorder;
extern crate bytes;
#[macro_use]
extern crate enum_primitive;
extern crate num;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
extern crate mio;
extern crate time;
extern crate snowflake;

pub use consts::*;
pub use proto::{Acl, Stat, WatchedEvent};
pub use zoodefs::acls;
pub use zoodefs::perms;
pub use zookeeper::{ZkResult, ZooKeeper};
pub use zookeeper_ext::ZooKeeperExt;
pub use watch::Watcher;

mod consts;
mod io;
mod listeners;
pub mod paths;
mod proto;
mod watch;
mod zoodefs;
mod zookeeper;
mod zookeeper_ext;
pub mod recipes;
