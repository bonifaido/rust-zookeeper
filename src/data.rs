/// Statistics about a znode, similar to the UNIX `stat` structure.
///
/// # Time in ZooKeeper
/// The concept of time is tricky in distributed systems. ZooKeeper keeps track of time in a number
/// of ways.
///
/// - **zxid**: Every change to a ZooKeeper cluster receives a stamp in the form of a *zxid*
///   (ZooKeeper Transaction ID). This exposes the total ordering of all changes to ZooKeeper. Each
///   change will have a unique *zxid* -- if *zxid:a* is smaller than *zxid:b*, then the associated
///   change to *zxid:a* happened before *zxid:b*.
/// - **Version Numbers**: Every change to a znode will cause an increase to one of the version
///   numbers of that node.
/// - **Clock Time**: ZooKeeper does not use clock time to make decisions, but it uses it to put
///   timestamps into the `Stat` structure.
#[derive(Debug)]
pub struct Stat {
    /// The transaction ID that created the znode.
    pub czxid: i64,
    /// The last transaction that modified the znode.
    pub mzxid: i64,
    /// Milliseconds since epoch when the znode was created.
    pub ctime: i64,
    /// Milliseconds since epoch when the znode was last modified.
    pub mtime: i64,
    /// The number of changes to the data of the znode.
    pub version: i32,
    /// The number of changes to the children of the znode.
    pub cversion: i32,
    /// The number of changes to the ACL of the znode.
    pub aversion: i32,
    /// The session ID of the owner of this znode, if it is an ephemeral entry.
    pub ephemeral_owner: i64,
    /// The length of the data field of the znode.
    pub data_length: i32,
    /// The number of children this znode has.
    pub num_children: i32,
    /// The transaction ID that last modified the children of the znode.
    pub pzxid: i64,
}

impl Stat {
    /// Is the znode an ephemeral entry?
    pub fn is_ephemeral(&self) -> bool {
        self.ephemeral_owner != 0
    }
}
