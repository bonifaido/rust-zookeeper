use std::error::Error;
use std::fmt;

/// Basic type for errors returned from the server.
#[derive(Debug, EnumConvertFromInt, EnumError, PartialEq)]
#[EnumConvertFromIntFallback = "Unimplemented"]
pub enum ZkError {
    /// This code is never returned from the server. It should not be used other than to indicate a
    /// range. Specifically error codes greater than this value are API errors (while values less
    /// than this indicate a system error.
    APIError = -100,
    /// Client authentication failed.
    AuthFailed = -115,
    /// Invalid arguments.
    BadArguments = -8,
    /// Version conflict in `set` operation. In case of reconfiguration: reconfig requested from
    /// config version X but last seen config has a different version Y.
    BadVersion = -103,
    /// Connection to the server has been lost.
    ConnectionLoss = -4,
    /// A data inconsistency was found.
    DataInconsistency = -3,
    /// Attempt to create ephemeral node on a local session.
    EphemeralOnLocalSession = -120,
    /// Invalid `Acl` specified.
    InvalidACL = -114,
    /// Invalid callback specified.
    InvalidCallback = -113,
    /// Error while marshalling or unmarshalling data.
    MarshallingError = -5,
    /// Not authenticated.
    NoAuth = -102,
    /// Ephemeral nodes may not have children.
    NoChildrenForEphemerals = -108,
    /// Request to create node that already exists.
    NodeExists = -110,
    /// Attempted to read a node that does not exist.
    NoNode = -101,
    /// The node has children.
    NotEmpty = -111,
    /// State-changing request is passed to read-only server.
    NotReadOnly = -119,
    /// Attempt to remove a non-existing watcher.
    NoWatcher = -121,
    /// Operation timeout.
    OperationTimeout = -7,
    /// A runtime inconsistency was found.
    RuntimeInconsistency = -2,
    /// The session has been expired by the server.
    SessionExpired = -112,
    /// Session moved to another server, so operation is ignored.
    SessionMoved = -118,
    /// System and server-side errors. This is never thrown by the server, it shouldn't be used
    /// other than to indicate a range. Specifically error codes greater than this value, but lesser
    /// than `APIError`, are system errors.
    SystemError = -1,
    /// Operation is unimplemented.
    Unimplemented = -6
}

impl fmt::Display for ZkError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Zookeeper Error: {}", self.description())
    }
}

/// CreateMode value determines how the znode is created on ZooKeeper.
#[derive(Clone, PartialEq)]
pub enum CreateMode {
    /// The znode will not be automatically deleted upon client's disconnect.
    Persistent = 0,
    /// The znode will be deleted upon the client's disconnect.
    Ephemeral = 1,
    /// The name of the znode will be appended with a monotonically increasing number. The actual
    /// path name of a sequential node will be the given path plus a suffix `"i"` where *i* is the
    /// current sequential number of the node. The sequence number is always fixed length of 10
    /// digits, 0 padded. Once such a node is created, the sequential number will be incremented by
    /// one.
    PersistentSequential = 2,
    /// The znode will be deleted upon the client's disconnect, and its name will be appended with a
    /// monotonically increasing number.
    EphemeralSequential = 3,
    /// Container nodes are special purpose nodes useful for recipes such as leader, lock, etc. When
    /// the last child of a container is deleted, the container becomes a candidate to be deleted by
    /// the server at some point in the future. Given this property, you should be prepared to get
    /// `ZkError::NoNode` when creating children inside of this container node.
    Container = 4,
}

/// Enumeration of states the client may be at a Watcher Event. It represents the state of the
/// server at the time the event was generated.
#[derive(Clone, Debug, EnumConvertFromInt, PartialEq)]
pub enum KeeperState {
    /// The client is in the disconnected state - it is not connected to any server in the ensemble.
    Disconnected = 0,
    /// The client is in the connected state - it is connected to a server in the ensemble (one of
    /// the servers specified in the host connection parameter during ZooKeeper client creation).
    SyncConnected = 3,
    /// Authentication has failed -- connection requires a new `ZooKeeper` instance.
    AuthFailed = 4,
    /// The client is connected to a read-only server, that is the server which is not currently
    /// connected to the majority. The only operations allowed after receiving this state is read
    /// operations. This state is generated for read-only clients only since read/write clients
    /// aren't allowed to connect to read-only servers.
    ConnectedReadOnly = 5,
    /// Used to notify clients that they are SASL-authenticated, so that they can perform ZooKeeper
    /// actions with their SASL-authorized permissions.
    SaslAuthenticated = 6,
    /// The serving cluster has expired this session. The ZooKeeper client connection (the session)
    /// is no longer valid. You must create a new client connection (instantiate a new `ZooKeeper`
    /// instance) if you with to access the ensemble.
    Expired = -112
}

/// Enumeration of types of events that may occur on the znode.
#[derive(Clone, Debug, EnumConvertFromInt)]
pub enum WatchedEventType {
    /// Nothing known has occurred on the znode. This value is issued as part of a `WatchedEvent`
    /// when the `KeeperState` changes.
    None = -1,
    /// Issued when a znode at a given path is created.
    NodeCreated = 1,
    /// Issued when a znode at a given path is deleted.
    NodeDeleted = 2,
    /// Issued when the data of a watched znode are altered. This event value is issued whenever a
    /// *set* operation occurs without an actual contents check, so there is no guarantee the data
    /// actually changed.
    NodeDataChanged = 3,
    /// Issued when the children of a watched znode are created or deleted. This event is not issued
    /// when the data within children is altered.
    NodeChildrenChanged = 4,
    /// Issued when the client removes a data watcher.
    DataWatchRemoved = 5,
    /// Issued when the client removes a child watcher.
    ChildWatchRemoved = 6
}

/// Enumeration of states the client may be at any time.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ZkState {
    /// Previously used to represent a state between connection (as in connected to a server) and
    /// authenticated. This is no longer used.
    #[deprecated]
    Associating,
    /// Authentication has failed. Operations will return `ZkError::AuthFailed`.
    AuthFailed,
    /// The session has ended. Operations will return `ZkError::SessionExpired`.
    Closed,
    /// Session has been fully established. Operations will proceed as normal.
    Connected,
    /// Connected to a read-only server. See `KeeperState::ConnectedReadOnly`.
    ConnectedReadOnly,
    /// Currently attempting to connect with an ensemble member. Operations are queued until a
    /// session is established.
    Connecting,
    /// Theoretically used as a special state to represent `ZkError::ConnectionLoss` for expected
    /// reasons (ensemble reconfiguration), but `Closed` has proven less error-prone. This is no
    /// longer used.
    #[deprecated]
    NotConnected,
}
