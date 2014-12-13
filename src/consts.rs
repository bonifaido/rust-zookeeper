#[allow(missing_copy_implementations)]
#[deriving(FromPrimitive, Show)]
pub enum ZkError {
    APIError = -100,
    AuthFailed = -115,
    BadArguments = -8,
    BadVersion = -103,
    ConnectionLoss = -4,
    DataInconsistency = -3,
    InvalidACL = -114,
    InvalidCallback = -113,
    MarshallingError = -5,
    NoAuth = -102,
    NoChildrenForEphemerals = -108,
    NodeExists = -110,
    NoNode = -101,
    NotEmpty = -111,
    OperationTimeout = -7,
    RuntimeInconsistency = -2,
    SessionExpired = -112,
    SystemError = -1,
    Unimplemented = -6
}

pub mod perms {
    pub const READ: i32 = 1 << 0;
    pub const WRITE: i32 = 1 << 1;
    pub const CREATE: i32 = 1 << 2;
    pub const DELETE: i32 = 1 << 3;
    pub const ADMIN: i32 = 1 << 4;
    pub const ALL: i32 = READ | WRITE | CREATE | DELETE | ADMIN;
}

#[allow(missing_copy_implementations)]
pub enum CreateMode {
    Persistent,
    Ephemeral,
    PersistentSequential,
    EphemeralSequential
}

#[allow(missing_copy_implementations)]
#[deriving(FromPrimitive,Show)]
pub enum KeeperState {
    Disconnected = 0,
    SyncConnected = 3,
    KSAuthFailed = 4,
    ConnectedReadOnly = 5,
    SaslAuthenticated = 6,
    Expired = -112
}

#[allow(missing_copy_implementations)]
#[deriving(FromPrimitive,Show)]
pub enum WatchedEventType {
    None = -1,
    NodeCreated = 1,
    NodeDeleted = 2,
    NodeDataChanged = 3,
    NodeChildrenChanged = 4,
    DataWatchRemoved = 5,
    ChildWatchRemoved = 6
}