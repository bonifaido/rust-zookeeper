enum_from_primitive! {
    #[derive(Debug)]
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
}

pub enum CreateMode {
    Persistent,
    Ephemeral,
    PersistentSequential,
    EphemeralSequential
}

enum_from_primitive! {
    /// Enumeration of states the client may be at a Watcher Event
    #[derive(Clone, Debug, PartialEq)]
    pub enum KeeperState {
        Disconnected = 0,
        SyncConnected = 3,
        AuthFailed = 4,
        ConnectedReadOnly = 5,
        SaslAuthenticated = 6,
        Expired = -112
    }
}

enum_from_primitive! {
    #[derive(Clone, Debug)]
    pub enum WatchedEventType {
        None = -1,
        NodeCreated = 1,
        NodeDeleted = 2,
        NodeDataChanged = 3,
        NodeChildrenChanged = 4,
        DataWatchRemoved = 5,
        ChildWatchRemoved = 6
    }
}

/// Enumeration of states the client may be at any time
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ZkState {
    Associating,
    AuthFailed,
    Closed,
    Connected,
    ConnectedReadOnly,
    Connecting,
    NotConnected
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum OpCode {
    Auth = 100,
    Create = 1,
    Delete = 2,
    Exists = 3,
    GetAcl = 6,
    SetAcl = 7,
    GetChildren = 8,
    GetData = 4,
    SetData = 5,
    Ping = 11,
    CloseSession = -11
}
