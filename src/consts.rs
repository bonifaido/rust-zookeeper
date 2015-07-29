use std::fmt;

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

// TODO For me this is ugly compared to the previous #[derive(Show)]
impl fmt::Display for ZkError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}", self)
    }
}

pub enum CreateMode {
    Persistent,
    Ephemeral,
    PersistentSequential,
    EphemeralSequential
}

enum_from_primitive! {
    #[derive(Debug)]
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
    #[derive(Debug)]
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