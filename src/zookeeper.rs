use consts::*;
use proto::*;
use io::ZkIo;
use listeners::{ListenerSet, Subscription};
use mio;
use num::FromPrimitive;
use watch::{Watch, Watcher, WatchType, ZkWatch};
use std::io::Read;
use std::net::{SocketAddr, ToSocketAddrs};
use std::result;
use std::sync::atomic::{AtomicIsize, Ordering};
use std::sync::mpsc::{sync_channel, SyncSender};
use std::time::Duration;
use std::thread;

pub type ZkResult<T> = result::Result<T, ZkError>;

pub struct RawRequest {
    pub opcode: OpCode,
    pub data: ByteBuf,
    pub listener: Option<SyncSender<RawResponse>>,
    pub watch: Option<Watch>,
}

pub struct RawResponse {
    pub header: ReplyHeader,
    pub data: ByteBuf,
}

pub struct ZooKeeper {
    chroot: Option<String>,
    xid: AtomicIsize,
    io: mio::Sender<RawRequest>,
    listeners: ListenerSet<ZkState>,
}

impl ZooKeeper {
    fn zk_thread<F>(name: &str, task: F) -> ZkResult<thread::JoinHandle<()>>
        where F: FnOnce() + Send + 'static
    {
        thread::Builder::new()
            .name(name.to_owned())
            .spawn(task)
            .map_err(|_| ZkError::SystemError)
    }

    pub fn connect<W>(connect_string: &str, timeout: Duration, watcher: W) -> ZkResult<ZooKeeper>
        where W: Watcher + 'static
    {

        let (addrs, chroot) = try!(Self::parse_connect_string(connect_string));

        debug!("Initiating connection to {}", connect_string);

        let watch = ZkWatch::new(watcher, chroot.clone());
        let listeners = ListenerSet::<ZkState>::new();
        let listeners1 = listeners.clone();
        let io = ZkIo::new(addrs.clone(), timeout, watch.sender(), listeners1);
        let sender = io.sender();

        try!(Self::zk_thread("event", move || watch.run().unwrap()));
        try!(Self::zk_thread("io", move || io.run().unwrap()));

        Ok(ZooKeeper {
            chroot: chroot,
            xid: AtomicIsize::new(1),
            io: sender,
            listeners: listeners,
        })
    }

    fn parse_connect_string(connect_string: &str) -> ZkResult<(Vec<SocketAddr>, Option<String>)> {
        let (chroot, end) = match connect_string.find('/') {
            Some(start) => {
                match &connect_string[start..connect_string.len()] {
                    "" | "/" => (None, start),
                    chroot => (Some(try!(Self::validate_path(chroot)).to_owned()), start),
                }
            }
            None => (None, connect_string.len()),
        };

        let mut addrs = Vec::new();
        for addr_str in connect_string[..end].split(',') {
            let addr = match addr_str.trim().to_socket_addrs() {
                Ok(mut addrs) => {
                    match addrs.nth(0) {
                        Some(addr) => addr,
                        None => return Err(ZkError::BadArguments),
                    }
                }
                Err(_) => return Err(ZkError::BadArguments),
            };
            addrs.push(addr);
        }

        Ok((addrs, chroot))
    }

    fn xid(&self) -> i32 {
        self.xid.fetch_add(1, Ordering::Relaxed) as i32
    }

    fn request<Req: WriteTo, Resp: ReadFrom>(&self,
                                             opcode: OpCode,
                                             xid: i32,
                                             req: Req,
                                             watch: Option<Watch>)
                                             -> ZkResult<Resp> {
        let rh = RequestHeader {
            xid: xid,
            opcode: opcode,
        };
        let buf = try!(to_len_prefixed_buf(rh, req).map_err(|_| ZkError::MarshallingError));

        let (resp_tx, resp_rx) = sync_channel(0);
        let request = RawRequest {
            opcode: opcode,
            data: buf,
            listener: Some(resp_tx),
            watch: watch,
        };

        try!(self.io.send(request).map_err(|err| {
            warn!("error sending request: {:?}", err);
            ZkError::ConnectionLoss
        }));

        let mut response = try!(resp_rx.recv().map_err(|err| {
            warn!("error receiving response: {:?}", err);
            ZkError::ConnectionLoss
        }));

        match response.header.err {
            0 => {
                Ok(try!(ReadFrom::read_from(&mut response.data)
                            .map_err(|_| ZkError::MarshallingError)))
            }
            e => Err(FromPrimitive::from_i32(e).unwrap()),
        }
    }

    fn validate_path(path: &str) -> ZkResult<&str> {
        match path {
            "" => Err(ZkError::BadArguments),
            path => {
                if path.len() > 1 && path.chars().last() == Some('/') {
                    Err(ZkError::BadArguments)
                } else {
                    Ok(path)
                }
            }
        }
    }

    fn path(&self, path: &str) -> ZkResult<String> {
        match self.chroot {
            Some(ref chroot) => {
                match path {
                    "/" => Ok(chroot.clone()),
                    path => Ok(chroot.clone() + try!(Self::validate_path(path))),
                }
            }
            None => Ok(try!(Self::validate_path(path)).to_owned()),
        }
    }

    fn cut_chroot(&self, path: String) -> String {
        if let Some(ref chroot) = self.chroot {
            path[chroot.len()..].to_owned()
        } else {
            path
        }
    }

    pub fn add_auth(&self, scheme: &str, auth: Vec<u8>) -> ZkResult<()> {
        let req = AuthRequest {
            typ: 0,
            scheme: scheme.to_owned(),
            auth: auth,
        };

        let _: EmptyResponse = try!(self.request(OpCode::Auth, -4, req, None));

        Ok(())
    }

    pub fn create(&self,
                  path: &str,
                  data: Vec<u8>,
                  acl: Vec<Acl>,
                  mode: CreateMode)
                  -> ZkResult<String> {
        let req = CreateRequest {
            path: try!(self.path(path)),
            data: data,
            acl: acl,
            flags: mode as i32,
        };

        let response: CreateResponse = try!(self.request(OpCode::Create, self.xid(), req, None));

        Ok(self.cut_chroot(response.path))
    }

    pub fn delete(&self, path: &str, version: i32) -> ZkResult<()> {
        let req = DeleteRequest {
            path: try!(self.path(path)),
            version: version,
        };

        let _: EmptyResponse = try!(self.request(OpCode::Delete, self.xid(), req, None));

        Ok(())
    }

    pub fn exists(&self, path: &str, watch: bool) -> ZkResult<Option<Stat>> {
        let req = ExistsRequest {
            path: try!(self.path(path)),
            watch: watch,
        };

        match self.request::<ExistsRequest, ExistsResponse>(OpCode::Exists, self.xid(), req, None) {
            Ok(response) => Ok(Some(response.stat)),
            Err(ZkError::NoNode) => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub fn exists_w<W: Watcher + 'static>(&self,
                                                               path: &str,
                                                               watcher: W)
                                                               -> ZkResult<Stat> {
        let req = ExistsRequest {
            path: try!(self.path(path)),
            watch: true,
        };

        let watch = Watch {
            path: path.to_owned(),
            watch_type: WatchType::Exist,
            watcher: Box::new(watcher),
        };

        let response: ExistsResponse = try!(self.request(OpCode::Exists,
                                                         self.xid(),
                                                         req,
                                                         Some(watch)));

        Ok(response.stat)
    }

    pub fn get_acl(&self, path: &str) -> ZkResult<(Vec<Acl>, Stat)> {
        let req = GetAclRequest { path: try!(self.path(path)) };

        let response: GetAclResponse = try!(self.request(OpCode::GetAcl, self.xid(), req, None));

        Ok(response.acl_stat)
    }

    pub fn get_children_w<W: Watcher + 'static>(&self,
                                                                     path: &str,
                                                                     watcher: W)
                                                                     -> ZkResult<Vec<String>> {
        let req = GetChildrenRequest {
            path: try!(self.path(path)),
            watch: true,
        };

        let watch = Watch {
            path: path.to_owned(),
            watch_type: WatchType::Child,
            watcher: Box::new(watcher),
        };

        let response: GetChildrenResponse = try!(self.request(OpCode::GetChildren,
                                                              self.xid(),
                                                              req,
                                                              Some(watch)));

        Ok(response.children)
    }

    pub fn get_children(&self, path: &str, watch: bool) -> ZkResult<Vec<String>> {
        let req = GetChildrenRequest {
            path: try!(self.path(path)),
            watch: watch,
        };

        let response: GetChildrenResponse = try!(self.request(OpCode::GetChildren,
                                                              self.xid(),
                                                              req,
                                                              None));

        Ok(response.children)
    }

    pub fn get_data(&self, path: &str, watch: bool) -> ZkResult<(Vec<u8>, Stat)> {
        let req = GetDataRequest {
            path: try!(self.path(path)),
            watch: watch,
        };

        let response: GetDataResponse = try!(self.request(OpCode::GetData, self.xid(), req, None));

        Ok(response.data_stat)
    }

    pub fn get_data_w<W: Watcher + 'static>(&self,
                                                                 path: &str,
                                                                 watcher: W)
                                                                 -> ZkResult<(Vec<u8>, Stat)> {
        let req = GetDataRequest {
            path: try!(self.path(path)),
            watch: true,
        };

        let watch = Watch {
            path: path.to_owned(),
            watch_type: WatchType::Data,
            watcher: Box::new(watcher),
        };

        let response: GetDataResponse = try!(self.request(OpCode::GetData,
                                                          self.xid(),
                                                          req,
                                                          Some(watch)));

        Ok(response.data_stat)
    }

    pub fn set_acl(&self, path: &str, acl: Vec<Acl>, version: i32) -> ZkResult<Stat> {
        let req = SetAclRequest {
            path: try!(self.path(path)),
            acl: acl,
            version: version,
        };

        let response: SetAclResponse = try!(self.request(OpCode::SetAcl, self.xid(), req, None));

        Ok(response.stat)
    }

    pub fn set_data(&self, path: &str, data: Vec<u8>, version: i32) -> ZkResult<Stat> {
        let req = SetDataRequest {
            path: try!(self.path(path)),
            data: data,
            version: version,
        };

        let response: SetDataResponse = try!(self.request(OpCode::SetData, self.xid(), req, None));

        Ok(response.stat)
    }

    pub fn add_listener<Listener: Fn(ZkState) + Send + 'static>(&self,
                                                                listener: Listener)
                                                                -> Subscription {
        self.listeners.subscribe(listener)
    }

    pub fn remove_listener(&self, sub: Subscription) {
        self.listeners.unsubscribe(sub);
    }

    pub fn close(&self) -> ZkResult<()> {
        let _: EmptyResponse = try!(self.request(OpCode::CloseSession, 0, EmptyRequest, None));

        Ok(())
    }
}

impl Drop for ZooKeeper {
    fn drop(&mut self) {
        if let Err(err) = self.close() {
            info!("error closing zookeeper connection in drop: {:?}", err);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ZooKeeper;
    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};

    // TODO This is flaky on Travis, it works on my Linux box though.
    #[test]
    #[cfg(target_os = "macos")]
    fn parse_connect_string() {
        let (addrs, chroot) = ZooKeeper::parse_connect_string("127.0.0.1:2181,::1:2181/mesos")
                                  .ok()
                                  .expect("Parse 1");
        assert_eq!(addrs,
                   vec![SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 2181)),
                        SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1),
                                                         2181,
                                                         0,
                                                         0))]);
        assert_eq!(chroot, Some("/mesos".to_owned()));

        let (addrs, chroot) = ZooKeeper::parse_connect_string("::1:2181").ok().expect("Parse 2");
        assert_eq!(addrs,
                   vec![SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1),
                                                         2181,
                                                         0,
                                                         0))]);
        assert_eq!(chroot, None);

        let (addrs, chroot) = ZooKeeper::parse_connect_string("::1:2181/").ok().expect("Parse 3");
        assert_eq!(addrs,
                   vec![SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1),
                                                         2181,
                                                         0,
                                                         0))]);
        assert_eq!(chroot, None);
    }

    #[test]
    #[should_panic(expected = "BadArguments")]
    fn parse_connect_string_fails() {
        // This fails with ZooKeeper.java: Path must not end with / character
        ZooKeeper::parse_connect_string("127.0.0.1:2181/mesos/").unwrap();
    }
}
