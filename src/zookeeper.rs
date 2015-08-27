use consts::*;
use proto::*;
use io::{RawRequest, ZkIo};
use mio;
use num::FromPrimitive;
use watch::{Watcher, ZkWatch};
use std::io::{Read, Result};
use std::net::{SocketAddr, ToSocketAddrs};
use std::result;
use std::sync::atomic::{AtomicIsize, Ordering};
use std::sync::mpsc::{Sender, sync_channel};
use std::time::Duration;
use std::thread;

pub type ZkResult<T> = result::Result<T, ZkError>;

pub struct ZooKeeper {
    chroot: Option<String>,
    xid: AtomicIsize,
    io: mio::Sender<RawRequest>
}

impl ZooKeeper {

    pub fn connect<W>(connect_string: &str, timeout: Duration, watcher: W) -> ZkResult<ZooKeeper>
    where W: Watcher + 'static {

        let (addrs, chroot) = try!(Self::parse_connect_string(connect_string));

        debug!("Initiating connection to {}", connect_string);

        let watch = ZkWatch::new(watcher);
        let io = ZkIo::new(addrs.clone(), timeout, watch.sender());
        let sender = io.sender();
        thread::Builder::new().name("event".to_string()).spawn(move || {
            watch.run().unwrap();
        });
        thread::Builder::new().name("io".to_string()).spawn(move || {
            io.run().unwrap();
        });

        let zk = ZooKeeper{chroot: chroot,
                            xid: AtomicIsize::new(1),
                            io: sender};

        Ok(zk)

        // // Writer thread
        // thread::spawn(move || {
        //     let mut conn_resp = ConnectResponse::initial(timeout);

        //     loop {
        //         debug!("Writer: connecting, trying to get new writer_sock");
        //         let (mut writer_sock, new_conn_resp) = match running.load(Ordering::Relaxed) {
        //             true => Self::reconnect(&addrs, conn_resp),
        //             false => {
        //                 debug!("Writer: closed, exiting");
        //                 return
        //             }
        //         };
                
        //         conn_resp = new_conn_resp;

        //         if conn_resp.timeout <= 0 {
        //             Self::send_watched_event(&event_tx_manual, KeeperState::Expired);
        //             return
        //         }

        //         debug!("Writer: negotiated session timeout: {}ms", conn_resp.timeout );

        //         let reader_sock = match writer_sock.try_clone() {
        //             Ok(sock) => sock,
        //             Err(e) => {
        //                 debug!("Writer: failed to clone socker for Reader: {}", e);
        //                 break
        //             }
        //         };

        //         // pass a clone of the socket to the reader thread for reading
        //         match reader_sock_tx.send(reader_sock) {
        //             Ok(()) => (),
        //             Err(e) => panic!("Writer: failed to pass socket to Reader: {}", e)
        //         }

        //         // Send Connected event through event thread to Watchers
        //         let keeper_state = if conn_resp.read_only {
        //             KeeperState::ConnectedReadOnly
        //         } else {
        //             KeeperState::SyncConnected
        //         };

        //         Self::send_watched_event(&event_tx_manual, keeper_state);

        //         let ping_timeout = periodic_ms(conn_resp.timeout as u32 * 2 / 3);

        //         loop {
        //             // do we have something to send or do we need to ping?
        //             select! {
        //                 res = packet_rx.recv() => {
        //                     let packet = match res {
        //                         Ok(packet) => packet,
        //                         Err(e) => {
        //                             debug!("Writer: {}, exiting", e);
        //                             return
        //                         }
        //                     };
        //                     match writer_sock.write_all(&packet.data) {
        //                         Ok(()) => (),
        //                         Err(e) => {
        //                             debug!("Writer: failed to send to server, reconnecting {}", e);
        //                             Self::send_watched_event(&event_tx_manual, KeeperState::Disconnected);
        //                             break
        //                         }
        //                     }
        //                     let opcode = packet.opcode;
        //                     match written_tx.send(packet) {
        //                         Ok(()) => (),
        //                         Err(e) => panic!("Writer: failed to communicate with Reader: {}", e)
        //                     }
        //                     match opcode {
        //                         OpCode::CloseSession => {
        //                             debug!("Writer: exiting");
        //                             return
        //                         },
        //                         _ => ()
        //                     }
        //                 },
        //                 _ = ping_timeout.recv() => {
        //                     debug!("Writer: pinging {:?}", writer_sock.peer_addr());
        //                     match writer_sock.write_all(&PING) {
        //                         Ok(()) => (),
        //                         Err(e) => {
        //                             debug!("Writer: failed to ping server {}, reconnecting", e);
        //                             Self::send_watched_event(&event_tx_manual, KeeperState::Disconnected);
        //                             break
        //                         }
        //                     }
        //                 }
        //             };
        //         }
        //     }
        // });

        // // Reader thread
        // thread::spawn(move || {
        //     loop {
        //         debug!("Reader: connecting, trying to get new reader_sock");
        //         let mut reader_sock = match reader_sock_rx.recv() {
        //             Ok(sock) => sock,
        //             Err(e) => {
        //                 debug!("Reader: Writer died {}, exiting", e);
        //                 return
        //             }
        //         };

        //         loop {
        //             let (reply_header, mut buf) = match reader_sock.read_reply() {
        //                 Ok(reply) => reply,
        //                 Err(e) => {
        //                     debug!("Reader: read_reply {}", e);
        //                     Self::send_watched_event(&event_tx, KeeperState::Disconnected);
        //                     break
        //                 }
        //             };
        //             match reply_header.xid {
        //                 -1 => match event_tx.send(WatchedEvent::read_from(&mut buf)) {
        //                     Ok(()) => (),
        //                     Err(e) => panic!("Reader: Event died {}", e)
        //                 },
        //                 -2 => debug!("Reader: received ping response"),
        //                 _xid => {
        //                     let packet = match written_rx.recv() {
        //                         Ok(packet) => packet,
        //                         Err(e) => {
        //                             debug!("Reader: Writer died {}", e);
        //                             break
        //                         }
        //                     };
        //                     let result = Self::parse_reply(reply_header.err, &packet, &mut buf);
        //                     match packet.resp_tx.send(result) {
        //                         Ok(()) => (),
        //                         Err(e) => debug!("Reader: failed to pass result to client {}", e)
        //                     }
        //                 }
        //             }
        //         }
        //     }
        // });
    }

    fn parse_connect_string(connect_string: &str) -> ZkResult<(Vec<SocketAddr>, Option<String>)> {
        let (chroot, end) = match connect_string.find('/') {
            Some(start) => {
                match &connect_string[start..connect_string.len()] {
                    "" | "/" => (None, start),
                    chroot => (Some(try!(Self::validate_path(chroot)).to_string()), start)
                }
            },
            None => (None, connect_string.len())
        };

        let mut addrs = Vec::new();
        for addr_str in connect_string[..end].split(',') {
            let addr = match addr_str.to_socket_addrs() {
                Ok(mut addrs) => match addrs.nth(0) {
                    Some(addr) => addr,
                    None => return Err(ZkError::BadArguments)
                },
                Err(_) => return Err(ZkError::BadArguments)
            };
            addrs.push(addr);
        }

        Ok((addrs, chroot))
    }

    fn xid(&self) -> i32 {
        self.xid.fetch_add(1, Ordering::Relaxed) as i32
    }

    fn request<T: WriteTo, R: ReadFrom>(&self, opcode: OpCode, xid: i32, req: T) -> ZkResult<R> {
        let rh = RequestHeader{xid: xid, opcode: opcode};
        let buf = to_len_prefixed_buf(rh, req);
        
        let (resp_tx, resp_rx) = sync_channel(0);
        let request = RawRequest{opcode: opcode, data: buf, listener: Some(resp_tx)};

        try!(self.io.send(request).map_err(|_|ZkError::ConnectionLoss));

        let mut response = try!(resp_rx.recv().map_err(|_|ZkError::ConnectionLoss));
        
        match response.header.err {
            0 => Ok(ReadFrom::read_from(&mut response.data)),
            e => Err(FromPrimitive::from_i32(e).unwrap())
        }
    }

    fn validate_path(path: &str) -> ZkResult<&str> {
        match path {
            "" => Err(ZkError::BadArguments),
            path => if path.len() > 1 && path.chars().last() == Some('/') {
                    Err(ZkError::BadArguments)
                } else {
                    Ok(path)
                }
        }
    }

    fn path(&self, path: &str) -> ZkResult<String> {
        match self.chroot {
            Some(ref chroot) => match path {
                "/" => Ok(chroot.clone()),
                path => Ok(chroot.clone() + try!(Self::validate_path(path)))
            },
            None => Ok(try!(Self::validate_path(path)).to_string())
        }
    }

    pub fn add_auth(&self, scheme: &str, auth: Vec<u8>) -> ZkResult<()> {
        let req = AuthRequest{typ: 0, scheme: scheme.to_string(), auth: auth};

        let _: EmptyResponse = try!(self.request(OpCode::Auth, -4, req));

        Ok(())
    }

    pub fn create(&self, path: &str, data: Vec<u8>, acl: Vec<Acl>, mode: CreateMode) -> ZkResult<String> {
        let req = CreateRequest{path: try!(self.path(path)), data: data, acl: acl, flags: mode as i32};

        let response: CreateResponse = try!(self.request(OpCode::Create, self.xid(), req));

        Ok(response.path)
    }

    pub fn delete(&self, path: &str, version: i32) -> ZkResult<()> {
        let req = DeleteRequest{path: try!(self.path(path)), version: version};

        let _: EmptyResponse = try!(self.request(OpCode::Delete, self.xid(), req));

        Ok(())
    }

    pub fn exists(&self, path: &str, watch: bool) -> ZkResult<Stat> {
        let req = ExistsRequest{path: try!(self.path(path)), watch: watch};

        let response: ExistsResponse = try!(self.request(OpCode::Exists, self.xid(), req));

        Ok(response.stat)
    }

    pub fn get_acl(&self, path: &str) -> ZkResult<(Vec<Acl>, Stat)> {
        let req = GetAclRequest{path: try!(self.path(path))};

        let response: GetAclResponse = try!(self.request(OpCode::GetAcl, self.xid(), req));

        Ok(response.acl_stat)
    }

    pub fn get_children(&self, path: &str, watch: bool) -> ZkResult<Vec<String>> {
        let req = GetChildrenRequest{path: try!(self.path(path)), watch: watch};

        let response: GetChildrenResponse = try!(self.request(OpCode::GetChildren, self.xid(), req));

        Ok(response.children)
    }

    pub fn get_data(&self, path: &str, watch: bool) -> ZkResult<(Vec<u8>, Stat)> {
        let req = GetDataRequest{path: try!(self.path(path)), watch: watch};

        let response: GetDataResponse = try!(self.request(OpCode::GetData, self.xid(), req));

        Ok(response.data_stat)
    }

    pub fn set_acl(&self, path: &str, acl: Vec<Acl>, version: i32) -> ZkResult<Stat> {
        let req = SetAclRequest{path: try!(self.path(path)), acl: acl, version: version};

        let response: SetAclResponse = try!(self.request(OpCode::SetAcl, self.xid(), req));

        Ok(response.stat)
    }

    pub fn set_data(&self, path: &str, data: Vec<u8>, version: i32) -> ZkResult<Stat> {
        let req = SetDataRequest{path: try!(self.path(path)), data: data, version: version};

        let response: SetDataResponse = try!(self.request(OpCode::SetData, self.xid(), req));

        Ok(response.stat)
    }

    pub fn close(&self) -> ZkResult<()> {
        let _: EmptyResponse = try!(self.request(OpCode::CloseSession, 0, EmptyRequest));

        Ok(())
    }
}

// impl Drop for ZooKeeper {
//     fn drop(&mut self) {
//         First check if state is closed
//         self.close().unwrap();
//     }
// }

#[cfg(test)]
mod tests {
    use super::ZooKeeper;
    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};

    // TODO This is flaky, host resolving happens in a hectic order, sort?
    #[test]
    fn parse_connect_string() {
        let (addrs, chroot) = ZooKeeper::parse_connect_string("127.0.0.1:2181,::1:2181/mesos").unwrap();
        assert_eq!(addrs,
                   vec![SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127,0,0,1), 2181)),
                        SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::new(0,0,0,0,0,0,0,1), 2181, 0, 0))]);
        assert_eq!(chroot.unwrap(), "/mesos");

        let (addrs, chroot) = ZooKeeper::parse_connect_string("::1:2181").unwrap();
        assert_eq!(addrs, vec![SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::new(0,0,0,0,0,0,0,1), 2181, 0, 0))]);
        assert_eq!(chroot, None);

        let (addrs, chroot) = ZooKeeper::parse_connect_string("::1:2181/").unwrap();
        assert_eq!(addrs, vec![SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::new(0,0,0,0,0,0,0,1), 2181, 0, 0))]);
        assert_eq!(chroot, None);
    }

    #[test]
    #[should_panic(expected = "BadArguments")]
    fn parse_connect_string_fails() {
        // This fails with ZooKeeper.java: Path must not end with / character
        ZooKeeper::parse_connect_string("127.0.0.1:2181/mesos/").unwrap();
    }
}
