use consts::*;
use proto::*;

use std::io::{Cursor, Read};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::result;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicIsize, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender, SyncSender, sync_channel};
use std::time::Duration;
use std::thread;
use num::FromPrimitive;
use schedule_recv::periodic_ms;

macro_rules! fetch_result(
    ($res:ident, $enu:ident::$hack:ident($item:ident)) => (
        match $res {
            $enu::$hack(response) => Ok(response.$item),
            Response::Error(e) => Err(e),
            _ => Err(ZkError::SystemError)
        }
    )
);

macro_rules! fetch_empty_result(
    ($res:ident, $enu:ident::$hack:ident) => (
        match $res {
            $enu::$hack => Ok(()),
            Response::Error(e) => Err(e),
            _ => Err(ZkError::SystemError)
        }
    )
);

#[derive(Clone, Copy, Debug)]
enum OpCode {
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

pub trait Watcher: Send {
    fn handle(&self, &WatchedEvent);
}

pub type ZkResult<T> = result::Result<T, ZkError>;

struct Packet {
    opcode: OpCode,
    data: Vec<u8>,
    resp_tx: SyncSender<Response>
}

#[derive(Debug, PartialEq)]
struct ConnectString {
    addrs: Vec<SocketAddr>,
    chroot: String,
}

#[derive(Clone)]
pub struct ZooKeeper {
    chroot: String,
    xid: Arc<AtomicIsize>,
    running: Arc<AtomicBool>,
    packet_tx: Sender<Packet> // sending Packets from methods to writer task
}

impl ZooKeeper {

    pub fn connect<W>(connect_string: &str, timeout: Duration, watcher: W) -> ZkResult<ZooKeeper>
    where W: Watcher + 'static {

        // communicating reader socket from writer to reader task
        let (reader_sock_tx, reader_sock_rx) = sync_channel(0);

        // comminucating requests (as Packets) from instance methods to writer task
        let (packet_tx, packet_rx): (Sender<Packet>, Receiver<Packet>) = channel();

        // communicating sent Packets from writer task to the reader task
        let (written_tx, written_rx) = channel();

        // event channel for passing WatchedEvents to watcher on a seperate task
        let (event_tx, event_rx) = channel();

        // event channel for passing WatchedEvents that this instance generates
        let event_tx_manual = event_tx.clone();

        let running = Arc::new(AtomicBool::new(true));
        let running1 = running.clone();

        let connect_string = try!(Self::parse_connect_string(connect_string));
        let chroot = connect_string.chroot.clone();

        // Event thread
        thread::spawn(move || {
            loop {
                match event_rx.recv() {
                    Ok(event) => {
                        println!("Event: {:?}", event);
                        watcher.handle(&event)
                    },
                    Err(e) => {
                        println!("Event: Reader died {}, exiting", e);
                        return
                    }
                }
            }
        });

        // Writer thread
        thread::spawn(move || {
            let ping_timeout = periodic_ms(timeout.secs() as u32 * 1000);
            let mut conn_resp = ConnectResponse::initial(timeout);
            let mut writer_sock;

            loop {
                println!("Writer: connecting, trying to get new writer_sock");
                let (new_writer_sock, new_conn_resp) = match running.load(Ordering::Relaxed) {
                    true => Self::reconnect(&connect_string, conn_resp),
                    false => {
                        println!("Writer: closed, exiting");
                        return
                    }
                };
                writer_sock = new_writer_sock;
                conn_resp = new_conn_resp;

                let reader_sock = match writer_sock.try_clone() {
                    Ok(sock) => sock,
                    Err(e) => {
                        println!("Writer: failed to clone socker for Reader: {}", e);
                        break
                    }
                };

                // pass a clone of the socket to the reader thread for reading
                match reader_sock_tx.send(reader_sock) {
                    Ok(()) => (),
                    Err(e) => panic!("Writer: failed to pass socket to Reader: {}", e)
                }

                // Send Connected event through event thread to Watchers
                let keeper_state = if conn_resp.read_only {
                    KeeperState::ConnectedReadOnly
                } else {
                    KeeperState::SyncConnected
                };

                Self::send_watched_event(&event_tx_manual, keeper_state);

                loop {
                    // do we have something to send or do we need to ping?
                    select! {
                        res = packet_rx.recv() => {
                            let packet = match res {
                                Ok(packet) => packet,
                                Err(e) => {
                                    println!("Writer: {}, exiting", e);
                                    return
                                }
                            };
                            match writer_sock.write_buffer(&packet.data) {
                                Ok(()) => (),
                                Err(e) => {
                                    println!("Writer: failed to send to server, reconnecting {}", e);
                                    Self::send_watched_event(&event_tx_manual, KeeperState::Disconnected);
                                    break
                                }
                            }
                            let opcode = packet.opcode;
                            match written_tx.send(packet) {
                                Ok(()) => (),
                                Err(e) => panic!("Writer: failed to communicate with Reader: {}", e)
                            }
                            match opcode {
                                OpCode::CloseSession => {
                                    println!("Writer: exiting");
                                    return
                                },
                                _ => ()
                            }
                        },
                        _ = ping_timeout.recv() => {
                            println!("Writer: Pinging {:?}", writer_sock.peer_addr());
                            let ping = RequestHeader{xid: -2, opcode: OpCode::Ping as i32}.to_buffer();
                            match writer_sock.write_buffer(&ping) {
                                Ok(()) => (),
                                Err(e) => {
                                    println!("Writer: failed to ping server {}, reconnecting", e);
                                    Self::send_watched_event(&event_tx_manual, KeeperState::Disconnected);
                                    break
                                }
                            }
                        }
                    };
                }
            }
        });

        // Reader thread
        thread::spawn(move || {
            loop {
                println!("Reader: connecting, trying to get new reader_sock");
                let mut reader_sock = match reader_sock_rx.recv() {
                    Ok(sock) => sock,
                    Err(e) => {
                        println!("Reader: Writer died {}, exiting", e);
                        return
                    }
                };

                loop {
                    let (reply_header, mut buf) = match reader_sock.read_reply() {
                        Ok(reply) => reply,
                        Err(e) => {
                            println!("Reader: read_reply {}", e);
                            Self::send_watched_event(&event_tx, KeeperState::Disconnected);
                            break
                        }
                    };
                    match reply_header.xid {
                        -1 => match event_tx.send(WatchedEvent::read_from(&mut buf)) {
                            Ok(()) => (),
                            Err(e) => panic!("Reader: Event died {}", e)
                        },
                        -2 => println!("Reader: got ping event"),
                        _xid => {
                            let packet = match written_rx.recv() {
                                Ok(packet) => packet,
                                Err(e) => {
                                    println!("Reader: Writer died {}", e);
                                    break
                                }
                            };
                            let result = Self::parse_reply(reply_header.err, &packet, &mut buf);
                            match packet.resp_tx.send(result) {
                                Ok(()) => (),
                                Err(e) => println!("Reader: failed to pass result to client {}", e)
                            }
                        }
                    }
                }
            }
        });

        Ok(ZooKeeper{chroot: chroot,
                     xid: Arc::new(AtomicIsize::new(1)),
                     running: running1,
                     packet_tx: packet_tx})
    }

    fn parse_connect_string(connect_string: &str) -> ZkResult<ConnectString> {
        let (chroot, end) = match connect_string.find('/') {
            Some(i) => {
                let len = connect_string.len();
                let j = if connect_string.chars().last().unwrap() == '/' {
                    len - 1
                } else {
                    len
                };
                (&connect_string[i..j], i)
            },
            None => ("", connect_string.len())
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

        Ok(ConnectString{addrs: addrs, chroot: chroot.to_string()})
    }

    fn parse_reply<R: Read>(err: i32, packet: &Packet, buf: &mut R) -> Response {
        match err {
            0 => match packet.opcode {
                OpCode::Auth => Response::Auth,
                OpCode::CloseSession => Response::Close,
                OpCode::Create => Response::Create(CreateResponse::read_from(buf)),
                OpCode::Delete => Response::Delete,
                OpCode::Exists => Response::Exists(StatResponse::read_from(buf)),
                OpCode::GetAcl => Response::GetAcl(GetAclResponse::read_from(buf)),
                OpCode::SetAcl => Response::SetAcl(StatResponse::read_from(buf)),
                OpCode::GetChildren => Response::GetChildren(GetChildrenResponse::read_from(buf)),
                OpCode::GetData => Response::GetData(GetDataResponse::read_from(buf)),
                OpCode::SetData => Response::SetData(StatResponse::read_from(buf)),
                ref opcode => panic!("{:?}Response not implemented yet", opcode)
            },
            e => {
                Response::Error(FromPrimitive::from_i32(e).unwrap())
            }
        }
    }

    fn reconnect(connect_string: &ConnectString, conn_resp: ConnectResponse) -> (TcpStream, ConnectResponse) {
        let conn_req = ConnectRequest::from(conn_resp, 0).to_buffer();

        loop {
            for host in connect_string.addrs.iter() {
                println!("Writer: Connecting to {}...", host);
                match TcpStream::connect(host) {
                    Ok(mut sock) => {
                        match sock.write_buffer(&conn_req) {
                            Ok(write) => write,
                            Err(_) => continue
                        }

                        let mut buffer = match sock.read_buffer() {
                            Ok(read) => Cursor::new(read),
                            Err(_) => continue
                        };

                        let conn_resp = ConnectResponse::read_from(&mut buffer);

                        println!("Writer: {:?}", conn_resp);

                        return (sock, conn_resp)
                    },
                    Err(e) => {
                        println!("Writer: Failed to connect to {} err: {}", host, e);
                        //thread::sleep_ms(500);
                    }
                }
            }
        }
    }

    fn send_watched_event(sender: &Sender<WatchedEvent>,
                          keeper_state: KeeperState) {
        match sender.send(WatchedEvent{event_type: WatchedEventType::None,
                                       keeper_state: keeper_state,
                                       path: None}) {
            Ok(()) => (),
            Err(e) => panic!("Reader/Writer: Event died {}", e)
        }
    }

    fn xid(&self) -> i32 {
        self.xid.fetch_add(1, Ordering::Relaxed) as i32
    }

    fn request<T: Archive>(&self, opcode: OpCode, xid: i32, req: T) -> Response {
        let rh = RequestHeader{xid: xid, opcode: opcode as i32};

        let mut buf = Vec::new();
        let _ = rh.write_to(&mut buf);
        let _ = req.write_to(&mut buf);

        let (resp_tx, resp_rx) = sync_channel(0);
        let packet = Packet{opcode: opcode, data: buf, resp_tx: resp_tx};

        match self.packet_tx.send(packet) {
            Ok(()) => (),
            Err(_) => return Response::Error(ZkError::SystemError)
        }

        match resp_rx.recv() {
            Ok(response) => response,
            Err(_) => Response::Error(ZkError::SystemError)
        }
    }

    fn path(&self, path: &str) -> ZkResult<String> {
        match path {
            "" => Err(ZkError::BadArguments),
            path => Ok(self.chroot.clone() + path)
        }
    }

    pub fn add_auth(&self, scheme: &str, auth: Vec<u8>) -> ZkResult<()> {
        let req = AuthRequest{typ: 0, scheme: scheme.to_string(), auth: auth};

        let result = self.request(OpCode::Auth, -4, req);

        fetch_empty_result!(result, Response::Auth)
    }

    pub fn create(&self, path: &str, data: Vec<u8>, acl: Vec<Acl>, mode: CreateMode) -> ZkResult<String> {
        let req = CreateRequest{path: try!(self.path(path)), data: data, acl: acl, flags: mode as i32};

        let result = self.request(OpCode::Create, self.xid(), req);

        fetch_result!(result, Response::Create(path))
    }

    pub fn delete(&self, path: &str, version: i32) -> ZkResult<()> {
        let req = DeleteRequest{path: try!(self.path(path)), version: version};

        let result = self.request(OpCode::Delete, self.xid(), req);

        fetch_empty_result!(result, Response::Delete)
    }

    pub fn exists(&self, path: &str, watch: bool) -> ZkResult<Stat> {
        let req = ExistsRequest{path: try!(self.path(path)), watch: watch};

        let result = self.request(OpCode::Exists, self.xid(), req);

        fetch_result!(result, Response::Exists(stat))
    }

    pub fn get_acl(&self, path: &str) -> ZkResult<(Vec<Acl>, Stat)> {
        let req = GetAclRequest{path: try!(self.path(path))};

        let result = self.request(OpCode::GetAcl, self.xid(), req);

        fetch_result!(result, Response::GetAcl(acl_stat))
    }

    pub fn get_children(&self, path: &str, watch: bool) -> ZkResult<Vec<String>> {
        let req = GetChildrenRequest{path: try!(self.path(path)), watch: watch};

        let result = self.request(OpCode::GetChildren, self.xid(), req);

        fetch_result!(result, Response::GetChildren(children))
    }

    pub fn get_data(&self, path: &str, watch: bool) -> ZkResult<(Vec<u8>, Stat)> {
        let req = GetDataRequest{path: try!(self.path(path)), watch: watch};

        let result = self.request(OpCode::GetData, self.xid(), req);

        fetch_result!(result, Response::GetData(data_stat))
    }

    pub fn set_acl(&self, path: &str, acl: Vec<Acl>, version: i32) -> ZkResult<Stat> {
        let req = SetAclRequest{path: try!(self.path(path)), acl: acl, version: version};

        let result = self.request(OpCode::SetAcl, self.xid(), req);

        fetch_result!(result, Response::SetAcl(stat))
    }

    pub fn set_data(&self, path: &str, data: Vec<u8>, version: i32) -> ZkResult<Stat> {
        let req = SetDataRequest{path: try!(self.path(path)), data: data, version: version};

        let result = self.request(OpCode::SetData, self.xid(), req);

        fetch_result!(result, Response::SetData(stat))
    }

    pub fn close(&self) {
        self.request(OpCode::CloseSession, 0, EmptyRequest);

        self.running.store(false, Ordering::Relaxed);
    }
}

impl Drop for ZooKeeper {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::{ConnectString, ZooKeeper};
    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};

    // TODO This is flaky, host resolving returns the addresses in hectic order, sort?
    #[test]
    fn test_parse_connect_string() {
        let connect_string = ZooKeeper::parse_connect_string("127.0.0.1:2181,localhost:2181,::1:2181/mesos").unwrap();
        assert_eq!(connect_string, ConnectString{
            addrs: vec![SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127,0,0,1), 2181)),
                        SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::new(0,0,0,0,0,0,0,1), 2181, 0, 0)),
                        SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::new(0,0,0,0,0,0,0,1), 2181, 0, 0))],
            chroot: "/mesos".to_string()
        });

        let connect_string = ZooKeeper::parse_connect_string("127.0.0.1:2181/mesos/").unwrap();
        assert_eq!(connect_string, ConnectString{
            addrs: vec![SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127,0,0,1), 2181))],
            chroot: "/mesos".to_string()
        });

        let connect_string = ZooKeeper::parse_connect_string("localhost:2181").unwrap();
        assert_eq!(connect_string, ConnectString{
            addrs: vec![SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::new(0,0,0,0,0,0,0,1), 2181, 0, 0))],
            chroot: "".to_string()
        });

        let connect_string = ZooKeeper::parse_connect_string("localhost:2181/").unwrap();
        assert_eq!(connect_string, ConnectString{
            addrs: vec![SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::new(0,0,0,0,0,0,0,1), 2181, 0, 0))],
            chroot: "".to_string()
        });
    }
}