use consts::*;
use proto::*;

use std::old_io::{IoResult, MemReader, MemWriter, TcpStream};
use std::old_io::net::ip::SocketAddr;
use std::old_io::timer::Timer;
use std::num::FromPrimitive;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicInt, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender, sync_channel};
use std::time::Duration;
use std::thread::Thread;

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

#[derive(Debug)]
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

impl Copy for OpCode {}

pub trait Watcher: Send {
    fn handle(&self, &WatchedEvent);
}

pub type ZkResult<T> = Result<T, ZkError>;

struct Packet {
    opcode: OpCode,
    data: Vec<u8>,
    resp_tx: Sender<Response>
}

#[derive(Clone)]
pub struct ZooKeeper {
    xid: Arc<AtomicInt>,
    running: Arc<AtomicBool>,
    packet_tx: Sender<Packet> // sending Packets from methods to writer task
}

impl ZooKeeper {

    pub fn connect<'a, W: Watcher>(connect_string: &'a str, timeout: Duration, watcher: W) -> ZkResult<ZooKeeper> {

        // communicating reader socket from writer to reader task
        let (reader_sock_tx, reader_sock_rx) = sync_channel(0);
        // comminucating requests (as Packets) from instance methods to writer task
        let (packet_tx, packet_rx): (Sender<Packet>, Receiver<Packet>) = channel();
        // communicating sent Packets from writer task to the reader task
        let (written_tx, written_rx) = channel();
        // event channel for passing WatchedEvents to watcher on a seperate task
        let (event_tx, event_rx) = channel();
        // event channel for passing WatchedEvents that the client should generate
        let event_tx_manual = event_tx.clone();

        let running = Arc::new(AtomicBool::new(true));
        let running1 = running.clone();

        let hosts = connect_string.split(',').map(|host| host.parse::<SocketAddr>().unwrap()).collect();

        Thread::spawn(move || {
            println!("Event: thread started");

            loop {
                match event_rx.recv() {
                    Ok(event) => watcher.handle(&event),
                    Err(_) => return
                }
            }
        });

        Thread::spawn(move || {
            println!("Writer: thread started");

            let mut timer = Timer::new().unwrap();
            let ping_timeout = timer.periodic(timeout);
            let mut writer_sock;
            let mut conn_resp = ConnectResponse::initial(timeout);

            loop {
                println!("Writer: connecting, trying to get new writer_sock");
                let (new_writer_sock, new_conn_resp) = match running.load(Ordering::SeqCst) {
                    true => ZooKeeper::reconnect(&hosts, conn_resp),
                    false => {
                        println!("Writer: exiting");
                        return
                    }
                };
                writer_sock = new_writer_sock;
                conn_resp = new_conn_resp;

                // pass a clone of the writer socket to the reader thread, will be used for reading
                match reader_sock_tx.send(writer_sock.clone()) {
                    Err(_) => panic!("Writer: failed to pass new socket to reader thread"),
                    _ => ()
                }

                // Send Connected event through event thread to the user
                let keeper_state = if conn_resp.read_only {
                    KeeperState::ConnectedReadOnly
                } else {
                    KeeperState::SyncConnected
                };
                event_tx_manual.send(
                    WatchedEvent{
                        event_type: WatchedEventType::None,
                        keeper_state: keeper_state,
                        path: None}
                ).unwrap();

                loop {
                    // do we have something to send or do we need to ping?
                    select! {
                        res = packet_rx.recv() => {
                            let packet = match res {
                                Ok(packet) => packet,
                                Err(_) => return
                            };
                            let res = writer_sock.write_buffer(&packet.data);
                            if res.is_err() {
                                println!("Writer: failed to send to server, trying to reconnect");
                                break;
                            }
                            let opcode = packet.opcode;
                            match written_tx.send(packet) {
                                Err(_) => panic!("Writer: failed to communicate with reader thread"),
                                _ => ()
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
                            println!("Writer: Pinging {}", writer_sock.peer_name().unwrap());
                            let ping = RequestHeader{xid: -2, opcode: OpCode::Ping as i32}.to_byte_vec();
                            let res = writer_sock.write_buffer(&ping);
                            if res.is_err() {
                                println!("Writer: failed to ping server, trying to reconnect");
                                break;
                            }
                        }
                    };
                }
            }
        });

        Thread::spawn(move || {
            println!("Reader: thread started");

            loop {
                println!("Reader: connecting, trying to get new reader_sock");
                let mut reader_sock = match reader_sock_rx.recv() {
                    Ok(sock) => sock,
                    Err(_) => {
                        println!("Reader: writer thread died, exiting");
                        return
                    }
                };

                loop {
                    let reply = ZooKeeper::read_reply(&mut reader_sock);
                    if reply.is_err() {
                        println!("Reader: read_reply {:?}", reply.err());
                        break;
                    }
                    let (reply_header, mut buf) = reply.unwrap();
                    match reply_header.xid {
                        -1 => event_tx.send(WatchedEvent::read_from(&mut buf)).unwrap(),
                        -2 => println!("Reader: got ping event"),
                        _xid => {
                            let packet = written_rx.recv().unwrap();
                            let result = ZooKeeper::parse_reply(reply_header.err, &packet, &mut buf);

                            match packet.resp_tx.send(result) {
                                Err(_) => println!("Reader: failed to pass back result to client thread"),
                                _ => ()
                            }
                         }
                    }
                }
            }
        });

        Ok(ZooKeeper{xid: Arc::new(AtomicInt::new(1)), running: running1, packet_tx: packet_tx})
    }

    fn read_reply<R: Reader>(sock: &mut R) -> IoResult<(ReplyHeader, MemReader)> {
        let buf = try!(sock.read_buffer());
        let mut reader = MemReader::new(buf);
        Ok((ReplyHeader::read_from(&mut reader), reader))
    }

    fn parse_reply<R: Reader>(err: i32, packet: &Packet, buf: &mut R) -> Response {
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

    fn reconnect(hosts: &Vec<SocketAddr>, conn_resp: ConnectResponse) -> (TcpStream, ConnectResponse) {
        let conn_req = ConnectRequest::from(conn_resp, 0).to_byte_vec();

        loop {
            for host in hosts.iter() {
                println!("Writer: Connecting to {}...", host);
                let mut sock = TcpStream::connect_timeout(*host, Duration::seconds(1));
                if sock.is_err() {
                    println!("Writer: Failed to connect to {}", host);
                    continue;
                }

                let write = sock.write_buffer(&conn_req);
                if write.is_err() {
                    continue;
                }

                let read = sock.read_buffer();
                if read.is_err() {
                    continue;
                }

                let mut buf = MemReader::new(read.unwrap());
                let conn_resp = ConnectResponse::read_from(&mut buf);

                println!("Writer: Connection: {:?}", conn_resp);

                return (sock.unwrap(), conn_resp)
            }
        }
    }

    fn xid(&self) -> i32 {
        self.xid.fetch_add(1, Ordering::SeqCst) as i32
    }

    fn request<T: Archive>(&self, opcode: OpCode, xid: i32, req: T) -> Response {
        let rh = RequestHeader{xid: xid, opcode: opcode as i32};

        let mut buf = MemWriter::new();
        rh.write_to(&mut buf);
        req.write_to(&mut buf);

        let (resp_tx, resp_rx) = channel();
        let packet = Packet{opcode: opcode, data: buf.into_inner(), resp_tx: resp_tx};

        self.packet_tx.send(packet);

        match resp_rx.recv() {
            Err(_) => Response::Error(ZkError::SystemError),
            Ok(response) => response
        }
    }

    pub fn add_auth(&self, scheme: &str, auth: Vec<u8>) -> ZkResult<()> {
        let req = AuthRequest{typ: 0, scheme: scheme.to_string(), auth: auth};

        let result = self.request(OpCode::Auth, -4, req);

        fetch_empty_result!(result, Response::Auth)
    }

    pub fn create(&self, path: &str, data: Vec<u8>, acl: Vec<Acl>, mode: CreateMode) -> ZkResult<String> {
        let req = CreateRequest{path: path.to_string(), data: data, acl: acl, flags: mode as i32};

        let result = self.request(OpCode::Create, self.xid(), req);

        fetch_result!(result, Response::Create(path))
    }

    pub fn delete(&self, path: &str, version: i32) -> ZkResult<()> {
        let req = DeleteRequest{path: path.to_string(), version: version};

        let result = self.request(OpCode::Delete, self.xid(), req);

        fetch_empty_result!(result, Response::Delete)
    }

    pub fn exists(&self, path: &str, watch: bool) -> ZkResult<Stat> {
        let req = ExistsRequest{path: path.to_string(), watch: watch};

        let result = self.request(OpCode::Exists, self.xid(), req);

        fetch_result!(result, Response::Exists(stat))
    }

    pub fn get_acl(&self, path: &str) -> ZkResult<(Vec<Acl>, Stat)> {
        let req = GetAclRequest{path: path.to_string()};

        let result = self.request(OpCode::GetAcl, self.xid(), req);

        fetch_result!(result, Response::GetAcl(acl_stat))
    }

    pub fn get_children(&self, path: &str, watch: bool) -> ZkResult<Vec<String>> {
        let req = GetChildrenRequest{path: path.to_string(), watch: watch};

        let result = self.request(OpCode::GetChildren, self.xid(), req);

        fetch_result!(result, Response::GetChildren(children))
    }

    pub fn get_data(&self, path: &str, watch: bool) -> ZkResult<(Vec<u8>, Stat)> {
        let req = GetDataRequest{path: path.to_string(), watch: watch};

        let result = self.request(OpCode::GetData, self.xid(), req);

        fetch_result!(result, Response::GetData(data_stat))
    }

    pub fn set_acl(&self, path: &str, acl: Vec<Acl>, version: i32) -> ZkResult<Stat> {
        let req = SetAclRequest{path: path.to_string(), acl: acl, version: version};

        let result = self.request(OpCode::SetAcl, self.xid(), req);

        fetch_result!(result, Response::SetAcl(stat))
    }

    pub fn set_data(&self, path: &str, data: Vec<u8>, version: i32) -> ZkResult<Stat> {
        let req = SetDataRequest{path: path.to_string(), data: data, version: version};

        let result = self.request(OpCode::SetData, self.xid(), req);

        fetch_result!(result, Response::SetData(stat))
    }

    pub fn close(&self) {
        self.request(OpCode::CloseSession, 0, EmptyRequest);

        self.running.store(false, Ordering::SeqCst);
    }
}
