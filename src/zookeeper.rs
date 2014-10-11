use consts::*;
use proto::*;

use std::io::{IoResult, MemReader, MemWriter, TcpStream};
use std::io::net::ip::SocketAddr;
use std::io::timer::Timer;
use std::num::FromPrimitive;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicInt, SeqCst};
use std::time::Duration;

macro_rules! fetch_result(
    ($res:ident, $enu:ident($item:ident)) => (
        match $res {
            $enu(response) => Ok(response.$item),
            ErrorResult(e) => Err(e),
            _ => Err(SystemError)
        }
    )
)

macro_rules! fetch_empty_result(
    ($res:ident, $enu:ident) => (
        match $res {
            $enu => Ok(()),
            ErrorResult(e) => Err(e),
            _ => Err(SystemError)
        }
    )
)

#[deriving(Show)]
enum OpCode {
    Auth = 100,
    Create = 1,
    Delete = 2,
    Exists = 3,
    GetAcl = 6,
    SetAcl = 7,
    GetChildren = 8,
    GetData = 4,
    Ping = 11,
    CloseSession = -11
}

pub trait Watcher: Send {
    fn handle(&self, &WatchedEvent);
}

pub type ZkResult<T> = Result<T, ZkError>;

struct Packet {
    opcode: OpCode,
    data: Vec<u8>,
    resp_tx: Sender<Response>
}

#[deriving(Clone)]
pub struct ZooKeeper {
    xid: Arc<AtomicInt>,
    running: Arc<AtomicBool>,
    packet_tx: Sender<Packet> // sending Packets from methods to writer task
}

impl ZooKeeper {

    pub fn new<W: Watcher>(connect_string: &str, timeout: Duration, watcher: W) -> Result<ZooKeeper, &str> {

        // comminucating reader socket from writer to reader task
        let (reader_sock_tx, reader_sock_rx) = sync_channel(0);
        // comminucating requests (as Packets) from instance methods to writer task
        let (packet_tx, packet_rx): (Sender<Packet>, Receiver<Packet>) = channel();
        // communicating sent Packets from writer task to the reader task
        let (written_tx, written_rx) = channel();
        // event channel for passing WatchedEvents to watcher on a seperate task
        let (event_tx, event_rx) = channel();

        let running = Arc::new(AtomicBool::new(true));
        let running1 = running.clone();

        let hosts = connect_string.split(',').map(|host| from_str::<SocketAddr>(host).unwrap()).collect();

        spawn(proc() {
            println!("event task started");

            loop {
                match event_rx.recv_opt() {
                    Ok(event) => watcher.handle(&event),
                    Err(_) => return
                }
            }
        });

        spawn(proc() {
            println!("writer task started");

            let mut timer = Timer::new().unwrap();
            let ping_timeout = timer.periodic(timeout);
            let mut writer_sock;
            let mut conn_resp = ConnectResponse::initial(timeout);

            loop {
                println!("connecting: trying to get new writer_sock");
                let (new_writer_sock, new_conn_resp) = match running.load(SeqCst) {
                    true => ZooKeeper::connect(&hosts, conn_resp),
                    false => return
                };
                writer_sock = new_writer_sock;
                conn_resp = new_conn_resp;

                reader_sock_tx.send(writer_sock.clone());

                loop {
                    // do we have something to send or do we need to ping?
                    select! {
                        res = packet_rx.recv_opt() => {
                            let packet = match res {
                                Ok(packet) => packet,
                                Err(_) => return
                            };
                            let res = write_buffer(&mut writer_sock, &packet.data);
                            if res.is_err() {
                                break;
                            }
                            written_tx.send(packet);
                        },
                        () = ping_timeout.recv() => {
                            println!("Pinging {}", writer_sock.peer_name());
                            let ping = RequestHeader{xid: -2, opcode: Ping as i32}.to_byte_vec();
                            let res = write_buffer(&mut writer_sock, &ping);
                            if res.is_err() {
                                println!("Failed to ping server");
                                break;
                            }
                        }
                    };
                }
            }
        });

        spawn(proc() {
            println!("reader task started");

            loop {
                println!("connecting: trying to get new reader_sock");
                let mut reader_sock = match reader_sock_rx.recv_opt() {
                    Ok(sock) => sock,
                    Err(_) => return
                };

                loop {
                    let reply = ZooKeeper::read_reply(&mut reader_sock);
                    if reply.is_err() {
                        println!("ZooKeeper::read_reply {}", reply.err());
                        break;
                    }
                    let (reply_header, mut buf) = reply.unwrap();
                    match reply_header.xid {
                        -1 => event_tx.send(WatchedEvent::read_from(&mut buf)),
                        -2 => println!("Got ping event"),
                        _xid => {
                            let packet = written_rx.recv();
                            let result = ZooKeeper::parse_reply(reply_header.err, &packet, &mut buf);
                            packet.resp_tx.send(result);
                         }
                    }
                }
            }
        });

        Ok(ZooKeeper{xid: Arc::new(AtomicInt::new(1)), running: running1, packet_tx: packet_tx})
    }

    fn read_reply(sock: &mut Reader) -> IoResult<(ReplyHeader, MemReader)> {
        let buf = try!(read_buffer(sock));
        let mut reader = MemReader::new(buf);
        Ok((ReplyHeader::read_from(&mut reader), reader))
    }

    fn parse_reply(err: i32, packet: &Packet, buf: &mut Reader) -> Response {
        match err {
            0 => match packet.opcode {
                Auth => AuthResult,
                CloseSession => CloseResult,
                Create => CreateResult(CreateResponse::read_from(buf)),
                Delete => DeleteResult,
                Exists => ExistsResult(StatResponse::read_from(buf)),
                GetAcl => GetAclResult(GetAclResponse::read_from(buf)),
                SetAcl => SetAclResult(StatResponse::read_from(buf)),
                GetChildren => GetChildrenResult(GetChildrenResponse::read_from(buf)),
                GetData => GetDataResult(GetDataResponse::read_from(buf)),
                opcode => fail!("{}Response not implemented yet", opcode)
            },
            e => {
                ErrorResult(FromPrimitive::from_i32(e).unwrap())
            }
        }
    }

    fn connect(hosts: &Vec<SocketAddr>, conn_resp: ConnectResponse) -> (TcpStream, ConnectResponse) {
        let conn_req = ConnectRequest::from(conn_resp, 0).to_byte_vec();

        loop {
            for host in hosts.iter() {
                println!("Connecting to {}...", host);
                let mut sock = TcpStream::connect_timeout(*host, Duration::seconds(1));
                if sock.is_err() {
                    println!("Failed to connect to {}", host);
                    continue;
                }

                let write = write_buffer(&mut sock, &conn_req);
                if write.is_err() {
                    continue;
                }

                let read = read_buffer(&mut sock);
                if read.is_err() {
                    continue;
                }

                let mut buf = MemReader::new(read.unwrap());
                let conn_resp = ConnectResponse::read_from(&mut buf);

                println!("{}", conn_resp);

                return (sock.unwrap(), conn_resp)
            }
        }
    }

    fn xid(&self) -> i32 {
        self.xid.fetch_add(1, SeqCst) as i32
    }

    fn request<T: Archive>(&self, opcode: OpCode, xid: i32, req: T) -> Response {
        let rh = RequestHeader{xid: xid, opcode: opcode as i32};

        let mut buf = MemWriter::new();
        rh.write_into(&mut buf);
        req.write_into(&mut buf);

        let (resp_tx, resp_rx) = channel();
        let packet = Packet{opcode: opcode, data: buf.unwrap(), resp_tx: resp_tx};

        self.packet_tx.send(packet);

        resp_rx.recv()
    }

    pub fn add_auth(&self, scheme: String, auth: Vec<u8>) -> ZkResult<()> {
        let req = AuthRequest{typ: 0, scheme: scheme, auth: auth};

        let result = self.request(Auth, -4, req);

        fetch_empty_result!(result, AuthResult)
    }

    pub fn create(&self, path: String, data: Vec<u8>, acl: Vec<Acl>, mode: CreateMode) -> ZkResult<String> {
        let req = CreateRequest{path: path, data: data, acl: acl, flags: mode as i32};

        let result = self.request(Create, self.xid(), req);

        fetch_result!(result, CreateResult(path))
    }

    pub fn delete(&self, path: String, version: i32) -> ZkResult<()> {
        let req = DeleteRequest{path: path, version: version};

        let result = self.request(Delete, self.xid(), req);

        fetch_empty_result!(result, DeleteResult)
    }

    pub fn exists(&self, path: String, watch: bool) -> ZkResult<Stat> {
        let req = ExistsRequest{path: path, watch: watch};

        let result = self.request(Exists, self.xid(), req);

        fetch_result!(result, ExistsResult(stat))
    }

    pub fn get_acl(&self, path: String) -> ZkResult<(Vec<Acl>, Stat)> {
        let req = GetAclRequest{path: path};

        let result = self.request(GetAcl, self.xid(), req);

        fetch_result!(result, GetAclResult(acl_stat))
    }

    pub fn get_children(&self, path: String, watch: bool) -> ZkResult<Vec<String>> {
        let req = GetChildrenRequest{path: path, watch: watch};

        let result = self.request(GetChildren, self.xid(), req);

        fetch_result!(result, GetChildrenResult(children))
    }

    pub fn get_data(&self, path: String, watch: bool) -> ZkResult<(Vec<u8>, Stat)> {
        let req = GetDataRequest{path: path, watch: watch};

        let result = self.request(GetData, self.xid(), req);

        fetch_result!(result, GetDataResult(data_stat))
    }

    pub fn set_acl(&self, path: String, acl: Vec<Acl>, version: i32) -> ZkResult<Stat> {
        let req = SetAclRequest{path: path, acl: acl, version: version};

        let result = self.request(SetAcl, self.xid(), req);

        fetch_result!(result, SetAclResult(stat))
    }

    #[allow(unused_must_use)]
    pub fn close(&self) {
        self.request(CloseSession, 0, EmptyRequest);

        self.running.store(false, SeqCst);
    }
}
