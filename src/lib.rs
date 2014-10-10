#![feature(macro_rules)]

// What's left:
// TODO Auth (ZooKeeper.addAuthInfo)
// TODO Copy ZooDefs.java's content here
// TODO Implement all operations
// TODO Notify Watcher about state changes
// TODO Write a lot of tests
// TODO Handle zxid at reconnect
// TODO Reconnect only until session is valid
// TODO chroot in connect_string
// TODO Handle server initiated close
// TODO Close socket before reconnection
// TODO Handle specific socket errors, set timeout

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
    GetChildren = 8,
    Ping = 11,
    CloseSession = -11
}

trait Archive {
    fn write_into(&self, writer: &mut Writer);

    fn to_byte_vec(&self) -> Vec<u8> {
        let mut w = MemWriter::new();
        self.write_into(&mut w);
        w.unwrap()
    }
}

struct ConnectRequest {
    protocol_version: i32,
    last_zxid_seen: i64,
    timeout: i32,
    session_id: i64,
    passwd: Vec<u8>,
    read_only: bool
}

impl ConnectRequest {
    fn from(conn_resp: ConnectResponse, last_zxid_seen: i64) -> ConnectRequest {
        ConnectRequest{
            protocol_version: conn_resp.protocol_version,
            last_zxid_seen: last_zxid_seen,
            timeout: conn_resp.timeout,
            session_id: conn_resp.session_id,
            passwd: conn_resp.passwd,
            read_only: conn_resp.read_only}
    }
}

impl Archive for ConnectRequest {
    #[allow(unused_must_use)]
    fn write_into(&self, w: &mut Writer) {
        w.write_be_i32(self.protocol_version);
        w.write_be_i64(self.last_zxid_seen);
        w.write_be_i32(self.timeout);
        w.write_be_i64(self.session_id);
        write_buffer(w, &self.passwd);
        w.write_u8(self.read_only as u8);
    }
}

#[deriving(Show)]
struct ConnectResponse {
    protocol_version: i32,
    timeout: i32,
    session_id: i64,
    passwd: Vec<u8>,
    read_only: bool
}

impl ConnectResponse {
    fn initial(timeout: Duration) -> ConnectResponse {
        ConnectResponse{
            protocol_version: 0,
            timeout: timeout.num_milliseconds() as i32,
            session_id: 0,
            passwd: [0, ..15].to_vec(),
            read_only: false}
    }

    fn read_from(reader: &mut Reader) -> ConnectResponse {
        let protocol_version = reader.read_be_i32().unwrap();
        let timeout = reader.read_be_i32().unwrap();
        let session_id = reader.read_be_i64().unwrap();
        let passwd = read_buffer(reader).unwrap();
        let read_only = reader.read_u8().unwrap() == 0;
        ConnectResponse{
            protocol_version: protocol_version,
            timeout: timeout,
            session_id: session_id,
            passwd: passwd,
            read_only: read_only}
    }
}

struct RequestHeader {
    xid: i32,
    opcode: i32
}

impl Archive for RequestHeader {
    #[allow(unused_must_use)]
    fn write_into(&self, writer: &mut Writer) {
        writer.write_be_i32(self.xid);
        writer.write_be_i32(self.opcode);
    }
}

#[deriving(Show)]
struct ReplyHeader {
    xid: i32,
    zxid: i64,
    err: i32
}

impl ReplyHeader {
    fn read_from(reader: &mut Reader) -> ReplyHeader {
        let xid = reader.read_be_i32().unwrap();
        let zxid = reader.read_be_i64().unwrap();
        let err = reader.read_be_i32().unwrap();
        ReplyHeader{xid: xid, zxid: zxid, err: err}
    }
}

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

pub type ZkResult<T> = Result<T, ZkError>;

enum Response {
    AuthResult,
    CreateResult(CreateResponse),
    DeleteResult,
    GetChildrenResult(GetChildrenResponse),
    CloseResult,
    ErrorResult(ZkError)
}

struct CreateRequest {
    path: String,
    data: Vec<u8>,
    acl: Vec<Acl>,
    flags: i32
}

impl Archive for CreateRequest {
    #[allow(unused_must_use)]
    fn write_into(&self, writer: &mut Writer) {
        write_string(writer, &self.path);
        write_buffer(writer, &self.data);
        writer.write_be_i32(self.acl.len() as i32);
        for a in self.acl.iter() {
            a.write_into(writer);
        }
        writer.write_be_i32(self.flags);
    }
}

#[deriving(Show)]
struct CreateResponse {
    path: String
}

impl CreateResponse {
    fn read_from(reader: &mut Reader) -> CreateResponse {
        CreateResponse{path: read_string(reader)}
    }
}

struct DeleteRequest {
    path: String,
    version: i32
}

impl Archive for DeleteRequest {
    #[allow(unused_must_use)]
    fn write_into(&self, writer: &mut Writer) {
        write_string(writer, &self.path);
        writer.write_be_i32(self.version);
    }
}

struct GetChildrenRequest {
    path: String,
    watch: bool
}

impl Archive for GetChildrenRequest {
    #[allow(unused_must_use)]
    fn write_into(&self, writer: &mut Writer) {
        write_string(writer, &self.path);
        writer.write_u8(self.watch as u8);
    }
}

#[deriving(Show)]
struct GetChildrenResponse {
    children: Vec<String>
}

impl GetChildrenResponse {
    fn read_from(reader: &mut Reader) -> GetChildrenResponse {
        let len = reader.read_be_i32().unwrap();
        let mut children = Vec::new();
        for _ in range(0, len) {
            children.push(read_string(reader));
        }
        GetChildrenResponse{children: children}
    }
}

struct AuthPacket {
    typ: i32,
    scheme: String,
    auth: Vec<u8>
}

impl Archive for AuthPacket {
    #[allow(unused_must_use)]
    fn write_into(&self, writer: &mut Writer) {
        writer.write_be_i32(self.typ);
        write_string(writer, &self.scheme);
        write_buffer(writer, &self.auth);
    }
}

struct EmptyRequest;

impl Archive for EmptyRequest {
    fn write_into(&self, _: &mut Writer) {}
}

#[allow(unused_must_use)]
fn write_buffer(writer: &mut Writer, buffer: &Vec<u8>) -> IoResult<()> {
    writer.write_be_i32(buffer.len() as i32);
    return writer.write(buffer.as_slice());
}

fn read_buffer(reader: &mut Reader) -> IoResult<Vec<u8>> {
    let len = try!(reader.read_be_i32());
    reader.read_exact(len as uint)
}

#[allow(unused_must_use)]
fn write_string(writer: &mut Writer, string: &String) {
    writer.write_be_i32(string.len() as i32);
    writer.write_str(string.as_slice());
}

fn read_string(reader: &mut Reader) -> String {
    let raw = read_buffer(reader).unwrap();
    String::from_utf8(raw).unwrap()
}

struct Packet {
    opcode: OpCode,
    data: Vec<u8>,
    resp_tx: Sender<Response>
}

pub mod perms {
    pub const READ: i32 = 1 << 0;
    pub const WRITE: i32 = 1 << 1;
    pub const CREATE: i32 = 1 << 2;
    pub const DELETE: i32 = 1 << 3;
    pub const ADMIN: i32 = 1 << 4;
    pub const ALL: i32 = READ | WRITE | CREATE | DELETE | ADMIN;
}

#[deriving(Show)]
pub struct Acl {
    pub perms: i32,
    pub scheme: String,
    pub id: String
}

impl Archive for Acl {
    #[allow(unused_must_use)]
    fn write_into(&self, writer: &mut Writer) {
        writer.write_be_i32(self.perms);
        write_string(writer, &self.scheme);
        write_string(writer, &self.id);
    }
}

pub enum CreateMode {
    Persistent,
    Ephemeral,
    PersistentSequential,
    EphemeralSequential
}

#[deriving(FromPrimitive,Show)]
pub enum KeeperState {
    Disconnected = 0,
    SyncConnected = 3,
    KSAuthFailed = 4,
    ConnectedReadOnly = 5,
    SaslAuthenticated = 6,
    Expired = -112
}

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

#[deriving(Show)]
pub struct WatchedEvent {
    pub event_type: WatchedEventType,
    pub keeper_state: KeeperState,
    pub path: String
}

impl WatchedEvent {
    fn read_from(reader: &mut Reader) -> WatchedEvent {
        let typ = reader.read_be_i32().unwrap();
        let state = reader.read_be_i32().unwrap();
        let path = read_string(reader);
        WatchedEvent{
            event_type: FromPrimitive::from_i32(typ).unwrap(),
            keeper_state: FromPrimitive::from_i32(state).unwrap(),
            path: path}
    }
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
                Create => CreateResult(CreateResponse::read_from(buf)),
                Delete => DeleteResult,
                GetChildren => GetChildrenResult(GetChildrenResponse::read_from(buf)),
                CloseSession => CloseResult,
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

    fn request<T: Archive>(&self, req: T, xid: i32, opcode: OpCode) -> Response {
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
        let req = AuthPacket{typ: 0, scheme: scheme, auth: auth};

        let result = self.request(req, -4, Auth);

        fetch_empty_result!(result, AuthResult)
    }

    pub fn create(&self, path: String, data: Vec<u8>, acl: Vec<Acl>, mode: CreateMode) -> ZkResult<String> {
        let req = CreateRequest{path: path, data: data, acl: acl, flags: mode as i32};

        let result = self.request(req, self.xid(), Create);

        fetch_result!(result, CreateResult(path))
    }

    pub fn delete(&self, path: String, version: i32) -> ZkResult<()> {
        let req = DeleteRequest{path: path, version: version};

        let result = self.request(req, self.xid(), Delete);

        fetch_empty_result!(result, DeleteResult)
    }

    pub fn get_children(&self, path: String, watch: bool) -> ZkResult<Vec<String>> {
        let req = GetChildrenRequest{path: path, watch: watch};

        let result = self.request(req, self.xid(), GetChildren);

        fetch_result!(result, GetChildrenResult(children))
    }

    #[allow(unused_must_use)]
    pub fn close(&self) {
        self.request(EmptyRequest, 0, CloseSession);
        self.running.store(false, SeqCst);
    }
}

pub trait Watcher: Send {
    fn handle(&self, &WatchedEvent);
}
