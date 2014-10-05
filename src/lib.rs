#![feature(macro_rules)]

use std::io::{IoResult, MemReader, MemWriter, Timer, TcpStream};
use std::io::net::ip::SocketAddr;
use std::num::FromPrimitive;
use std::sync::{Arc, Barrier};
use std::sync::atomic::{AtomicBool, AtomicInt, AtomicOption, SeqCst};
use std::time::Duration;

macro_rules! fetch_result (
    ($res:ident, $enu:ident($item:ident)) => (
        match $res {
            $enu(response) => Ok(response.$item),
            ErrorResult(error) => Err(error),
            _ => Err(SystemError)
        }
    );
)

#[deriving(Show)]
enum OpCode {
    Create = 1,
    GetChildren = 8,
    Ping = 11, // xid
    CloseSession = -11 // xid
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
    fn new(timeout: Duration) -> ConnectRequest {
        ConnectRequest{protocol_version:0, last_zxid_seen:0, timeout:timeout.num_milliseconds() as i32, session_id:0, passwd:[0, ..15].to_vec(), read_only:false}
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
    len: i32,
    protocol_version: i32,
    timeout: i32,
    session_id: i64,
    passwd: Vec<u8>,
    read_only: bool
}

impl ConnectResponse {
    fn read_from(reader: &mut Reader) -> ConnectResponse {
        let len = reader.read_be_i32().unwrap();
        let protocol_version = reader.read_be_i32().unwrap();
        let timeout = reader.read_be_i32().unwrap();
        let session_id = reader.read_be_i64().unwrap();
        let passwd = read_buffer(reader);
        let read_only = reader.read_u8().unwrap() == 0;
        ConnectResponse{len:len, protocol_version:protocol_version, timeout:timeout, session_id:session_id, passwd:passwd, read_only:read_only}
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
    len: i32,
    xid: i32,
    zxid: i64,
    err: i32
}

impl ReplyHeader {
    fn read_from(reader: &mut Reader) -> ReplyHeader {
        let len = reader.read_be_i32().unwrap();
        let xid = reader.read_be_i32().unwrap();
        let zxid = reader.read_be_i64().unwrap();
        let err = reader.read_be_i32().unwrap();
        ReplyHeader{len: len, xid: xid, zxid: zxid, err: err}
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

#[deriving(Show)]
enum Response {
    GetChildrenResult(GetChildrenResponse),
    CreateResult(CreateResponse),
    CloseResult,
    ErrorResult(ZkError)
}

#[deriving(Show)]
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

#[deriving(Show)]
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
        if len > 0 {
            for _ in range(0, len) {
                children.push(read_string(reader));
            }
        }
        GetChildrenResponse{children: children} // copied - box?
    }
}

struct EmptyRequest;

impl Archive for EmptyRequest {
    fn write_into(&self, _: &mut Writer) {}
}

#[allow(unused_must_use)]
fn write_buffer(writer: &mut Writer, buffer: &Vec<u8>) {
    writer.write_be_i32(buffer.len() as i32);
    writer.write(buffer.as_slice());
}

fn read_buffer(reader: &mut Reader) -> Vec<u8> {
    let len = reader.read_be_i32().unwrap();
    reader.read_exact(len as uint).unwrap()
}

#[allow(unused_must_use)]
fn write_string(writer: &mut Writer, string: &String) {
    writer.write_be_i32(string.len() as i32);
    writer.write_str(string.as_slice());
}

fn read_string(reader: &mut Reader) -> String {
    let raw = read_buffer(reader);
    String::from_utf8(raw).unwrap()
} 

fn copy(reader: &mut Reader, len: uint) -> Result<MemReader, String> {
    let mut buf = Vec::<u8>::with_capacity(len);
    if len == 0 {
        return Ok(MemReader::new(buf))
    }
    match reader.push(len, &mut buf) {
        Ok(read) => {
            if read == len {
                Ok(MemReader::new(buf))
            } else {
                Err("Couldn't read enough bytes".to_string())
            }
        },
        Err(err) => {
            println!("{}", err);
            Err(err.desc.to_string())
        }
    }
}

struct Packet {
    done: Barrier,
    data: Vec<u8>,
    opcode: OpCode,
    response: AtomicOption<Response>
}

mod perms {
    pub static Read: i32 = 1 << 0;
    pub static Write: i32 = 1 << 1;
    pub static Create: i32 = 1 << 2;
    pub static Delete: i32 = 1 << 3;
    pub static Admin: i32 = 1 << 4;
    pub static All: i32 = Read | Write | Create | Delete | Admin;
}

#[deriving(Show)]
pub struct Acl {
    perms: i32,
    scheme: String,
    id: String
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
    event_type: WatchedEventType,
    keeper_state: KeeperState,
    path: String
}

impl WatchedEvent {
    fn read_from(reader: &mut Reader) -> WatchedEvent {
        let typ = reader.read_be_i32().unwrap();
        let state = reader.read_be_i32().unwrap();
        let path = read_string(reader);
        WatchedEvent{event_type: FromPrimitive::from_i32(typ).unwrap(), keeper_state: FromPrimitive::from_i32(state).unwrap(), path: path}
    }
}

pub struct Zookeeper {
    sock: TcpStream,
    xid: AtomicInt,
    packet_tx: Sender<Arc<Packet>> // sending Packets from methods to writer thread
}

impl Zookeeper {

    pub fn new<W: Watcher>(connect_string: &str, timeout: Duration, watcher: W) -> Result<Zookeeper, &'static str> {

        let sock = Zookeeper::connect(connect_string, timeout).unwrap();

        // comminucating requests (as Packets) from instance methods to writer thread
        let (packet_tx, packet_rx): (Sender<Arc<Packet>>, Receiver<Arc<Packet>>) = channel();
        // communicating sent Packets from writer thread to the reader thread
        let (written_tx, written_rx) = channel();
        // event channel for passing WatchedEvents to watcher on a seperate thread
        let (event_tx, event_rx) = channel();

        // start writer thread in background
        let mut writer_sock = sock.clone();
        let mut reader_sock = sock.clone();

        let reading = Arc::new(AtomicBool::new(true));
        let writing = reading.clone();
        let eventing = reading.clone();

        spawn(proc() {
            while eventing.load(SeqCst) {
                let event = event_rx.recv();
                watcher.handle(&event);
            }
        });

        // TODO need to shut this down clearly, poison pill?
        spawn(proc() {
            println!("writer thread started");

            let mut timer = Timer::new().unwrap();
            let ping_timeout = timer.periodic(timeout);

            while writing.load(SeqCst) {

                // do we have something to send or do we need to ping?
                select! {
                    packet = packet_rx.recv() => {
                        println!("writer thread sending {}", packet.opcode);
                        write_buffer(&mut writer_sock, &packet.data);
                        written_tx.send(packet);
                    },
                    () = ping_timeout.recv() => {
                        println!("Sending Ping to server");
                        let ping = RequestHeader{xid: -2, opcode: Ping as i32}.to_byte_vec();
                        write_buffer(&mut writer_sock, &ping);
                    }
                };
            }
        });

        spawn(proc() {
            println!("reader thread started");

            while reading.load(SeqCst) {
                let reply_header = ReplyHeader::read_from(&mut reader_sock);
                let mut reader = copy(&mut reader_sock, reply_header.len as uint - 16).unwrap(); // TODO
                match reply_header.xid {
                    -2 => println!("Got ping event"),
                    -1 => {
                        let event = WatchedEvent::read_from(&mut reader);
                        event_tx.send(event);
                    },
                   xid => {
                        println!("Got response, gotta find last pending request for xid {}", xid);
                        let packet = written_rx.recv();
                        let result = match reply_header.err {
                            0 => match packet.opcode {
                                Create => CreateResult(CreateResponse::read_from(&mut reader)),
                                GetChildren => GetChildrenResult(GetChildrenResponse::read_from(&mut reader)),
                                CloseSession => { reading.store(false, SeqCst); CloseResult },
                                opcode => fail!("{}Response not implemented yet", opcode)
                            },
                            error => {
                                ErrorResult(FromPrimitive::from_i32(error).unwrap())
                            }
                        };
                        packet.response.fill(box result, SeqCst);
                        packet.done.wait();
                     }
                }
            }
        });

        Ok(Zookeeper{sock: sock, xid: AtomicInt::new(1), packet_tx: packet_tx})
    }

    fn connect(connect_string: &str, timeout: Duration) -> IoResult<TcpStream> {

        let hosts: Vec<SocketAddr> = connect_string.split(',').map(|host| from_str::<SocketAddr>(host).unwrap()).collect();

        loop {
            for host in hosts.iter() {
                println!("Connecting to {}...", host);
                let mut sock = TcpStream::connect_timeout(*host, timeout);
                if sock.is_err() {
                    println!("Connection timeout {}", host);
                    continue;
                }

                write_buffer(&mut sock, &ConnectRequest::new(timeout).to_byte_vec());

                let conn_resp = ConnectResponse::read_from(&mut sock);

                println!("{}", conn_resp);

                return sock
            }
        }
    }

    fn xid(&self) -> i32 {
        self.xid.fetch_add(1, SeqCst) as i32
    }

    fn request<T: Archive>(&mut self, req: T, xid: i32, opcode: OpCode) -> Response {
        let rh = RequestHeader{xid: xid, opcode: opcode as i32};

        let mut buf = MemWriter::new();

        rh.write_into(&mut buf);
        req.write_into(&mut buf);

        let barrier = Barrier::new(2);
        let packet = Arc::new(Packet{data: buf.unwrap(), done: barrier, opcode: opcode, response: AtomicOption::empty()});

        self.packet_tx.send(packet.clone());

        println!("barrier.wait()");
        packet.done.wait();
        println!("barrier.wait() done");
        *packet.response.take(SeqCst).unwrap()
    }

    pub fn get_children(&mut self, path: String, watch: bool) -> ZkResult<Vec<String>> {
        let req = GetChildrenRequest{path: path, watch: watch};

        let xid = self.xid();

        let result = self.request(req, xid, GetChildren);

        fetch_result!(result, GetChildrenResult(children))
    }

    pub fn create(&mut self, path: String, data: Vec<u8>, acl: Vec<Acl>, mode: CreateMode) -> ZkResult<String> {
        let req = CreateRequest{path: path, data: data, acl: acl, flags: mode as i32};

        let xid = self.xid();

        let result = self.request(req, xid, Create);

        fetch_result!(result, CreateResult(path))
    }

    #[allow(unused_must_use)]
    pub fn close(&mut self) {
        self.request(EmptyRequest, 0, CloseSession);
        self.sock.close_write();
        self.sock.close_read();
    }
}

pub trait Watcher: Send {
    fn handle(&self, &WatchedEvent);
}

#[test]
fn it_works() {
    struct LoggingWatcher;
    impl Watcher for LoggingWatcher {
        fn handle(&self, e: &WatchedEvent) {
            println!("{}", e)
        }
    }

    match Zookeeper::new("127.0.0.1:2181", Duration::seconds(2), LoggingWatcher) {
        Ok(mut zk) => {
            let path = zk.create("/test".to_string(), vec![], vec![Acl{perms: perms::All, scheme: "world".to_string(), id: "anyone".to_string()}], Ephemeral);

            println!("created path -> {}", path);

            let children = zk.get_children("/".to_string(), true);

            println!("children of / -> {}", children);

            std::io::stdin().read_line();

            zk.close();
        },
        Err(error) => {
            println!("Error connecting to Zookeeper: {}", error)
        }
    }
}
