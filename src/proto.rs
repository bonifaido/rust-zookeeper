use consts::{KeeperState, WatchedEventType, ZkError};
use std::io::{IoResult, MemWriter, Reader, Writer};
use std::time::Duration;

trait StringReader: Reader {
    fn read_string(&mut self) -> IoResult<String>;
}

pub trait BufferReader: Reader {
    fn read_buffer(&mut self) -> IoResult<Vec<u8>>;
}

pub trait BufferWriter: Writer {
    fn write_buffer(&mut self, buffer: &Vec<u8>) -> IoResult<()>;
}

impl<R: Reader> StringReader for R {
    fn read_string(&mut self) -> IoResult<String> {
        let raw = try!(self.read_buffer());
        Ok(String::from_utf8(raw).unwrap())
    }
}

// A buffer is an u8 string prefixed with it's length as u32
impl<R: Reader> BufferReader for R {
    fn read_buffer(&mut self) -> IoResult<Vec<u8>> {
        let len = try!(self.read_be_i32());
        self.read_exact(len as uint)
    }
}

impl<W: Writer> BufferWriter<> for W {
    fn write_buffer(&mut self, buffer: &Vec<u8>) -> IoResult<()> {
        try!(self.write_be_i32(buffer.len() as i32));
        self.write(buffer.as_slice())
    }
}

pub trait Archive {
    fn write_to(&self, writer: &mut Writer) -> IoResult<()>;

    #[allow(unused_must_use)]
    fn to_byte_vec(&self) -> Vec<u8> {
        let mut w = MemWriter::new();
        self.write_to(&mut w);
        w.unwrap()
    }
}

impl Archive for u8 {
    fn write_to(&self, writer: &mut Writer) -> IoResult<()> {
        writer.write_u8(*self)
    }
}

impl Archive for String {
    #[allow(unused_must_use)]
    fn write_to(&self, writer: &mut Writer) -> IoResult<()> {
        writer.write_be_i32(self.len() as i32);
        writer.write_str(self.as_slice())
    }
}

impl<T: Archive> Archive for Vec<T> {
    #[allow(unused_must_use)]
    fn write_to(&self, writer: &mut Writer) -> IoResult<()> {
        writer.write_be_i32(self.len() as i32);
        let mut res = Ok(());
        for i in self.iter() {
            res = i.write_to(writer)
        }
        res
    }
}

#[deriving(Show)]
pub struct Acl {
    pub perms: i32,
    pub scheme: String,
    pub id: String
}

impl Acl {
    fn read_from<R: Reader>(reader: &mut R) -> Acl {
        let perms = reader.read_be_i32().unwrap();
        let scheme = reader.read_string().unwrap();
        let id = reader.read_string().unwrap();
        Acl{perms: perms, scheme: scheme, id: id}
    }
}

impl Archive for Acl {
    #[allow(unused_must_use)]
    fn write_to(&self, writer: &mut Writer) -> IoResult<()> {
        writer.write_be_i32(self.perms);
        self.scheme.write_to(writer);
        self.id.write_to(writer)
    }
}

#[deriving(Show)]
pub struct Stat {
    pub czxid: i64,
    pub mzxid: i64,
    pub ctime: i64,
    pub mtime: i64,
    pub version: i32,
    pub cversion: i32,
    pub aversion: i32,
    pub ephemeral_owner: i64,
    pub data_length: i32,
    pub num_children: i32,
    pub pzxid: i64
}

impl Stat {
    fn read_from(reader: &mut Reader) -> Stat {
        Stat{
            czxid: reader.read_be_i64().unwrap(),
            mzxid: reader.read_be_i64().unwrap(),
            ctime: reader.read_be_i64().unwrap(),
            mtime: reader.read_be_i64().unwrap(),
            version: reader.read_be_i32().unwrap(),
            cversion: reader.read_be_i32().unwrap(),
            aversion: reader.read_be_i32().unwrap(),
            ephemeral_owner: reader.read_be_i64().unwrap(),
            data_length: reader.read_be_i32().unwrap(),
            num_children: reader.read_be_i32().unwrap(),
            pzxid: reader.read_be_i64().unwrap()}
    }
}

pub struct ConnectRequest {
    protocol_version: i32,
    last_zxid_seen: i64,
    timeout: i32,
    session_id: i64,
    passwd: Vec<u8>,
    read_only: bool
}

impl ConnectRequest {
    pub fn from(conn_resp: ConnectResponse, last_zxid_seen: i64) -> ConnectRequest {
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
    fn write_to(&self, writer: &mut Writer) -> IoResult<()> {
        writer.write_be_i32(self.protocol_version);
        writer.write_be_i64(self.last_zxid_seen);
        writer.write_be_i32(self.timeout);
        writer.write_be_i64(self.session_id);
        self.passwd.write_to(writer);
        writer.write_u8(self.read_only as u8)
    }
}

#[deriving(Show)]
pub struct ConnectResponse {
    protocol_version: i32,
    timeout: i32,
    session_id: i64,
    passwd: Vec<u8>,
    read_only: bool
}

impl ConnectResponse {
    pub fn initial(timeout: Duration) -> ConnectResponse {
        ConnectResponse{
            protocol_version: 0,
            timeout: timeout.num_milliseconds() as i32,
            session_id: 0,
            passwd: [0, ..15].to_vec(),
            read_only: false}
    }

    pub fn read_from<R: Reader>(reader: &mut R) -> ConnectResponse {
        ConnectResponse{
            protocol_version: reader.read_be_i32().unwrap(),
            timeout: reader.read_be_i32().unwrap(),
            session_id: reader.read_be_i64().unwrap(),
            passwd: reader.read_buffer().unwrap(),
            read_only: reader.read_u8().unwrap() != 0}
    }
}

pub struct RequestHeader {
    pub xid: i32,
    pub opcode: i32
}

impl Archive for RequestHeader {
    #[allow(unused_must_use)]
    fn write_to(&self, writer: &mut Writer) -> IoResult<()> {
        writer.write_be_i32(self.xid);
        writer.write_be_i32(self.opcode)
    }
}

#[deriving(Show)]
pub struct ReplyHeader {
    pub xid: i32,
    pub zxid: i64,
    pub err: i32
}

impl ReplyHeader {
    pub fn read_from(reader: &mut Reader) -> ReplyHeader {
        let xid = reader.read_be_i32().unwrap();
        let zxid = reader.read_be_i64().unwrap();
        let err = reader.read_be_i32().unwrap();
        ReplyHeader{xid: xid, zxid: zxid, err: err}
    }
}

pub struct CreateRequest {
    pub path: String,
    pub data: Vec<u8>,
    pub acl: Vec<Acl>,
    pub flags: i32
}

impl Archive for CreateRequest {
    #[allow(unused_must_use)]
    fn write_to(&self, writer: &mut Writer) -> IoResult<()> {
        self.path.write_to(writer);
        self.data.write_to(writer);
        self.acl.write_to(writer);
        writer.write_be_i32(self.flags)
    }
}

pub struct CreateResponse {
    pub path: String
}

impl CreateResponse {
    pub fn read_from<R: Reader>(reader: &mut R) -> CreateResponse {
        CreateResponse{path: reader.read_string().unwrap()}
    }
}

pub struct DeleteRequest {
    pub path: String,
    pub version: i32
}

impl Archive for DeleteRequest {
    #[allow(unused_must_use)]
    fn write_to(&self, writer: &mut Writer) -> IoResult<()> {
        self.path.write_to(writer);
        writer.write_be_i32(self.version)
    }
}

struct StringAndBoolRequest {
    pub path: String,
    pub watch: bool
}

impl Archive for StringAndBoolRequest {
    #[allow(unused_must_use)]
    fn write_to(&self, writer: &mut Writer) -> IoResult<()> {
        self.path.write_to(writer);
        writer.write_u8(self.watch as u8)
    }
}

pub type ExistsRequest = StringAndBoolRequest;

pub struct StatResponse {
    pub stat: Stat
}

impl StatResponse {
    pub fn read_from(reader: &mut Reader) -> StatResponse {
        StatResponse{stat: Stat::read_from(reader)}
    }
}

pub struct GetAclRequest {
    pub path: String
}

impl Archive for GetAclRequest {
    #[allow(unused_must_use)]
    fn write_to(&self, writer: &mut Writer) -> IoResult<()> {
        self.path.write_to(writer)
    }
}

pub struct GetAclResponse {
    pub acl_stat: (Vec<Acl>, Stat)
}

impl GetAclResponse {
    pub fn read_from<R: Reader>(reader: &mut R) -> GetAclResponse {
        let len = reader.read_be_i32().unwrap();
        let mut acl = Vec::new();
        for _ in range(0, len) {
            acl.push(Acl::read_from(reader));
        }
        let stat = Stat::read_from(reader);
        GetAclResponse{acl_stat: (acl, stat)}
    }
}

pub struct SetAclRequest {
    pub path: String,
    pub acl: Vec<Acl>,
    pub version: i32
}

impl Archive for SetAclRequest {
    #[allow(unused_must_use)]
    fn write_to(&self, writer: &mut Writer) -> IoResult<()> {
        self.path.write_to(writer);
        self.acl.write_to(writer);
        writer.write_be_i32(self.version)
    }
}

pub struct SetDataRequest {
    pub path: String,
    pub data: Vec<u8>,
    pub version: i32
}

impl Archive for SetDataRequest {
    #[allow(unused_must_use)]
    fn write_to(&self, writer: &mut Writer) -> IoResult<()> {
        self.path.write_to(writer);
        self.data.write_to(writer);
        writer.write_be_i32(self.version)
    }
}

pub type GetChildrenRequest = StringAndBoolRequest;

pub struct GetChildrenResponse {
    pub children: Vec<String>
}

impl GetChildrenResponse {
    pub fn read_from<R: Reader>(reader: &mut R) -> GetChildrenResponse {
        let len = reader.read_be_i32().unwrap();
        let mut children = Vec::new();
        for _ in range(0, len) {
            children.push(reader.read_string().unwrap());
        }
        GetChildrenResponse{children: children}
    }
}

pub type GetDataRequest = StringAndBoolRequest;

pub struct GetDataResponse {
    pub data_stat: (Vec<u8>, Stat)
}

impl GetDataResponse {
    pub fn read_from<R: Reader>(reader: &mut R) -> GetDataResponse {
        let data = reader.read_buffer().unwrap();
        let stat = Stat::read_from(reader);
        GetDataResponse{data_stat: (data, stat)}
    }
}

pub struct AuthRequest {
    pub typ: i32,
    pub scheme: String,
    pub auth: Vec<u8>
}

impl Archive for AuthRequest {
    #[allow(unused_must_use)]
    fn write_to(&self, writer: &mut Writer) -> IoResult<()> {
        writer.write_be_i32(self.typ);
        self.scheme.write_to(writer);
        self.auth.write_to(writer)
    }
}

pub struct EmptyRequest;

impl Archive for EmptyRequest {
    fn write_to(&self, _: &mut Writer) -> IoResult<()> { Ok(()) }
}

#[deriving(Show)]
pub struct WatchedEvent {
    pub event_type: WatchedEventType,
    pub keeper_state: KeeperState,
    pub path: String
}

impl WatchedEvent {
    pub fn read_from<R: Reader>(reader: &mut R) -> WatchedEvent {
        let typ = reader.read_be_i32().unwrap();
        let state = reader.read_be_i32().unwrap();
        let path = reader.read_string().unwrap();
        WatchedEvent{
            event_type: FromPrimitive::from_i32(typ).unwrap(),
            keeper_state: FromPrimitive::from_i32(state).unwrap(),
            path: path}
    }
}

pub enum Response {
    AuthResult,
    CloseResult,
    CreateResult(CreateResponse),
    DeleteResult,
    ErrorResult(ZkError),
    ExistsResult(StatResponse),
    GetAclResult(GetAclResponse),
    GetChildrenResult(GetChildrenResponse),
    GetDataResult(GetDataResponse),
    SetDataResult(StatResponse),
    SetAclResult(StatResponse)
}
