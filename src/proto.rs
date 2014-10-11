use consts::{KeeperState, WatchedEventType, ZkError};
use std::io::{IoResult, MemWriter, Reader, Writer};
use std::time::Duration;

#[allow(unused_must_use)]
pub fn write_buffer(writer: &mut Writer, buffer: &Vec<u8>) -> IoResult<()> {
    writer.write_be_i32(buffer.len() as i32);
    return writer.write(buffer.as_slice());
}

pub fn read_buffer(reader: &mut Reader) -> IoResult<Vec<u8>> {
    let len = try!(reader.read_be_i32());
    reader.read_exact(len as uint)
}

#[allow(unused_must_use)]
pub fn write_string(writer: &mut Writer, string: &String) {
    writer.write_be_i32(string.len() as i32);
    writer.write_str(string.as_slice());
}

pub fn read_string(reader: &mut Reader) -> String {
    let raw = read_buffer(reader).unwrap();
    String::from_utf8(raw).unwrap()
}

pub trait Archive {
    fn write_into(&self, writer: &mut Writer);

    fn to_byte_vec(&self) -> Vec<u8> {
        let mut w = MemWriter::new();
        self.write_into(&mut w);
        w.unwrap()
    }
}

#[deriving(Show)]
pub struct Acl {
    pub perms: i32,
    pub scheme: String,
    pub id: String
}

impl Acl {
    fn read_from(reader: &mut Reader) -> Acl {
        let perms = reader.read_be_i32().unwrap();
        let scheme = read_string(reader);
        let id = read_string(reader);
        Acl{perms: perms, scheme: scheme, id: id}
    }
}

impl Archive for Acl {
    #[allow(unused_must_use)]
    fn write_into(&self, writer: &mut Writer) {
        writer.write_be_i32(self.perms);
        write_string(writer, &self.scheme);
        write_string(writer, &self.id);
    }
}

impl<T: Archive> Archive for Vec<T> {
    #[allow(unused_must_use)]
    fn write_into(&self, writer: &mut Writer) {
        writer.write_be_i32(self.len() as i32);
        for i in self.iter() {
            i.write_into(writer);
        }
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

    pub fn read_from(reader: &mut Reader) -> ConnectResponse {
        ConnectResponse{
            protocol_version: reader.read_be_i32().unwrap(),
            timeout: reader.read_be_i32().unwrap(),
            session_id: reader.read_be_i64().unwrap(),
            passwd: read_buffer(reader).unwrap(),
            read_only: reader.read_u8().unwrap() != 0}
    }
}

pub struct RequestHeader {
    pub xid: i32,
    pub opcode: i32
}

impl Archive for RequestHeader {
    #[allow(unused_must_use)]
    fn write_into(&self, writer: &mut Writer) {
        writer.write_be_i32(self.xid);
        writer.write_be_i32(self.opcode);
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
    fn write_into(&self, writer: &mut Writer) {
        write_string(writer, &self.path);
        write_buffer(writer, &self.data);
        self.acl.write_into(writer);
        writer.write_be_i32(self.flags);
    }
}

pub struct CreateResponse {
    pub path: String
}

impl CreateResponse {
    pub fn read_from(reader: &mut Reader) -> CreateResponse {
        CreateResponse{path: read_string(reader)}
    }
}

pub struct DeleteRequest {
    pub path: String,
    pub version: i32
}

impl Archive for DeleteRequest {
    #[allow(unused_must_use)]
    fn write_into(&self, writer: &mut Writer) {
        write_string(writer, &self.path);
        writer.write_be_i32(self.version);
    }
}

pub struct ExistsRequest {
    pub path: String,
    pub watch: bool
}

impl Archive for ExistsRequest {
    #[allow(unused_must_use)]
    fn write_into(&self, writer: &mut Writer) {
        write_string(writer, &self.path);
        writer.write_u8(self.watch as u8);
    }
}

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
    fn write_into(&self, writer: &mut Writer) {
        write_string(writer, &self.path);
    }
}

pub struct GetAclResponse {
    pub acl_stat: (Vec<Acl>, Stat)
}

impl GetAclResponse {
    pub fn read_from(reader: &mut Reader) -> GetAclResponse {
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
    fn write_into(&self, writer: &mut Writer) {
        write_string(writer, &self.path);
        self.acl.write_into(writer);
        writer.write_be_i32(self.version as i32);
    }
}

pub struct SetDataRequest {
    pub path: String,
    pub data: Vec<u8>,
    pub version: i32
}

impl Archive for SetDataRequest {
    #[allow(unused_must_use)]
    fn write_into(&self, writer: &mut Writer) {
        write_string(writer, &self.path);
        write_buffer(writer, &self.data);
        writer.write_be_i32(self.version as i32);
    }
}

pub struct GetChildrenRequest {
    pub path: String,
    pub watch: bool
}

impl Archive for GetChildrenRequest {
    #[allow(unused_must_use)]
    fn write_into(&self, writer: &mut Writer) {
        write_string(writer, &self.path);
        writer.write_u8(self.watch as u8);
    }
}

pub struct GetChildrenResponse {
    pub children: Vec<String>
}

impl GetChildrenResponse {
    pub fn read_from(reader: &mut Reader) -> GetChildrenResponse {
        let len = reader.read_be_i32().unwrap();
        let mut children = Vec::new();
        for _ in range(0, len) {
            children.push(read_string(reader));
        }
        GetChildrenResponse{children: children}
    }
}

pub struct GetDataRequest {
    pub path: String,
    pub watch: bool
}

impl Archive for GetDataRequest {
    #[allow(unused_must_use)]
    fn write_into(&self, writer: &mut Writer) {
        write_string(writer, &self.path);
        writer.write_u8(self.watch as u8);
    }
}

pub struct GetDataResponse {
    pub data_stat: (Vec<u8>, Stat)
}

impl GetDataResponse {
    pub fn read_from(reader: &mut Reader) -> GetDataResponse {
        let data = read_buffer(reader).unwrap();
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
    fn write_into(&self, writer: &mut Writer) {
        writer.write_be_i32(self.typ);
        write_string(writer, &self.scheme);
        write_buffer(writer, &self.auth);
    }
}

pub struct EmptyRequest;

impl Archive for EmptyRequest {
    fn write_into(&self, _: &mut Writer) {}
}

#[deriving(Show)]
pub struct WatchedEvent {
    pub event_type: WatchedEventType,
    pub keeper_state: KeeperState,
    pub path: String
}

impl WatchedEvent {
    pub fn read_from(reader: &mut Reader) -> WatchedEvent {
        let typ = reader.read_be_i32().unwrap();
        let state = reader.read_be_i32().unwrap();
        let path = read_string(reader);
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
