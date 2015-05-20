use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};
use consts::{KeeperState, WatchedEventType, ZkError};
use std::io::{Read, Write, Result, Error, ErrorKind};
use std::time::Duration;
use num::FromPrimitive;

trait StringReader: Read {
    fn read_string(&mut self) -> Result<String>;
}

pub trait BufferReader: Read {
    fn read_buffer(&mut self) -> Result<Vec<u8>>;
}

pub trait BufferWriter: Write {
    fn write_buffer(&mut self, buffer: &Vec<u8>) -> Result<()>;
}

impl<R: Read> StringReader for R {
    fn read_string(&mut self) -> Result<String> {
        let raw = try!(self.read_buffer());
        Ok(String::from_utf8(raw).unwrap())
    }
}

// A buffer is an u8 string prefixed with it's length as u32
impl<R: Read> BufferReader for R {
    fn read_buffer(&mut self) -> Result<Vec<u8>> {
        let len = try!(self.read_i32::<BigEndian>()) as usize;
        let mut buf = vec![0; len];
        let read = try!(self.read(&mut buf[..]));
        if read == len {
            Ok(buf)
        } else {
            Err(Error::new(ErrorKind::Other, "read_buffer failed"))
        }
    }
}

impl<W: Write> BufferWriter<> for W {
    fn write_buffer(&mut self, buffer: &Vec<u8>) -> Result<()> {
        try!(self.write_i32::<BigEndian>(buffer.len() as i32));
        self.write_all(buffer)
    }
}

pub trait Archive {
    fn write_to(&self, writer: &mut Write) -> Result<()>;

    #[allow(unused_must_use)]
    fn to_byte_vec(&self) -> Vec<u8> {
        let mut w = Vec::new();
        self.write_to(&mut w); // should never fail
        w
    }
}

impl Archive for u8 {
    fn write_to(&self, writer: &mut Write) -> Result<()> {
        try!(writer.write_u8(*self));
        Ok(())
    }
}

impl Archive for String {
    #[allow(unused_must_use)]
    fn write_to(&self, writer: &mut Write) -> Result<()> {
        try!(writer.write_i32::<BigEndian>(self.len() as i32));
        writer.write_all(self.as_ref())
    }
}

impl<T: Archive> Archive for Vec<T> {
    #[allow(unused_must_use)]
    fn write_to(&self, writer: &mut Write) -> Result<()> {
        try!(writer.write_i32::<BigEndian>(self.len() as i32));
        let mut res = Ok(());
        for elem in self.iter() {
            res = elem.write_to(writer);
            if res.is_err() {
                return res
            }
        }
        res
    }
}

#[derive(Debug)]
pub struct Acl {
    pub perms: i32,
    pub scheme: String,
    pub id: String
}

impl Acl {
    fn read_from<R: Read>(reader: &mut R) -> Acl {
        let perms = reader.read_i32::<BigEndian>().unwrap();
        let scheme = reader.read_string().unwrap();
        let id = reader.read_string().unwrap();
        Acl{perms: perms, scheme: scheme, id: id}
    }
}

impl Archive for Acl {
    #[allow(unused_must_use)]
    fn write_to(&self, writer: &mut Write) -> Result<()> {
        try!(writer.write_i32::<BigEndian>(self.perms));
        try!(self.scheme.write_to(writer));
        self.id.write_to(writer)
    }
}

#[allow(missing_copy_implementations)]
#[derive(Debug)]
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
    fn read_from(reader: &mut Read) -> Stat {
        Stat{
            czxid: reader.read_i64::<BigEndian>().unwrap(),
            mzxid: reader.read_i64::<BigEndian>().unwrap(),
            ctime: reader.read_i64::<BigEndian>().unwrap(),
            mtime: reader.read_i64::<BigEndian>().unwrap(),
            version: reader.read_i32::<BigEndian>().unwrap(),
            cversion: reader.read_i32::<BigEndian>().unwrap(),
            aversion: reader.read_i32::<BigEndian>().unwrap(),
            ephemeral_owner: reader.read_i64::<BigEndian>().unwrap(),
            data_length: reader.read_i32::<BigEndian>().unwrap(),
            num_children: reader.read_i32::<BigEndian>().unwrap(),
            pzxid: reader.read_i64::<BigEndian>().unwrap()}
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
    fn write_to(&self, writer: &mut Write) -> Result<()> {
        try!(writer.write_i32::<BigEndian>(self.protocol_version));
        try!(writer.write_i64::<BigEndian>(self.last_zxid_seen));
        try!(writer.write_i32::<BigEndian>(self.timeout));
        try!(writer.write_i64::<BigEndian>(self.session_id));
        try!(self.passwd.write_to(writer));
        try!(writer.write_u8(self.read_only as u8));
        Ok(())
    }
}

#[derive(Debug)]
pub struct ConnectResponse {
    protocol_version: i32,
    timeout: i32,
    session_id: i64,
    passwd: Vec<u8>,
    pub read_only: bool
}

impl ConnectResponse {
    pub fn initial(timeout: Duration) -> ConnectResponse {
        ConnectResponse{
            protocol_version: 0,
            timeout: timeout.secs() as i32 * 1000,
            session_id: 0,
            passwd: [0;16].to_vec(),
            read_only: false}
    }

    pub fn read_from<R: Read>(reader: &mut R) -> ConnectResponse {
        ConnectResponse{
            protocol_version: reader.read_i32::<BigEndian>().unwrap(),
            timeout: reader.read_i32::<BigEndian>().unwrap(),
            session_id: reader.read_i64::<BigEndian>().unwrap(),
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
    fn write_to(&self, writer: &mut Write) -> Result<()> {
        try!(writer.write_i32::<BigEndian>(self.xid));
        try!(writer.write_i32::<BigEndian>(self.opcode));
        Ok(())
    }
}

#[derive(Debug)]
pub struct ReplyHeader {
    pub xid: i32,
    pub zxid: i64,
    pub err: i32
}

impl ReplyHeader {
    pub fn read_from(reader: &mut Read) -> ReplyHeader {
        let xid = reader.read_i32::<BigEndian>().unwrap();
        let zxid = reader.read_i64::<BigEndian>().unwrap();
        let err = reader.read_i32::<BigEndian>().unwrap();
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
    fn write_to(&self, writer: &mut Write) -> Result<()> {
        try!(self.path.write_to(writer));
        try!(self.data.write_to(writer));
        try!(self.acl.write_to(writer));
        try!(writer.write_i32::<BigEndian>(self.flags));
        Ok(())
    }
}

pub struct CreateResponse {
    pub path: String
}

impl CreateResponse {
    pub fn read_from<R: Read>(reader: &mut R) -> CreateResponse {
        CreateResponse{path: reader.read_string().unwrap()}
    }
}

pub struct DeleteRequest {
    pub path: String,
    pub version: i32
}

impl Archive for DeleteRequest {
    #[allow(unused_must_use)]
    fn write_to(&self, writer: &mut Write) -> Result<()> {
        try!(self.path.write_to(writer));
        try!(writer.write_i32::<BigEndian>(self.version));
        Ok(())
    }
}

struct StringAndBoolRequest {
    pub path: String,
    pub watch: bool
}

impl Archive for StringAndBoolRequest {
    #[allow(unused_must_use)]
    fn write_to(&self, writer: &mut Write) -> Result<()> {
        try!(self.path.write_to(writer));
        try!(writer.write_u8(self.watch as u8));
        Ok(())
    }
}

pub type ExistsRequest = StringAndBoolRequest;

pub struct StatResponse {
    pub stat: Stat
}

impl StatResponse {
    pub fn read_from(reader: &mut Read) -> StatResponse {
        StatResponse{stat: Stat::read_from(reader)}
    }
}

pub struct GetAclRequest {
    pub path: String
}

impl Archive for GetAclRequest {
    #[allow(unused_must_use)]
    fn write_to(&self, writer: &mut Write) -> Result<()> {
        self.path.write_to(writer)
    }
}

pub struct GetAclResponse {
    pub acl_stat: (Vec<Acl>, Stat)
}

impl GetAclResponse {
    pub fn read_from<R: Read>(reader: &mut R) -> GetAclResponse {
        let len = reader.read_i32::<BigEndian>().unwrap();
        let mut acl = Vec::new();
        for _ in 0..len {
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
    fn write_to(&self, writer: &mut Write) -> Result<()> {
        try!(self.path.write_to(writer));
        try!(self.acl.write_to(writer));
        try!(writer.write_i32::<BigEndian>(self.version));
        Ok(())
    }
}

pub struct SetDataRequest {
    pub path: String,
    pub data: Vec<u8>,
    pub version: i32
}

impl Archive for SetDataRequest {
    #[allow(unused_must_use)]
    fn write_to(&self, writer: &mut Write) -> Result<()> {
        try!(self.path.write_to(writer));
        try!(self.data.write_to(writer));
        try!(writer.write_i32::<BigEndian>(self.version));
        Ok(())
    }
}

pub type GetChildrenRequest = StringAndBoolRequest;

pub struct GetChildrenResponse {
    pub children: Vec<String>
}

impl GetChildrenResponse {
    pub fn read_from<R: Read>(reader: &mut R) -> GetChildrenResponse {
        let len = reader.read_i32::<BigEndian>().unwrap();
        let mut children = Vec::new();
        for _ in 0..len {
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
    pub fn read_from<R: Read>(reader: &mut R) -> GetDataResponse {
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
    fn write_to(&self, writer: &mut Write) -> Result<()> {
        writer.write_i32::<BigEndian>(self.typ);
        self.scheme.write_to(writer);
        self.auth.write_to(writer)
    }
}

pub struct EmptyRequest;

impl Archive for EmptyRequest {
    fn write_to(&self, _: &mut Write) -> Result<()> { Ok(()) }
}

#[derive(Debug)]
pub struct WatchedEvent {
    pub event_type: WatchedEventType,
    pub keeper_state: KeeperState,
    pub path: Option<String>
}

impl WatchedEvent {
    pub fn read_from<R: Read>(reader: &mut R) -> WatchedEvent {
        let typ = reader.read_i32::<BigEndian>().unwrap();
        let state = reader.read_i32::<BigEndian>().unwrap();
        let path = reader.read_string().unwrap();
        WatchedEvent{
            event_type: FromPrimitive::from_i32(typ).unwrap(),
            keeper_state: FromPrimitive::from_i32(state).unwrap(),
            path: Some(path)}
    }
}

pub enum Response {
    Auth,
    Close,
    Create(CreateResponse),
    Delete,
    Error(ZkError),
    Exists(StatResponse),
    GetAcl(GetAclResponse),
    GetChildren(GetChildrenResponse),
    GetData(GetDataResponse),
    SetData(StatResponse),
    SetAcl(StatResponse)
}
