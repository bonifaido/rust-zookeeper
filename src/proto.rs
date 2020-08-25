use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::convert::TryFrom;
use std::io::{Cursor, Error, ErrorKind, Read, Result, Write};

use crate::{Acl, KeeperState, Permission, Stat, WatchedEvent, WatchedEventType};

/// Operation code for messages. See `RequestHeader`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum OpCode {
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
    CloseSession = -11,
}

pub type ByteBuf = Cursor<Vec<u8>>;

pub trait ReadFrom: Sized {
    fn read_from<R: Read>(read: &mut R) -> Result<Self>;
}

pub trait WriteTo {
    fn write_to(&self, writer: &mut dyn Write) -> Result<()>;

    fn to_len_prefixed_buf(&self) -> Result<ByteBuf> {
        let mut buf = Cursor::new(Vec::new());
        buf.set_position(4);
        self.write_to(&mut buf)?;
        let len = buf.position() - 4;
        buf.set_position(0);
        buf.write_i32::<BigEndian>(len as i32)?;
        buf.set_position(0);
        Ok(buf)
    }
}

pub fn to_len_prefixed_buf<Request: WriteTo>(rh: RequestHeader, req: Request) -> Result<ByteBuf> {
    let mut buf = Cursor::new(Vec::new());
    buf.set_position(4);
    rh.write_to(&mut buf)?;
    req.write_to(&mut buf)?;
    let len = buf.position() - 4;
    buf.set_position(0);
    buf.write_i32::<BigEndian>(len as i32)?;
    buf.set_position(0);
    Ok(buf)
}

fn error(msg: &str) -> Error {
    Error::new(ErrorKind::InvalidInput, msg)
}

trait StringReader: Read {
    fn read_string(&mut self) -> Result<String>;
}

pub trait BufferReader: Read {
    fn read_buffer(&mut self) -> Result<Vec<u8>>;
}

impl<R: Read> StringReader for R {
    fn read_string(&mut self) -> Result<String> {
        let raw = self.read_buffer()?;
        Ok(String::from_utf8(raw).unwrap())
    }
}

// A buffer is an u8 string prefixed with it's length as i32
impl<R: Read> BufferReader for R {
    fn read_buffer(&mut self) -> Result<Vec<u8>> {
        let len = self.read_i32::<BigEndian>()?;
        let len = if len < 0 { 0 } else { len as usize };
        let mut buf = vec![0; len];
        let read = self.read(&mut buf)?;
        if read == len {
            Ok(buf)
        } else {
            Err(error("read_buffer failed"))
        }
    }
}

impl WriteTo for u8 {
    fn write_to(&self, writer: &mut dyn Write) -> Result<()> {
        writer.write_u8(*self)?;
        Ok(())
    }
}

impl WriteTo for String {
    fn write_to(&self, writer: &mut dyn Write) -> Result<()> {
        writer.write_i32::<BigEndian>(self.len() as i32)?;
        writer.write_all(self.as_ref())
    }
}

impl<T: WriteTo> WriteTo for Vec<T> {
    fn write_to(&self, writer: &mut dyn Write) -> Result<()> {
        writer.write_i32::<BigEndian>(self.len() as i32)?;
        let mut res = Ok(());
        for elem in self.iter() {
            res = elem.write_to(writer);
            if res.is_err() {
                return res;
            }
        }
        res
    }
}

impl ReadFrom for Acl {
    fn read_from<R: Read>(read: &mut R) -> Result<Acl> {
        Ok(Acl {
            perms: Permission::from_raw(read.read_u32::<BigEndian>()?),
            scheme: read.read_string()?,
            id: read.read_string()?,
        })
    }
}

impl WriteTo for Acl {
    fn write_to(&self, writer: &mut dyn Write) -> Result<()> {
        writer.write_u32::<BigEndian>(self.perms.code())?;
        self.scheme.write_to(writer)?;
        self.id.write_to(writer)
    }
}

impl ReadFrom for Stat {
    fn read_from<R: Read>(read: &mut R) -> Result<Stat> {
        Ok(Stat {
            czxid: read.read_i64::<BigEndian>()?,
            mzxid: read.read_i64::<BigEndian>()?,
            ctime: read.read_i64::<BigEndian>()?,
            mtime: read.read_i64::<BigEndian>()?,
            version: read.read_i32::<BigEndian>()?,
            cversion: read.read_i32::<BigEndian>()?,
            aversion: read.read_i32::<BigEndian>()?,
            ephemeral_owner: read.read_i64::<BigEndian>()?,
            data_length: read.read_i32::<BigEndian>()?,
            num_children: read.read_i32::<BigEndian>()?,
            pzxid: read.read_i64::<BigEndian>()?,
        })
    }
}

pub struct ConnectRequest {
    protocol_version: i32,
    last_zxid_seen: i64,
    timeout: i32,
    session_id: i64,
    passwd: Vec<u8>,
    read_only: bool,
}

impl ConnectRequest {
    pub fn from(conn_resp: &ConnectResponse, last_zxid_seen: i64) -> ConnectRequest {
        ConnectRequest {
            protocol_version: conn_resp.protocol_version,
            last_zxid_seen: last_zxid_seen,
            timeout: conn_resp.timeout as i32,
            session_id: conn_resp.session_id,
            passwd: conn_resp.passwd.clone(),
            read_only: conn_resp.read_only,
        }
    }
}

impl WriteTo for ConnectRequest {
    fn write_to(&self, writer: &mut dyn Write) -> Result<()> {
        writer.write_i32::<BigEndian>(self.protocol_version)?;
        writer.write_i64::<BigEndian>(self.last_zxid_seen)?;
        writer.write_i32::<BigEndian>(self.timeout)?;
        writer.write_i64::<BigEndian>(self.session_id)?;
        self.passwd.write_to(writer)?;
        writer.write_u8(self.read_only as u8)?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct ConnectResponse {
    protocol_version: i32,
    pub timeout: u64, // is handled as i32
    pub session_id: i64,
    passwd: Vec<u8>,
    pub read_only: bool,
}

impl ConnectResponse {
    pub fn initial(timeout: u64) -> ConnectResponse {
        ConnectResponse {
            protocol_version: 0,
            timeout,
            session_id: 0,
            passwd: vec![0; 16],
            read_only: false,
        }
    }
}

impl ReadFrom for ConnectResponse {
    fn read_from<R: Read>(reader: &mut R) -> Result<ConnectResponse> {
        Ok(ConnectResponse {
            protocol_version: reader.read_i32::<BigEndian>()?,
            timeout: reader.read_i32::<BigEndian>()? as u64,
            session_id: reader.read_i64::<BigEndian>()?,
            passwd: reader.read_buffer()?,
            read_only: reader.read_u8()? != 0,
        })
    }
}

pub struct RequestHeader {
    pub xid: i32,
    pub opcode: OpCode,
}

impl WriteTo for RequestHeader {
    fn write_to(&self, writer: &mut dyn Write) -> Result<()> {
        writer.write_i32::<BigEndian>(self.xid)?;
        writer.write_i32::<BigEndian>(self.opcode as i32)?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct ReplyHeader {
    pub xid: i32,
    pub zxid: i64,
    pub err: i32,
}

impl ReadFrom for ReplyHeader {
    fn read_from<R: Read>(read: &mut R) -> Result<ReplyHeader> {
        Ok(ReplyHeader {
            xid: read.read_i32::<BigEndian>()?,
            zxid: read.read_i64::<BigEndian>()?,
            err: read.read_i32::<BigEndian>()?,
        })
    }
}

pub struct CreateRequest {
    pub path: String,
    pub data: Vec<u8>,
    pub acl: Vec<Acl>,
    pub flags: i32,
}

impl WriteTo for CreateRequest {
    fn write_to(&self, writer: &mut dyn Write) -> Result<()> {
        self.path.write_to(writer)?;
        self.data.write_to(writer)?;
        self.acl.write_to(writer)?;
        writer.write_i32::<BigEndian>(self.flags)?;
        Ok(())
    }
}

pub struct CreateResponse {
    pub path: String,
}

impl ReadFrom for CreateResponse {
    fn read_from<R: Read>(reader: &mut R) -> Result<CreateResponse> {
        Ok(CreateResponse {
            path: reader.read_string()?,
        })
    }
}

pub struct DeleteRequest {
    pub path: String,
    pub version: i32,
}

impl WriteTo for DeleteRequest {
    fn write_to(&self, writer: &mut dyn Write) -> Result<()> {
        self.path.write_to(writer)?;
        writer.write_i32::<BigEndian>(self.version)?;
        Ok(())
    }
}

pub struct StringAndBoolRequest {
    pub path: String,
    pub watch: bool,
}

impl WriteTo for StringAndBoolRequest {
    fn write_to(&self, writer: &mut dyn Write) -> Result<()> {
        self.path.write_to(writer)?;
        writer.write_u8(self.watch as u8)?;
        Ok(())
    }
}

pub type ExistsRequest = StringAndBoolRequest;
pub type ExistsResponse = StatResponse;

pub struct StatResponse {
    pub stat: Stat,
}

impl ReadFrom for StatResponse {
    fn read_from<R: Read>(read: &mut R) -> Result<StatResponse> {
        Ok(StatResponse {
            stat: Stat::read_from(read)?,
        })
    }
}

pub struct GetAclRequest {
    pub path: String,
}

impl WriteTo for GetAclRequest {
    fn write_to(&self, writer: &mut dyn Write) -> Result<()> {
        self.path.write_to(writer)
    }
}

pub struct GetAclResponse {
    pub acl_stat: (Vec<Acl>, Stat),
}

impl ReadFrom for GetAclResponse {
    fn read_from<R: Read>(reader: &mut R) -> Result<GetAclResponse> {
        let len = reader.read_i32::<BigEndian>()?;
        let mut acl = Vec::with_capacity(len as usize);
        for _ in 0..len {
            acl.push(Acl::read_from(reader)?);
        }
        let stat = Stat::read_from(reader)?;
        Ok(GetAclResponse {
            acl_stat: (acl, stat),
        })
    }
}

pub struct SetAclRequest {
    pub path: String,
    pub acl: Vec<Acl>,
    pub version: i32,
}

impl WriteTo for SetAclRequest {
    fn write_to(&self, writer: &mut dyn Write) -> Result<()> {
        self.path.write_to(writer)?;
        self.acl.write_to(writer)?;
        writer.write_i32::<BigEndian>(self.version)?;
        Ok(())
    }
}

pub type SetAclResponse = StatResponse;

pub struct SetDataRequest {
    pub path: String,
    pub data: Vec<u8>,
    pub version: i32,
}

impl WriteTo for SetDataRequest {
    fn write_to(&self, writer: &mut dyn Write) -> Result<()> {
        self.path.write_to(writer)?;
        self.data.write_to(writer)?;
        writer.write_i32::<BigEndian>(self.version)?;
        Ok(())
    }
}

pub type SetDataResponse = StatResponse;

pub type GetChildrenRequest = StringAndBoolRequest;

pub struct GetChildrenResponse {
    pub children: Vec<String>,
}

impl ReadFrom for GetChildrenResponse {
    fn read_from<R: Read>(reader: &mut R) -> Result<GetChildrenResponse> {
        let len = reader.read_i32::<BigEndian>()?;
        let mut children = Vec::with_capacity(len as usize);
        for _ in 0..len {
            children.push(reader.read_string()?);
        }
        Ok(GetChildrenResponse { children })
    }
}

pub type GetDataRequest = StringAndBoolRequest;

pub struct GetDataResponse {
    pub data_stat: (Vec<u8>, Stat),
}

impl ReadFrom for GetDataResponse {
    fn read_from<R: Read>(reader: &mut R) -> Result<GetDataResponse> {
        let data = reader.read_buffer()?;
        let stat = Stat::read_from(reader)?;
        Ok(GetDataResponse {
            data_stat: (data, stat),
        })
    }
}

pub struct AuthRequest {
    pub typ: i32,
    pub scheme: String,
    pub auth: Vec<u8>,
}

impl WriteTo for AuthRequest {
    fn write_to(&self, writer: &mut dyn Write) -> Result<()> {
        writer.write_i32::<BigEndian>(self.typ)?;
        self.scheme.write_to(writer)?;
        self.auth.write_to(writer)
    }
}

pub struct EmptyRequest;
pub struct EmptyResponse;

impl WriteTo for EmptyRequest {
    fn write_to(&self, _: &mut dyn Write) -> Result<()> {
        Ok(())
    }
}

impl ReadFrom for EmptyResponse {
    fn read_from<R: Read>(_: &mut R) -> Result<EmptyResponse> {
        Ok(EmptyResponse)
    }
}

impl ReadFrom for WatchedEvent {
    fn read_from<R: Read>(reader: &mut R) -> Result<WatchedEvent> {
        let type_raw = reader.read_i32::<BigEndian>()?;
        let state_raw = reader.read_i32::<BigEndian>()?;
        let path = reader.read_string()?;
        let event_type = WatchedEventType::try_from(type_raw)
            .map_err(|error| Error::new(ErrorKind::Other, error))?;
        let state = KeeperState::try_from(state_raw)
            .map_err(|error| Error::new(ErrorKind::Other, error))?;
        Ok(WatchedEvent {
            event_type,
            keeper_state: state,
            path: Some(path),
        })
    }
}
