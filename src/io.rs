use consts::{OpCode, ZkError, ZkState};
use proto::{ByteBuf, ConnectRequest, ConnectResponse, ReadFrom, ReplyHeader, RequestHeader, WriteTo};

use byteorder::{ReadBytesExt, BigEndian};
use bytes::Buf;
use mio::{EventLoop, EventSet, Handler, PollOpt, Sender, Timeout, Token, TryRead, TryWrite};
use mio::tcp::TcpStream;
use std::collections::VecDeque;
use std::io;
use std::io::Cursor;
use std::mem;
use std::net::SocketAddr;
use std::sync::mpsc::SyncSender;
use std::time::Duration;

const ZK: Token = Token(0);

lazy_static! {
    static ref PING: ByteBuf = RequestHeader{xid: -2, opcode: OpCode::Ping}.to_len_prefixed_buf();
}

#[derive(Clone)]
pub struct RawRequest {
    pub opcode: OpCode,
    pub data: ByteBuf,
    pub listener: Option<SyncSender<RawResponse>>
}

pub struct RawResponse {
    pub header: ReplyHeader,
    pub data: ByteBuf
}

struct Hosts {
    addrs: Vec<SocketAddr>,
    index: usize
}

impl Hosts {
    fn new(addrs: Vec<SocketAddr>) -> Hosts {
        Hosts{addrs: addrs, index: 0}
    }

    fn next(&mut self) -> &SocketAddr {
        let addr = &self.addrs[self.index];
        if self.addrs.len() == self.index+1 {
            self.index = 0;
        } else {
            self.index += 1;
        }
        addr
    }
}

struct ZkHandler {
    sock: TcpStream,
    state: ZkState,
    hosts: Hosts,
    buffer: VecDeque<RawRequest>,
    inflight: VecDeque<RawRequest>,
    response: Vec<u8>,
    response_len: usize,
    timeout: Option<Timeout>,
    timeout_ms: u64,
    event_sender: Sender<RawResponse>,
}

impl ZkHandler {
    fn new(addrs: Vec<SocketAddr>, timeout_ms: u64, event_sender: Sender<RawResponse>) -> ZkHandler {
        ZkHandler{
            sock: unsafe{mem::dropped()},
            state: ZkState::NotConnected,
            hosts: Hosts::new(addrs),
            buffer: VecDeque::new(),
            inflight: VecDeque::new(),
            response: vec![0; 1500],
            response_len: 0,
            timeout: None,
            timeout_ms: timeout_ms,
            event_sender: event_sender
        }
    }

    fn register(&mut self, event_loop: &mut EventLoop<Self>, events: EventSet) {
        event_loop.register_opt(&self.sock, ZK, events, PollOpt::edge() | PollOpt::oneshot()).unwrap();
    }

    fn read_response_len(&mut self) -> usize {
        match self.sock.read_i32::<BigEndian>() {
            Ok(len) => {
                debug!("Response, len = {:?}", len);
                len as usize
            }
            Err(e) => {
                error!("Failed to read response len {:?}", e);
                0
            }
        }
    }

    fn handle_response(&mut self, event_loop: &mut EventLoop<Self>) {
        debug!("handle_response [{}] {:?}", self.response_len, &self.response[..self.response_len]);
        let mut data = Cursor::new(self.response.clone()); // TODO
        if self.state != ZkState::Closed {
            let header = ReplyHeader::read_from(&mut data);
            let response = RawResponse{header: header, data: data};
            match response.header.xid {
                -1 => {
                    debug!("handle_response Got a watch event!");
                    self.event_sender.send(response).unwrap();
                },
                -2 => {
                    debug!("handle_response Got a ping response!")
                },
                _ => match self.inflight.pop_front() {
                    Some(ref request) => {
                        Self::send_response(request, response);
                        if request.opcode == OpCode::CloseSession {
                            self.state = ZkState::Closed;
                            event_loop.shutdown();
                        }
                    },
                    None => panic!("Shouldn't happen, no inflight request")
                }
            }
        } else {
            self.inflight.pop_front(); // drop the request
            let conn_resp = ConnectResponse::read_from(&mut data);
            info!("Connected: {:?}", conn_resp);
            self.timeout_ms = conn_resp.timeout;
            self.state = ZkState::Connected;
        }
    }

    fn send_response(request: &RawRequest, response: RawResponse) {
        match request.listener {
            Some(ref listener) => {
                debug!("handle_response Opcode is {:?}", request.opcode);
                listener.send(response).unwrap();
            },
            None => debug!("Nobody is interested in respone {:?}", request.opcode)
        }
    }

    fn reconnect(&mut self, event_loop: &mut EventLoop<Self>) {
        info!("Connecting to new server");

        self.buffer.clear(); // drop all requests
        // self.response.clear();
        self.response_len = 0;

        self.state = ZkState::Connecting;

        self.sock = TcpStream::connect(&self.hosts.next()).unwrap();
        
        let request = self.connect_request();

        self.buffer.push_back(request);

        self.register(event_loop, EventSet::all());
    }

    fn connect_request(&self) -> RawRequest {
        let conn_req = ConnectRequest::from(ConnectResponse::initial(self.timeout_ms), 0);
        let buf = conn_req.to_len_prefixed_buf();
        RawRequest{opcode: OpCode::Auth, data: buf, listener: None}
    }
}

impl Handler for ZkHandler {
    type Message = RawRequest;
    type Timeout = ();

    fn ready(&mut self, event_loop: &mut EventLoop<Self>, token: Token, events: EventSet) {
        // It is not sure that we need to write, but we always need to read, because of watches
        let mut next_events = EventSet::all();
        next_events.remove(EventSet::writable());

        debug!("ready {:?} {:?}", token, events);
        if events.is_writable() {
            if let Some(mut request) = self.buffer.pop_front() {
                let written = self.sock.try_write_buf(&mut request.data).unwrap();
                debug!("Written {:?} bytes", written);
                if let Some(timeout) = self.timeout {
                    event_loop.clear_timeout(timeout);
                }
                if request.data.has_remaining() {
                    self.buffer.push_front(request);
                } else {
                    self.inflight.push_back(request);
                }
                if !self.buffer.is_empty() {
                    // Output buffer still has content, so we need to write again!
                    next_events.insert(EventSet::writable());
                } else {
                    self.timeout = Some(event_loop.timeout_ms((), self.timeout_ms).unwrap());
                }
            }
        }
        if events.is_readable() {
            if self.response_len == 0 {
                self.response_len = self.read_response_len();
            }
            // TODO Try to request to read response_len bytes
            // TODO don't handle empty responses, e.g.: len = 0
            match self.sock.try_read(&mut self.response[0..self.response_len]) {
                Ok(Some(amount)) => {
                    debug!("Read {:?} bytes", amount);//, &self.response[0..self.response_len]);
                    // The response is fully read
                    if amount == self.response_len {
                        self.handle_response(event_loop);
                        self.response_len = 0;
                        //self.response.clear();
                    }
                },
                Ok(None) => debug!("Read Ok(None) would block"),
                Err(e) => error!("Failed to read response {:?}", e)
            }
        }
        if events.is_hup() {
            if self.state != ZkState::Closed {
                self.state = ZkState::NotConnected;
                self.reconnect(event_loop);
            }
        }

        // This tick is done, subscribe to a forthcoming one
        self.register(event_loop, next_events);
    }

    fn notify(&mut self, event_loop: &mut EventLoop<Self>, request: Self::Message) {
        debug!("notify {:?}", request.opcode);
        if self.state != ZkState::Closed {
            self.buffer.push_back(request);
            self.register(event_loop, EventSet::all());
        } else {
            let header = ReplyHeader{xid: 0, zxid: 0, err: ZkError::ConnectionLoss as i32};
            let response = RawResponse{header: header, data: ByteBuf::new(vec![])};
            Self::send_response(&request, response);
        }
    }

    fn timeout(&mut self, event_loop: &mut EventLoop<Self>, _: Self::Timeout) {
        debug!("Pinging {:?}", self.sock.peer_addr().unwrap());
        let ping = RawRequest{
            opcode: OpCode::Ping,
            data: PING.clone(),
            listener: None
        };
        event_loop.channel().send(ping).unwrap();
    }
}

pub struct ZkIo {
    event_loop: EventLoop<ZkHandler>,
    handler: ZkHandler,
}

impl ZkIo {
    pub fn new(addrs: Vec<SocketAddr>, timeout: Duration, event_sender: Sender<RawResponse>) -> ZkIo {
        let event_loop = EventLoop::new().unwrap();
        let timeout_ms = timeout.as_secs() * 1000 + timeout.subsec_nanos() as u64 / 1000000;
        let handler = ZkHandler::new(addrs, timeout_ms, event_sender);
        ZkIo{event_loop: event_loop, handler: handler}
    }

    pub fn sender(&self) -> Sender<RawRequest> {
        self.event_loop.channel()
    }

    pub fn run(mut self) -> io::Result<()> {
        self.handler.reconnect(&mut self.event_loop);
        self.event_loop.run(&mut self.handler)
    }
}
