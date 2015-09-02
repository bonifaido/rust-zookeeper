use consts::{OpCode, ZkError, ZkState};
use proto::{ByteBuf, ConnectRequest, ConnectResponse, ReadFrom, ReplyHeader, RequestHeader, WriteTo};

use byteorder::{BigEndian, ByteOrder};
use bytes::{Buf, RingBuf};
use mio::{EventLoop, EventSet, Handler, PollOpt, Sender, Timeout, Token, TryRead, TryWrite};
use mio::tcp::TcpStream;
use std::collections::VecDeque;
use std::io;
use std::io::Cursor;
use std::mem;
use std::net::SocketAddr;
use std::sync::mpsc::SyncSender;
use std::time::Duration;

const ZK: Token = Token(1);

lazy_static! {
    static ref PING: ByteBuf =
    RequestHeader{xid: -2, opcode: OpCode::Ping}.to_len_prefixed_buf().unwrap();
}

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
    response: RingBuf,
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
            response: RingBuf::new(1024 * 1024), // TODO server reads max up to 1MB, otherwise drops the connection, size should be 1MB + tcp rcvBufsize
            timeout: None,
            timeout_ms: timeout_ms,
            event_sender: event_sender
        }
    }

    fn register(&mut self, event_loop: &mut EventLoop<Self>, events: EventSet) {
        event_loop.register_opt(&self.sock, ZK, events, PollOpt::edge() | PollOpt::oneshot())
        .ok().expect("Failed to register ZK handle");
    }

    fn reregister(&mut self, event_loop: &mut EventLoop<Self>, events: EventSet) {
        event_loop.reregister(&self.sock, ZK, events, PollOpt::edge() | PollOpt::oneshot())
        .ok().expect("Failed to reregister ZK handle");
    }

    fn handle_response(&mut self, event_loop: &mut EventLoop<Self>) {
        loop {
            if self.response.remaining() <= 4 {
                return
            }
            let len = BigEndian::read_i32(&self.response.bytes()[..4]) as usize;

            debug!("response chunk len = {} buf len is {}", len, self.response.bytes().len());

            if self.response.remaining() - 4 < len {
                return
            } else {

                self.response.advance(4);

                {
                    let mut data = Cursor::new(&self.response.bytes()[..len]);

                    debug!("handle_response in {:?} state [{}]", self.state, data.bytes().len());

                    // Could be moved to a handle_data() function
                    // or this whole stuff could be move to an Iterator
                    if self.state != ZkState::NotConnected {
                        let header = match ReplyHeader::read_from(&mut data) {
                            Ok(header) => header,
                            Err(e) => panic!("Failed to parse ReplyHeader {:?}", e) // TODO skip this chunk
                        };
                        let response = RawResponse{header: header, data: Cursor::new(data.bytes().to_vec())}; // TODO COPY!
                        match response.header.xid {
                            -1 => {
                                debug!("handle_response Got a watch event!");
                                self.event_sender.send(response).unwrap();
                            },
                            -2 => {
                                debug!("handle_response Got a ping response!");
                                self.inflight.pop_front();
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
                        self.inflight.pop_front(); // drop the connect request
                        let conn_resp = match ConnectResponse::read_from(&mut data) {
                            Ok(conn_resp) => conn_resp,
                            Err(e) => panic!("Failed to parse ConnectResponse {:?}", e) // TODO skip this chunk
                        };
                        info!("Connected: {:?}", conn_resp);
                        self.timeout_ms = conn_resp.timeout / 3 * 2;
                        self.state = ZkState::Connected;
                    }
                }
                
                self.response.advance(len);
            }
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

    fn clear_ping_timeout(&mut self, event_loop: &mut EventLoop<Self>) {
        if let Some(timeout) = self.timeout {
            event_loop.clear_timeout(timeout);
        }
    }

    fn reconnect(&mut self, event_loop: &mut EventLoop<Self>) {
        info!("Connecting to new server");

        self.buffer.clear(); // drop all requests

        self.response.mark(); self.response.reset(); // TODO drop all read bytes once RingBuf.clear() is merged

        //self.state = ZkState::Connecting;

        self.clear_ping_timeout(event_loop);

        // TODO reconnect loop
        let host = self.hosts.next().clone();
        self.sock = match TcpStream::connect(&host) {
            Ok(sock) => sock,
            Err(e) => panic!("Failed to open connection {:?}: {:?}", host, e)
        };
        
        let request = self.connect_request();

        self.buffer.push_back(request);

        self.register(event_loop, EventSet::all());
    }

    fn connect_request(&self) -> RawRequest {
        let conn_req = ConnectRequest::from(ConnectResponse::initial(self.timeout_ms), 0);
        let buf = conn_req.to_len_prefixed_buf().unwrap();
        RawRequest{opcode: OpCode::Auth, data: buf, listener: None}
    }
}

impl Handler for ZkHandler {
    type Message = RawRequest;
    type Timeout = ();

    fn ready(&mut self, event_loop: &mut EventLoop<Self>, token: Token, events: EventSet) {

        debug!("ready {:?} {:?}", token, events);
        if events.is_writable() {
            loop {
                match self.buffer.pop_front() {
                    Some(mut request) => match self.sock.try_write_buf(&mut request.data) {
                        Ok(Some(written)) if written > 0 => {
                            debug!("Written {:?} bytes", written);
                            if request.data.has_remaining() {
                                self.buffer.push_front(request);
                                break
                            } else {
                                self.inflight.push_back(request);

                                // Sent a full message, clear the ping timeout
                                self.clear_ping_timeout(event_loop);
                            }
                        },
                        Ok(None) => trace!("Spurious write"),
                        Ok(Some(_)) => warn!("Connection closed: write"),
                        Err(e) => error!("Failed to write socket: {:?}", e)
                    },
                    None => break
                }
            }
            self.timeout = Some(event_loop.timeout_ms((), self.timeout_ms).unwrap());
        }
        if events.is_readable() {
            match self.sock.try_read_buf(&mut self.response) {
                Ok(Some(read)) if read > 0 => {
                    debug!("Read {:?} bytes", read);
                    self.handle_response(event_loop);
                },
                Ok(None) => trace!("Spurious read"),
                Ok(Some(_)) => warn!("Connection closed: read"),
                Err(e) => error!("Failed to read socket: {:?}", e)
            }
        }
        if events.is_hup() {
            if self.state != ZkState::Closed {
                self.state = ZkState::NotConnected;
                self.reconnect(event_loop);
            }
        }

        // Not sure that we need to write, but we always need to read, because of watches
        // If the output buffer has no content, we don't need to write again
        let mut event_set = EventSet::all();
        if self.buffer.is_empty() {
            event_set.remove(EventSet::writable());
        }

        // This tick is done, subscribe to a forthcoming one
        self.reregister(event_loop, event_set);
    }

    fn notify(&mut self, event_loop: &mut EventLoop<Self>, request: Self::Message) {
        debug!("notify {:?}", request.opcode);
        if self.state != ZkState::Closed {
            if self.buffer.is_empty() {
                self.reregister(event_loop, EventSet::all());
            }
            self.buffer.push_back(request);
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
