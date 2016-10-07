use consts::{OpCode, ZkError, ZkState};
use proto::{ByteBuf, ConnectRequest, ConnectResponse, ReadFrom, ReplyHeader, RequestHeader,
            WriteTo};
use watch::WatchMessage;
use zookeeper::{RawResponse, RawRequest};
use listeners::ListenerSet;

use byteorder::{BigEndian, ByteOrder};
use bytes::Buf;
use bytes::buf::{RingBuf, ReadExt, WriteExt};
use mio::{Events, Poll, Ready, PollOpt, Token};
use mio::channel::{Receiver, Sender};
use mio::tcp::TcpStream;
use mio::timer::{Timer, Timeout};
use std::collections::VecDeque;
use std::io::Cursor;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

const ZK: Token = Token(1);
const TIMER: Token = Token(2);
const CHANNEL: Token = Token(3);

lazy_static! {
    static ref PING: ByteBuf =
    RequestHeader{xid: -2, opcode: OpCode::Ping}.to_len_prefixed_buf().unwrap();
}

struct Hosts {
    addrs: Vec<SocketAddr>,
    index: usize,
}

impl Hosts {
    fn new(addrs: Vec<SocketAddr>) -> Hosts {
        Hosts {
            addrs: addrs,
            index: 0,
        }
    }

    fn get(&mut self) -> &SocketAddr {
        let addr = &self.addrs[self.index];
        if self.addrs.len() == self.index + 1 {
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
    io_receiver: Receiver<RawRequest>,
    watch_sender: Sender<WatchMessage>,
    conn_resp: ConnectResponse,
    zxid: i64,
    ping_sent: Instant,
    state_listeners: ListenerSet<ZkState>,
    poll: Poll,
    timer: Timer<()>, // ping timer
    running: bool
}

impl ZkHandler {
    fn new(addrs: Vec<SocketAddr>,
           timeout_ms: u64,
           io_receiver: Receiver<RawRequest>,
           watch_sender: Sender<WatchMessage>,
           state_listeners: ListenerSet<ZkState>,
           poll: Poll)
           -> ZkHandler {
        ZkHandler {
            sock: TcpStream::connect(&addrs[0]).unwrap(), // TODO I need a socket here, sorry.
            state: ZkState::NotConnected,
            hosts: Hosts::new(addrs),
            buffer: VecDeque::new(),
            inflight: VecDeque::new(),
            // TODO server reads max up to 1MB, otherwise drops the connection, size should be 1MB + tcp rcvBufsize
            response: RingBuf::with_capacity(1024 * 1024 * 2),
            timeout: None,
            timeout_ms: timeout_ms,
            io_receiver: io_receiver,
            watch_sender: watch_sender,
            conn_resp: ConnectResponse::initial(timeout_ms),
            zxid: 0,
            ping_sent: Instant::now(),
            state_listeners: state_listeners,
            poll: poll,
            timer: Timer::default(),
            running: true
        }
    }

    fn run(&mut self) {
        self.reconnect();
        self.poll.register(&self.timer, TIMER, Ready::readable(), PollOpt::edge()).unwrap();
        self.poll.register(&self.io_receiver, CHANNEL, Ready::readable(), PollOpt::edge()).unwrap();
        let mut events = Events::with_capacity(1024);
        while self.running {
            self.poll.poll(&mut events, None).unwrap();
            for event in events.iter() {
                match event.token() {
                    ZK => self.ready(event.token(), event.kind()),
                    TIMER => self.timeout(),
                    CHANNEL => {
                        let message = self.io_receiver.try_recv().unwrap();
                        self.notify(message)
                    },
                    _ => unreachable!(),
                }
            }
        }
    }

    fn register(&mut self, interest: Ready) {
        self.poll.register(&self.sock, ZK, interest, PollOpt::edge() | PollOpt::oneshot())
                  .ok()
                  .expect("Failed to register ZK handle");
    }

    fn reregister(&mut self, interest: Ready) {
        self.poll.reregister(&self.sock, ZK, interest, PollOpt::edge() | PollOpt::oneshot())
                  .ok()
                  .expect("Failed to reregister ZK handle");
    }

    fn notify_state(&self, old_state: ZkState, new_state: ZkState) {
        if new_state != old_state {
            self.state_listeners.notify(&new_state);
        }
    }

    fn handle_response(&mut self) {
        loop {
            if self.response.remaining() <= 4 {
                return;
            }
            let len = BigEndian::read_i32(&self.response.bytes()[..4]) as usize;

            trace!("Response chunk len = {} buf len is {}",
                   len,
                   self.response.bytes().len());

            if self.response.remaining() - 4 < len {
                return;
            } else {
                self.response.advance(4);
                {
                    self.handle_chunk(len);
                }
                self.response.advance(len);
            }
        }
    }

    fn handle_chunk(&mut self, len: usize) {

        let mut data = Cursor::new(&self.response.bytes()[..len]);

        trace!("handle_response in {:?} state [{}]",
               self.state,
               data.bytes().len());

        if self.state != ZkState::Connecting {
            let header = match ReplyHeader::read_from(&mut data) {
                Ok(header) => header,
                Err(e) => {
                    warn!("Failed to parse ReplyHeader {:?}", e);
                    self.inflight.pop_front();
                    return;
                }
            };
            self.zxid = header.zxid;
            let response = RawResponse {
                header: header,
                data: Cursor::new(data.bytes().to_vec()),
            }; // TODO COPY!
            match response.header.xid {
                -1 => {
                    trace!("handle_response Got a watch event!");
                    self.watch_sender.send(WatchMessage::Event(response));
                }
                -2 => {
                    trace!("Got ping response in {:?}",
                           self.ping_sent.elapsed());
                    self.inflight.pop_front();
                }
                _ => {
                    match self.inflight.pop_front() {
                        Some(request) => {
                            if request.opcode == OpCode::CloseSession {
                                let old_state = self.state;
                                self.state = ZkState::Closed;
                                self.notify_state(old_state, self.state);
                                self.running = false
                            }
                            self.send_response(request, response);
                        }
                        None => panic!("Shouldn't happen, no inflight request"),
                    }
                }
            }
        } else {
            self.inflight.pop_front(); // drop the connect request

            let conn_resp = match ConnectResponse::read_from(&mut data) {
                Ok(conn_resp) => conn_resp,
                Err(e) => {
                    panic!("Failed to parse ConnectResponse {:?}", e);
                    // self.reconnect(poll);
                    // return
                }
            };

            let old_state = self.state;

            if conn_resp.timeout == 0 {
                info!("session {} expired", self.conn_resp.session_id);
                self.conn_resp.session_id = 0;
                self.state = ZkState::NotConnected;
            } else {
                self.conn_resp = conn_resp;
                info!("Connected: {:?}", self.conn_resp);
                self.timeout_ms = self.conn_resp.timeout / 3 * 2;

                self.state = if self.conn_resp.read_only {
                    ZkState::ConnectedReadOnly
                } else {
                    ZkState::Connected
                };
            }

            self.notify_state(old_state, self.state);
        }
    }

    fn send_response(&self, request: RawRequest, response: RawResponse) {
        match request.listener {
            Some(ref listener) => {
                trace!("send_response Opcode is {:?}", request.opcode);
                listener.send(response).unwrap();
            }
            None => info!("Nobody is interested in response {:?}", request.opcode),
        }
        if let Some(watch) = request.watch {
            self.watch_sender.send(WatchMessage::Watch(watch));
        }
    }

    fn clear_ping_timeout(&mut self) {
        if let Some(ref timeout) = self.timeout {
            self.timer.cancel_timeout(timeout);
        }
    }

    fn reconnect(&mut self) {
        let old_state = self.state;
        self.state = ZkState::Connecting;
        self.notify_state(old_state, self.state);

        info!("Establishing Zk connection");

        loop {
            self.buffer.clear();
            self.inflight.clear();
            self.response.clear();

            // Check if the session is still alive according to our knowledge
            if self.ping_sent.elapsed().as_secs() * 1000 > self.timeout_ms {
                warn!("Zk session timeout, closing io event loop");
                self.state = ZkState::Closed;
                self.notify_state(ZkState::Connecting, self.state);
                self.running = false;
                break;
            }

            self.clear_ping_timeout();

            {
                let host = self.hosts.get();
                info!("Connecting to new server {:?}", host);
                self.sock = match TcpStream::connect(host) {
                    Ok(sock) => sock,
                    Err(e) => {
                        error!("Failed to connect {:?}: {:?}", host, e);
                        continue;
                    }
                };
                info!("Started connecting to {:?}", host);
            }

            let request = self.connect_request();
            self.buffer.push_back(request);

            self.register(Ready::all());

            break;
        }
    }

    fn connect_request(&self) -> RawRequest {
        let conn_req = ConnectRequest::from(&self.conn_resp, self.zxid);
        let buf = conn_req.to_len_prefixed_buf().unwrap();
        RawRequest {
            opcode: OpCode::Auth,
            data: buf,
            listener: None,
            watch: None,
        }
    }

    fn ready(&mut self, token: Token, events: Ready) {

        trace!("ready {:?} {:?}", token, events);
        if events.is_writable() {
            while let Some(mut request) = self.buffer.pop_front() {
                match self.sock.write_buf(&mut request.data) {
                    Ok(written) if written > 0 => {
                        trace!("Written {:?} bytes", written);
                        if request.data.has_remaining() {
                            self.buffer.push_front(request);
                            break;
                        } else {
                            self.inflight.push_back(request);

                            // Sent a full message, clear the ping timeout
                            self.clear_ping_timeout();
                        }
                    }
                    Ok(_) => warn!("Connection closed: write"),
                    Err(e) => {
                        error!("Failed to write socket: {:?}", e);
                        self.reconnect();
                    }
                }
            }
            self.timeout = Some(self.timer.set_timeout(Duration::from_millis(self.timeout_ms), ()).unwrap());
        }
        if events.is_readable() {
            match self.sock.read_buf(&mut self.response) {
                Ok(read) if read > 0 => {
                    trace!("Read {:?} bytes", read);
                    self.handle_response();
                }
                Ok(_) => warn!("Connection closed: read"),
                Err(e) => {
                    error!("Failed to read socket: {:?}", e);
                    self.reconnect();
                }
            }
        }
        if (events.is_hup()) && (self.state != ZkState::Closed) {
            // If we were connected
            // fn send_watched_event(keeper_state: KeeperState) {
            //     match sender.send(WatchedEvent{event_type: WatchedEventType::None,
            //                                    keeper_state: keeper_state,
            //                                    path: None}) {
            //         Ok(()) => (),
            //         Err(e) => panic!("Reader/Writer: Event died {}", e)
            //     }
            // }
            let old_state = self.state;
            self.state = ZkState::NotConnected;
            self.notify_state(old_state, self.state);

            self.reconnect();
        }

        // Not sure that we need to write, but we always need to read, because of watches
        // If the output buffer has no content, we don't need to write again
        let mut event_set = Ready::all();
        if self.buffer.is_empty() {
            event_set.remove(Ready::writable());
        }

        // This tick is done, subscribe to a forthcoming one
        self.reregister(event_set);
    }

    fn notify(&mut self, request: RawRequest) {
        trace!("notify {:?}", request.opcode);
        if self.state != ZkState::Closed {
            if self.buffer.is_empty() {
                self.reregister(Ready::all());
            }
            self.buffer.push_back(request);
        } else {
            let header = ReplyHeader {
                xid: 0,
                zxid: 0,
                err: ZkError::ConnectionLoss as i32,
            };
            let response = RawResponse {
                header: header,
                data: ByteBuf::new(vec![]),
            };
            self.send_response(request, response);
        }
    }

    fn timeout(&mut self) {
        if self.inflight.is_empty() {
            trace!("Pinging {:?}", self.sock.peer_addr().unwrap());
            let ping = RawRequest {
                opcode: OpCode::Ping,
                data: PING.clone(),
                listener: None,
                watch: None,
            };
            self.notify(ping);
            self.ping_sent = Instant::now();
        } else {
            self.reconnect();
        }
    }
}

pub struct ZkIo {
    handler: ZkHandler
}

impl ZkIo {
    pub fn new(addrs: Vec<SocketAddr>,
               timeout: Duration,
               io_receiver: Receiver<RawRequest>,
               event_sender: Sender<WatchMessage>,
               state_listeners: ListenerSet<ZkState>)
               -> ZkIo {
        let poll = Poll::new().unwrap();
        let timeout_ms = timeout.as_secs() * 1000 + timeout.subsec_nanos() as u64 / 1000000;
        let handler = ZkHandler::new(addrs, timeout_ms, io_receiver, event_sender, state_listeners, poll);
        ZkIo {
            handler: handler,
        }
    }

    pub fn run(mut self) {
        self.handler.run();
    }
}
