use byteorder::{BigEndian, ByteOrder};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::future::{abortable, AbortHandle};
use lazy_static::lazy_static;
use std::collections::VecDeque;
use std::io::Cursor;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use tracing::*;

use crate::listeners::ListenerSet;
use crate::proto::{
    ByteBuf, ConnectRequest, ConnectResponse, OpCode, ReadFrom, ReplyHeader, RequestHeader, WriteTo,
};
use crate::watch::WatchMessage;
use crate::zookeeper::{RawRequest, RawResponse};
use crate::{ZkError, ZkState};

lazy_static! {
    static ref PING: ByteBuf = RequestHeader {
        xid: -2,
        opcode: OpCode::Ping
    }
    .to_len_prefixed_buf()
    .unwrap();
}

struct Hosts {
    addrs: Vec<SocketAddr>,
    index: usize,
}

impl Hosts {
    fn new(addrs: Vec<SocketAddr>) -> Hosts {
        Hosts { addrs, index: 0 }
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

type Timeout = AbortHandle;

pub struct ZkIo {
    sock_tx: Option<OwnedWriteHalf>,
    state: ZkState,
    hosts: Hosts,
    buffer: VecDeque<RawRequest>,
    inflight: VecDeque<RawRequest>,
    response: BytesMut,
    ping_timeout: Option<Timeout>,
    conn_timeout: Option<Timeout>,
    timeout_ms: u64,
    ping_timeout_duration: Duration,
    conn_timeout_duration: Duration,
    watch_sender: Sender<WatchMessage>,
    conn_resp: ConnectResponse,
    zxid: i64,
    ping_sent: Instant,
    state_listeners: ListenerSet<ZkState>,
    shutdown: bool,
    tx: Sender<RawRequest>,
    rx: Option<Receiver<RawRequest>>,
    ping_tx: Sender<()>,
    ping_rx: Option<Receiver<()>>,
    connect_tx: Sender<()>,
    connect_rx: Option<Receiver<()>>,
    data_tx: Sender<BytesMut>,
    data_rx: Option<Receiver<BytesMut>>,
    recv_task_jh: Option<JoinHandle<()>>,
    recv_task_tx: Option<broadcast::Sender<()>>,
}

#[derive(Clone, Debug)]
enum ZkTimeout {
    Ping,
    Connect,
}

impl ZkIo {
    pub async fn new(
        addrs: Vec<SocketAddr>,
        ping_timeout_duration: Duration,
        watch_sender: Sender<WatchMessage>,
        state_listeners: ListenerSet<ZkState>,
    ) -> ZkIo {
        trace!("ZkIo::new");
        let timeout_ms = ping_timeout_duration.as_secs() * 1000
            + ping_timeout_duration.subsec_nanos() as u64 / 1000000;
        let (tx, rx) = channel(64);
        let (ping_tx, ping_rx) = channel(1);
        let (connect_tx, connect_rx) = channel(1);
        let (data_tx, data_rx) = channel(64);

        let mut zkio = ZkIo {
            sock_tx: None,
            state: ZkState::Connecting,
            hosts: Hosts::new(addrs),
            buffer: VecDeque::new(),
            inflight: VecDeque::new(),
            // TODO server reads max up to 1MB, otherwise drops the connection,
            // size should be 1MB + tcp rcvBufsize
            response: BytesMut::with_capacity(1024 * 1024 * 2),
            ping_timeout: None,
            conn_timeout: None,
            ping_timeout_duration,
            conn_timeout_duration: Duration::from_secs(2),
            timeout_ms,
            watch_sender,
            conn_resp: ConnectResponse::initial(timeout_ms),
            zxid: 0,
            ping_sent: Instant::now(),
            state_listeners,
            shutdown: false,
            tx,
            rx: Some(rx),
            ping_tx,
            ping_rx: Some(ping_rx),
            connect_tx,
            connect_rx: Some(connect_rx),
            data_tx,
            data_rx: Some(data_rx),
            recv_task_jh: None,
            recv_task_tx: None,
        };

        zkio.reconnect().await;
        zkio
    }

    fn notify_state(&self, old_state: ZkState, new_state: ZkState) {
        if new_state != old_state {
            self.state_listeners.notify(&new_state);
        }
    }

    async fn handle_response(&mut self) {
        loop {
            if self.response.len() <= 4 {
                return;
            }

            let len = BigEndian::read_i32(&self.response[..4]) as usize;

            trace!(
                "Response chunk len = {} buf len is {}",
                len,
                self.response.len()
            );

            if self.response.len() - 4 < len {
                return;
            } else {
                self.response.advance(4);
                let bytes = self.response.split_to(len);
                self.handle_chunk(bytes.freeze()).await;

                self.response.reserve(1024 * 1024 * 2);
            }
        }
    }

    async fn handle_chunk(&mut self, bytes: Bytes) {
        let len = bytes.len();
        trace!("handle_response in {:?} state [{}]", self.state, len);

        let mut data = &*bytes;

        if self.state != ZkState::Connecting {
            let header = match ReplyHeader::read_from(&mut data) {
                Ok(header) => header,
                Err(e) => {
                    warn!("Failed to parse ReplyHeader {:?}", e);
                    self.inflight.pop_front();
                    return;
                }
            };
            if header.zxid > 0 {
                // Update last-seen zxid when this is a request response
                self.zxid = header.zxid;
            }
            let response = RawResponse {
                header,
                data: Cursor::new(data.chunk().to_vec()),
            }; // TODO COPY!
            match response.header.xid {
                -1 => {
                    trace!("handle_response Got a watch event!");
                    self.watch_sender
                        .send(WatchMessage::Event(response))
                        .await
                        .unwrap();
                }
                -2 => {
                    trace!("Got ping response in {:?}", self.ping_sent.elapsed());
                    self.inflight.pop_front();
                }
                _ => match self.inflight.pop_front() {
                    Some(request) => {
                        self.send_response(request, response).await;
                    }
                    None => {
                        warn!("Got response with no inflight request - probably already closed")
                    }
                },
            }
        } else {
            self.inflight.pop_front(); // drop the connect request

            let conn_resp = match ConnectResponse::read_from(&mut data) {
                Ok(conn_resp) => conn_resp,
                Err(e) => {
                    error!("Failed to parse ConnectResponse {:?}", e);
                    self.reconnect().await;
                    return;
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
                self.timeout_ms = self.conn_resp.timeout;
                self.ping_timeout_duration = Duration::from_millis(self.conn_resp.timeout / 3 * 2);

                self.state = if self.conn_resp.read_only {
                    ZkState::ConnectedReadOnly
                } else {
                    ZkState::Connected
                };
            }

            self.notify_state(old_state, self.state);
        }
    }

    async fn send_response(&mut self, request: RawRequest, response: RawResponse) {
        match request.listener {
            Some(listener) => {
                trace!("send_response Opcode is {:?}", request.opcode);
                if let Err(error) = listener.send(response) {
                    error!("Error sending listener event: {:?}", error);
                }
            }
            None => info!("Nobody is interested in response {:?}", request.opcode),
        }
        if let Some(watch) = request.watch {
            if let Err(error) = self.watch_sender.send(WatchMessage::Watch(watch)).await {
                error!("Error sending watch event: {:?}", error);
            }
        }
    }

    fn clear_timeout(&mut self, atype: ZkTimeout) {
        trace!("clear_timeout: {:?}", atype);
        let timeout = match atype {
            ZkTimeout::Ping => self.ping_timeout.take(),
            ZkTimeout::Connect => self.conn_timeout.take(),
        };

        if let Some(timeout) = &timeout {
            timeout.abort();
        }
    }

    fn start_timeout(&mut self, atype: ZkTimeout) {
        self.clear_timeout(atype.clone());
        trace!("start_timeout: {:?}", atype);
        match atype {
            ZkTimeout::Ping => {
                let duration = self.ping_timeout_duration;
                let (future, handle) = abortable(sleep(duration));
                self.ping_timeout = Some(handle);

                let tx = self.ping_tx.clone();
                tokio::spawn(async move {
                    if future.await.is_ok() {
                        let _ = tx.send(()).await;
                    }
                });
            }
            ZkTimeout::Connect => {
                let duration = self.conn_timeout_duration;
                let (future, handle) = abortable(sleep(duration));
                self.conn_timeout = Some(handle);

                let tx = self.connect_tx.clone();
                tokio::spawn(async move {
                    if future.await.is_ok() {
                        let _ = tx.send(()).await;
                    }
                });
            }
        }
    }

    async fn reconnect(&mut self) {
        trace!("reconnect");
        let old_state = self.state;
        self.state = ZkState::Connecting;
        self.notify_state(old_state, self.state);

        info!("Establishing Zk connection");

        // TODO only until session times out
        loop {
            self.buffer.clear();
            self.inflight.clear();
            self.response.clear(); // TODO drop all read bytes once RingBuf.clear() is merged

            // Check if the session is still alive according to our knowledge
            if self.ping_sent.elapsed().as_secs() * 1000 > self.timeout_ms {
                warn!("Zk session timeout");
                self.zxid = 0;
            }

            self.clear_timeout(ZkTimeout::Ping);
            self.clear_timeout(ZkTimeout::Connect);

            {
                // notify recv task exit
                if let Some(tx) = self.recv_task_tx.take() {
                    tx.send(()).ok();
                }

                // wait for recv task exit
                if let Some(h) = self.recv_task_jh.take() {
                    h.await.ok();
                }

                let host = self.hosts.get();
                info!("Connecting to new server {:?}", host);
                let sock = match TcpStream::connect(host).await {
                    Ok(sock) => sock,
                    Err(e) => {
                        error!("Failed to connect {:?}: {:?}", host, e);
                        //retry too fast, sleep 1s
                        sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                };
                info!("Started connecting to {:?}", host);

                let (mut rx, tx) = sock.into_split();
                self.sock_tx = Some(tx);
                let (task_tx, mut task_rx) = broadcast::channel::<()>(1);
                self.recv_task_tx = Some(task_tx);
                let time_out_sleep = self.ping_timeout_duration.as_secs() * 2;
                let data_tx = self.data_tx.clone();
                self.recv_task_jh = Some(tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    loop {
                        tokio::select! {
                            rd_data = rx.read(&mut buf) => if let Ok(read) = rd_data {
                                trace!("Received {:?} bytes", read);
                                
                                if read == 0 {
                                    break;
                                }

                                if data_tx.send(buf[..read].into()).await.is_err() {
                                    return;
                                }
                            },
                            // recv exit signal
                            _ = task_rx.recv() => {
                                return;
                            },
                            _ = sleep(Duration::from_secs(time_out_sleep)) => {
                                error!("Reconnect due to double ping timeout.");
                                break;
                            }
                        }
                    }
                    trace!("Exiting read loop");
                    let _ = data_tx.send(BytesMut::new()).await;
                }));
            }

            let request = self.connect_request();
            self.buffer.push_back(request);

            self.start_timeout(ZkTimeout::Connect);

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
    
    async fn write_data(&mut self) {
        if let Some(tx) = self.sock_tx.as_mut() {
            while let Some(request) = self.buffer.pop_front() {
                trace!("Writing data: {:?}", request.opcode);
                match tx.write_all(request.data.chunk()).await {
                    Ok(()) => {
                        if request.opcode == OpCode::CloseSession {
                            let old_state = self.state;
                            self.state = ZkState::Closed;
                            self.notify_state(old_state, self.state);
                            self.shutdown = true;

                            // closing session translates to empty response
                            self.send_response(
                                request,
                                RawResponse {
                                    header: ReplyHeader {
                                        xid: 0,
                                        zxid: 0,
                                        err: 0,
                                    },
                                    data: Default::default(),
                                },
                            )
                            .await;
                            break;
                        }

                        self.inflight.push_back(request);
                    }
                    Err(e) => {
                        error!("Failed to write socket: {:?}", e);
                        self.reconnect().await;
                        return;
                    }
                }
            }
        }
    }

    fn is_idle(&self) -> bool {
        self.inflight.is_empty() && self.buffer.is_empty() && self.ping_timeout.is_none()
    }

    pub fn sender(&self) -> Sender<RawRequest> {
        self.tx.clone()
    }

    pub async fn run(mut self) {
        let mut rx = self.rx.take().expect("Missing request receiver!");
        let mut ping_rx = self.ping_rx.take().expect("Missing ping receiver!");
        let mut connect_rx = self.connect_rx.take().expect("Missing connect receiver!");
        let mut data_rx = self.data_rx.take().expect("Missing data receiver!");

        while !self.shutdown {
            self.write_data().await;
            if self.shutdown {
                break;
            }

            tokio::select! {
                request = rx.recv() => if let Some(request) = request {
                    trace!("read_channel {:?}", request.opcode);

                    match self.state {
                        ZkState::Closed => {
                            // If zk is unavailable, respond with a ConnectionLoss error.
                            let header = ReplyHeader {
                                xid: 0,
                                zxid: 0,
                                err: ZkError::ConnectionLoss as i32,
                            };
                            let response = RawResponse {
                                header,
                                data: ByteBuf::new(vec![]),
                            };
                            self.send_response(request, response).await;
                        }
                        _ => {
                            // Otherwise, queue request for processing.
                            self.buffer.push_back(request);
                        }
                    }
                },
                _ = ping_rx.recv() => {
                    self.ping_timeout = None;

                    if self.inflight.is_empty() {
                        // No inflight request indicates an idle connection. Send a ping.
                        trace!("Pinging");
                        self.tx
                            .send(RawRequest {
                                opcode: OpCode::Ping,
                                data: PING.clone(),
                                listener: None,
                                watch: None,
                            })
                            .await
                            .unwrap();
                        self.ping_sent = Instant::now();
                    }
                },
                _ = connect_rx.recv() => {
                    self.conn_timeout = None;

                    if self.state == ZkState::Connecting {
                        info!("Reconnect due to connection timeout");
                        self.reconnect().await;
                    }
                },
                data = data_rx.recv() => if let Some(data) = data {
                    let read = data.len();
                    trace!("Read {:?} bytes", read);

                    match read {
                        0 => {
                            warn!("Connection closed: read");

                            if self.state != ZkState::Closed {
                                self.reconnect().await;
                            }
                        }
                        _ => {
                            self.response.put(data);
                            self.handle_response().await;
                        }
                    }
                }
            }

            if self.is_idle() {
                self.start_timeout(ZkTimeout::Ping);
            }
        }
    }
}
