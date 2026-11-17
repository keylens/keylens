//! Test-only fault injection: a TCP proxy in front of a real server.
//!
//! Mocks cannot tell you how the client library behaves when the network misbehaves --
//! whether a command times out, whether a scan hangs, whether a reconnect happens. This
//! sits between keylens and a real Redis/Valkey and breaks the link on demand.

#![allow(dead_code, clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

/// What the proxy does with traffic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Mode {
    /// Pass bytes both ways.
    Forward = 0,
    /// Keep every socket open and swallow everything: a half-open link that accepted
    /// the request and will never answer.
    Blackhole = 1,
    /// Close every new connection immediately, like a server that is down.
    Refuse = 2,
}

pub struct FaultProxy {
    pub addr: SocketAddr,
    mode: Arc<AtomicU8>,
    conns: Arc<Mutex<Vec<JoinHandle<()>>>>,
    accepted: Arc<AtomicU64>,
    /// Bytes forwarded client -> server, a proxy for command volume.
    upstream_bytes: Arc<AtomicU64>,
    listener: JoinHandle<()>,
}

impl FaultProxy {
    /// Start a proxy to `upstream` (`host:port`) on an ephemeral local port.
    pub async fn start(upstream: &str) -> Self {
        let upstream = upstream.to_string();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mode = Arc::new(AtomicU8::new(Mode::Forward as u8));
        let conns: Arc<Mutex<Vec<JoinHandle<()>>>> = Arc::default();
        let accepted = Arc::new(AtomicU64::new(0));
        let upstream_bytes = Arc::new(AtomicU64::new(0));

        let task = {
            let (mode, conns, accepted, upstream_bytes) = (
                mode.clone(),
                conns.clone(),
                accepted.clone(),
                upstream_bytes.clone(),
            );
            tokio::spawn(async move {
                loop {
                    let Ok((client, _)) = listener.accept().await else {
                        return;
                    };
                    accepted.fetch_add(1, Ordering::SeqCst);
                    if mode.load(Ordering::SeqCst) == Mode::Refuse as u8 {
                        drop(client);
                        continue;
                    }
                    let upstream = upstream.clone();
                    let mode = mode.clone();
                    let upstream_bytes = upstream_bytes.clone();
                    let handle = tokio::spawn(async move {
                        let Ok(server) = TcpStream::connect(&upstream).await else {
                            return;
                        };
                        let _ = client.set_nodelay(true);
                        let _ = server.set_nodelay(true);
                        let (cr, cw) = client.into_split();
                        let (sr, sw) = server.into_split();
                        let up = pump(cr, sw, mode.clone(), Some(upstream_bytes));
                        let down = pump(sr, cw, mode, None);
                        tokio::select! {
                            () = up => {}
                            () = down => {}
                        }
                    });
                    conns.lock().await.push(handle);
                }
            })
        };

        Self {
            addr,
            mode,
            conns,
            accepted,
            upstream_bytes,
            listener: task,
        }
    }

    /// `redis://127.0.0.1:<port>` for this proxy, with optional credentials.
    pub fn url(&self) -> String {
        format!("redis://{}", self.addr)
    }

    pub fn set_mode(&self, mode: Mode) {
        self.mode.store(mode as u8, Ordering::SeqCst);
    }

    /// Close every open connection, as a server restart or a load balancer reset would.
    pub async fn sever(&self) {
        for task in self.conns.lock().await.drain(..) {
            task.abort();
        }
    }

    /// Connections accepted so far, including refused ones.
    pub fn accepted(&self) -> u64 {
        self.accepted.load(Ordering::SeqCst)
    }

    /// Bytes forwarded to the server so far.
    pub fn upstream_bytes(&self) -> u64 {
        self.upstream_bytes.load(Ordering::SeqCst)
    }
}

impl Drop for FaultProxy {
    fn drop(&mut self) {
        self.listener.abort();
        if let Ok(mut conns) = self.conns.try_lock() {
            for task in conns.drain(..) {
                task.abort();
            }
        }
    }
}

async fn pump(
    mut from: tokio::net::tcp::OwnedReadHalf,
    mut to: tokio::net::tcp::OwnedWriteHalf,
    mode: Arc<AtomicU8>,
    counter: Option<Arc<AtomicU64>>,
) {
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = match from.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        if mode.load(Ordering::SeqCst) == Mode::Blackhole as u8 {
            continue; // read and discard: the peer sees an open, silent socket
        }
        if let Some(c) = &counter {
            c.fetch_add(n as u64, Ordering::SeqCst);
        }
        if to.write_all(&buf[..n]).await.is_err() {
            return;
        }
    }
}

/// `host:port` of the primary fixture.
pub fn upstream() -> String {
    std::env::var("KEYLENS_TEST_URL")
        .unwrap_or_else(|_| "redis://127.0.0.1:6379".into())
        .trim_start_matches("redis://")
        .to_string()
}
