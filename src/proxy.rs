use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use tokio::{
    io::copy_bidirectional,
    net::{TcpListener, TcpStream},
};

#[derive(Clone)]
pub struct ProxyHandle {
    accepting: Arc<AtomicBool>,
    active_conns: Arc<AtomicUsize>,
}

impl ProxyHandle {
    pub fn new() -> Self {
        Self {
            accepting: Arc::new(AtomicBool::new(false)),
            active_conns: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn open(&self) {
        self.accepting.store(true, Ordering::SeqCst);
    }

    pub fn close(&self) {
        self.accepting.store(false, Ordering::SeqCst);
    }

    pub fn active_connections(&self) -> usize {
        self.active_conns.load(Ordering::SeqCst)
    }

    pub async fn wait_for_drain(&self, timeout: Duration, refresh: Duration) -> bool {
        // use tokio instants to be able to pause
        use tokio::time::{sleep, Instant};

        let deadline = Instant::now() + timeout;
        loop {
            if self.active_connections() == 0 {
                return true;
            }

            if Instant::now() >= deadline {
                return false;
            }

            sleep(refresh).await;
        }
    }
}

pub async fn run_proxy(
    listen_addr: SocketAddr,
    upstream_addr: SocketAddr,
    handle: ProxyHandle,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(listen_addr).await?;

    loop {
        // TODO: Log peer?
        let (mut inbound, _peer) = listener.accept().await?;

        if !handle.accepting.load(Ordering::SeqCst) {
            drop(inbound);
            continue;
        }

        handle.active_conns.fetch_add(1, Ordering::SeqCst);
        let active = handle.active_conns.clone();

        tokio::spawn(async move {
            let mut outbound = match TcpStream::connect(upstream_addr).await {
                Ok(outbound) => outbound,
                Err(_e) => {
                    active.fetch_sub(1, Ordering::SeqCst);
                    return;
                }
            };

            if let Err(_e) = copy_bidirectional(&mut inbound, &mut outbound).await {
                todo!("Log?");
            }

            active.fetch_sub(1, Ordering::SeqCst);
        });
    }
}
