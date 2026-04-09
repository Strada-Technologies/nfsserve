use std::io;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::trace;

use crate::context::RPCContext;
use crate::rpcwire::*;
use crate::transaction_tracker::TransactionTracker;
use crate::vfs::NFSFileSystem;

/// A NFS Tcp Connection Handler
pub struct NFSTcpListener<T: NFSFileSystem + Send + Sync + 'static> {
    listener: TcpListener,
    port: u16,
    fs: Arc<T>,
    mount_signal: Option<flume::Sender<bool>>,
    export_name: Arc<String>,
    transaction_tracker: Arc<TransactionTracker>,
    socket_tasks: TaskTracker,
    cancellation_token: CancellationToken,
}

pub fn generate_host_ip(hostnum: u16) -> String {
    format!(
        "127.88.{}.{}",
        ((hostnum >> 8) & 0xFF) as u8,
        (hostnum & 0xFF) as u8
    )
}

/// processes an established socket
async fn process_socket(
    socket: tokio::net::TcpStream,
    context: RPCContext,
    socket_tasks: &TaskTracker,
    cancellation_token: CancellationToken,
) {
    let (mut message_handler, mut socksend, mut msgrecvchan) = SocketMessageHandler::new(&context);
    let _ = socket.set_nodelay(true);
    // https://blog.netherlabs.nl/articles/2009/01/18/the-ultimate-so_linger-page-or-why-is-my-tcp-not-reliable
    let _ = socket.set_zero_linger();
    let _ = socket.set_ttl(86400);

    let ct = cancellation_token.clone();
    socket_tasks.spawn(async move {
        loop {
            tokio::select! {
                _ = ct.cancelled() => {
                    debug!("socket pipe task cancelled");

                    message_handler.wait().await;

                    break;
                }
                res = message_handler.read() => {
                    match res {
                        Ok(_) => {},
                        Err(error) => {
                            debug!("socket pipe loop broken due to {}", error);
                            message_handler.wait().await;

                            // Cancels the other tasks
                            ct.cancel();

                            break;
                        },
                    }
                }
            }
        }

        debug!("socket pipe task exiting");
    });

    let (mut rx, mut tx) = socket.into_split();

    let _ = rx.readable().await;

    let ct = cancellation_token.clone();
    socket_tasks.spawn(async move {
        loop {
            let mut buf = [0; 128 * 1024];

            tokio::select! {
                _ = ct.cancelled() => {
                    debug!("read socket task cancelled");

                    break;
                }
                res = rx.read(&mut buf) => {
                    match res {
                        Ok(0) => {
                            // Cancels the other tasks
                            ct.cancel();

                            break;
                        }
                        Ok(n) => {
                            trace!(num_of_bytes = %n, "bytes read from socket");

                            if let Err(error) = socksend.write_all(&buf[..n]).await {
                                error!(error = %error, "error writing to simplex");

                                // Cancels the other tasks
                                ct.cancel();

                                break;
                            }
                        }
                        Err(ref e)
                            if matches!(
                                e.kind(),
                                io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                            ) =>
                        {
                            trace!(warning = %e, "nfs error");

                            continue;
                        }
                        Err(e) => {
                            debug!("Message handling closed : {:?}", e);

                            // Cancels the other tasks
                            ct.cancel();

                            break;
                        }
                    }
                }
            }
        }

        debug!("socket read task exiting");
    });

    let ct = cancellation_token.clone();
    socket_tasks.spawn(async move {
        loop {
            tokio::select! {
                _ = ct.cancelled() => {
                    debug!("socket write task cancelled");

                    break;
                }
                Some(Ok(msg)) = msgrecvchan.recv() => {
                    if let Err(e) = write_fragment(&mut tx, &msg).await {
                        error!("Write error {:?}", e);

                        // Cancels the other tasks
                        ct.cancel();

                        // Flush any remaining data before exiting
                        if let Err(error) = tx.shutdown().await {
                            error!(%error, "error while flushing data");
                        }

                        break;
                    }
                }
            }
        }

        debug!("socket write task exiting");
    });
}

#[async_trait]
pub trait NFSTcp: Send + Sync {
    /// Gets the true listening port. Useful if the bound port number is 0
    fn get_listen_port(&self) -> u16;

    /// Gets the true listening IP. Useful on windows when the IP may be random
    fn get_listen_ip(&self) -> IpAddr;

    /// Sets a mount listener. A "true" signal will be sent on a mount
    /// and a "false" will be sent on an unmount
    fn set_mount_listener(&mut self, signal: flume::Sender<bool>);

    /// Loops forever and never returns handling all incoming connections.
    async fn handle_forever(&self) -> io::Result<()>;
}

impl<T: NFSFileSystem + Send + Sync + 'static> NFSTcpListener<T> {
    pub fn new(
        listener: TcpListener,
        port: u16,
        fs: Arc<T>,
        mount_signal: Option<flume::Sender<bool>>,
        export_name: Arc<String>,
        transaction_tracker: Arc<TransactionTracker>,
        cancellation_token: CancellationToken,
    ) -> Self {
        let socket_tasks = TaskTracker::new();

        Self {
            listener,
            port,
            fs,
            mount_signal,
            export_name,
            transaction_tracker,
            socket_tasks,
            cancellation_token,
        }
    }

    /// Binds to a ipstr of the form [ip address]:port. For instance
    /// "127.0.0.1:12000". fs is an instance of an implementation
    /// of NFSFileSystem.
    pub async fn bind(
        ipstr: &str,
        fs: Arc<T>,
        cancellation_token: CancellationToken,
    ) -> io::Result<NFSTcpListener<T>> {
        let (ip, port) = ipstr.split_once(':').ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                "IP Address must be of form ip:port",
            )
        })?;
        let port = port.parse::<u16>().map_err(|_| {
            io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                "Port not in range 0..=65535",
            )
        })?;

        if ip == "auto" {
            let mut num_tries_left = 32;

            for try_ip in 1u16.. {
                let ip = generate_host_ip(try_ip);

                let result = NFSTcpListener::bind_internal(
                    &ip,
                    port,
                    fs.clone(),
                    cancellation_token.clone(),
                )
                .await;

                match &result {
                    Err(_) => {
                        if num_tries_left == 0 {
                            return result;
                        } else {
                            num_tries_left -= 1;
                            continue;
                        }
                    }
                    Ok(_) => {
                        return result;
                    }
                }
            }

            unreachable!(); // Does not detect automatically that loop above never terminates.
        } else {
            // Otherwise, try this.
            NFSTcpListener::bind_internal(ip, port, fs, cancellation_token).await
        }
    }

    async fn bind_internal(
        ip: &str,
        port: u16,
        fs: Arc<T>,
        cancellation_token: CancellationToken,
    ) -> io::Result<NFSTcpListener<T>> {
        let ipstr = format!("{ip}:{port}");
        let listener = TcpListener::bind(&ipstr).await?;
        info!("Listening on {:?}", &ipstr);

        let port = match listener.local_addr().unwrap() {
            SocketAddr::V4(s) => s.port(),
            SocketAddr::V6(s) => s.port(),
        };
        Ok(NFSTcpListener {
            listener,
            port,
            fs,
            mount_signal: None,
            export_name: Arc::from("/".to_string()),
            transaction_tracker: Arc::new(TransactionTracker::new(Duration::from_secs(60))),
            socket_tasks: TaskTracker::new(),
            cancellation_token,
        })
    }

    /// Sets an optional NFS export name.
    ///
    /// - `export_name`: The desired export name without slashes.
    ///
    /// Example: Name `foo` results in the export path `/foo`.
    /// Default path is `/` if not set.
    pub fn with_export_name<S: AsRef<str>>(&mut self, export_name: S) {
        self.export_name = Arc::new(format!(
            "/{}",
            export_name
                .as_ref()
                .trim_end_matches('/')
                .trim_start_matches('/')
        ))
    }
}

#[async_trait]
impl<T: NFSFileSystem + Send + Sync + 'static> NFSTcp for NFSTcpListener<T> {
    /// Gets the true listening port. Useful if the bound port number is 0
    fn get_listen_port(&self) -> u16 {
        let addr = self.listener.local_addr().unwrap();
        addr.port()
    }

    fn get_listen_ip(&self) -> IpAddr {
        let addr = self.listener.local_addr().unwrap();
        addr.ip()
    }

    /// Sets a mount listener. A "true" signal will be sent on a mount
    /// and a "false" will be sent on an unmount
    fn set_mount_listener(&mut self, signal: flume::Sender<bool>) {
        self.mount_signal = Some(signal);
    }

    /// Loops forever and never returns handling all incoming connections.
    async fn handle_forever(&self) -> io::Result<()> {
        loop {
            tokio::select! {
                _ = self.cancellation_token.cancelled() => {
                    info!("nfs server task cancelled, exiting");

                    break;
                }
                Ok((socket, _)) = self.listener.accept() => {
                    let context = RPCContext {
                        local_port: self.port,
                        client_addr: socket.peer_addr().unwrap().to_string(),
                        auth: crate::rpc::auth_unix::default(),
                        vfs: self.fs.clone(),
                        mount_signal: self.mount_signal.clone(),
                        export_name: self.export_name.clone(),
                        transaction_tracker: self.transaction_tracker.clone(),
                    };
                    info!("Accepting connection from {}", context.client_addr);
                    debug!("Accepting socket {:?} {:?}", socket, context);

                    process_socket(
                        socket,
                        context,
                        &self.socket_tasks,
                        self.cancellation_token.child_token(),
                    )
                    .await;
                }
            }
        }

        info!("cleaning up nfs tasks before exiting");

        self.socket_tasks.wait().await;

        Ok(())
    }
}
