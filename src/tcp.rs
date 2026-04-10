use std::io;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::debug;
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
    cancellation_token: CancellationToken,
) {
    let (mut message_handler, mut socksend, mut msgrecvchan, message_tasks) = SocketMessageHandler::new(&context, cancellation_token.clone());
    let _ = socket.set_nodelay(true);
    // https://blog.netherlabs.nl/articles/2009/01/18/the-ultimate-so_linger-page-or-why-is-my-tcp-not-reliable
    let _ = socket.set_zero_linger();

    let (mut rx, mut tx) = socket.into_split();

    let mut tasks: JoinSet<anyhow::Result<_>> = JoinSet::new();

    // Reader task
    tasks.spawn(async move {
        let mut buffer = [0; 128 * 1024];

        loop {
            match rx.read(&mut buffer).await {
                Ok(n) => {
                    if n > 0 {
                        trace!(num_of_bytes = %n, "bytes read from socket");
                        socksend.write_all(&buffer[..n]).await?;
                    } else {
                        // Exit requested
                        break;
                    }
                }
                Err(err) => {
                    // Recoverable errors
                    if matches!(
                        err.kind(),
                        io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                    ) {
                        trace!(warning = %err, "Recoverable socket reader error");
                        continue;
                    }

                    return Err(err.into());
                }
            }
        }

        Ok(())
    });

    // Writer task
    tasks.spawn(async move {
        while let Some(result) = msgrecvchan.recv().await {
            let message = result?;
            write_fragment(&mut tx, &message).await?;
        }

        Ok(())
    });

    // Message handler task
    tasks.spawn({
        async move {
            loop {
                let fragment = message_handler.read_next().await?;
                message_handler.dispatch(fragment);
            }
        }
    });

    // All the 3 tasks must be running for the nfs socket to function.
    // Shutsdown the socket in either of the following:
    // 1. One of the tasks exits.
    // 2. Cancellation token is called.
    tokio::select! {
        result = tasks.join_next() => {
            if let Some(Err(err)) = result {
                tracing::error!("Socket task error: {err:?}. Shutting down.");
            }
            cancellation_token.cancel();
        }

        _ = cancellation_token.cancelled() => {}
    }

    tasks.shutdown().await;

    message_tasks.close();
    message_tasks.wait().await;
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

    async fn run(&self) -> io::Result<()>;
    async fn shutdown(&self);
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

    /// Receives incoming connections and processes sockets.
    async fn run(&self) -> io::Result<()> {
        loop {
            tokio::select! {
                _ = self.cancellation_token.cancelled() => {
                    info!("nfs server task cancelled, exiting");

                    break;
                }
                result = self.listener.accept() => {
                    let (socket, _) = result?;

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

                    // Creates a child token barrier so that
                    // each socket can cancel individually.
                    let socket_cancellation_token = self.cancellation_token.child_token();

                    self.socket_tasks.spawn(async move {
                        process_socket(socket, context, socket_cancellation_token).await;
                    });
                }
            }
        }

        Ok(())
    }

    async fn shutdown(&self) {
        self.cancellation_token.cancel();

        self.socket_tasks.close();
        self.socket_tasks.wait().await;
    }
}
