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
fn process_socket(
    socket: tokio::net::TcpStream,
    context: RPCContext,
    socket_tasks: &TaskTracker,
    cancellation_token: CancellationToken,
) {
    let (message_handler, socksend, msgrecvchan) = SocketMessageHandler::new(&context);
    let _ = socket.set_nodelay(true);
    // https://blog.netherlabs.nl/articles/2009/01/18/the-ultimate-so_linger-page-or-why-is-my-tcp-not-reliable
    let _ = socket.set_zero_linger();

    let (rx, tx) = socket.into_split();

    socket_tasks.spawn({
        let cancellation_token = cancellation_token.clone();
        async move {
            if let Err(e) = run_message_handler(message_handler, cancellation_token.clone()).await {
                error!(error = %e, "message handler error");
            }
            cancellation_token.cancel();
        }
    });

    socket_tasks.spawn({
        let cancellation_token = cancellation_token.clone();
        async move {
            if let Err(e) = run_socket_reader(rx, socksend, cancellation_token.clone()).await {
                error!(error = %e, "socket reader error");
            }
            cancellation_token.cancel();
        }
    });

    socket_tasks.spawn({
        let cancellation_token = cancellation_token.clone();
        async move {
            if let Err(e) = run_socket_writer(tx, msgrecvchan, cancellation_token.clone()).await {
                error!(error = %e, "socket writer error");
            }
            cancellation_token.cancel();
        }
    });
}

async fn run_message_handler(
    mut message_handler: SocketMessageHandler,
    cancellation_token: CancellationToken,
) -> Result<(), anyhow::Error> {
    loop {
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                message_handler.wait().await;
                return Ok(());
            }
            result = message_handler.process_next() => {
                match result {
                    Ok(_) => {},
                    Err(error) => {
                        message_handler.wait().await;
                        return Err(error);
                    },
                }
            }
        }
    }
}

async fn run_socket_reader(
    mut rx: tokio::net::tcp::OwnedReadHalf,
    mut socksend: tokio::io::WriteHalf<tokio::io::SimplexStream>,
    cancellation_token: CancellationToken,
) -> Result<(), anyhow::Error> {
    loop {
        let mut buffer = [0; 128 * 1024];

        tokio::select! {
            _ = cancellation_token.cancelled() => {
                return Ok(());
            }
            result = rx.read(&mut buffer) => {
                match result {
                    Ok(0) => {
                        return Ok(());
                    }
                    Ok(n) => {
                        trace!(num_of_bytes = %n, "bytes read from socket");
                        socksend.write_all(&buffer[..n]).await?;
                    }
                    Err(ref e)
                        if matches!(
                            e.kind(),
                            io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                        ) =>
                    {
                        trace!(warning = %e, "Recoverable socket reader error");
                        continue;
                    }
                    Err(e) => {
                        return Err(e.into());
                    }
                }
            }
        }
    }
}

async fn run_socket_writer(
    mut tx: tokio::net::tcp::OwnedWriteHalf,
    mut msgrecvchan: tokio::sync::mpsc::UnboundedReceiver<SocketMessageType>,
    cancellation_token: CancellationToken,
) -> Result<(), anyhow::Error> {
    loop {
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                return Ok(());
            }
            reply = msgrecvchan.recv() => {
                match reply {
                    Some(Ok(msg)) => {
                        write_fragment(&mut tx, &msg).await?;
                    }
                    Some(Err(e)) => {
                        return Err(e);
                    }
                    None => {
                        return Ok(());
                    }
                }
            }
        }
    }
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
                result = self.listener.accept() => {
                    match result {
                        Ok((socket, _)) => {
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

                            let socket_tasks = self.socket_tasks.clone();
                            let cancellation_token = self.cancellation_token.child_token();
                            process_socket(socket, context, &socket_tasks, cancellation_token);
                        }
                        Err(e) => {
                            error!("Listener accept error: {:?}", e);
                            break;
                        }
                    }
                }
            }
        }

        info!("cleaning up nfs tasks before exiting");

        self.socket_tasks.wait().await;

        Ok(())
    }
}
