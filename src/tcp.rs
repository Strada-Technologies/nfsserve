use crate::context::RPCContext;
use crate::rpcwire::*;
use crate::vfs::NFSFileSystem;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use std::net::SocketAddr;
use std::sync::Arc;
use std::{io, net::IpAddr};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tracing::{info, debug};
use crate::transaction_tracker::TransactionTracker;

/// A NFS Tcp Connection Handler
pub struct NFSTcpListener<T: NFSFileSystem + Send + Sync + 'static> {
    listener: TcpListener,
    port: u16,
    fs: Arc<T>,
    mount_signal: Option<mpsc::Sender<bool>>,
    export_name: Arc<String>,
    transaction_tracker: Arc<TransactionTracker>,
    socket_tasks: JoinSet<()>,
    cancellation_token: CancellationToken
}

impl<T: NFSFileSystem + Send + Sync + 'static> NFSTcpListener<T> {
    /// Binds to a ipstr of the form [ip address]:port. For instance
    /// "127.0.0.1:12000". fs is an instance of an implementation
    /// of NFSFileSystem.
    pub async fn bind(ipstr: &str, fs: Arc<T>, cancellation_token: CancellationToken) -> io::Result<NFSTcpListener<T>> {
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

                let result = NFSTcpListener::bind_internal(&ip, port, fs.clone(), cancellation_token.clone()).await;

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

    async fn bind_internal(ip: &str, port: u16, fs: Arc<T>, cancellation_token: CancellationToken) -> io::Result<NFSTcpListener<T>> {
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
            socket_tasks: JoinSet::new(),
            cancellation_token
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

    /// Gets the true listening port. Useful if the bound port number is 0
    pub fn get_listen_port(&self) -> u16 {
        let addr = self.listener.local_addr().unwrap();
        addr.port()
    }

    /// Gets the true listening IP. Useful on windows when the IP may be random
    pub fn get_listen_ip(&self) -> IpAddr {
        let addr = self.listener.local_addr().unwrap();
        addr.ip()
    }

    /// Sets a mount listener. A "true" signal will be sent on a mount
    /// and a "false" will be sent on an unmount
    pub fn set_mount_listener(&mut self, signal: mpsc::Sender<bool>) {
        self.mount_signal = Some(signal);
    }

    /// Starts a task that handles all incoming connections.
    pub async fn run(mut self) -> anyhow::Result<()> {
        loop {
            tokio::select! {
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

                            let socket_handler = SocketHandler::new(socket, &context, self.cancellation_token.clone());
                            self.socket_tasks.spawn(async move {
                                if let Err(err) = socket_handler.run().await {
                                    tracing::error!("Nfs process_socket error: {err:?}")
                                }
                            });
                        }

                        Err(e) => anyhow::bail!("Listener accept error: {e:?}")
                    }
                }
                _ = self.socket_tasks.join_next() => {
                    tracing::trace!("Socket task finished");
                }
                _ = self.cancellation_token.cancelled() => {
                    self.socket_tasks.join_all().await;
                    return Ok(());
                }
            }
        }
    }
}

pub fn generate_host_ip(hostnum: u16) -> String {
    format!(
        "127.88.{}.{}",
        ((hostnum >> 8) & 0xFF) as u8,
        (hostnum & 0xFF) as u8
    )
}