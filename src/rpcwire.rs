use anyhow::anyhow;
use tokio::net::TcpStream;
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;
use std::io::Cursor;
use std::io::{Read, Write};
use tracing::{debug, error, trace, warn};

use crate::context::RPCContext;
use crate::rpc::*;
use crate::xdr::*;

use crate::mount;
use crate::mount_handlers;

use crate::nfs;
use crate::nfs_handlers;

use crate::portmap;
use crate::portmap_handlers;
use tokio::io::{self, AsyncReadExt};
use tokio::io::AsyncWriteExt;
use tokio::io::DuplexStream;
use tokio::sync::mpsc::{self, UnboundedSender};

// Information from RFC 5531
// https://datatracker.ietf.org/doc/html/rfc5531

const NFS_ACL_PROGRAM: u32 = 100227;
const NFS_ID_MAP_PROGRAM: u32 = 100270;
const NFS_METADATA_PROGRAM: u32 = 200024;

async fn handle_rpc(
    input: &mut impl Read,
    output: &mut impl Write,
    mut context: RPCContext,
) -> Result<bool, anyhow::Error> {
    let mut recv = rpc_msg::default();
    recv.deserialize(input)?;
    let xid = recv.xid;
    if let rpc_body::CALL(call) = recv.body {
        if let auth_flavor::AUTH_UNIX = call.cred.flavor {
            let mut auth = auth_unix::default();
            auth.deserialize(&mut Cursor::new(&call.cred.body))?;
            context.auth = auth;
        }
        if call.rpcvers != 2 {
            warn!("Invalid RPC version {} != 2", call.rpcvers);
            rpc_vers_mismatch(xid).serialize(output)?;
            return Ok(true);
        }

        if context.transaction_tracker.is_retransmission(xid, &context.client_addr) {
            // This is a retransmission
            // Drop the message and return
            debug!("Retransmission detected, xid: {}, client_addr: {}, call: {:?}", xid, context.client_addr, call);
            return Ok(false);
        }

        let res = {
            if call.prog == nfs::PROGRAM {
                nfs_handlers::handle_nfs(xid, call, input, output, &context).await
            } else if call.prog == portmap::PROGRAM {
                portmap_handlers::handle_portmap(xid, call, input, output, &context)
            } else if call.prog == mount::PROGRAM {
                mount_handlers::handle_mount(xid, call, input, output, &context).await
            } else if call.prog == NFS_ACL_PROGRAM
                || call.prog == NFS_ID_MAP_PROGRAM
                || call.prog == NFS_METADATA_PROGRAM
            {
                trace!("ignoring NFS_ACL packet");
                prog_unavail_reply_message(xid).serialize(output)?;
                Ok(())
            } else {
                warn!(
                    "Unknown RPC Program number {} != {}",
                    call.prog,
                    nfs::PROGRAM
                );
                prog_unavail_reply_message(xid).serialize(output)?;
                Ok(())
            }
        }.map(|_| true);
        context.transaction_tracker.mark_processed(xid, &context.client_addr);
        res
    } else {
        error!("Unexpectedly received a Reply instead of a Call");
        Err(anyhow!("Bad RPC Call format"))
    }
}

/// RFC 1057 Section 10
/// When RPC messages are passed on top of a byte stream transport
/// protocol (like TCP), it is necessary to delimit one message from
/// another in order to detect and possibly recover from protocol errors.
/// This is called record marking (RM).  Sun uses this RM/TCP/IP
/// transport for passing RPC messages on TCP streams.  One RPC message
/// fits into one RM record.
///
/// A record is composed of one or more record fragments.  A record
/// fragment is a four-byte header followed by 0 to (2**31) - 1 bytes of
/// fragment data.  The bytes encode an unsigned binary number; as with
/// XDR integers, the byte order is from highest to lowest.  The number
/// encodes two values -- a boolean which indicates whether the fragment
/// is the last fragment of the record (bit value 1 implies the fragment
/// is the last fragment) and a 31-bit unsigned binary value which is the
/// length in bytes of the fragment's data.  The boolean value is the
/// highest-order bit of the header; the length is the 31 low-order bits.
/// (Note that this record specification is NOT in XDR standard form!)
async fn read_fragment(
    socket: &mut DuplexStream,
    append_to: &mut Vec<u8>,
) -> Result<bool, anyhow::Error> {
    let mut header_buf = [0_u8; 4];
    socket.read_exact(&mut header_buf).await?;
    let fragment_header = u32::from_be_bytes(header_buf);
    let is_last = (fragment_header & (1 << 31)) > 0;
    let length = (fragment_header & ((1 << 31) - 1)) as usize;
    trace!("Reading fragment length:{}, last:{}", length, is_last);
    let start_offset = append_to.len();
    append_to.resize(append_to.len() + length, 0);
    socket.read_exact(&mut append_to[start_offset..]).await?;
    trace!(
        "Finishing Reading fragment length:{}, last:{}",
        length,
        is_last
    );
    Ok(is_last)
}

pub async fn write_fragment(
    socket: &mut TcpStream,
    buf: &Vec<u8>,
) -> Result<(), anyhow::Error> {
    // TODO: split into many fragments
    assert!(buf.len() < (1 << 31));
    // set the last flag
    let fragment_header = buf.len() as u32 + (1 << 31);
    let header_buf = u32::to_be_bytes(fragment_header);
    socket.write_all(&header_buf).await?;
    trace!("Writing fragment length:{}", buf.len());
    socket.write_all(buf).await?;
    Ok(())
}

/// The Socket Message Handler reads from a TcpStream and spawns off
/// subtasks to handle each message. replies are queued into the
/// reply_send_channel.
#[derive(Debug)]
pub struct SocketHandler {
    socket: TcpStream,
    context: RPCContext,
    message_tasks: JoinSet<()>,
    cancellation_token: CancellationToken,
}

impl SocketHandler {
    /// Creates a new SocketMessageHandler with the receiver for queued message replies
    pub fn new(socket: TcpStream, context: &RPCContext, cancellation_token: CancellationToken) -> Self {
        Self {
            socket,
            context: context.clone(),
            message_tasks: JoinSet::new(),
            cancellation_token,
        }
    }

    pub async fn run(mut self) -> Result<(), anyhow::Error> {

        let (mut socket_tx, mut socket_rx) = tokio::io::duplex(256000);
        let (fragment_tx, mut fragment_rx) = mpsc::unbounded_channel();
        let (response_tx, mut response_rx) = mpsc::unbounded_channel();

        let _ = self.socket.set_nodelay(true);

        // Reads fragments from the socket.
        // Cannot be in the tokio::select! because read_fragment is not cancel safe
        let _: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
            let mut current_fragment = Vec::new();

            loop {
                let is_last =
                    read_fragment(&mut socket_rx, &mut current_fragment).await?;
                if is_last {
                    let fragment = std::mem::take(&mut current_fragment);
                    fragment_tx.send(fragment)?;
                }
            }
        });

        loop {
            tokio::select! {
                _ = self.socket.readable() => {
                    let mut buf = [0; 128000];

                    match self.socket.try_read(&mut buf) {
                        Ok(0) => {
                            return Ok(());
                        }
                        Ok(n) => {
                            let _ = socket_tx.write_all(&buf[..n]).await;
                        }
                        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                            continue;
                        }
                        Err(e) => {
                            debug!("Message handling closed: {:?}", e);
                            return Err(e.into());
                        }
                    }

                },

                Some(fragment) = fragment_rx.recv() => {
                    self.handle_message(fragment, response_tx.clone(), self.cancellation_token.clone()).await;
                }

                Some(response) = response_rx.recv() => {
                    if let Err(e) = write_fragment(&mut self.socket, &response).await {
                        error!("Write error {:?}", e);
                    }
                }

                _ = self.cancellation_token.cancelled() => {
                    self.message_tasks.join_all().await;
                    return Ok(())
                }
            }
        }
    }

    pub async fn handle_message(&mut self, fragment: Vec<u8>, response_tx: UnboundedSender<Vec<u8>>, cancellation_token: CancellationToken) {

        let context = self.context.clone();

        self.message_tasks.spawn(async move {

            let mut fragment_cursor = Cursor::new(fragment);
            
            let mut write_buf: Vec<u8> = Vec::new();
            let mut write_cursor = Cursor::new(&mut write_buf);

            let rpc = handle_rpc(&mut fragment_cursor, &mut write_cursor, context);

            let result = tokio::select! {
                result = rpc => result,
                _ = cancellation_token.cancelled() => return
            };
                
            match result {
                Ok(true) => {
                    let _ = std::io::Write::flush(&mut write_cursor);
                    let _ = response_tx.send(write_buf);
                }
                Ok(false) => (),
                Err(e) => {
                    error!("RPC Error: {e:?}");
                }
            }
        });
    }
}