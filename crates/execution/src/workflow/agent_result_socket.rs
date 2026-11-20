//! The private Unix socket through which a harness's result tool reaches its adapter. Each
//! connection carries one request frame (a 4-byte big-endian length, then a JSON payload) and
//! one response frame, after which the server closes it. Pi's result bridge and the Claude Code
//! result channel serve it, each with its own request and response types.

use std::fs::{self, OpenOptions};
use std::io::{self, Write as _};
use std::num::NonZeroU64;
use std::ops::Add as _;
use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};

use rustix::net::{RecvFlags, recv};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use super::agent::PositiveDuration;
use super::coordinator::CoordinatorClock;
use super::socket_alias::{
    SOCKET_ALIAS_NAME, create_socket_alias_retry, ignore_not_found, remove_socket_alias,
};

fn invalid_socket_data(error: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}

/// A response type of the result socket, which names its own answer to a request the socket
/// could not read or that broke the one-request framing.
pub(crate) trait ResultSocketResponse: Serialize + Send + Sync + 'static {
    fn channel_failure() -> Self;
}

#[derive(Debug)]
pub(crate) enum ResultSocketEvent<Request, Response> {
    Request(IncomingResultRequest<Request, Response>),
    ProtocolFailure,
    Closed,
}

#[derive(Debug)]
pub(crate) struct IncomingResultRequest<Request, Response> {
    request: Request,
    response: oneshot::Sender<ResponseCommand<Response>>,
}

impl<Request, Response> IncomingResultRequest<Request, Response> {
    pub(crate) fn request(&self) -> &Request {
        &self.request
    }

    pub(crate) async fn respond(self, response: Response) -> Result<(), io::Error> {
        let (delivered, delivery) = oneshot::channel();
        self.response
            .send(ResponseCommand {
                response,
                delivered,
            })
            .map_err(|_| invalid_socket_data("response channel closed"))?;
        delivery.await.map_err(invalid_socket_data)?
    }
}

#[derive(Debug)]
struct ResponseCommand<Response> {
    response: Response,
    delivered: oneshot::Sender<Result<(), io::Error>>,
}

#[derive(Debug)]
pub(crate) struct ResultSocketServer<Request, Response> {
    events: mpsc::Receiver<ResultSocketEvent<Request, Response>>,
    stop: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl<Request, Response> ResultSocketServer<Request, Response>
where
    Request: DeserializeOwned + Send + 'static,
    Response: ResultSocketResponse,
{
    pub(crate) fn start<Clock: CoordinatorClock>(
        listener: UnixListener,
        maximum_frame_bytes: NonZeroU64,
        receive_deadline: PositiveDuration,
        clock: Clock,
    ) -> Self {
        let (events_sender, events) = mpsc::channel(1);
        let (stop, stop_receiver) = watch::channel(false);
        let task = tokio::spawn(serve_socket(
            listener,
            events_sender,
            stop_receiver,
            maximum_frame_bytes,
            receive_deadline,
            clock,
        ));
        Self { events, stop, task }
    }

    pub(crate) async fn receive(&mut self) -> ResultSocketEvent<Request, Response> {
        self.events
            .recv()
            .await
            .unwrap_or(ResultSocketEvent::Closed)
    }

    pub(crate) async fn shutdown(self) -> Result<(), io::Error> {
        let Self { events, stop, task } = self;
        drop(events);
        stop.send_replace(true);
        task.await.map_err(invalid_socket_data)
    }
}

async fn serve_socket<Request, Response, Clock>(
    listener: UnixListener,
    events: mpsc::Sender<ResultSocketEvent<Request, Response>>,
    mut stop: watch::Receiver<bool>,
    maximum_frame_bytes: NonZeroU64,
    receive_deadline: PositiveDuration,
    mut clock: Clock,
) where
    Request: DeserializeOwned + Send + 'static,
    Response: ResultSocketResponse,
    Clock: CoordinatorClock,
{
    loop {
        let accepted = tokio::select! {
            biased;
            _ = wait_for_stop(&mut stop) => return,
            accepted = listener.accept() => accepted,
        };
        let Ok((stream, _address)) = accepted else {
            let _ = events.send(ResultSocketEvent::ProtocolFailure).await;
            return;
        };
        let deadline = clock.now().add(receive_deadline.get());
        if !serve_connection(
            stream,
            &events,
            &mut stop,
            maximum_frame_bytes,
            &clock,
            deadline,
        )
        .await
        {
            return;
        }
    }
}

async fn serve_connection<Request, Response, Clock>(
    mut stream: UnixStream,
    events: &mpsc::Sender<ResultSocketEvent<Request, Response>>,
    stop: &mut watch::Receiver<bool>,
    maximum_frame_bytes: NonZeroU64,
    clock: &Clock,
    deadline: Clock::Instant,
) -> bool
where
    Request: DeserializeOwned + Send + 'static,
    Response: ResultSocketResponse,
    Clock: CoordinatorClock,
{
    let request = match read_request_frame(&mut stream, stop, maximum_frame_bytes, clock, deadline)
        .await
    {
        FrameRead::Request(request) => request,
        FrameRead::ProtocolFailure => {
            let response = Response::channel_failure();
            let _ = write_response_frame(&mut stream, stop, &response, maximum_frame_bytes).await;
            let _ = events.send(ResultSocketEvent::ProtocolFailure).await;
            return true;
        }
        FrameRead::Stopped => return false,
    };

    let (response_sender, response) = oneshot::channel();
    if events
        .send(ResultSocketEvent::Request(IncomingResultRequest {
            request,
            response: response_sender,
        }))
        .await
        .is_err()
    {
        return false;
    }
    let command = match wait_for_response_command(&mut stream, stop, response).await {
        Ok(command) => command,
        Err(ResponseWaitFailure::TrailingRequest) => {
            discard_buffered_request(&stream);
            let response = Response::channel_failure();
            let _ = write_response_frame(&mut stream, stop, &response, maximum_frame_bytes).await;
            let _ = events.send(ResultSocketEvent::ProtocolFailure).await;
            return true;
        }
        Err(ResponseWaitFailure::SenderDropped) => {
            let _ = events.send(ResultSocketEvent::ProtocolFailure).await;
            return true;
        }
        Err(ResponseWaitFailure::Stopped) => return false,
    };
    let delivered =
        write_response_frame(&mut stream, stop, &command.response, maximum_frame_bytes).await;
    let _ = command.delivered.send(delivered);
    true
}

enum ResponseWaitFailure {
    TrailingRequest,
    SenderDropped,
    Stopped,
}

async fn wait_for_response_command<Response>(
    stream: &mut UnixStream,
    stop: &mut watch::Receiver<bool>,
    response: oneshot::Receiver<ResponseCommand<Response>>,
) -> Result<ResponseCommand<Response>, ResponseWaitFailure> {
    let mut response = response;
    let mut trailing = [0_u8; 1];
    tokio::select! {
        biased;
        _ = wait_for_stop(stop) => Err(ResponseWaitFailure::Stopped),
        read = stream.read(&mut trailing) => match read {
            Ok(0) => tokio::select! {
                biased;
                _ = wait_for_stop(stop) => Err(ResponseWaitFailure::Stopped),
                command = &mut response => command.map_err(|_| ResponseWaitFailure::SenderDropped),
            },
            Ok(_) | Err(_) => Err(ResponseWaitFailure::TrailingRequest),
        },
        command = &mut response => match command {
            Ok(_) if has_trailing_request(stream) => Err(ResponseWaitFailure::TrailingRequest),
            Ok(command) => Ok(command),
            Err(_) => Err(ResponseWaitFailure::SenderDropped),
        },
    }
}

// Query the socket directly because Tokio's cached readiness can lag a peer write
// that completed before the validator's response became ready.
fn has_trailing_request(stream: &UnixStream) -> bool {
    let mut trailing = [0_u8; 1];
    match recv(stream, &mut trailing, RecvFlags::DONTWAIT | RecvFlags::PEEK) {
        Ok((_, 0)) => false,
        Err(rustix::io::Errno::AGAIN) => false,
        Ok(_) | Err(_) => true,
    }
}

fn discard_buffered_request(stream: &UnixStream) {
    let mut trailing = [0_u8; 1024];
    loop {
        match recv(stream, &mut trailing, RecvFlags::DONTWAIT) {
            Ok((_, 0)) | Err(rustix::io::Errno::AGAIN) => return,
            Ok(_) => {}
            Err(_) => return,
        }
    }
}

enum FrameRead<Request> {
    Request(Request),
    ProtocolFailure,
    Stopped,
}

async fn read_request_frame<Request: DeserializeOwned, Clock: CoordinatorClock>(
    stream: &mut UnixStream,
    stop: &mut watch::Receiver<bool>,
    maximum_frame_bytes: NonZeroU64,
    clock: &Clock,
    deadline: Clock::Instant,
) -> FrameRead<Request> {
    let mut length = [0_u8; 4];
    let deadline_wait = clock.wait_until(deadline.clone());
    tokio::pin!(deadline_wait);
    let read_length = tokio::select! {
        biased;
        _ = wait_for_stop(stop) => return FrameRead::Stopped,
        () = &mut deadline_wait => return FrameRead::ProtocolFailure,
        result = stream.read_exact(&mut length) => result,
    };
    if read_length.is_err() {
        return FrameRead::ProtocolFailure;
    }
    let payload_length = u64::from(u32::from_be_bytes(length));
    if payload_length > maximum_frame_bytes.get() {
        return FrameRead::ProtocolFailure;
    }
    let Ok(payload_length) = usize::try_from(payload_length) else {
        return FrameRead::ProtocolFailure;
    };
    let mut payload = Vec::new();
    if payload.try_reserve_exact(payload_length).is_err() {
        return FrameRead::ProtocolFailure;
    }
    payload.resize(payload_length, 0);
    let deadline_wait = clock.wait_until(deadline.clone());
    tokio::pin!(deadline_wait);
    let read_payload = tokio::select! {
        biased;
        _ = wait_for_stop(stop) => return FrameRead::Stopped,
        () = &mut deadline_wait => return FrameRead::ProtocolFailure,
        result = stream.read_exact(&mut payload) => result,
    };
    if read_payload.is_err() {
        return FrameRead::ProtocolFailure;
    }

    // Pi 0.84's Bun node:net compatibility closes both socket halves on end(),
    // so a request-side EOF would also discard the response. Reject any already
    // buffered trailing frame bytes, then let the length prefix delimit the one
    // request while the server owns closing the connection after its response.
    let mut trailing = [0_u8; 1];
    match stream.try_read(&mut trailing) {
        Ok(0) => {}
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
        Ok(_) | Err(_) => return FrameRead::ProtocolFailure,
    }
    useful_support::strict_json_from_slice(&payload)
        .and_then(serde_json::from_value)
        .map(FrameRead::Request)
        .unwrap_or(FrameRead::ProtocolFailure)
}

async fn write_response_frame<Response: Serialize>(
    stream: &mut UnixStream,
    stop: &mut watch::Receiver<bool>,
    response: &Response,
    maximum_frame_bytes: NonZeroU64,
) -> Result<(), io::Error> {
    let payload = serde_json::to_vec(response).map_err(invalid_socket_data)?;
    let payload_length = u64::try_from(payload.len()).map_err(invalid_socket_data)?;
    if payload_length > maximum_frame_bytes.get() {
        return Err(invalid_socket_data("bridge response exceeds frame limit"));
    }
    let payload_length = u32::try_from(payload_length).map_err(invalid_socket_data)?;
    let mut frame = Vec::with_capacity(4_usize.saturating_add(payload.len()));
    frame.extend_from_slice(&payload_length.to_be_bytes());
    frame.extend_from_slice(&payload);
    let written = tokio::select! {
        biased;
        _ = wait_for_stop(stop) => return Err(io::Error::new(io::ErrorKind::Interrupted, "bridge stopped")),
        result = stream.write_all(&frame) => result,
    };
    written?;
    stream.shutdown().await
}

async fn wait_for_stop(stop: &mut watch::Receiver<bool>) {
    if *stop.borrow_and_update() {
        return;
    }
    while stop.changed().await.is_ok() {
        if *stop.borrow_and_update() {
            return;
        }
    }
}

/// A listening socket at `<staging>/<file name>`, mode 0600, bound through a short alias
/// directory whose address fits `sockaddr_un` however long the staging path is.
#[derive(Debug)]
pub(crate) struct BoundResultSocket {
    pub(crate) listener: UnixListener,
    pub(crate) socket_path: PathBuf,
    pub(crate) alias_directory: PathBuf,
    pub(crate) address: PathBuf,
}

pub(crate) fn bind_result_socket(
    staging_directory: &Path,
    file_name: &str,
    next_alias_directory: impl FnMut() -> io::Result<PathBuf>,
) -> io::Result<BoundResultSocket> {
    let socket_path = staging_directory.join(file_name);
    let alias_directory = create_socket_alias_retry(staging_directory, next_alias_directory)?;
    let address = alias_directory.join(SOCKET_ALIAS_NAME).join(file_name);
    let listener = match UnixListener::bind(&address) {
        Ok(listener) => listener,
        Err(error) => {
            let _ = remove_alias(&alias_directory);
            return Err(io::Error::new(
                error.kind(),
                format!("socket bind: {error}"),
            ));
        }
    };
    if let Err(error) = make_socket_private(&socket_path) {
        drop(listener);
        let _ = remove_result_socket(&socket_path, &alias_directory);
        return Err(io::Error::new(
            error.kind(),
            format!("socket permissions: {error}"),
        ));
    }
    Ok(BoundResultSocket {
        listener,
        socket_path,
        alias_directory,
        address,
    })
}

/// Removes a socket `bind_result_socket` bound and its alias; either may already be gone.
pub(crate) fn remove_result_socket(socket_path: &Path, alias_directory: &Path) -> io::Result<()> {
    let socket_result = remove_materialized_file(socket_path);
    socket_result.and(remove_alias(alias_directory))
}

fn remove_alias(alias_directory: &Path) -> io::Result<()> {
    remove_socket_alias(alias_directory, &alias_directory.join(SOCKET_ALIAS_NAME))
}

pub(crate) fn validate_result_endpoint_directory(directory: &Path) -> Result<(), io::Error> {
    let metadata = fs::symlink_metadata(directory)?;
    if !metadata.file_type().is_dir() || metadata.permissions().mode() & 0o077 != 0 {
        return Err(invalid_socket_data(
            "result endpoint is not a private directory",
        ));
    }
    if metadata.permissions().mode() & 0o200 == 0 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "result endpoint is not writable",
        ));
    }
    Ok(())
}

fn make_socket_private(path: &Path) -> io::Result<()> {
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
}

pub(crate) fn write_private_file(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(bytes)?;
    file.flush()?;
    file.set_permissions(fs::Permissions::from_mode(0o400))
}

pub(crate) fn remove_materialized_file(path: &Path) -> Result<(), io::Error> {
    ignore_not_found(fs::remove_file(path))
}
