use std::ffi::OsStr;
use std::io::{Read as _, Write as _};
use std::net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use std::process::{Command, Stdio};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::thread::{self, JoinHandle};
use std::time::Duration;

pub fn fixture_git_command(program: impl AsRef<OsStr>) -> Command {
    let path = std::env::var_os("PATH");
    let mut command = Command::new(program);
    command
        .args([
            "--no-pager",
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "commit.gpgSign=false",
            "-c",
            "tag.gpgSign=false",
            "-c",
            "gc.auto=0",
            "-c",
            "maintenance.auto=false",
        ])
        .env_clear()
        .env("HOME", "/nonexistent")
        .env("LC_ALL", "C")
        .env("TZ", "UTC")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_PAGER", "cat")
        .env("GIT_EDITOR", "true")
        .env("GIT_SEQUENCE_EDITOR", "true")
        .env("PAGER", "cat")
        .env("EDITOR", "true")
        .stdin(Stdio::null());
    if let Some(path) = path {
        command.env("PATH", path);
    }
    command
}

const MAX_REQUEST_BYTES: usize = 128 * 1024;
pub const REQUEST_IDEMPOTENCY_KEY_ECHO: &str = "{request-idempotency-key}";

pub struct ScriptedHttpServer {
    pub api_url: String,
    requests: Receiver<String>,
    thread: JoinHandle<()>,
    remaining_requests: usize,
    response_release: Option<SyncSender<()>>,
}

impl ScriptedHttpServer {
    pub fn respond(response: Vec<u8>) -> Self {
        Self::respond_in_sequence(vec![response])
    }

    // Deadline tests keep the response pending until the production timer has
    // classified the request, then release the fixture through this channel.
    #[allow(
        dead_code,
        reason = "not every test target uses the shared fixture's single-response pause"
    )]
    pub fn respond_when_released(response: Vec<u8>) -> Self {
        Self::respond_in_sequence_with_pause(vec![response], Some(0))
    }

    pub fn respond_in_sequence(responses: Vec<Vec<u8>>) -> Self {
        Self::respond_in_sequence_with_pause(responses, None)
    }

    #[allow(
        clippy::disallowed_macros,
        clippy::expect_used,
        reason = "the fixture validates its caller-supplied pause slot before launching a test server"
    )]
    pub fn respond_in_sequence_with_pause(
        responses: Vec<Vec<u8>>,
        pause_response: Option<usize>,
    ) -> Self {
        if let Some(index) = pause_response {
            assert!(index < responses.len(), "paused response should exist");
        }
        let (response_release, released) = if pause_response.is_some() {
            let (release, released) = mpsc::sync_channel(0);
            (Some(release), Some(released))
        } else {
            (None, None)
        };
        let mut released = released;
        let responses = responses
            .into_iter()
            .enumerate()
            .map(|(index, response)| ScriptedResponse {
                release: (pause_response == Some(index)).then(|| {
                    released
                        .take()
                        .expect("paused response receiver should exist")
                }),
                response,
            })
            .collect();
        Self::start(responses, response_release)
    }

    #[allow(
        clippy::expect_used,
        reason = "fixture setup and its server thread must fail the invoking test when local I/O or synchronization cannot start"
    )]
    fn start(responses: Vec<ScriptedResponse>, response_release: Option<SyncSender<()>>) -> Self {
        let remaining_requests = responses.len();
        let listener = TcpListener::bind("127.0.0.1:0").expect("fixture listener should bind");
        let address = listener.local_addr().expect("fixture address should exist");
        let (sender, requests) = mpsc::channel();
        let thread = thread::spawn(move || {
            for response in responses {
                let (mut stream, _) = listener.accept().expect("fixture request should arrive");
                let request =
                    String::from_utf8(read_request(&mut stream)).expect("request should be text");
                let ScriptedResponse { release, response } = response;
                let response = response_for_request(response, &request);
                sender
                    .send(request)
                    .expect("fixture request receiver should remain available");
                if let Some(release) = release {
                    release
                        .recv()
                        .expect("controlled fixture response should be released");
                }
                let _ = stream.write_all(&response);
            }
        });

        Self {
            api_url: format!("http://{address}/api/"),
            requests,
            thread,
            remaining_requests,
            response_release,
        }
    }

    #[allow(
        clippy::expect_used,
        reason = "misusing the fixture's explicit release handshake must fail the invoking test"
    )]
    pub fn release_response(&mut self) {
        self.response_release
            .take()
            .expect("fixture should have a controlled response")
            .send(())
            .expect("controlled fixture response should be released");
    }

    #[allow(
        clippy::expect_used,
        reason = "a missing scripted request must fail the invoking test at the fixture boundary"
    )]
    #[expect(
        clippy::disallowed_methods,
        reason = "wall time only bounds the external HTTP fixture's readiness messages"
    )]
    pub fn next_request(&mut self) -> String {
        let request = self
            .requests
            .recv_timeout(Duration::from_secs(2))
            .expect("fixture should capture request");
        self.remaining_requests -= 1;
        request
    }

    /// Waits for the fixture's request readiness signal without a scheduler deadline.
    ///
    /// Use this at controlled external-process boundaries where the test runner's
    /// per-test watchdog, rather than machine speed, should bound a missing request.
    pub fn wait_for_request(&mut self) -> Result<String, mpsc::RecvError> {
        let request = self.requests.recv()?;
        self.remaining_requests -= 1;
        Ok(request)
    }

    #[allow(
        clippy::disallowed_macros,
        reason = "the single-request fixture asserts the cardinality promised by its method name"
    )]
    pub fn finish_one(self) -> String {
        let mut requests = self.finish();
        assert_eq!(requests.len(), 1, "fixture should capture one request");
        requests.remove(0)
    }

    #[allow(
        clippy::disallowed_macros,
        clippy::expect_used,
        reason = "finishing the fixture must fail its invoking test on an unreleased response or failed server thread"
    )]
    pub fn finish(mut self) -> Vec<String> {
        assert!(
            self.response_release.is_none(),
            "controlled fixture response should be released before finishing"
        );
        let mut requests = Vec::with_capacity(self.remaining_requests);
        while self.remaining_requests > 0 {
            requests.push(self.next_request());
        }
        self.thread.join().expect("fixture server should stop");
        requests
    }
}

#[allow(
    clippy::expect_used,
    clippy::panic,
    reason = "the idempotency echo is an explicit test-fixture contract and malformed fixture data must fail the invoking test"
)]
fn response_for_request(response: Vec<u8>, request: &str) -> Vec<u8> {
    if response
        .windows(REQUEST_IDEMPOTENCY_KEY_ECHO.len())
        .any(|window| window == REQUEST_IDEMPOTENCY_KEY_ECHO.as_bytes())
    {
        let idempotency_key = request.lines().find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("idempotency-key")
                .then(|| value.trim())
        });
        // Name the request line only: the other headers hold the bearer credential.
        let Some(idempotency_key) = idempotency_key else {
            panic!(
                "the scripted response echoes the request's Idempotency-Key, and the request \
                 carries none; request line: {:?}",
                request.lines().next().unwrap_or_default()
            );
        };
        String::from_utf8(response)
            .expect("an idempotency-echo response should be text")
            .replace(REQUEST_IDEMPOTENCY_KEY_ECHO, idempotency_key)
            .into_bytes()
    } else {
        response
    }
}

struct ScriptedResponse {
    release: Option<Receiver<()>>,
    response: Vec<u8>,
}

/// Reads one loopback HTTP request for a fixture thread its test joins, as
/// `ScriptedHttpServer`'s is: a request that cannot be read panics and so
/// fails the test at the fixture. A serve loop that must keep its listener
/// for the whole test reads with [`try_read_request`] instead.
#[allow(
    clippy::expect_used,
    reason = "a joined loopback HTTP fixture thread must fail its invoking test when its request cannot be read"
)]
pub fn read_request(stream: &mut TcpStream) -> Vec<u8> {
    try_read_request(stream).expect("request should be readable")
}

/// Frames one loopback HTTP request for a serve loop that must keep its
/// listener for the whole test (INV-TESTING-22): a read that fails or times
/// out, or a request past the size bound, returns the error, so the loop can
/// skip the connection instead of dropping its address. The request ends at
/// its declared `Content-Length`, or where the peer closed first.
pub fn try_read_request(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    let mut request = Vec::new();
    let mut buffer = [0_u8; 1024];
    let mut expected_length = None;

    loop {
        let read = stream.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        request.extend_from_slice(&buffer[..read]);
        if request.len() > MAX_REQUEST_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "fixture request is unexpectedly large",
            ));
        }

        if let Some(header_end) = request.windows(4).position(|part| part == b"\r\n\r\n") {
            let body_start = header_end + 4;
            let length = *expected_length.get_or_insert_with(|| {
                String::from_utf8_lossy(&request[..body_start])
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().ok())
                            .flatten()
                    })
                    .unwrap_or_default()
            });
            if body_start.saturating_add(length) > MAX_REQUEST_BYTES {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "fixture request declares an unexpectedly large body",
                ));
            }
            if request.len() >= body_start + length {
                request.truncate(body_start + length);
                break;
            }
        }
    }

    Ok(request)
}

/// Holds a loopback port that refuses every connect for as long as the
/// value lives. Nothing may listen there: a dropped listener frees its port
/// for a parallel test's next bind, which would then answer the request.
/// A socket that is bound but never listens is refused on Linux but macOS
/// drops its SYNs until the connect times out, and the client end of a live
/// connection is refused on both but Linux can hand its auto-assigned port
/// to the client's own connect, which then reaches itself. So the holder is
/// bound explicitly, which Linux never reuses as a source port, and then
/// connected, which both kernels answer with a reset.
pub struct RefusedPort {
    _holder: rustix::fd::OwnedFd,
    _peer: TcpListener,
    _accepted: TcpStream,
    address: SocketAddr,
}

impl RefusedPort {
    #[allow(
        clippy::expect_used,
        reason = "a test that cannot hold its refusing loopback port must fail at the fixture boundary"
    )]
    pub fn hold() -> Self {
        let holder = rustix::net::socket(
            rustix::net::AddressFamily::INET,
            rustix::net::SocketType::STREAM,
            None,
        )
        .expect("refused-port holder socket should open");
        rustix::net::bind(&holder, &SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .expect("refused-port holder should bind");
        let address = SocketAddr::try_from(
            rustix::net::getsockname(&holder).expect("refused-port holder address should exist"),
        )
        .expect("refused-port holder address should be an IP address");
        let peer = TcpListener::bind("127.0.0.1:0").expect("refused-port peer should bind");
        rustix::net::connect(
            &holder,
            &peer
                .local_addr()
                .expect("refused-port peer address should exist"),
        )
        .expect("refused-port holder should connect to its peer");
        let (accepted, _) = peer.accept().expect("refused-port peer should accept");
        Self {
            _holder: holder,
            _peer: peer,
            _accepted: accepted,
            address,
        }
    }

    pub fn address(&self) -> SocketAddr {
        self.address
    }
}

#[cfg(test)]
#[allow(
    clippy::disallowed_macros,
    reason = "fixture unit tests use Rust equality assertions"
)]
mod tests {
    use std::io::ErrorKind;

    use super::*;

    // INV-TESTING-22
    #[test]
    fn a_held_refused_port_refuses_connects_and_cannot_be_bound() -> rustix::io::Result<()> {
        let refused = RefusedPort::hold();
        for _ in 0..2 {
            assert_eq!(
                TcpStream::connect(refused.address())
                    .map(|_| ())
                    .map_err(|error| error.kind()),
                Err(ErrorKind::ConnectionRefused)
            );
        }
        // No bind to port 0 may be given the held port. Both kernels refuse the address to
        // a bind that sets no reuse option, which is how macOS looks a port up when it
        // assigns one. Linux assigns a listener's port by the rules of its explicit bind,
        // `SO_REUSEADDR` included, so there the listener's own bind is refused too; macOS
        // lets that bind name the address of a connected socket.
        let plain = rustix::net::socket(
            rustix::net::AddressFamily::INET,
            rustix::net::SocketType::STREAM,
            None,
        )?;
        assert_eq!(
            rustix::net::bind(&plain, &refused.address()),
            Err(rustix::io::Errno::ADDRINUSE)
        );
        if cfg!(target_os = "linux") {
            assert_eq!(
                TcpListener::bind(refused.address())
                    .map(|_| ())
                    .map_err(|error| error.kind()),
                Err(ErrorKind::AddrInUse)
            );
        }
        Ok(())
    }

    // INV-TESTING-22
    #[test]
    fn an_echo_response_rejects_a_keyless_request_by_its_request_line() {
        let response =
            format!("HTTP/1.1 200 OK\r\n\r\n{REQUEST_IDEMPOTENCY_KEY_ECHO}").into_bytes();
        let message = std::panic::catch_unwind(move || {
            response_for_request(
                response,
                "GET /api/v1/stray HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n",
            )
        })
        .err()
        .and_then(|payload| payload.downcast::<String>().ok());
        assert!(
            message
                .as_deref()
                .is_some_and(|message| message.contains("GET /api/v1/stray HTTP/1.1")),
            "{message:?}"
        );
    }

    // INV-TESTING-25
    #[test]
    fn a_request_declaring_a_body_past_the_bound_is_an_error_not_a_panic() -> std::io::Result<()> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
        let mut client = TcpStream::connect(listener.local_addr()?)?;
        client.write_all(b"POST /graphql HTTP/1.1\r\nContent-Length: 131072\r\n\r\n")?;
        let (mut server, _) = listener.accept()?;
        let read = try_read_request(&mut server);
        drop(client);
        assert_eq!(
            read.map_err(|error| error.kind()),
            Err(ErrorKind::InvalidData)
        );
        Ok(())
    }

    // INV-TESTING-25
    #[test]
    fn a_request_ends_at_its_declared_length_while_the_peer_stays_open() -> std::io::Result<()> {
        let head = b"POST /graphql HTTP/1.1\r\nContent-Length: 2\r\n\r\n";
        let mut sent = head.to_vec();
        sent.extend_from_slice(b"{}GET /next HTTP/1.1\r\n\r\n");
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
        let mut client = TcpStream::connect(listener.local_addr()?)?;
        client.write_all(&sent)?;
        let (mut server, _) = listener.accept()?;
        let request = try_read_request(&mut server)?;
        drop(client);
        assert_eq!(request, [&head[..], b"{}"].concat());
        Ok(())
    }
}
