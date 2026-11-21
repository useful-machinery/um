use std::fmt;
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::time::Duration;

use super::agent_login::setup::SYNC_BOUND;
use super::control_protocol::{
    AssignmentAnswer, AssignmentQuery, OVERVIEW_RESPONSE_LIMIT, Operation, OverviewAnswer,
    ProtocolFailure, RESPONSE_LIMIT, Response, decode_assignment_response,
    decode_overview_response, decode_response, encode_assignment_request, encode_overview_request,
    encode_request,
};

const IO_TIMEOUT: Duration = Duration::from_secs(2);
const RELOAD_RESPONSE_TIMEOUT: Duration = Duration::from_secs(32);
/// Runner Serve bounds an agent-login sync at `SYNC_BOUND`; the client waits
/// a little longer for its answer.
pub(crate) const SYNC_RESPONSE_TIMEOUT: Duration =
    SYNC_BOUND.saturating_add(Duration::from_secs(2));

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum RequestFailure {
    NotReachable,
    Protocol(ProtocolFailure),
}

impl fmt::Display for RequestFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotReachable => formatter.write_str("Runner Serve is not reachable"),
            Self::Protocol(failure) => failure.fmt(formatter),
        }
    }
}

impl std::error::Error for RequestFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::NotReachable => None,
            Self::Protocol(failure) => Some(failure),
        }
    }
}

pub fn request(socket_path: &Path, operation: Operation) -> Result<Response, RequestFailure> {
    let response_timeout = match operation {
        Operation::Status => IO_TIMEOUT,
        Operation::ReloadCredential => RELOAD_RESPONSE_TIMEOUT,
        Operation::SyncAgentLogins => SYNC_RESPONSE_TIMEOUT,
    };
    let response = exchange(
        socket_path,
        &encode_request(operation),
        response_timeout,
        RESPONSE_LIMIT,
    )?;
    decode_response(&response).map_err(RequestFailure::Protocol)
}

/// Asks the serving process for its overview (REQ-RUNNERENROLL-135). A serving process
/// of an earlier release refuses the operation, which is answered as
/// `OverviewAnswer::Refused`, not as a failure of the request.
pub fn request_overview(socket_path: &Path) -> Result<OverviewAnswer, RequestFailure> {
    let response = exchange(
        socket_path,
        &encode_overview_request(),
        IO_TIMEOUT,
        OVERVIEW_RESPONSE_LIMIT,
    )?;
    decode_overview_response(&response).map_err(RequestFailure::Protocol)
}

/// Asks the serving process for one assignment it holds, the run `query` names and a
/// step's output or the end of the run log (REQ-RUNNERENROLL-144). The request is written
/// only when it fits its bound; a serving process of an earlier release refuses the
/// operation, which is answered as `AssignmentAnswer::Refused`.
pub fn request_assignment(
    socket_path: &Path,
    query: &AssignmentQuery,
) -> Result<AssignmentAnswer, RequestFailure> {
    let request = encode_assignment_request(query).map_err(RequestFailure::Protocol)?;
    let response = exchange(socket_path, &request, IO_TIMEOUT, OVERVIEW_RESPONSE_LIMIT)?;
    decode_assignment_response(&response).map_err(RequestFailure::Protocol)
}

/// Writes one request and reads its one LF-terminated answer of at most
/// `response_limit` bytes.
fn exchange(
    socket_path: &Path,
    request: &[u8],
    response_timeout: Duration,
    response_limit: usize,
) -> Result<Vec<u8>, RequestFailure> {
    let mut stream = UnixStream::connect(socket_path).map_err(|_| RequestFailure::NotReachable)?;
    stream
        .set_read_timeout(Some(response_timeout))
        .and_then(|()| stream.set_write_timeout(Some(IO_TIMEOUT)))
        .map_err(|_| RequestFailure::NotReachable)?;
    stream
        .write_all(request)
        .and_then(|()| stream.flush())
        .map_err(|_| RequestFailure::NotReachable)?;

    let mut response = Vec::with_capacity(512);
    let mut chunk = [0_u8; 1024];
    loop {
        let read = stream
            .read(&mut chunk)
            .map_err(|_| RequestFailure::NotReachable)?;
        if read == 0 {
            return Err(if response.is_empty() {
                RequestFailure::NotReachable
            } else {
                RequestFailure::Protocol(ProtocolFailure::Invalid)
            });
        }
        let read = &chunk[..read];
        response.extend_from_slice(read);
        if response.len() > response_limit {
            return Err(RequestFailure::Protocol(ProtocolFailure::Oversized));
        }
        // An earlier chunk held no LF, so only the one just read can end the answer.
        if let Some(newline) = read.iter().position(|byte| *byte == b'\n') {
            if newline + 1 != read.len() {
                return Err(RequestFailure::Protocol(ProtocolFailure::Invalid));
            }
            return Ok(response);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::{BufRead, BufReader, Write};
    use std::os::unix::net::UnixListener;
    use std::path::{Path, PathBuf};
    use std::thread;

    use super::{RequestFailure, request_assignment, request_overview};
    use crate::control_protocol::{
        AssignmentAnswer, AssignmentQuery, ControlError, OverviewAnswer, OverviewRunElement,
        ProtocolFailure, decode_assignment_response, decode_overview_response,
        encode_assignment_response, encode_overview_response,
    };

    fn fixture(path: &str) -> Vec<u8> {
        std::fs::read(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../tests/fixtures/runner-control/v1")
                .join(path),
        )
        .unwrap()
    }

    /// Asks for an overview of a server that reads one request line and writes `answer`,
    /// answering what the client gave and the line the server received.
    fn exchange(answer: Vec<u8>) -> (Result<OverviewAnswer, RequestFailure>, Vec<u8>) {
        exchange_with(answer, request_overview)
    }

    /// Asks `ask` of a server that reads one request line and writes `answer`, answering
    /// what the client gave and the line the server received.
    fn exchange_with<T>(answer: Vec<u8>, ask: impl FnOnce(&Path) -> T) -> (T, Vec<u8>) {
        // A socket's path is bounded by `sun_path`, which a long TMPDIR can pass.
        let root = tempfile::tempdir_in("/tmp").unwrap();
        let socket: PathBuf = root.path().join("control.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(stream);
            let mut line = Vec::new();
            reader.read_until(b'\n', &mut line).unwrap();
            reader.get_mut().write_all(&answer).unwrap();
            line
        });
        let answered = ask(&socket);
        (answered, server.join().unwrap())
    }

    // REQ-RUNNERENROLL-135: the client asks by name and reads the overview answered.
    #[test]
    fn an_overview_request_is_the_fixture_line_and_its_answer_decodes() {
        let idle = fixture("valid/overview-idle-response.json");
        let (answered, received) = exchange(idle.clone());
        assert_eq!(received, fixture("valid/overview-request.json"));
        assert_eq!(
            received,
            b"{\"schemaVersion\":1,\"operation\":\"overview\"}\n"
        );
        assert_eq!(answered, Ok(decode_overview_response(&idle).unwrap()));
    }

    // REQ-RUNNERENROLL-135: a serving process of an earlier release refuses the
    // operation, which is an answer and not a failure of the request.
    #[test]
    fn an_earlier_server_refuses_the_overview_with_invalid_request() {
        let (answered, _) = exchange(
            b"{\"schemaVersion\":1,\"outcome\":\"error\",\"error\":\"invalid_request\"}\n".to_vec(),
        );
        assert_eq!(
            answered,
            Ok(OverviewAnswer::Refused(ControlError::InvalidRequest))
        );
    }

    // REQ-RUNNERENROLL-135: an answer the client cannot read is a protocol failure.
    #[test]
    fn an_unreadable_overview_answer_is_a_protocol_failure() {
        let (answered, _) = exchange(b"{\"schemaVersion\":1,\"outcome\":\"overview\"}\n".to_vec());
        assert_eq!(
            answered,
            Err(RequestFailure::Protocol(ProtocolFailure::Invalid))
        );
    }

    // REQ-RUNNERENROLL-135, REQ-RUNNERENROLL-136: an overview answer past the bound of
    // the other answers is read whole.
    #[test]
    fn an_overview_answer_longer_than_the_status_bound_is_read_whole() {
        let Ok(OverviewAnswer::Overview(mut overview)) =
            decode_overview_response(&fixture("valid/overview-response.json"))
        else {
            panic!("the overview fixture did not decode");
        };
        let entry = overview.assignments[0].clone();
        overview.assignments = (0..200)
            .map(|index| {
                let mut entry = entry.clone();
                entry.assignment_id = format!("asn_01k0z6r1w8f4jy2m7q9v3x{index:04}");
                entry.workflow_path = format!(".useful/{}/workflow.yaml", "w".repeat(400));
                entry
            })
            .collect();
        let answer = encode_overview_response(&overview).unwrap();
        assert!(answer.len() > 65_536, "{}", answer.len());
        let (answered, _) = exchange(answer);
        assert_eq!(answered, Ok(OverviewAnswer::Overview(overview)));
    }

    /// The query `valid/assignment-request.json` states.
    fn stated_query() -> AssignmentQuery {
        let element = |step: &str, item, ordinal| OverviewRunElement {
            step: step.to_owned(),
            item,
            ordinal,
        };
        AssignmentQuery {
            assignment_id: "asn_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            run: vec![element("build", Some(1), 2), element("fix", None, 1)],
            step: Some("implement".to_owned()),
        }
    }

    // REQ-RUNNERENROLL-144: the client writes the fixture's request line for its query
    // and reads an answer past the bound of the closed answers whole.
    #[test]
    fn an_assignment_request_is_the_fixture_line_and_a_long_answer_is_read_whole() {
        let Ok(AssignmentAnswer::Assignment(mut detail)) =
            decode_assignment_response(&fixture("valid/assignment-response.json"))
        else {
            panic!("the assignment fixture did not decode");
        };
        if let Some(log) = detail.log.as_mut()
            && let Some(stdout) = log.stdout.as_mut()
        {
            stdout.text = "x".repeat(200_000);
        }
        let answer = encode_assignment_response(&detail).unwrap();
        assert!(answer.len() > 65_536, "{}", answer.len());
        let (answered, received) =
            exchange_with(answer, |socket| request_assignment(socket, &stated_query()));
        assert_eq!(received, fixture("valid/assignment-request.json"));
        assert_eq!(answered, Ok(AssignmentAnswer::Assignment(detail)));
    }

    // REQ-RUNNERENROLL-144: a serving process of an earlier release refuses the
    // operation, which is an answer and not a failure of the request.
    #[test]
    fn an_earlier_server_refuses_the_assignment_with_invalid_request() {
        let (answered, _) = exchange_with(
            b"{\"schemaVersion\":1,\"outcome\":\"error\",\"error\":\"invalid_request\"}\n".to_vec(),
            |socket| request_assignment(socket, &stated_query()),
        );
        assert_eq!(
            answered,
            Ok(AssignmentAnswer::Refused(ControlError::InvalidRequest))
        );
    }

    // REQ-RUNNERENROLL-144: a query whose request would pass the request bound is refused
    // before the client connects, so nothing reaches the socket.
    #[test]
    fn an_assignment_request_past_its_bound_is_never_written() {
        let root = tempfile::tempdir_in("/tmp").unwrap();
        let socket = root.path().join("control.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        listener.set_nonblocking(true).unwrap();
        let mut query = stated_query();
        query.run = (0..64)
            .map(|ordinal| OverviewRunElement {
                step: format!("s{}", "x".repeat(63)),
                item: None,
                ordinal,
            })
            .collect();
        assert_eq!(
            request_assignment(&socket, &query),
            Err(RequestFailure::Protocol(ProtocolFailure::Oversized))
        );
        assert_eq!(
            listener.accept().map(|_| ()).map_err(|error| error.kind()),
            Err(std::io::ErrorKind::WouldBlock)
        );
    }
}
