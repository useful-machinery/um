use super::*;

use std::os::unix::net::UnixListener;

use super::runner_status::write_status_config;

/// The control socket request `useful runner watch` sends for an overview.
fn overview_request() -> Vec<u8> {
    fixture("valid/overview-request.json")
}

fn fixture(path: &str) -> Vec<u8> {
    fs::read(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/runner-control/v1")
            .join(path),
    )
    .unwrap()
}

// REQ-RUNNERENROLL-139: without a terminal the command reads no configuration's socket,
// asks the runner nothing and names `useful runner status`.
#[test]
fn runner_watch_without_a_terminal_names_runner_status_and_asks_nothing() {
    let directory = private_credential_directory();
    let runtime = tempfile::tempdir_in("/tmp").unwrap();
    let socket_path = runtime.path().join("runner.sock");
    let listener = UnixListener::bind(&socket_path).unwrap();
    listener.set_nonblocking(true).unwrap();
    let config = write_status_config(&directory, &socket_path);

    let output = run(&["runner", "watch", "--config", &config]);

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("useful runner status"));
    assert_eq!(
        listener.accept().map(|_| ()).unwrap_err().kind(),
        io::ErrorKind::WouldBlock,
        "the command connected to the control socket"
    );
}

#[cfg(target_os = "linux")]
mod terminal {
    use super::*;

    use std::io::{BufRead as _, BufReader};
    use std::os::unix::net::UnixStream;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    use rustix::process::{Pid, Signal, kill_process};

    use super::super::poll_until;
    use super::super::workflow_run::{TuiProcess, open_tui_pty, spawn_tui_run};

    const ENTER_ALTERNATE_SCREEN: &str = "\u{1b}[?1049h";
    const LEAVE_ALTERNATE_SCREEN: &str = "\u{1b}[?1049l";
    /// The boot ID of the serving process that answers after the fixture's went away.
    const SECOND_BOOT_ID: &str = "rbt_7nnnnnnnnnnnnnnnnnnnnnnnnn";

    /// A serving runner's control socket that answers each connection by its request line
    /// and records each request line.
    struct ControlServer {
        socket_path: PathBuf,
        requests: Arc<Mutex<Vec<Vec<u8>>>>,
        stop: Arc<AtomicBool>,
        thread: Option<thread::JoinHandle<()>>,
    }

    impl ControlServer {
        /// Answers every connection with `answer`.
        fn start(socket_path: &Path, answer: Vec<u8>) -> Self {
            Self::answering(socket_path, move |_| answer.clone())
        }

        fn answering(
            socket_path: &Path,
            answer: impl Fn(&[u8]) -> Vec<u8> + Send + 'static,
        ) -> Self {
            let listener = UnixListener::bind(socket_path).unwrap();
            let requests = Arc::new(Mutex::new(Vec::new()));
            let stop = Arc::new(AtomicBool::new(false));
            let thread = thread::spawn({
                let requests = Arc::clone(&requests);
                let stop = Arc::clone(&stop);
                move || {
                    for stream in listener.incoming() {
                        if stop.load(Ordering::SeqCst) {
                            break;
                        }
                        let Ok(stream) = stream else { continue };
                        stream
                            .set_read_timeout(Some(Duration::from_secs(5)))
                            .unwrap();
                        let mut reader = BufReader::new(stream);
                        let mut line = Vec::new();
                        if reader.read_until(b'\n', &mut line).is_err() {
                            continue;
                        }
                        let answer = answer(&line);
                        requests.lock().unwrap().push(line);
                        let _ = reader.get_mut().write_all(&answer);
                    }
                }
            });
            Self {
                socket_path: socket_path.to_owned(),
                requests,
                stop,
                thread: Some(thread),
            }
        }

        fn requests(&self) -> Vec<Vec<u8>> {
            self.requests.lock().unwrap().clone()
        }

        fn wait_for_requests(&self, count: usize) {
            self.wait_until(|requests| requests.len() >= count);
        }

        fn wait_until(&self, wanted: impl Fn(&[Vec<u8>]) -> bool) {
            poll_until(
                "runner watch requests",
                || self.requests(),
                |requests| wanted(requests),
            );
        }

        /// Stops answering and removes the socket, as a serving process that went away.
        fn stop(mut self) -> Vec<Vec<u8>> {
            self.shut_down();
            self.requests()
        }

        fn shut_down(&mut self) {
            let Some(thread) = self.thread.take() else {
                return;
            };
            self.stop.store(true, Ordering::SeqCst);
            drop(UnixStream::connect(&self.socket_path));
            thread.join().unwrap();
            let _ = fs::remove_file(&self.socket_path);
        }
    }

    impl Drop for ControlServer {
        fn drop(&mut self) {
            self.shut_down();
        }
    }

    struct Watch {
        _directory: tempfile::TempDir,
        _runtime: tempfile::TempDir,
        socket_path: PathBuf,
        config: String,
    }

    impl Watch {
        fn new() -> Self {
            let directory = private_credential_directory();
            let runtime = tempfile::tempdir_in("/tmp").unwrap();
            let socket_path = runtime.path().join("runner.sock");
            let config = write_status_config(&directory, &socket_path);
            Self {
                _directory: directory,
                _runtime: runtime,
                socket_path,
                config,
            }
        }

        fn spawn(&self) -> TuiProcess {
            let (master, slave) = open_tui_pty();
            let args = ["runner", "watch", "--config", &self.config].map(str::to_owned);
            spawn_tui_run(&args, master, slave)
        }

        /// Runs the command to its end without input.
        fn run_to_exit(&self) -> (Option<i32>, String) {
            let mut process = self.spawn();
            let status = wait_for_exit(&mut process);
            let (_, transcript) = process.wait_and_finish();
            (status, String::from_utf8_lossy(&transcript).into_owned())
        }
    }

    fn wait_for_exit(process: &mut TuiProcess) -> Option<i32> {
        poll_until(
            "runner watch exit",
            || process.child_mut().try_wait().unwrap(),
            Option::is_some,
        )
        .and_then(|status| status.code())
    }

    /// Waits until the viewer has taken the terminal and asked again after its first
    /// answer, so the first frame is drawn and the loop polls.
    fn wait_until_watching(process: &mut TuiProcess, server: &ControlServer) {
        server.wait_for_requests(2);
        assert!(
            process.child_mut().try_wait().unwrap().is_none(),
            "runner watch ended while the runner answered"
        );
    }

    fn finish(process: TuiProcess) -> String {
        let (_, transcript) = process.wait_and_finish();
        let transcript = String::from_utf8_lossy(&transcript).into_owned();
        let entered = transcript
            .find(ENTER_ALTERNATE_SCREEN)
            .expect("runner watch must enter the alternate screen");
        assert!(
            transcript[entered..].contains(LEAVE_ALTERNATE_SCREEN),
            "runner watch must leave the alternate screen"
        );
        transcript
    }

    /// The 100-by-30 screen as the viewer last drew it before leaving the alternate screen.
    /// A redraw writes only the cells that changed, so a text is read from the screen the
    /// cursor moves and characters build, not from the transcript's bytes.
    fn last_screen(transcript: &str) -> Vec<String> {
        replay(transcript, |_| {})
            .into_iter()
            .map(|cells| cells.into_iter().collect())
            .collect()
    }

    /// Whether `text` was ever whole on one row of the screen the viewer drew.
    fn ever_on_screen(transcript: &str, text: &str) -> bool {
        let mut seen = false;
        replay(transcript, |row| {
            seen = seen || row.iter().collect::<String>().contains(text);
        });
        seen
    }

    /// Replays the transcript up to leaving the alternate screen onto a 100-by-30 screen,
    /// showing `row_left` each row the cursor leaves and, at the end, every row.
    fn replay(transcript: &str, mut row_left: impl FnMut(&[char])) -> Vec<Vec<char>> {
        let drawn = transcript
            .rfind(LEAVE_ALTERNATE_SCREEN)
            .map_or(transcript, |end| &transcript[..end]);
        let mut screen = vec![vec![' '; 100]; 30];
        let (mut row, mut column) = (0_usize, 0_usize);
        let mut characters = drawn.chars();
        while let Some(character) = characters.next() {
            if character != '\u{1b}' {
                if let Some(cell) = screen.get_mut(row).and_then(|cells| cells.get_mut(column)) {
                    *cell = character;
                }
                column += 1;
                continue;
            }
            if characters.next() != Some('[') {
                continue;
            }
            let mut parameters = String::new();
            for parameter in characters.by_ref() {
                if ('@'..='~').contains(&parameter) {
                    if parameter == 'H' {
                        if let Some(cells) = screen.get(row) {
                            row_left(cells);
                        }
                        let mut position = parameters
                            .split(';')
                            .map(|value| value.parse::<usize>().unwrap_or(1).max(1) - 1);
                        row = position.next().unwrap_or(0);
                        column = position.next().unwrap_or(0);
                    }
                    break;
                }
                parameters.push(parameter);
            }
        }
        for cells in &screen {
            row_left(cells);
        }
        screen
    }

    /// A request line's operation.
    fn operation(request: &[u8]) -> String {
        let request: serde_json::Value = serde_json::from_slice(request).unwrap();
        request["operation"].as_str().unwrap_or_default().to_owned()
    }

    fn press(process: &mut TuiProcess, keys: &[u8]) {
        process.master_writer().write_all(keys).unwrap();
        process.master_writer().flush().unwrap();
    }

    // SC-RUNNERENROLL-11, REQ-RUNNERENROLL-146: Enter opens the overview's first assignment
    // with its run and its Cloud URL, `k` selects a settled step whose output it reads, and
    // Esc and `q` leave having sent nothing but reads.
    #[test]
    fn runner_watch_opens_an_assignment_and_reads_a_settled_step_s_output() {
        let watch = Watch::new();
        let overview = fixture("valid/overview-response.json");
        let assignment = fixture("valid/assignment-response.json");
        let server = ControlServer::answering(&watch.socket_path, move |request| {
            if operation(request) == "assignment" {
                assignment.clone()
            } else {
                overview.clone()
            }
        });
        let root = fixture("valid/assignment-root-request.json");
        let names_plan = |request: &Vec<u8>| {
            let request: serde_json::Value = serde_json::from_slice(request).unwrap();
            request["step"] == "plan"
        };
        let mut process = watch.spawn();
        wait_until_watching(&mut process, &server);

        press(&mut process, b"\r");
        // A second read of the root follows the first one's answer.
        server.wait_until(|requests| {
            requests.iter().filter(|request| **request == root).count() >= 2
        });
        press(&mut process, b"k");
        server.wait_until(|requests| {
            requests
                .iter()
                .filter(|request| names_plan(request))
                .count()
                >= 2
        });
        press(&mut process, b"\x1b");
        let overviews = |requests: &[Vec<u8>]| {
            requests
                .iter()
                .filter(|request| **request == overview_request())
                .count()
        };
        let before = overviews(&server.requests());
        server.wait_until(|requests| overviews(requests) >= before + 2);
        press(&mut process, b"q");
        let status = wait_for_exit(&mut process);
        let transcript = finish(process);

        assert_eq!(status, Some(0), "{transcript:?}");
        for text in [
            "implement",
            "https://cloud.example.test/runs/run_01k0z6r1w8f4jy2m7q9v3x5abc",
            "plan ready",
        ] {
            assert!(ever_on_screen(&transcript, text), "{text}: {transcript:?}");
        }
        let requests = server.stop();
        assert!(requests.contains(&root));
        assert!(
            requests
                .iter()
                .all(|request| matches!(operation(request).as_str(), "overview" | "assignment"))
        );
    }

    // REQ-RUNNERENROLL-139, REQ-RUNNERENROLL-44: an absent socket is unreachable, exit 4,
    // and the terminal is never taken over.
    #[test]
    fn runner_watch_exits_unreachable_before_taking_the_terminal_when_no_runner_answers() {
        let watch = Watch::new();

        let (status, transcript) = watch.run_to_exit();

        assert_eq!(status, Some(4), "{transcript:?}");
        assert!(!transcript.contains(ENTER_ALTERNATE_SCREEN));
    }

    // REQ-RUNNERENROLL-139, REQ-RUNNERENROLL-135: a serving process of an earlier release
    // refuses the overview, and the command says to restart it.
    #[test]
    fn runner_watch_names_runner_serve_when_an_earlier_release_refuses_the_overview() {
        let watch = Watch::new();
        let server = ControlServer::start(
            &watch.socket_path,
            b"{\"schemaVersion\":1,\"outcome\":\"error\",\"error\":\"invalid_request\"}\n".to_vec(),
        );

        let (status, transcript) = watch.run_to_exit();

        assert_eq!(status, Some(1), "{transcript:?}");
        assert!(!transcript.contains(ENTER_ALTERNATE_SCREEN));
        assert!(transcript.contains("useful runner serve"), "{transcript:?}");
        assert_eq!(server.stop(), [overview_request()]);
    }

    // REQ-RUNNERENROLL-139, REQ-RUNNERENROLL-44: an answer that is not an overview is a
    // protocol failure before the terminal is taken over.
    #[test]
    fn runner_watch_fails_before_taking_the_terminal_on_an_unrecognized_answer() {
        let watch = Watch::new();
        let _server = ControlServer::start(&watch.socket_path, b"not-json\n".to_vec());

        let (status, transcript) = watch.run_to_exit();

        assert_eq!(status, Some(1), "{transcript:?}");
        assert!(!transcript.contains(ENTER_ALTERNATE_SCREEN));
    }

    // REQ-RUNNERENROLL-139, REQ-RUNNERENROLL-140, REQ-RUNNERENROLL-143: the viewer draws the
    // runner's overview, sends nothing but overview reads, and `q` leaves it with exit 0.
    #[test]
    fn runner_watch_draws_the_overview_and_quits_on_q_having_sent_only_reads() {
        let watch = Watch::new();
        let server =
            ControlServer::start(&watch.socket_path, fixture("valid/overview-response.json"));
        let mut process = watch.spawn();
        wait_until_watching(&mut process, &server);

        process.master_writer().write_all(b"q").unwrap();
        process.master_writer().flush().unwrap();
        let status = wait_for_exit(&mut process);
        let transcript = finish(process);

        assert_eq!(status, Some(0), "{transcript:?}");
        assert!(transcript.contains("0.83.0"), "{transcript:?}");
        let requests = server.stop();
        assert!(requests.len() >= 2);
        assert!(
            requests
                .iter()
                .all(|request| *request == overview_request())
        );
    }

    // REQ-RUNNERENROLL-143: SIGTERM ends the viewer with 143 and Ctrl-C with 130, each
    // having restored the terminal.
    #[test]
    fn runner_watch_ends_with_143_on_sigterm_and_130_on_ctrl_c() {
        for (end, expected) in [("SIGTERM", 143), ("Ctrl-C", 130)] {
            let watch = Watch::new();
            let server =
                ControlServer::start(&watch.socket_path, fixture("valid/overview-response.json"));
            let mut process = watch.spawn();
            wait_until_watching(&mut process, &server);

            if end == "SIGTERM" {
                let pid = Pid::from_raw(i32::try_from(process.child_mut().id()).unwrap()).unwrap();
                kill_process(pid, Signal::TERM).unwrap();
            } else {
                process.master_writer().write_all(b"\x03").unwrap();
                process.master_writer().flush().unwrap();
            }
            let status = wait_for_exit(&mut process);
            let transcript = finish(process);

            assert_eq!(status, Some(expected), "{end}: {transcript:?}");
            assert!(
                server
                    .stop()
                    .iter()
                    .all(|request| *request == overview_request())
            );
        }
    }

    // REQ-RUNNERENROLL-141: the viewer outlives the serving process and draws the next one
    // that answers at the same socket, by its boot ID.
    #[test]
    fn runner_watch_stays_up_while_the_runner_restarts_and_draws_the_new_boot() {
        let watch = Watch::new();
        let first =
            ControlServer::start(&watch.socket_path, fixture("valid/overview-response.json"));
        let mut process = watch.spawn();
        wait_until_watching(&mut process, &first);
        first.stop();

        let restarted = String::from_utf8(fixture("valid/overview-response.json"))
            .unwrap()
            .replace("rbt_01k0z6r1w8f4jy2m7q9v3x5abc", SECOND_BOOT_ID);
        let second = ControlServer::start(&watch.socket_path, restarted.into_bytes());
        wait_until_watching(&mut process, &second);

        process.master_writer().write_all(b"q").unwrap();
        process.master_writer().flush().unwrap();
        let status = wait_for_exit(&mut process);
        let transcript = finish(process);

        assert_eq!(status, Some(0), "{transcript:?}");
        let screen = last_screen(&transcript);
        assert!(
            screen.iter().any(|row| row.contains(SECOND_BOOT_ID)),
            "{screen:#?}"
        );
        assert!(
            second
                .stop()
                .iter()
                .all(|request| *request == overview_request())
        );
    }
}
