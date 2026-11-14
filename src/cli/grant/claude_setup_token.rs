//! A fresh Claude Code sign-in, `claude setup-token`, run on a pseudo-terminal `um` owns in a
//! private temporary configuration directory, its prompts passed to the member and the token it
//! prints taken out of what the member sees (REQ-GRANT-71, REQ-GRANT-74, INV-CLI-13,
//! INV-CLI-14).
//!
//! Only the provider's documented names are used: the command `claude setup-token`, the
//! variable `CLAUDE_CONFIG_DIR` and the form of the token `CLAUDE_CODE_OAUTH_TOKEN` takes. The
//! member's own Claude Code configuration is never read: the child's `CLAUDE_CONFIG_DIR`
//! replaces it, and `um` reads and writes no file in the directory it made: the token comes
//! from the sign-in's output alone.

use std::fmt;
use std::io::{self, IsTerminal, Write};
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::process::{ExitStatus, Stdio};
use std::thread::JoinHandle;

use anyhow::{Context, anyhow};
use rustix::event::{PollFd, PollFlags, Timespec};
use rustix::termios::{OptionalActions, Termios, Winsize};
use zeroize::Zeroizing;

use super::super::ApiFailureReport;
use super::material::MAX_CLAUDE_CODE_TOKEN;
use super::program::{self, End, Signals, Stop};
use crate::exit_code::OutcomeClass;

const PROGRAM: &str = "claude";
const ARGUMENT: &str = "setup-token";
// A token of the usual length fits on one line, so the sign-in does not wrap it; one that does
// not is joined across the wrap (INV-CLI-14).
const MIN_COLUMNS: u16 = 256;
const DEFAULT_ROWS: u16 = 24;
const ESC: u8 = 0x1b;
// What every Claude Code token begins with.
const MARKER: &[u8] = b"sk-ant-";

/// Why the sign-in produced no token to import (`provider_login_failed`).
#[derive(Debug)]
pub(in crate::cli) enum Failure {
    /// Standard input is not a terminal, so the member could not answer the sign-in.
    NoTerminal,
    NotFound,
    Exited(ExitStatus),
    /// A signal `um` received was passed to the sign-in.
    Interrupted(Stop),
    NoToken,
    Unusable(anyhow::Error),
}

impl Failure {
    const fn remedy(&self) -> &'static str {
        match self {
            Self::NoTerminal => {
                "Run claude setup-token where you can answer it and import the token it prints:\n  um grant create claude_code --token-file -"
            }
            Self::NotFound => {
                "Install Claude Code so that claude is on your PATH, or import the token claude setup-token printed with --token-file PATH."
            }
            Self::NoToken => {
                "Run claude setup-token yourself and import the token it prints with --token-file PATH."
            }
            _ => {
                "Run the command again and complete the Claude Code sign-in, or import the token claude setup-token printed with --token-file PATH."
            }
        }
    }

    /// The signal that ended the sign-in, when one asked for it to stop: one `um` passed to
    /// it, or SIGINT, SIGTERM or SIGHUP ending it.
    pub(super) fn stop(&self) -> Option<Stop> {
        match self {
            Self::Interrupted(stop) => Some(*stop),
            Self::Exited(status) => Stop::ended(*status),
            _ => None,
        }
    }

    /// The failure as the member reads it: nothing was sent to the deployment.
    pub(in crate::cli) fn report(&self) -> ApiFailureReport {
        ApiFailureReport::new(
            "provider_login_failed",
            format!("error: sign in to Claude Code: {self}"),
            self.remedy(),
            OutcomeClass::GeneralFailure,
        )
    }
}

impl fmt::Display for Failure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoTerminal => formatter.write_str("standard input is not a terminal"),
            Self::NotFound => formatter.write_str("claude not found on PATH"),
            Self::Exited(status) => write!(formatter, "run claude setup-token: {status}"),
            Self::Interrupted(_) => formatter.write_str("interrupted"),
            Self::NoToken => formatter.write_str("claude setup-token printed no token"),
            // These errors name an operation, never the output.
            Self::Unusable(error) => write!(formatter, "{error:#}"),
        }
    }
}

/// The failure a sign-in ends in before anything is run, read or sent when the member could
/// not answer it: standard input is not a terminal (REQ-GRANT-71). `um grant create
/// claude_code` asks this before its first request, and [`sign_in`] before anything else.
pub(in crate::cli) fn unanswerable() -> Option<Failure> {
    (!io::stdin().is_terminal()).then_some(Failure::NoTerminal)
}

/// Runs `claude setup-token` on a new pseudo-terminal with `CLAUDE_CONFIG_DIR` a new `0700`
/// directory and returns the token it printed. The directory is removed whatever the outcome;
/// failing to remove it is reported, not fatal. Where standard input is not a terminal it runs
/// nothing and fails as [`unanswerable`] says. The outer error is a local fault; the inner one
/// is the sign-in's.
pub(in crate::cli) fn sign_in() -> anyhow::Result<Result<Zeroizing<Vec<u8>>, Failure>> {
    if let Some(failure) = unanswerable() {
        return Ok(Err(failure));
    }
    let Some(claude) = program::find(std::env::var_os("PATH").as_deref(), PROGRAM) else {
        return Ok(Err(Failure::NotFound));
    };
    let directory = tempfile::Builder::new()
        .prefix("um-claude-setup-token-")
        .permissions(std::fs::Permissions::from_mode(0o700))
        .tempdir()
        .context("create temporary Claude Code configuration directory")?;
    let path = directory.path().to_owned();
    let result = sign_in_at(&claude, &path);
    if let Err(error) = directory.close() {
        // `um` wrote nothing there, but the sign-in may have.
        let _ = writeln!(
            io::stderr().lock(),
            "warning: remove temporary Claude Code configuration directory {}: {error}\n\nDelete that directory; it may hold Claude Code sign-in state.",
            path.display()
        );
    }
    result
}

/// Runs the sign-in on its own terminal: the member's input is relayed to it, and its output
/// to standard error through [`Withhold`], so that a `--json` result stays one document and the
/// token is never shown. A signal that would stop `um` is passed to the child instead, so the
/// directory is still removed. However it ends, the child has been waited for and the relay
/// reads no more of the member's input when this returns.
fn sign_in_at(
    claude: &Path,
    directory: &Path,
) -> anyhow::Result<Result<Zeroizing<Vec<u8>>, Failure>> {
    let (primary, secondary, columns) =
        open_terminal().context("open a terminal for claude setup-token")?;
    let raw = RawInput::enter();
    let runtime = super::super::blocking_signal_runtime("Claude Code sign-in")?;
    let (ended, relay) = runtime.block_on(async {
        let signals =
            Signals::observe().context("observe signals during the Claude Code sign-in")?;
        let mut child = match spawn(claude, directory, secondary) {
            Ok(child) => child,
            // A `claude` that cannot be started is the sign-in failing, not a fault of `um`.
            Err(error) => {
                return Ok((
                    Err(Failure::Unusable(
                        anyhow::Error::new(error).context("start claude setup-token"),
                    )),
                    None,
                ));
            }
        };
        let relay = or_stop(&mut child, || Relay::start(&primary, columns))
            .await
            .context("relay the claude setup-token terminal")?;
        let end = match signals.wait(&mut child).await {
            Ok(end) => end,
            Err(error) => {
                stop(&mut child).await;
                return Err(anyhow::Error::new(error).context("wait for claude setup-token"));
            }
        };
        let ended = match end {
            End::Success => Ok(()),
            End::Exited(status) => Err(Failure::Exited(status)),
            End::Interrupted(stop) => Err(Failure::Interrupted(stop)),
        };
        anyhow::Ok((ended, Some(relay)))
    })?;
    // A token printed before a failure is dropped.
    let token = match relay {
        Some(relay) => relay.finish()?,
        None => None,
    };
    drop(raw);
    Ok(ended.and_then(|()| token.ok_or(Failure::NoToken)))
}

/// Runs `start` for the started `child`; when it fails, the child is stopped first.
async fn or_stop<T>(
    child: &mut tokio::process::Child,
    start: impl FnOnce() -> io::Result<T>,
) -> io::Result<T> {
    let started = start();
    if started.is_err() {
        stop(child).await;
    }
    started
}

/// Kills a sign-in `um` no longer follows and waits for it, so that it has ended before its
/// directory is removed.
async fn stop(child: &mut tokio::process::Child) {
    // It may already have ended; either way it has been waited for.
    let _ = child.kill().await;
}

/// After the sign-in has ended, how long its terminal's output may stay idle, and how much more
/// of it is relayed, before the relay stops: a descendant still holding the terminal does not
/// keep `um` waiting.
const DRAIN_IDLE: Timespec = Timespec {
    tv_sec: 0,
    tv_nsec: 200_000_000,
};
const DRAIN_BYTES: usize = 64 * 1024;

/// The relay of the sign-in's terminal: the member's input to it on one thread, and its output,
/// `columns` wide, to standard error on another that returns the token in it. Both threads wait
/// on the terminal together with `ended`, whose closing tells them the sign-in has ended.
struct Relay {
    ended: UnixStream,
    input: JoinHandle<()>,
    output: JoinHandle<Option<Zeroizing<Vec<u8>>>>,
}

impl Relay {
    fn start(primary: &OwnedFd, columns: u16) -> io::Result<Self> {
        // Each thread waits for readiness, so neither is ever blocked in a read or a write it
        // cannot leave once the sign-in has ended.
        let flags = rustix::fs::fcntl_getfl(primary)?;
        rustix::fs::fcntl_setfl(primary, flags | rustix::fs::OFlags::NONBLOCK)?;
        let (ended, wake) = UnixStream::pair()?;
        let wake = OwnedFd::from(wake);
        let input = {
            let (primary, wake) = (primary.try_clone()?, wake.try_clone()?);
            std::thread::Builder::new().spawn(move || relay_input(&primary, &wake))?
        };
        let primary = primary.try_clone()?;
        let output =
            std::thread::Builder::new().spawn(move || relay_output(&primary, &wake, columns))?;
        Ok(Self {
            ended,
            input,
            output,
        })
    }

    /// Once the sign-in has been waited for, stops the relay: the member's input is read no
    /// more, and the output is relayed until it ends, stays idle for [`DRAIN_IDLE`] or has
    /// given [`DRAIN_BYTES`] more. Returns the token in it.
    fn finish(self) -> anyhow::Result<Option<Zeroizing<Vec<u8>>>> {
        drop(self.ended);
        let failed = || anyhow!("relay the claude setup-token terminal");
        self.input.join().map_err(|_| failed())?;
        self.output.join().map_err(|_| failed())
    }
}

/// Waits until `fd` is ready for `events`, `wake` is readable or `timeout` passes, and says
/// which of the two are ready; an interrupted wait reports neither.
fn ready(
    fd: BorrowedFd<'_>,
    events: PollFlags,
    wake: Option<BorrowedFd<'_>>,
    timeout: Option<&Timespec>,
) -> io::Result<(bool, bool)> {
    let mut fds = vec![PollFd::from_borrowed_fd(fd, events)];
    fds.extend(wake.map(|wake| PollFd::from_borrowed_fd(wake, PollFlags::IN)));
    match rustix::event::poll(&mut fds, timeout) {
        Ok(_) => {}
        Err(rustix::io::Errno::INTR) => return Ok((false, false)),
        Err(error) => return Err(error.into()),
    }
    let is_ready = |fd: &PollFd<'_>| !fd.revents().is_empty();
    Ok((is_ready(&fds[0]), fds.get(1).is_some_and(is_ready)))
}

/// Copies the member's input to the terminal until the input ends or `wake` says the sign-in
/// has: input that arrives after that is left to whatever reads it next.
fn relay_input(primary: &OwnedFd, wake: &OwnedFd) {
    let stdin = io::stdin();
    let mut buffer = [0_u8; 1024];
    loop {
        let Ok((readable, woken)) = ready(stdin.as_fd(), PollFlags::IN, Some(wake.as_fd()), None)
        else {
            return;
        };
        if woken {
            return;
        }
        if !readable {
            continue;
        }
        let count = match rustix::io::read(&stdin, &mut buffer) {
            Ok(0) => return,
            Ok(count) => count,
            Err(rustix::io::Errno::INTR | rustix::io::Errno::AGAIN) => continue,
            Err(_) => return,
        };
        let mut pending = &buffer[..count];
        while !pending.is_empty() {
            match ready(primary.as_fd(), PollFlags::OUT, Some(wake.as_fd()), None) {
                Ok((_, true)) | Err(_) => return,
                Ok((false, false)) => continue,
                Ok((true, false)) => {}
            }
            match rustix::io::write(primary, pending) {
                Ok(written) => pending = &pending[written..],
                Err(rustix::io::Errno::INTR | rustix::io::Errno::AGAIN) => {}
                Err(_) => return,
            }
        }
    }
}

/// Relays the terminal's output to standard error, withholding the token, until it ends or,
/// once `wake` says the sign-in has ended, the drain [`Relay::finish`] describes is over.
fn relay_output(primary: &OwnedFd, wake: &OwnedFd, columns: u16) -> Option<Zeroizing<Vec<u8>>> {
    let mut withhold = Withhold::new(columns);
    let mut buffer = Zeroizing::new([0_u8; 4096]);
    let mut relay = Vec::new();
    // How much has been relayed since the sign-in ended, once it has.
    let mut drained: Option<usize> = None;
    loop {
        let readable = match drained {
            None => match ready(primary.as_fd(), PollFlags::IN, Some(wake.as_fd()), None) {
                Ok((readable, woken)) => {
                    if woken {
                        drained = Some(0);
                    }
                    readable
                }
                Err(_) => break,
            },
            Some(count) if count >= DRAIN_BYTES => break,
            Some(_) => match ready(primary.as_fd(), PollFlags::IN, None, Some(&DRAIN_IDLE)) {
                Ok((true, _)) => true,
                // Idle for the whole wait, or the wait failed.
                Ok((false, _)) | Err(_) => break,
            },
        };
        if !readable {
            continue;
        }
        // The end of the output is EIO on Linux and the end of file elsewhere.
        let count = match rustix::io::read(primary, &mut buffer[..]) {
            Ok(0) => break,
            Ok(count) => count,
            Err(rustix::io::Errno::INTR | rustix::io::Errno::AGAIN) => continue,
            Err(_) => break,
        };
        if let Some(drained) = &mut drained {
            *drained = drained.saturating_add(count);
        }
        withhold.push(&buffer[..count], &mut relay);
        show(&mut relay);
    }
    let token = withhold.finish(&mut relay);
    show(&mut relay);
    token
}

/// Writes what the member may see of the sign-in's output to standard error. A member's
/// terminal that has gone away does not stop the sign-in, whose own status decides.
fn show(relay: &mut Vec<u8>) {
    let mut stderr = io::stderr().lock();
    let _ = stderr.write_all(relay).and_then(|()| stderr.flush());
    relay.clear();
}

/// Starts `claude setup-token` with the secondary side of the terminal as its three standard
/// streams, through the helper that makes that side its controlling terminal.
fn spawn(claude: &Path, directory: &Path, secondary: OwnedFd) -> io::Result<tokio::process::Child> {
    let mut command = super::controlling_terminal::command(claude)?;
    command
        .arg(ARGUMENT)
        .env("CLAUDE_CONFIG_DIR", directory)
        .stdin(Stdio::from(secondary.try_clone()?))
        .stdout(Stdio::from(secondary.try_clone()?))
        .stderr(Stdio::from(secondary));
    // The command holds this process's copies of the secondary side; dropping it leaves the
    // child the only holder, so the output relay ends when the sign-in does.
    command.spawn()
}

/// Opens a pseudo-terminal pair sized to the member's terminal rows and at least
/// [`MIN_COLUMNS`] columns, and its width. Both ends are closed on exec, so only the child the
/// secondary side is given to holds it.
fn open_terminal() -> io::Result<(OwnedFd, OwnedFd, u16)> {
    use rustix::fs::{Mode, OFlags, open};
    use rustix::io::{FdFlags, fcntl_setfd};
    use rustix::pty::{OpenptFlags, grantpt, openpt, ptsname, unlockpt};

    let primary = openpt(OpenptFlags::RDWR | OpenptFlags::NOCTTY)?;
    fcntl_setfd(&primary, FdFlags::CLOEXEC)?;
    grantpt(&primary)?;
    unlockpt(&primary)?;
    let name = ptsname(&primary, Vec::new())?;
    let secondary = open(
        name,
        OFlags::RDWR | OFlags::NOCTTY | OFlags::CLOEXEC,
        Mode::empty(),
    )?;
    let member = rustix::termios::tcgetwinsize(io::stderr())
        .or_else(|_| rustix::termios::tcgetwinsize(io::stdin()))
        .ok();
    let columns = member.map_or(0, |size| size.ws_col).max(MIN_COLUMNS);
    rustix::termios::tcsetwinsize(
        &secondary,
        Winsize {
            ws_row: member
                .map(|size| size.ws_row)
                .filter(|rows| *rows > 0)
                .unwrap_or(DEFAULT_ROWS),
            ws_col: columns,
            ws_xpixel: 0,
            ws_ypixel: 0,
        },
    )?;
    Ok((primary, secondary, columns))
}

/// The member's terminal in raw mode while the sign-in runs, so that each key reaches the
/// sign-in's own terminal as typed; restored when dropped. Nothing is changed when standard
/// input is not a terminal.
struct RawInput {
    original: Termios,
}

impl RawInput {
    fn enter() -> Option<Self> {
        let stdin = io::stdin();
        if !stdin.is_terminal() {
            return None;
        }
        let original = rustix::termios::tcgetattr(&stdin).ok()?;
        let mut raw = original.clone();
        raw.make_raw();
        rustix::termios::tcsetattr(&stdin, OptionalActions::Now, &raw).ok()?;
        Some(Self { original })
    }
}

impl Drop for RawInput {
    fn drop(&mut self) {
        let _ = rustix::termios::tcsetattr(io::stdin(), OptionalActions::Now, &self.original);
    }
}

/// Relays a sign-in's output byte for byte except a Claude Code token, which it takes instead
/// (INV-CLI-14). Terminal control sequences are relayed unchanged and skipped while matching
/// [`MARKER`]; only a suffix that could begin it is held back, so a prompt without a line ending
/// is relayed at once. Once the marker is seen, the run that follows is withheld up to a space, a
/// tab, a line break that ends a line narrower than the terminal, or the end of the output. The
/// run's bytes in `A-Z`, `a-z`, `0-9`, `_` and `-` are joined into the token across the control
/// sequences and the line breaks of full lines in it, which are relayed when it ends, since the
/// provider wraps a token longer than its terminal; any other byte in it is dropped. The output
/// yields a token only when every run is that same token, unbroken by another byte and of a
/// length Cloud accepts, so a change in the provider's output fails closed.
struct Withhold {
    escape: Escape,
    // The terminal's width, and how much of the current line the printing bytes have filled;
    // cursor movement is not followed.
    columns: usize,
    column: usize,
    // Whether the last byte was a CR ending a full line, so that an LF after it does too.
    wrapping_cr: bool,
    // How many bytes of the marker the held bytes match.
    matched: usize,
    // The bytes held while they could begin the marker, control sequences included, in order.
    held: Vec<u8>,
    // The control sequences and wrapping line breaks among them, relayed alone once the marker
    // is complete.
    held_controls: Vec<u8>,
    run: Option<Run>,
    taken: Taken,
}

/// The output after the marker, up to the byte that ends it.
struct Run {
    // The token, allocated once at its largest so that it is never copied.
    token: Zeroizing<Vec<u8>>,
    // Whether a byte outside the token's alphabet broke it.
    broken: bool,
    // The control sequences and wrapping line breaks in it, relayed when it ends.
    controls: Vec<u8>,
}

impl Run {
    fn new() -> Self {
        let mut token = Zeroizing::new(Vec::with_capacity(MAX_CLAUDE_CODE_TOKEN + 1));
        token.extend_from_slice(MARKER);
        Self {
            token,
            broken: false,
            controls: Vec::new(),
        }
    }

    /// A token Cloud accepts (REQ-GRANT-12): unbroken, more than the marker and at most
    /// [`MAX_CLAUDE_CODE_TOKEN`] bytes.
    fn well_formed(&self) -> bool {
        !self.broken && self.token.len() > MARKER.len() && self.token.len() <= MAX_CLAUDE_CODE_TOKEN
    }
}

/// What the runs so far have yielded.
enum Taken {
    Nothing,
    Token(Zeroizing<Vec<u8>>),
    // A run that is no token, or a second distinct token.
    Refused,
}

/// Where a byte falls in an ECMA-48 control sequence.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum Escape {
    #[default]
    None,
    // After ESC.
    Start,
    // After ESC and an intermediate byte.
    Intermediate,
    // In a control sequence, after ESC [.
    Csi,
    // In a string (OSC, DCS, SOS, PM or APC), ended by BEL or ESC \.
    String,
    // After ESC in a string.
    StringEnd,
}

// Cancel and substitute, which abort any control sequence or string.
const CAN: u8 = 0x18;
const SUB: u8 = 0x1a;

impl Escape {
    const fn next(self, byte: u8) -> Self {
        match (self, byte) {
            (_, CAN | SUB)
            | (Self::Csi, 0x40..=0x7e)
            | (Self::StringEnd, b'\\')
            | (Self::String, 0x07) => Self::None,
            (Self::String, ESC) => Self::StringEnd,
            // An ESC in a string that does not end it ends the string and begins a sequence.
            (Self::StringEnd, _) => Self::Start.next(byte),
            (Self::String, _) => Self::String,
            (_, ESC) => Self::Start,
            (Self::Start, b'[') => Self::Csi,
            (Self::Start, b']' | b'P' | b'X' | b'^' | b'_') => Self::String,
            (Self::Start | Self::Intermediate, 0x20..=0x2f) => Self::Intermediate,
            (Self::Csi, _) => Self::Csi,
            (Self::None | Self::Start | Self::Intermediate, _) => Self::None,
        }
    }
}

const fn token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-'
}

impl Withhold {
    fn new(columns: u16) -> Self {
        Self {
            escape: Escape::None,
            columns: usize::from(columns),
            column: 0,
            wrapping_cr: false,
            matched: 0,
            held: Vec::new(),
            held_controls: Vec::new(),
            run: None,
            taken: Taken::Nothing,
        }
    }

    /// Appends to `relay` what of `input` the member may see.
    fn push(&mut self, input: &[u8], relay: &mut Vec<u8>) {
        for &byte in input {
            self.byte(byte, relay);
        }
    }

    fn byte(&mut self, byte: u8, relay: &mut Vec<u8>) {
        if self.escape != Escape::None || byte == ESC {
            self.wrapping_cr = false;
            self.escape = self.escape.next(byte);
            if let Some(run) = &mut self.run {
                run.controls.push(byte);
            } else if self.matched > 0 {
                self.held.push(byte);
                self.held_controls.push(byte);
            } else {
                relay.push(byte);
            }
            return;
        }
        let wraps = self.advance(byte);
        if let Some(run) = &mut self.run {
            match (byte, wraps) {
                (_, Some(true)) => {
                    run.controls.push(byte);
                    return;
                }
                (b' ' | b'\t' | 0x0b | 0x0c, _) | (_, Some(false)) => self.end_run(relay),
                _ => {
                    if !token_byte(byte) {
                        run.broken = true;
                    } else if run.token.len() <= MAX_CLAUDE_CODE_TOKEN {
                        // A token past the bound is refused when it ends; the rest is not kept.
                        run.token.push(byte);
                    }
                    return;
                }
            }
        }
        if self.matched > 0 && wraps == Some(true) {
            // A full line may break the marker as it breaks a token.
            self.held.push(byte);
            self.held_controls.push(byte);
            return;
        }
        if byte != MARKER[self.matched] && self.matched > 0 {
            // The marker has no prefix that is also its suffix, so only this byte can begin
            // it again.
            relay.append(&mut self.held);
            self.held_controls.clear();
            self.matched = 0;
        }
        if byte != MARKER[self.matched] {
            relay.push(byte);
            return;
        }
        self.held.push(byte);
        self.matched += 1;
        if self.matched == MARKER.len() {
            relay.append(&mut self.held_controls);
            self.held.clear();
            self.matched = 0;
            self.run = Some(Run::new());
        }
    }

    /// Follows the column over a byte outside a control sequence and, for a line break, says
    /// whether it ends a full line: a wrap, the second byte of a wrapping CR LF included.
    fn advance(&mut self, byte: u8) -> Option<bool> {
        let wrapping_cr = std::mem::take(&mut self.wrapping_cr);
        match byte {
            b'\r' | b'\n' => {
                let wraps = self.column >= self.columns || (byte == b'\n' && wrapping_cr);
                self.wrapping_cr = byte == b'\r' && wraps;
                self.column = 0;
                Some(wraps)
            }
            b'\t' => {
                self.column = (self.column / 8).saturating_add(1).saturating_mul(8);
                None
            }
            // A UTF-8 continuation byte, DEL and the other C0 controls take no column.
            0x20..=0x7e | 0xc0..=0xff => {
                self.column = self.column.saturating_add(1);
                None
            }
            _ => None,
        }
    }

    /// Ends the run being withheld: relays its control sequences and keeps its token when it is
    /// the first, or the same as the first.
    fn end_run(&mut self, relay: &mut Vec<u8>) {
        let Some(mut run) = self.run.take() else {
            return;
        };
        relay.append(&mut run.controls);
        let well_formed = run.well_formed();
        self.taken = match std::mem::replace(&mut self.taken, Taken::Refused) {
            Taken::Nothing if well_formed => Taken::Token(run.token),
            Taken::Token(token) if well_formed && *token == *run.token => Taken::Token(token),
            Taken::Nothing | Taken::Token(_) | Taken::Refused => Taken::Refused,
        };
    }

    /// Ends the output: relays what was held and returns the one token in it.
    fn finish(mut self, relay: &mut Vec<u8>) -> Option<Zeroizing<Vec<u8>>> {
        relay.append(&mut self.held);
        self.end_run(relay);
        match self.taken {
            Taken::Token(token) => Some(token),
            Taken::Nothing | Taken::Refused => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{MIN_COLUMNS, Withhold};

    const TOKEN: &str = "sk-ant-oat01-Ab_9-unit-test-token";

    fn relayed(chunks: &[&[u8]]) -> (Vec<u8>, Option<Vec<u8>>) {
        relayed_at(MIN_COLUMNS, chunks)
    }

    // What a terminal `columns` wide is shown of the output, and the token taken from it.
    fn relayed_at(columns: u16, chunks: &[&[u8]]) -> (Vec<u8>, Option<Vec<u8>>) {
        let mut withhold = Withhold::new(columns);
        let mut relay = Vec::new();
        for chunk in chunks {
            withhold.push(chunk, &mut relay);
        }
        let token = withhold.finish(&mut relay).map(|token| token.to_vec());
        (relay, token)
    }

    // INV-CLI-14
    #[test]
    fn a_token_interrupted_by_a_control_sequence_or_a_wrapped_line_is_withheld_and_joined() {
        // The first line of each fills a terminal of this width, so its line break is a wrap.
        let columns = u16::try_from("sk-ant-oat01-AAAA".len()).unwrap_or(u16::MAX);
        let cases = [
            ("sk-ant-oat01-AAAA\x1b[5CBBBB next\r\n", "\x1b[5C next\r\n"),
            ("sk-ant-oat01-AAAA\x1b[0mBBBB next\r\n", "\x1b[0m next\r\n"),
            ("sk-ant-oat01-AAAA\r\nBBBB\r\nnext\r\n", "\r\n\r\nnext\r\n"),
            (
                "sk-ant-oat01-AAAA\x1b[39m\r\n\x1b[32mBBBB \x1b[0mnext\r\n",
                "\x1b[39m\r\n\x1b[32m \x1b[0mnext\r\n",
            ),
        ];
        for (output, expected) in cases {
            let bytes = output.as_bytes();
            for split in 0..=bytes.len() {
                let (relay, token) = relayed_at(columns, &[&bytes[..split], &bytes[split..]]);
                assert_eq!(
                    String::from_utf8_lossy(&relay),
                    expected,
                    "{output:?} split at {split}"
                );
                assert_eq!(
                    token.as_deref(),
                    Some(&b"sk-ant-oat01-AAAABBBB"[..]),
                    "{output:?} split at {split}"
                );
            }
        }
        // A line break before the terminal's width ends the token, and a blank line ends a
        // wrapped one.
        let (relay, token) = relayed(&[b"sk-ant-oat01-AAAA\r\nnext\r\n"]);
        assert_eq!(token.as_deref(), Some(&b"sk-ant-oat01-AAAA"[..]));
        assert_eq!(relay, b"\r\nnext\r\n");
        let (relay, token) = relayed_at(columns, &[b"sk-ant-oat01-AAAA\r\n\r\nnext\r\n"]);
        assert_eq!(token.as_deref(), Some(&b"sk-ant-oat01-AAAA"[..]));
        assert_eq!(relay, b"\r\n\r\nnext\r\n");
    }

    // INV-CLI-14
    #[test]
    fn a_run_interrupted_by_another_character_is_withheld_and_yields_none() {
        for (output, expected) in [
            (
                "Your token: sk-ant-oat01-AAAA.BBBB\r\nmore\r\n",
                "Your token: \r\nmore\r\n",
            ),
            ("sk-ant-oat01-AAAA:BBBB more\r\n", " more\r\n"),
            ("sk-ant-oat01-AAAA\x07BBBB\r\n", "\r\n"),
            ("sk-ant-oat01-AAAA\u{e9}BBBB\r\n", "\r\n"),
            (&format!("{TOKEN}\r\nsk-ant-oat01-AA/BB\r\n"), "\r\n\r\n"),
        ] {
            let (relay, token) = relayed(&[output.as_bytes()]);
            assert_eq!(token, None, "{output:?}");
            assert_eq!(String::from_utf8_lossy(&relay), expected, "{output:?}");
        }
    }

    // INV-CLI-14
    #[test]
    fn an_aborted_control_string_does_not_stop_the_token_being_withheld() {
        for (output, expected) in [
            // ESC and a byte other than `\` ends a string and begins a new sequence.
            (
                format!("\x1b]0;title\x1b[0m Your token: {TOKEN}\r\n"),
                "\x1b]0;title\x1b[0m Your token: \r\n",
            ),
            // CAN and SUB abort it.
            (
                format!("\x1b]0;title\x18{TOKEN}\r\n"),
                "\x1b]0;title\x18\r\n",
            ),
            (format!("\x1bPq\x1a{TOKEN}\r\n"), "\x1bPq\x1a\r\n"),
            // As they do a control sequence.
            (format!("\x1b[12\x1b[1m{TOKEN}\r\n"), "\x1b[12\x1b[1m\r\n"),
        ] {
            let (relay, token) = relayed(&[output.as_bytes()]);
            assert_eq!(String::from_utf8_lossy(&relay), expected, "{output:?}");
            assert_eq!(token.as_deref(), Some(TOKEN.as_bytes()), "{output:?}");
        }
    }

    // INV-CLI-14
    #[test]
    fn the_token_is_taken_out_of_the_relay_and_everything_else_is_relayed_unchanged() {
        let colour = format!("Your token:\r\n\x1b[32m{TOKEN}\x1b[39m\r\nStore it safely.\r\n");
        let cases = [
            (format!("Your token: {TOKEN}"), "Your token: "),
            (format!("{TOKEN}\r\n"), "\r\n"),
            (
                colour.clone(),
                "Your token:\r\n\x1b[32m\x1b[39m\r\nStore it safely.\r\n",
            ),
            (
                format!(
                    "sk-ask \x1b]8;;https://example.test\x07link\x1b]8;;\x07 {TOKEN}\r\nmore sk-an"
                ),
                "sk-ask \x1b]8;;https://example.test\x07link\x1b]8;;\x07 \r\nmore sk-an",
            ),
            (format!("s\x1b[1mk-ant-{}\r\n", &TOKEN[7..]), "\x1b[1m\r\n"),
        ];
        for (output, expected) in cases {
            let bytes = output.as_bytes();
            // Delivered whole, and split across two reads at every offset.
            for split in 0..=bytes.len() {
                let (relay, token) = relayed(&[&bytes[..split], &bytes[split..]]);
                assert_eq!(
                    String::from_utf8_lossy(&relay),
                    expected,
                    "{output:?} split at {split}"
                );
                assert_eq!(token.as_deref(), Some(TOKEN.as_bytes()), "{output:?}");
            }
            let bytewise: Vec<&[u8]> = bytes.chunks(1).collect();
            assert_eq!(relayed(&bytewise).0, expected.as_bytes());
        }
    }

    // INV-CLI-14
    #[test]
    fn a_second_distinct_token_or_output_without_one_yields_none() {
        let second = "sk-ant-oat01-the-second";
        let (relay, token) = relayed(&[format!("{TOKEN}\r\n{second}\r\n").as_bytes()]);
        assert_eq!(relay, b"\r\n\r\n");
        assert_eq!(token, None);
        // The same token shown again, as a redrawn screen shows it, is still the one token.
        let (relay, token) = relayed(&[format!("{TOKEN}\r\n\x1b[2K{TOKEN}\r\n").as_bytes()]);
        assert_eq!(relay, b"\r\n\x1b[2K\r\n");
        assert_eq!(token.as_deref(), Some(TOKEN.as_bytes()));

        for output in ["sk-ant-\r\n", "no token here\r\n", "sk-ant\r\n"] {
            let (relay, token) = relayed(&[output.as_bytes()]);
            assert_eq!(token, None, "{output:?}");
            assert_eq!(
                relay,
                output.replace("sk-ant-", "").as_bytes(),
                "{output:?}"
            );
        }
        let long = format!("sk-ant-{}\r\n", "a".repeat(super::MAX_CLAUDE_CODE_TOKEN));
        let (relay, token) = relayed(&[long.as_bytes()]);
        assert_eq!(token, None);
        assert_eq!(relay, b"\r\n");
    }

    // REQ-GRANT-71
    #[test]
    fn a_relay_that_cannot_start_kills_and_waits_for_the_started_sign_in() {
        use std::os::unix::process::ExitStatusExt;

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let mut child = tokio::process::Command::new("sleep")
                .arg("60")
                .spawn()
                .unwrap();
            let pid = rustix::process::Pid::from_raw(i32::try_from(child.id().unwrap()).unwrap())
                .unwrap();

            let started =
                super::or_stop(&mut child, || Err::<(), _>(std::io::Error::other("relay"))).await;

            assert!(started.is_err());
            // Waited for as well as killed: not even a zombie of it is left.
            assert_eq!(
                rustix::process::test_kill_process(pid),
                Err(rustix::io::Errno::SRCH)
            );
            let status = child.try_wait().unwrap().unwrap();
            assert_eq!(status.signal(), Some(libc::SIGKILL));
        });
    }

    // INV-CLI-14
    #[test]
    fn a_prompt_without_a_line_ending_is_relayed_without_waiting_for_one() {
        let prompt = b"Paste code here if prompted > ";
        let mut withhold = Withhold::new(MIN_COLUMNS);
        let mut relay = Vec::new();
        withhold.push(b"\x1b[1m", &mut relay);
        withhold.push(prompt, &mut relay);
        let mut expected = b"\x1b[1m".to_vec();
        expected.extend_from_slice(prompt);
        assert_eq!(relay, expected);
    }
}
