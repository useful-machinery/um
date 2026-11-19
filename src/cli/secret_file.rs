//! Reading a secret the member gives: a private file, `-` for standard input, or a line typed
//! at a prompt on the terminal that does not echo it. `useful runner serve --join-key` and
//! `useful integration connect` for a token service read a typed secret through the one prompt
//! here.

use std::fs::{self, OpenOptions};
use std::io::{self, Read, Write};
use std::os::fd::{AsFd, BorrowedFd};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::Path;
use std::process;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;

use anyhow::{Context, bail};
use nix::sys::signal::{SigSet, SigmaskHow, Signal};
use rustix::termios::{LocalModes, OptionalActions, QueueSelector, Termios};
use zeroize::Zeroizing;

use crate::exit_code::ExitCode;

/// Whether a file's mode is checked: a file the member names must be `0600`, but one a
/// provider's sign-in wrote into a `0700` directory the command made keeps the mode the
/// provider gave it.
#[derive(Clone, Copy)]
pub(super) enum FileMode {
    Private,
    Unchecked,
}

/// A kind of secret read from a file or standard input, and the bound on what is read.
pub(super) struct SecretInput {
    /// What messages call the secret, such as `GitLab token`.
    pub(super) name: &'static str,
    /// The most bytes accepted, including any line ending the caller trims.
    pub(super) max_bytes: u64,
    /// The bound as messages state it, such as `16 KiB`.
    pub(super) max_text: &'static str,
}

impl SecretInput {
    /// Reads the secret into a zeroizing buffer. A file must be a regular, non-symlink file
    /// owned by the effective user with mode `0600`, checked before and after opening it
    /// without following a link; at most one byte past the bound is read.
    pub(super) fn read(&self, path: &Path) -> anyhow::Result<Zeroizing<Vec<u8>>> {
        self.read_with(path, FileMode::Private)
    }

    /// Reads the secret as [`Self::read`] does, checking a file's mode only when `mode` is
    /// [`FileMode::Private`].
    pub(super) fn read_with(
        &self,
        path: &Path,
        mode: FileMode,
    ) -> anyhow::Result<Zeroizing<Vec<u8>>> {
        let name = self.name;
        let mut bytes = Zeroizing::new(Vec::new());
        if path == Path::new("-") {
            io::stdin()
                .lock()
                .take(self.max_bytes + 1)
                .read_to_end(&mut bytes)
                .with_context(|| format!("read {name} from standard input"))?;
        } else {
            let uid = rustix::process::geteuid().as_raw();
            let validate = |metadata: &fs::Metadata| match mode {
                FileMode::Private => self.validate(metadata, uid),
                FileMode::Unchecked => self.validate_file(metadata, uid, false),
            };
            validate(&fs::symlink_metadata(path).with_context(|| format!("inspect {name} file"))?)?;
            let file = OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(path)
                .with_context(|| format!("open {name} file"))?;
            validate(
                &file
                    .metadata()
                    .with_context(|| format!("inspect opened {name} file"))?,
            )?;
            file.take(self.max_bytes + 1)
                .read_to_end(&mut bytes)
                .with_context(|| format!("read {name} file"))?;
        }
        if bytes.len() as u64 > self.max_bytes {
            bail!("{name} input exceeds {}", self.max_text);
        }
        Ok(bytes)
    }

    /// Accepts only a regular file owned by `uid` with mode `0600` within the bound.
    pub(super) fn validate(&self, metadata: &fs::Metadata, uid: u32) -> anyhow::Result<()> {
        self.validate_file(metadata, uid, true)
    }

    /// Accepts only a regular file owned by `uid` within the bound, with mode `0600` when
    /// `private`.
    fn validate_file(
        &self,
        metadata: &fs::Metadata,
        uid: u32,
        private: bool,
    ) -> anyhow::Result<()> {
        if !metadata.file_type().is_file()
            || metadata.uid() != uid
            || (private && metadata.mode() & 0o7777 != 0o600)
        {
            bail!(
                "{} file must be a regular non-symlink file owned by you{}",
                self.name,
                if private { " with mode 0600" } else { "" }
            );
        }
        if metadata.len() > self.max_bytes {
            bail!("{} file exceeds {}", self.name, self.max_text);
        }
        Ok(())
    }
}

/// Trims one trailing LF or CRLF.
pub(super) fn trim_line_ending(bytes: &mut Vec<u8>) {
    if bytes.last() == Some(&b'\n') {
        bytes.pop();
        if bytes.last() == Some(&b'\r') {
            bytes.pop();
        }
    }
}

/// The longest line the prompt reads: a join key artifact's bound, and an integration token's.
const PROMPTED_LINE_LIMIT: usize = 4096;

/// Writes `prompt` on standard error and reads one line of at most [`PROMPTED_LINE_LIMIT`] bytes
/// from the terminal on standard input with echo off, line ending included. Echo is turned off
/// before the prompt is written, so nothing typed once it shows is echoed, and the terminal is
/// restored however the read ends (see [`QuietTerminal`]). The caller checks that standard input
/// is a terminal.
pub(super) fn prompt_without_echo(prompt: &str) -> anyhow::Result<Zeroizing<Vec<u8>>> {
    let stdin = io::stdin();
    let input = stdin.as_fd();
    let quiet = QuietTerminal::enter(input)?;
    let mut stderr = io::stderr();
    stderr
        .write_all(prompt.as_bytes())
        .and_then(|()| stderr.flush())
        .context("write the prompt")?;
    let line = read_line_without_echo(input);
    drop(quiet);
    // The newline the terminal did not echo.
    let _ = stderr.write_all(b"\n");
    line
}

/// Reads one line of at most [`PROMPTED_LINE_LIMIT`] bytes from the terminal `input`, whose echo
/// a [`QuietTerminal`] holds off, in canonical mode. The rest of a longer line is discarded so
/// that it never reaches the next program to read the terminal.
fn read_line_without_echo(input: BorrowedFd<'_>) -> anyhow::Result<Zeroizing<Vec<u8>>> {
    let mut line = Zeroizing::new(vec![0; PROMPTED_LINE_LIMIT + 1]);
    let mut filled = 0;
    while filled < line.len() && !line[..filled].ends_with(b"\n") {
        match rustix::io::read(input, &mut line[filled..]) {
            Ok(0) => break,
            Ok(count) => filled += count,
            Err(rustix::io::Errno::INTR) => {}
            Err(error) => return Err(anyhow::Error::new(error).context("read the terminal")),
        }
    }
    if filled == line.len() && !line.ends_with(b"\n") {
        rustix::termios::tcflush(input, QueueSelector::IFlush)
            .context("discard the rest of the line")?;
    }
    line.truncate(filled);
    Ok(line)
}

/// The signals whose default action would end the process during the prompt,
/// and [`QUIET_TERMINAL_WAKE`], which ends the wait for them.
const QUIET_TERMINAL_SIGNALS: [Signal; 5] = [
    Signal::SIGINT,
    Signal::SIGTERM,
    Signal::SIGHUP,
    Signal::SIGQUIT,
    QUIET_TERMINAL_WAKE,
];
const QUIET_TERMINAL_WAKE: Signal = Signal::SIGUSR1;

/// A terminal with echo turned off, restored when this is dropped. A guard's
/// `Drop` does not run when a signal's default action ends the process, so
/// while it lives the calling thread, which is the process's only thread at
/// the prompt, blocks [`QUIET_TERMINAL_SIGNALS`] and a thread waits for them:
/// it restores the saved settings first and then lets the signal take its
/// default action. Dropping it restores the settings, wakes and joins that
/// thread, and unblocks the signals, so one that arrived meanwhile takes its
/// default action then.
struct QuietTerminal<'a> {
    input: BorrowedFd<'a>,
    saved: Termios,
    previous_mask: SigSet,
    done: Arc<AtomicBool>,
    waiter: Option<JoinHandle<()>>,
}

impl<'a> QuietTerminal<'a> {
    fn enter(input: BorrowedFd<'a>) -> anyhow::Result<Self> {
        let saved = rustix::termios::tcgetattr(input).context("read the terminal's settings")?;
        let signals = QUIET_TERMINAL_SIGNALS.into_iter().collect::<SigSet>();
        let previous_mask = signals
            .thread_swap_mask(SigmaskHow::SIG_BLOCK)
            .context("block signals during the prompt")?;
        let done = Arc::new(AtomicBool::new(false));
        let mut quiet = Self {
            input,
            saved: saved.clone(),
            previous_mask,
            done: Arc::clone(&done),
            waiter: None,
        };
        // The waiter inherits the blocked signals, so it alone takes them.
        quiet.waiter = Some(
            std::thread::Builder::new()
                .name("hidden-prompt-signals".to_owned())
                .spawn(move || restore_terminal_on_signal(signals, &saved, &done))
                .context("watch signals during the prompt")?,
        );
        let mut silent = quiet.saved.clone();
        silent.local_modes.remove(LocalModes::ECHO);
        rustix::termios::tcsetattr(input, OptionalActions::Now, &silent)
            .context("turn off the terminal's echo")?;
        Ok(quiet)
    }
}

impl Drop for QuietTerminal<'_> {
    fn drop(&mut self) {
        let _ = rustix::termios::tcsetattr(self.input, OptionalActions::Now, &self.saved);
        if let Some(waiter) = self.waiter.take() {
            self.done.store(true, Ordering::Release);
            // Blocked in both threads, the signal stays pending until the
            // waiter takes it; a waiter that has already returned takes none.
            if !waiter.is_finished()
                && nix::sys::signal::kill(nix::unistd::Pid::this(), QUIET_TERMINAL_WAKE).is_ok()
            {
                let _ = waiter.join();
            }
        }
        let _ = self.previous_mask.thread_set_mask();
    }
}

fn restore_terminal_on_signal(signals: SigSet, saved: &Termios, done: &AtomicBool) {
    let Ok(signal) = signals.wait() else {
        return;
    };
    if signal == QUIET_TERMINAL_WAKE && done.load(Ordering::Acquire) {
        return;
    }
    let _ = rustix::termios::tcsetattr(io::stdin(), OptionalActions::Now, saved);
    let _ = rustix::io::write(io::stderr(), b"\n");
    // Unblocked in this thread, the signal raised again takes its default
    // action, which ends the process; exit as it would, should it not.
    let _ = SigSet::from(signal).thread_unblock();
    let _ = nix::sys::signal::raise(signal);
    let exit_code = if signal == Signal::SIGINT {
        ExitCode::Interrupted
    } else {
        ExitCode::Terminated
    };
    process::exit(i32::from(exit_code.as_u8()));
}
