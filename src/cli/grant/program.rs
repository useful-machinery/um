//! Running a provider's own program for a sign-in or a consent: finding it on `PATH` only and
//! waiting for it while a signal that would stop `um` is passed to it instead (INV-CLI-13).

use std::ffi::OsStr;
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::ExitStatusExt;
use std::path::PathBuf;
use std::process::ExitStatus;

use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use tokio::signal::unix::{SignalKind, signal};

use crate::exit_code::ExitCode;

/// Resolves `program` on `path` only: the first executable regular file of that name in an
/// absolute `PATH` directory.
pub(super) fn find(path: Option<&OsStr>, program: &str) -> Option<PathBuf> {
    std::env::split_paths(path?)
        .filter(|directory| directory.is_absolute())
        .map(|directory| directory.join(program))
        .find(|candidate| {
            candidate.metadata().is_ok_and(|metadata| {
                metadata.is_file() && metadata.permissions().mode() & 0o111 != 0
            })
        })
}

/// How a program `um` waited for ended.
pub(super) enum End {
    Success,
    Exited(ExitStatus),
    /// A signal that would have stopped `um` arrived and was passed to the program; the first
    /// one says how.
    Interrupted(Stop),
}

/// A signal that asked for a sign-in to stop, which stops several sign-ins with its status
/// (REQ-GRANT-75): an interrupt, or a request to terminate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::cli) enum Stop {
    Interrupt,
    Terminate,
}

impl Stop {
    /// The stop a signal `um` passed to the program asked for: SIGINT interrupts, and SIGTERM,
    /// SIGHUP and SIGQUIT terminate.
    const fn passed(signal: Signal) -> Self {
        match signal {
            Signal::SIGINT => Self::Interrupt,
            _ => Self::Terminate,
        }
    }

    /// The stop a program's status shows when SIGINT, SIGTERM or SIGHUP ended it, as a
    /// member's interrupt on a terminal the program alone owns does; SIGHUP terminates.
    pub(super) fn ended(status: ExitStatus) -> Option<Self> {
        match Signal::try_from(status.signal()?).ok()? {
            Signal::SIGINT => Some(Self::Interrupt),
            Signal::SIGTERM | Signal::SIGHUP => Some(Self::Terminate),
            _ => None,
        }
    }

    pub(super) const fn exit_code(self) -> ExitCode {
        match self {
            Self::Interrupt => ExitCode::Interrupted,
            Self::Terminate => ExitCode::Terminated,
        }
    }
}

/// The four signals that would stop `um`, observed from before the program starts until it
/// ends. Observing needs the Tokio runtime the wait runs on.
pub(super) struct Signals {
    interrupt: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
    hangup: tokio::signal::unix::Signal,
    quit: tokio::signal::unix::Signal,
}

impl Signals {
    pub(super) fn observe() -> io::Result<Self> {
        Ok(Self {
            interrupt: signal(SignalKind::interrupt())?,
            terminate: signal(SignalKind::terminate())?,
            hangup: signal(SignalKind::hangup())?,
            quit: signal(SignalKind::quit())?,
        })
    }

    /// Waits for `child` to end, passing it each observed signal instead of letting the signal
    /// stop `um`, so that what `um` made for the program is still removed. Once a signal has
    /// been passed, the end is [`End::Interrupted`] whatever the program's status.
    pub(super) async fn wait(mut self, child: &mut tokio::process::Child) -> io::Result<End> {
        let mut interrupted = None;
        loop {
            let forward = tokio::select! {
                status = child.wait() => {
                    let status = status?;
                    return Ok(if let Some(stop) = interrupted {
                        End::Interrupted(stop)
                    } else if status.success() {
                        End::Success
                    } else {
                        End::Exited(status)
                    });
                }
                _ = self.interrupt.recv() => Signal::SIGINT,
                _ = self.terminate.recv() => Signal::SIGTERM,
                _ = self.hangup.recv() => Signal::SIGHUP,
                _ = self.quit.recv() => Signal::SIGQUIT,
            };
            interrupted.get_or_insert(Stop::passed(forward));
            if let Some(id) = child.id().and_then(|id| i32::try_from(id).ok()) {
                // The child may already have exited; its status follows.
                let _ = kill(Pid::from_raw(id), forward);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};

    const PROGRAM: &str = "provider-program";

    fn place(directory: &Path, mode: u32) -> PathBuf {
        let program = directory.join(PROGRAM);
        std::fs::write(&program, "#!/bin/sh\n").unwrap();
        std::fs::set_permissions(&program, std::fs::Permissions::from_mode(mode)).unwrap();
        program
    }

    /// `directory` written relative to the current directory, so that it names the same place.
    fn relative(directory: &Path) -> PathBuf {
        let up: PathBuf = std::env::current_dir()
            .unwrap()
            .components()
            .skip(1)
            .map(|_| "..")
            .collect();
        up.join(directory.strip_prefix("/").unwrap())
    }

    // INV-CLI-13
    #[test]
    fn the_first_executable_regular_file_in_an_absolute_directory_is_taken() {
        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        let expected = place(first.path(), 0o755);
        place(second.path(), 0o755);
        let path = std::env::join_paths([first.path(), second.path()]).unwrap();

        assert_eq!(super::find(Some(&path), PROGRAM), Some(expected));
        assert_eq!(super::find(None, PROGRAM), None);
    }

    // INV-CLI-13
    #[test]
    fn a_relative_directory_a_directory_of_the_name_and_a_non_executable_file_are_skipped() {
        let in_relative = tempfile::tempdir().unwrap();
        let named_directory = tempfile::tempdir().unwrap();
        let not_executable = tempfile::tempdir().unwrap();
        let executable = tempfile::tempdir().unwrap();
        place(in_relative.path(), 0o755);
        let relative = relative(in_relative.path());
        assert!(relative.is_relative());
        // The relative entry does name an executable program; only its form is refused.
        let mode = relative
            .join(PROGRAM)
            .metadata()
            .unwrap()
            .permissions()
            .mode();
        assert_ne!(mode & 0o111, 0);
        std::fs::create_dir(named_directory.path().join(PROGRAM)).unwrap();
        std::fs::set_permissions(
            named_directory.path().join(PROGRAM),
            std::fs::Permissions::from_mode(0o755),
        )
        .unwrap();
        place(not_executable.path(), 0o644);
        let skipped = [
            relative.as_path(),
            named_directory.path(),
            not_executable.path(),
        ];

        let only_skipped = std::env::join_paths(skipped).unwrap();
        assert_eq!(super::find(Some(&only_skipped), PROGRAM), None);

        let expected = place(executable.path(), 0o755);
        let path = std::env::join_paths(skipped.into_iter().chain([executable.path()])).unwrap();
        assert_eq!(super::find(Some(&path), PROGRAM), Some(expected));
    }
}
