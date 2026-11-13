//! The internal helper that runs a program as the leader of a new session whose controlling
//! terminal is the helper's standard input, so that the Claude Code sign-in's `claude
//! setup-token` has the pseudo-terminal `um` opened as its own (INV-CLI-14).
//!
//! A process takes a controlling terminal only for itself, so the sign-in starts this
//! executable again with [`REQUEST`] set rather than running code in a forked child before it
//! executes the program: the helper starts the session, takes the terminal and replaces itself
//! with the program, which keeps its process. `main.rs` dispatches the
//! helper before any argument parsing; nothing of it enters the command tree.

use std::ffi::OsString;
use std::io::{self, Write};
use std::os::unix::process::CommandExt;
use std::path::Path;

use anyhow::{Context, anyhow};

use crate::exit_code::ExitCode;

/// Requests the helper. Only `um` sets it, for its own child, and the program never sees it.
const REQUEST: &str = "UM_INTERNAL_CONTROLLING_TERMINAL_HELPER";
const REQUESTED: &str = "1";

pub(crate) fn requested() -> bool {
    std::env::var_os(REQUEST).is_some_and(|value| value == REQUESTED)
}

/// A command that runs `program` through the helper. The caller adds the program's arguments
/// and environment and the terminal as its three standard streams.
pub(crate) fn command(program: &Path) -> io::Result<tokio::process::Command> {
    let mut command = tokio::process::Command::new(std::env::current_exe()?);
    command.env(REQUEST, REQUESTED).arg(program);
    Ok(command)
}

/// Runs the helper, whose first argument is the program and the rest the program's arguments.
/// It returns only when it could not become the program: the failure is written to standard
/// error, the terminal, and the program's status is a general failure.
pub(crate) fn run() -> ExitCode {
    let mut arguments = std::env::args_os().skip(1);
    let error = match arguments.next() {
        Some(program) => become_leader(program, arguments),
        None => anyhow!("name no program"),
    };
    let _ = writeln!(
        io::stderr().lock(),
        "error: run on its own terminal: {error:#}"
    );
    ExitCode::GeneralFailure
}

/// Makes this process the leader of a new session whose controlling terminal is standard input
/// and replaces it with `program`; returns why it could not.
fn become_leader(program: OsString, arguments: impl Iterator<Item = OsString>) -> anyhow::Error {
    let led = rustix::process::setsid()
        .map_err(io::Error::from)
        .context("start a new session")
        .and_then(|_| {
            rustix::process::ioctl_tiocsctty(io::stdin())
                .map_err(io::Error::from)
                .context("take standard input as the controlling terminal")
        });
    if let Err(error) = led {
        return error;
    }
    let error = std::process::Command::new(&program)
        .args(arguments)
        .env_remove(REQUEST)
        .exec();
    anyhow::Error::new(error).context(format!("execute {}", Path::new(&program).display()))
}
