//! `import ORGANIZATION codex --login`: a fresh Codex sign-in in a private temporary Codex home
//! (REQ-AGENTLOGIN-24, INV-CLI-13).
//!
//! Only the provider's documented names are used: `codex login`, `CODEX_HOME`, the
//! `cli_auth_credentials_store = "file"` setting and the `auth.json` it makes Codex write. The
//! member's own Codex home is never read: the child's `CODEX_HOME` replaces it.

use std::fmt;
use std::fs::OpenOptions;
use std::io::{self, Write};
use std::os::fd::AsFd;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};

use anyhow::Context;
use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use tokio::signal::unix::{SignalKind, signal};
use zeroize::Zeroizing;

use super::super::super::secret_file::FileMode;

const PROGRAM: &str = "codex";
const CONFIG: &[u8] = b"cli_auth_credentials_store = \"file\"\n";

/// Why the sign-in produced no login to import (`provider_login_failed`).
#[derive(Debug)]
pub(super) enum Failure {
    NotFound,
    Exited(ExitStatus),
    Interrupted,
    NoLogin,
    Unusable(anyhow::Error),
}

impl Failure {
    pub(super) const fn remedy(&self) -> &'static str {
        match self {
            Self::NotFound => {
                "Install Codex so that codex is on your PATH, or import an auth.json with --from-file."
            }
            _ => {
                "Run the command again and complete the Codex sign-in, or import an auth.json with --from-file."
            }
        }
    }
}

impl fmt::Display for Failure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound => formatter.write_str("codex not found on PATH"),
            Self::Exited(status) => write!(formatter, "run codex login: {status}"),
            Self::Interrupted => formatter.write_str("interrupted"),
            Self::NoLogin => formatter.write_str("codex login wrote no auth.json"),
            // The reader's messages name the input, never its contents.
            Self::Unusable(error) => write!(formatter, "{error:#}"),
        }
    }
}

/// Runs `codex login` in a new `0700` Codex home and reads the `auth.json` it wrote. The home
/// is removed whatever the outcome; failing to remove it is reported, not fatal. The outer
/// error is a local fault; the inner one is the sign-in's.
pub(super) fn sign_in(
    read: impl FnOnce(&Path, FileMode) -> anyhow::Result<Zeroizing<Vec<u8>>>,
) -> anyhow::Result<Result<Zeroizing<Vec<u8>>, Failure>> {
    let Some(codex) = find_on_path() else {
        return Ok(Err(Failure::NotFound));
    };
    let home = tempfile::Builder::new()
        .prefix("um-codex-login-")
        .permissions(std::fs::Permissions::from_mode(0o700))
        .tempdir()
        .context("create temporary Codex home")?;
    let path = home.path().to_owned();
    let result = sign_in_at(&codex, &path).map(|signed_in| {
        signed_in.and_then(|()| {
            let login = path.join("auth.json");
            match login.symlink_metadata() {
                Err(error) if error.kind() == io::ErrorKind::NotFound => Err(Failure::NoLogin),
                _ => read(&login, FileMode::Unchecked).map_err(Failure::Unusable),
            }
        })
    });
    if let Err(error) = home.close() {
        // Removal failing does not undo an import; the member learns where the login is left.
        let _ = writeln!(
            io::stderr().lock(),
            "warning: remove temporary Codex home {}: {error}\n\nDelete that directory; it may hold the Codex login.",
            path.display()
        );
    }
    result
}

/// Resolves `codex` on `PATH` only: the first executable regular file of that name in an
/// absolute `PATH` directory.
fn find_on_path() -> Option<PathBuf> {
    std::env::split_paths(&std::env::var_os("PATH")?)
        .filter(|directory| directory.is_absolute())
        .map(|directory| directory.join(PROGRAM))
        .find(|candidate| {
            candidate.metadata().is_ok_and(|metadata| {
                metadata.is_file() && metadata.permissions().mode() & 0o111 != 0
            })
        })
}

/// Writes the configuration and runs `codex login` there with the member's terminal. Its
/// standard output goes to standard error so that `--json` output stays one document. A
/// signal that would stop `um` is passed to the child instead, so the home is still removed.
fn sign_in_at(codex: &Path, home: &Path) -> anyhow::Result<Result<(), Failure>> {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(home.join("config.toml"))
        .and_then(|mut file| file.write_all(CONFIG))
        .context("write temporary Codex configuration")?;
    let output = io::stderr()
        .as_fd()
        .try_clone_to_owned()
        .context("share standard error with codex login")?;
    let runtime = super::super::super::blocking_signal_runtime("Codex sign-in")?;
    runtime.block_on(async {
        let observe = |kind| signal(kind).context("observe signals during the Codex sign-in");
        let mut interrupt = observe(SignalKind::interrupt())?;
        let mut terminate = observe(SignalKind::terminate())?;
        let mut hangup = observe(SignalKind::hangup())?;
        let mut quit = observe(SignalKind::quit())?;
        let started = tokio::process::Command::new(codex)
            .arg("login")
            .env("CODEX_HOME", home)
            .stdin(Stdio::inherit())
            .stdout(Stdio::from(output))
            .stderr(Stdio::inherit())
            .spawn();
        let mut child = match started {
            Ok(child) => child,
            // A `codex` that cannot be started is the sign-in failing, not a fault of `um`.
            Err(error) => {
                return Ok(Err(Failure::Unusable(
                    anyhow::Error::new(error).context("start codex login"),
                )));
            }
        };
        let mut interrupted = false;
        loop {
            let forward = tokio::select! {
                status = child.wait() => {
                    let status = status.context("wait for codex login")?;
                    return Ok(if interrupted {
                        Err(Failure::Interrupted)
                    } else if status.success() {
                        Ok(())
                    } else {
                        Err(Failure::Exited(status))
                    });
                }
                _ = interrupt.recv() => Signal::SIGINT,
                _ = terminate.recv() => Signal::SIGTERM,
                _ = hangup.recv() => Signal::SIGHUP,
                _ = quit.recv() => Signal::SIGQUIT,
            };
            interrupted = true;
            if let Some(id) = child.id().and_then(|id| i32::try_from(id).ok()) {
                // The child may already have exited; its status follows.
                let _ = kill(Pid::from_raw(id), forward);
            }
        }
    })
}
