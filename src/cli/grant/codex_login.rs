//! A fresh Codex sign-in in a private temporary Codex home, which `um grant create codex` and
//! the earlier `import` and `reconnect` with `--login` run (REQ-GRANT-34, REQ-GRANT-70,
//! REQ-GRANT-74, INV-CLI-13).
//!
//! Only the provider's documented names are used: `codex login`, its device-code form
//! `codex login --device-auth`, `CODEX_HOME`, the `cli_auth_credentials_store = "file"`
//! setting and the `auth.json` it makes Codex write. The member's own Codex home is never
//! read: the child's `CODEX_HOME` replaces it.

use std::fmt;
use std::fs::OpenOptions;
use std::io::{self, Write};
use std::os::fd::AsFd;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::Path;
use std::process::{ExitStatus, Stdio};

use anyhow::Context;
use zeroize::Zeroizing;

use super::super::ApiFailureReport;
use super::super::secret_file::FileMode;
use super::program::{self, End, Signals, Stop};
use crate::exit_code::OutcomeClass;

const PROGRAM: &str = "codex";
const CONFIG: &[u8] = b"cli_auth_credentials_store = \"file\"\n";

/// The sign-in in a browser, which `codex login` opens on this machine.
pub(in crate::cli) const BROWSER: &[&str] = &["login"];
/// The device-code sign-in, completed in a browser on any machine.
pub(in crate::cli) const DEVICE: &[&str] = &["login", "--device-auth"];

/// Why the sign-in produced no login to import (`provider_login_failed`).
#[derive(Debug)]
pub(in crate::cli) enum Failure {
    NotFound,
    Exited(ExitStatus),
    /// A signal `um` received was passed to the sign-in.
    Interrupted(Stop),
    NoLogin,
    Unusable(anyhow::Error),
}

impl Failure {
    pub(in crate::cli) const fn remedy(&self) -> &'static str {
        match self {
            Self::NotFound => {
                "Install Codex so that codex is on your PATH, or import an auth.json:\n  um grant create codex --from-file PATH"
            }
            _ => {
                "Run the command again and complete the Codex sign-in, or import an auth.json:\n  um grant create codex --from-file PATH"
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
            format!("error: sign in to Codex: {self}"),
            self.remedy(),
            OutcomeClass::GeneralFailure,
        )
    }
}

impl fmt::Display for Failure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound => formatter.write_str("codex not found on PATH"),
            Self::Exited(status) => write!(formatter, "run codex login: {status}"),
            Self::Interrupted(_) => formatter.write_str("interrupted"),
            Self::NoLogin => formatter.write_str("codex login wrote no auth.json"),
            // The reader's messages name the input, never its contents.
            Self::Unusable(error) => write!(formatter, "{error:#}"),
        }
    }
}

/// Runs `codex` with `arguments`, [`BROWSER`] or [`DEVICE`], in a new `0700` Codex home and
/// reads the `auth.json` it wrote. The home is removed whatever the outcome; failing to remove
/// it is reported, not fatal. The outer error is a local fault; the inner one is the sign-in's.
pub(in crate::cli) fn sign_in(
    arguments: &[&str],
    read: impl FnOnce(&Path, FileMode) -> anyhow::Result<Zeroizing<Vec<u8>>>,
) -> anyhow::Result<Result<Zeroizing<Vec<u8>>, Failure>> {
    let Some(codex) = program::find(std::env::var_os("PATH").as_deref(), PROGRAM) else {
        return Ok(Err(Failure::NotFound));
    };
    let home = tempfile::Builder::new()
        .prefix("um-codex-login-")
        .permissions(std::fs::Permissions::from_mode(0o700))
        .tempdir()
        .context("create temporary Codex home")?;
    let path = home.path().to_owned();
    let result = sign_in_at(&codex, &path, arguments).map(|signed_in| {
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

/// Writes the configuration and runs `codex` with `arguments` there with the member's
/// terminal. Its standard output goes to standard error so that `--json` output stays
/// unmixed. A signal that would stop `um` is passed to the child instead, so the home is still
/// removed.
fn sign_in_at(
    codex: &Path,
    home: &Path,
    arguments: &[&str],
) -> anyhow::Result<Result<(), Failure>> {
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
    let runtime = super::super::blocking_signal_runtime("Codex sign-in")?;
    runtime.block_on(async {
        let signals = Signals::observe().context("observe signals during the Codex sign-in")?;
        let started = tokio::process::Command::new(codex)
            .args(arguments)
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
        let end = signals.wait(&mut child).await;
        Ok(match end.context("wait for codex login")? {
            End::Success => Ok(()),
            End::Exited(status) => Err(Failure::Exited(status)),
            End::Interrupted(stop) => Err(Failure::Interrupted(stop)),
        })
    })
}
