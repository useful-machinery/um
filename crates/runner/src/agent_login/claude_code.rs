//! Keeping a delivered Claude Code login, the token `claude setup-token`
//! printed, in a private file in the runner's agent-login directory and
//! offering it to each assignment as `CLAUDE_CODE_OAUTH_TOKEN`
//! (REQ-AGENTLOGIN-34), while standing back when the service has a Claude
//! Code login of its own (REQ-AGENTLOGIN-35).

use std::ffi::OsString;
use std::fs;
use std::io;
use std::os::unix::ffi::OsStringExt as _;
use std::path::{Path, PathBuf};

use time::OffsetDateTime;
use zeroize::Zeroizing;

use super::cloud::Delivery;
use super::directory::{self, LoginDirectory};
use super::readiness::Installation;
use super::{FailureCause, FailureStage, InstallRefusal, ServiceEnvironment, StepFailure};
use crate::enrollment::read_private_file_if_present;

/// The variable Claude Code reads the token from, which the runner offers to
/// each assignment.
pub(crate) const TOKEN_VARIABLE: &str = "CLAUDE_CODE_OAUTH_TOKEN";
/// The file a `/login` keeps in Claude Code's configuration directory.
const CREDENTIALS_FILE: &str = ".credentials.json";
/// A token is one line of at most this many printable characters
/// (REQ-AGENTLOGIN-2).
const TOKEN_LIMIT: usize = 4096;
/// The prefix of the temporary files the token file is written through.
pub(crate) const TEMPORARY_PREFIX: &str = ".um-claude-code.token.tmp";

/// Whether the service already has a Claude Code login of its own: it
/// carries `CLAUDE_CODE_OAUTH_TOKEN`, or a credentials file is present in
/// Claude Code's configuration directory (`CLAUDE_CONFIG_DIR`, else `.claude`
/// under `HOME`). The file is only looked at, never opened; one the runner
/// cannot tell is absent counts as present. A login kept only in an
/// operating-system credential store is not seen (REQ-AGENTLOGIN-36).
pub(crate) fn foreign_login(environment: &ServiceEnvironment) -> bool {
    if environment.claude_code_oauth_token {
        return true;
    }
    let Some(directory) = configuration_directory(environment) else {
        return false;
    };
    match fs::symlink_metadata(directory.join(CREDENTIALS_FILE)) {
        Ok(_) => true,
        Err(error) => error.kind() != io::ErrorKind::NotFound,
    }
}

fn configuration_directory(environment: &ServiceEnvironment) -> Option<PathBuf> {
    environment
        .claude_config_dir
        .as_ref()
        .map(PathBuf::from)
        .or_else(|| {
            environment
                .home
                .as_ref()
                .map(|home| PathBuf::from(home).join(".claude"))
        })
        .filter(|path| path.is_absolute())
}

/// Keeps `delivery` as the token file in the runner's directory, replacing
/// any earlier one in one step. `recorded` is what the readiness document
/// says the runner installed for Claude Code; a delivery older than its
/// generation is refused.
pub(crate) fn install_claude_code(
    directory: &LoginDirectory,
    recorded: Option<&Installation>,
    delivery: &Delivery,
    now: OffsetDateTime,
) -> Result<Installation, InstallRefusal> {
    if let Some(installation) = recorded
        && installation.agent_login_id == delivery.agent_login_id
        && delivery.generation < installation.generation
    {
        return Err(InstallRefusal::StaleGeneration);
    }
    if !valid_token(delivery.material.as_bytes()) {
        return Err(InstallRefusal::InstallFailed(StepFailure {
            stage: FailureStage::Delivery,
            cause: FailureCause::InvalidResponse,
        }));
    }
    directory::ensure_private_directory(directory.path())
        .map_err(|error| InstallRefusal::failed(FailureStage::Directory, &error))?;
    let path = directory.claude_code_token();
    directory::write_file(&path, delivery.material.as_bytes(), TEMPORARY_PREFIX)
        .map_err(|error| InstallRefusal::failed(FailureStage::Write, &error))?;
    Ok(Installation {
        agent_login_id: delivery.agent_login_id.clone(),
        generation: delivery.generation,
        installed_at: now,
        installed_path: path,
    })
}

/// The token as the runner reads it back: cleared when dropped and never
/// shown by `Debug`.
pub(crate) struct ClaudeCodeToken(Zeroizing<Vec<u8>>);

impl std::fmt::Debug for ClaudeCodeToken {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ClaudeCodeToken([redacted])")
    }
}

/// Reads the token file at `path` through the runner's bounded private-file
/// read; `None` when there is none, or when it is not a private file holding
/// one valid token.
pub(crate) fn read_token(path: &Path) -> Option<ClaudeCodeToken> {
    let bytes = Zeroizing::new(
        read_private_file_if_present(path, TOKEN_LIMIT as u64)
            .ok()
            .flatten()?,
    );
    valid_token(&bytes).then_some(ClaudeCodeToken(bytes))
}

/// `CLAUDE_CODE_OAUTH_TOKEN` with the token the runner keeps in `directory`,
/// when it keeps one and the service has no Claude Code login of its own.
pub(crate) fn offered_token(
    service: &ServiceEnvironment,
    directory: &LoginDirectory,
) -> Option<(OsString, OsString)> {
    if foreign_login(service) {
        return None;
    }
    let token = read_token(&directory.claude_code_token())?;
    Some((
        OsString::from(TOKEN_VARIABLE),
        OsString::from_vec(token.0.to_vec()),
    ))
}

/// One line of 1 to 4096 printable characters without spaces, the form
/// Cloud accepts (REQ-AGENTLOGIN-2) and an environment variable can carry.
fn valid_token(bytes: &[u8]) -> bool {
    (1..=TOKEN_LIMIT).contains(&bytes.len()) && bytes.iter().all(u8::is_ascii_graphic)
}

#[cfg(test)]
mod tests;
