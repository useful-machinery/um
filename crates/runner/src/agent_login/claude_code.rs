//! Keeping a delivered Claude Code login, the token `claude setup-token`
//! printed, in a private file in the runner's agent-login directory and
//! offering it to each assignment as `CLAUDE_CODE_OAUTH_TOKEN`
//! (REQ-GRANT-44), while standing back when the service has a Claude
//! Code login of its own (REQ-GRANT-45).

use std::ffi::OsString;
use std::fs;
use std::io;
use std::os::unix::ffi::OsStringExt as _;
use std::path::{Path, PathBuf};

use time::OffsetDateTime;
use um_execution::CLAUDE_CODE_LOGIN_VARIABLE;
use zeroize::Zeroizing;

use super::cloud::Delivery;
use super::directory::{self, LoginDirectory};
use super::readiness::Installation;
use super::{
    FailureCause, FailureStage, InstallRefusal, ServiceEnvironment, StepFailure, valid_token,
};

/// The file a `/login` keeps in Claude Code's configuration directory.
const CREDENTIALS_FILE: &str = ".credentials.json";
/// The prefix of the temporary files the token file is written through.
pub(crate) const TEMPORARY_PREFIX: &str = ".um-claude-code.token.tmp";

/// Whether the service already has a Claude Code login of its own: it
/// carries `CLAUDE_CODE_OAUTH_TOKEN`, or a credentials file is present in
/// Claude Code's configuration directory (`CLAUDE_CONFIG_DIR`, else `.claude`
/// under `HOME`). The file is only looked at, never opened; one the runner
/// cannot tell is absent counts as present. A login kept only in an
/// operating-system credential store is not seen (REQ-GRANT-46).
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
    directory::read_token(path).map(ClaudeCodeToken)
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
        OsString::from(CLAUDE_CODE_LOGIN_VARIABLE),
        OsString::from_vec(token.0.to_vec()),
    ))
}

#[cfg(test)]
mod tests;
