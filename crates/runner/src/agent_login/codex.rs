//! Keeping a delivered Codex login as `auth.json` in a Codex home of the
//! runner's own (REQ-AGENTLOGIN-26) and offering that home to each
//! assignment as `CODEX_HOME`, while standing back when the service sets
//! `CODEX_HOME` itself (REQ-AGENTLOGIN-27). The service's own Codex home is
//! never written.

use std::ffi::OsString;
use std::path::PathBuf;

use time::OffsetDateTime;

use super::cloud::Delivery;
use super::directory::{self, LoginDirectory};
use super::readiness::Installation;
use super::{FailureStage, InstallRefusal, ServiceEnvironment};

/// The variable Codex reads its home from, which the runner offers to each
/// assignment.
pub(crate) const HOME_VARIABLE: &str = "CODEX_HOME";
const AUTH_FILE: &str = "auth.json";
const CONFIG_FILE: &str = "config.toml";
/// The runner's Codex home's whole configuration: keep the login in
/// `auth.json`. The same bytes `um auth agent-login import codex --login`
/// gives the Codex it signs in with.
const CONFIG: &[u8] = b"cli_auth_credentials_store = \"file\"\n";
const AUTH_TEMPORARY_PREFIX: &str = ".um-auth.json.tmp";
const CONFIG_TEMPORARY_PREFIX: &str = ".um-config.toml.tmp";
/// The prefixes of the temporary files the runner writes its Codex home
/// through.
pub(crate) const TEMPORARY_PREFIXES: [&str; 2] = [AUTH_TEMPORARY_PREFIX, CONFIG_TEMPORARY_PREFIX];

/// Where the runner keeps a Codex login: `auth.json` in its Codex home.
pub(crate) fn auth_path(directory: &LoginDirectory) -> PathBuf {
    directory.codex_home().join(AUTH_FILE)
}

/// Whether the service sets a Codex home of its own, which the runner never
/// replaces: it then neither keeps nor offers a Codex login.
pub(crate) const fn foreign_login(environment: &ServiceEnvironment) -> bool {
    environment.codex_home
}

/// Keeps `delivery` as `auth.json` in the runner's Codex home, creating the
/// home with its `config.toml` as needed. `recorded` is what the readiness
/// document says the runner installed for Codex there; a generation older
/// than it is refused. Setup asks for no delivery of the generation it
/// installed while its file is there, so a login Codex has refreshed since
/// is never overwritten.
pub(crate) fn install_codex(
    directory: &LoginDirectory,
    recorded: Option<&Installation>,
    delivery: &Delivery,
    now: OffsetDateTime,
) -> Result<Installation, InstallRefusal> {
    let target = auth_path(directory);
    if let Some(installation) = recorded
        && installation.installed_path == target
        && installation.agent_login_id == delivery.agent_login_id
        && delivery.generation < installation.generation
    {
        return Err(InstallRefusal::StaleGeneration);
    }
    let home = directory.codex_home();
    directory::ensure_private_directory(directory.path())
        .and_then(|()| directory::ensure_private_directory(&home))
        .map_err(|error| InstallRefusal::failed(FailureStage::Directory, &error))?;
    directory::write_file(&home.join(CONFIG_FILE), CONFIG, CONFIG_TEMPORARY_PREFIX)
        .and_then(|()| {
            directory::write_file(&target, delivery.material.as_bytes(), AUTH_TEMPORARY_PREFIX)
        })
        .map_err(|error| InstallRefusal::failed(FailureStage::Write, &error))?;
    Ok(Installation {
        agent_login_id: delivery.agent_login_id.clone(),
        generation: delivery.generation,
        installed_at: now,
        installed_path: target,
    })
}

/// `CODEX_HOME` naming the runner's Codex home, when the runner keeps a
/// Codex login there and the service sets no `CODEX_HOME` of its own.
pub(crate) fn offered_home(
    service: &ServiceEnvironment,
    directory: &LoginDirectory,
) -> Option<(OsString, OsString)> {
    if foreign_login(service) {
        return None;
    }
    directory::holds_file(&auth_path(directory)).then(|| {
        (
            OsString::from(HOME_VARIABLE),
            directory.codex_home().into_os_string(),
        )
    })
}

#[cfg(test)]
mod tests;
