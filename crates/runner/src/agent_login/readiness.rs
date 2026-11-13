//! The readiness document, `.runner-agent-logins.json` beside the runner
//! state (`runner-agent-logins-v1`): each provider's readiness and the record
//! of what the runner installed. It holds no login material
//! (REQ-GRANT-38). A missing document means the
//! runner has installed nothing. An entry for a provider this build does not
//! know, which a newer `um` may have written, is kept as it was read.
//! A coding agent's entry records whether its provider is selected; one an
//! earlier `um` wrote without it reads as selected by its state. Beside an
//! install it records how the provider answered the login's last use
//! (REQ-GRANT-52).

use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::control_protocol::{
    AgentLoginProvider, AgentLoginReadiness, AgentLoginState, ProviderReadiness,
};
use crate::enrollment::{parse_rfc3339, read_private_document, write_private_json};

/// The document's file name in the directory of `runnerStatePath`.
pub(crate) const READINESS_FILE: &str = ".runner-agent-logins.json";
/// The prefix of the temporary files the document is written through, its
/// own so a pass can remove what a killed write left without touching
/// enrollment's.
pub(crate) const TEMPORARY_PREFIX: &str = ".runner-agent-logins.json.tmp";
const READINESS_LIMIT: u64 = 64 * 1024;
/// The most entries the document holds (the schema's `maxItems`).
const ENTRY_LIMIT: usize = 16;

/// The providers whose tokens Cloud brokers per assignment and does not yet
/// issue to a runner; the document lists each as `not_delivered`.
fn brokered_providers() -> impl Iterator<Item = AgentLoginProvider> {
    AgentLoginProvider::ALL
        .into_iter()
        .filter(|provider| provider.is_brokered())
}

/// Where the readiness document of the runner whose protected state is at
/// `runner_state_path` lives.
pub(crate) fn readiness_path(runner_state_path: &Path) -> PathBuf {
    runner_state_path.with_file_name(READINESS_FILE)
}

/// What the runner records about agent logins.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ReadinessDocument {
    pub(crate) updated_at: OffsetDateTime,
    /// One entry for each installable provider the runner has set up, at
    /// most one per provider.
    pub(crate) logins: Vec<LoginReadiness>,
    /// The entries of providers this build does not know, as they were read,
    /// kept for the build that wrote them.
    pub(crate) unknown: Vec<UnknownEntry>,
}

/// An entry for a provider this build does not know, kept verbatim.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct UnknownEntry(WireEntry);

/// One installable provider's readiness.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LoginReadiness {
    pub(crate) provider: AgentLoginProvider,
    pub(crate) state: AgentLoginState,
    pub(crate) checked_at: Option<OffsetDateTime>,
    pub(crate) login: Option<RecordedLogin>,
    /// Whether Cloud's last answer named an authorization in force for the
    /// provider: `authorized`, `unauthorized` or `unsupported_target`
    /// (REQ-GRANT-38).
    pub(crate) selected: bool,
    /// How the provider answered the last use of the login installed, and
    /// when; only ever beside an install (REQ-GRANT-52).
    pub(crate) last_use: Option<LastUse>,
}

/// How the provider answered an agent step that received the login's
/// variable (REQ-GRANT-52), as the control socket also reports it.
pub(crate) use crate::control_protocol::AgentLoginUse as UseOutcome;

/// The last use of an installed login: how the provider answered it, and
/// when the runner noted it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LastUse {
    pub(crate) outcome: UseOutcome,
    pub(crate) at: OffsetDateTime,
}

/// Whether an entry written before selection was recorded reads as
/// selected: exactly when its state is one only an authorized login has
/// (REQ-GRANT-38).
pub(crate) const fn selected_by_state(state: AgentLoginState) -> bool {
    state.selected_by_state()
}

/// The agent login an entry concerns.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RecordedLogin {
    /// A login the runner installed and has not removed.
    Installed(Installation),
    /// A login Cloud named that the runner does not hold, by its ID.
    Named(String),
}

/// What the runner installed for a provider: the record that vouches for the
/// file at `installed_path` and only there.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Installation {
    pub(crate) agent_login_id: String,
    pub(crate) generation: u64,
    pub(crate) installed_at: OffsetDateTime,
    pub(crate) installed_path: PathBuf,
}

impl LoginReadiness {
    pub(crate) fn installation(&self) -> Option<&Installation> {
        match &self.login {
            Some(RecordedLogin::Installed(installation)) => Some(installation),
            Some(RecordedLogin::Named(_)) | None => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReadinessError {
    /// The document exists but could not be read: `PermissionDenied` when it
    /// is not a private file of the effective user, `InvalidData` when it is
    /// over its size bound.
    Unreadable(io::ErrorKind),
    /// The document is not a valid `runner-agent-logins-v1` document.
    Invalid,
    /// The document could not be replaced durably: `PermissionDenied` when
    /// what is at its path is not a private file of the effective user,
    /// `InvalidData` when the record cannot be written as the document.
    WriteFailed(io::ErrorKind),
}

impl ReadinessError {
    /// Whether a load failed because what is at the path is not the runner's
    /// document (not its private file, over its bound, or not a valid
    /// document), which a setup replaces (REQ-GRANT-38). Any other I/O
    /// failure says nothing about the document, which stays.
    pub(crate) const fn replaceable(self) -> bool {
        matches!(
            self,
            Self::Invalid
                | Self::Unreadable(io::ErrorKind::PermissionDenied | io::ErrorKind::InvalidData)
        )
    }

    /// The kind of I/O failure the error is; an invalid document is
    /// `InvalidData`.
    pub(crate) const fn kind(self) -> io::ErrorKind {
        match self {
            Self::Unreadable(kind) | Self::WriteFailed(kind) => kind,
            Self::Invalid => io::ErrorKind::InvalidData,
        }
    }
}

impl ReadinessDocument {
    /// The installable provider's entry, if the runner recorded one.
    pub(crate) fn login(&self, provider: AgentLoginProvider) -> Option<&LoginReadiness> {
        self.logins.iter().find(|login| login.provider == provider)
    }

    /// Reads the document; `None` when there is none. An entry for a provider
    /// this build does not know is kept in `unknown`.
    pub(crate) fn load(path: &Path) -> Result<Option<Self>, ReadinessError> {
        let Some(bytes) = read_private_document(path, READINESS_LIMIT)
            .map_err(|failure| ReadinessError::Unreadable(io::Error::from(failure).kind()))?
        else {
            return Ok(None);
        };
        let wire: WireDocument =
            serde_json::from_slice(&bytes).map_err(|_| ReadinessError::Invalid)?;
        Self::from_wire(wire)
            .map(Some)
            .ok_or(ReadinessError::Invalid)
    }

    /// Replaces the document atomically as a private file beside the runner
    /// state.
    pub(crate) fn write(&self, path: &Path) -> Result<(), ReadinessError> {
        let wire = self
            .to_wire()
            .ok_or(ReadinessError::WriteFailed(io::ErrorKind::InvalidData))?;
        write_private_json(path, &wire, TEMPORARY_PREFIX)
            .map_err(|error| ReadinessError::WriteFailed(error.kind()))
    }

    /// The readiness as the control socket returns it: the document without
    /// the path of any install (REQ-RUNNERENROLL-63), the time of a last use
    /// or the entries of providers this build does not know.
    pub(crate) fn reported(&self) -> Option<AgentLoginReadiness> {
        let wire = self.to_wire()?;
        Some(AgentLoginReadiness {
            updated_at: wire.updated_at,
            providers: wire
                .providers
                .into_iter()
                .filter_map(WireEntry::reported)
                .collect(),
        })
    }

    fn to_wire(&self) -> Option<WireDocument> {
        let mut providers = Vec::with_capacity(self.unknown.len() + AgentLoginProvider::ALL.len());
        for login in &self.logins {
            let installation = login.installation();
            // A last use is recorded only beside the install it concerns.
            let last_use = installation.and(login.last_use);
            providers.push(WireEntry {
                provider: login.provider.as_str().to_owned(),
                state: EntryState::Login(login.state),
                agent_login_id: login.login.as_ref().map(|recorded| match recorded {
                    RecordedLogin::Installed(installation) => installation.agent_login_id.clone(),
                    RecordedLogin::Named(agent_login_id) => agent_login_id.clone(),
                }),
                generation: installation.map(|installation| installation.generation),
                checked_at: login.checked_at.map(format_time).transpose().ok()?,
                installed_at: installation
                    .map(|installation| format_time(installation.installed_at))
                    .transpose()
                    .ok()?,
                installed_path: installation
                    .map(|installation| {
                        installation
                            .installed_path
                            .to_str()
                            .map(str::to_owned)
                            .ok_or(())
                    })
                    .transpose()
                    .ok()?,
                selected: Some(login.selected),
                last_use: last_use.map(|last_use| last_use.outcome),
                last_use_at: last_use
                    .map(|last_use| format_time(last_use.at))
                    .transpose()
                    .ok()?,
            });
        }
        providers.extend(self.unknown.iter().map(|UnknownEntry(entry)| entry.clone()));
        providers.extend(brokered_providers().map(|provider| WireEntry {
            provider: provider.as_str().to_owned(),
            state: EntryState::Login(AgentLoginState::NotDelivered),
            agent_login_id: None,
            generation: None,
            checked_at: None,
            installed_at: None,
            installed_path: None,
            selected: None,
            last_use: None,
            last_use_at: None,
        }));
        Some(WireDocument {
            schema_version: 1,
            updated_at: format_time(self.updated_at).ok()?,
            providers,
        })
    }

    fn from_wire(wire: WireDocument) -> Option<Self> {
        if wire.schema_version != 1 {
            return None;
        }
        let mut logins: Vec<LoginReadiness> = Vec::new();
        let mut unknown: Vec<UnknownEntry> = Vec::new();
        for entry in wire.providers {
            if AgentLoginProvider::named(&entry.provider)
                .is_some_and(AgentLoginProvider::is_brokered)
            {
                if entry.state != EntryState::Login(AgentLoginState::NotDelivered)
                    || entry.agent_login_id.is_some()
                    || entry.generation.is_some()
                    || entry.checked_at.is_some()
                    || entry.installed_at.is_some()
                    || entry.installed_path.is_some()
                    || entry.selected.is_some()
                    || entry.last_use.is_some()
                    || entry.last_use_at.is_some()
                {
                    return None;
                }
            } else if let Some(provider) = AgentLoginProvider::installable(&entry.provider) {
                let EntryState::Login(state) = entry.state else {
                    return None;
                };
                if state == AgentLoginState::NotDelivered {
                    return None;
                }
                if logins.iter().any(|login| login.provider == provider) {
                    return None;
                }
                let selected = entry.selected.unwrap_or(selected_by_state(state));
                let Recorded {
                    checked_at,
                    login,
                    last_use,
                } = recorded_from_wire(entry)?;
                logins.push(LoginReadiness {
                    provider,
                    state,
                    checked_at,
                    login,
                    selected,
                    last_use,
                });
            } else {
                // Kept only as the schema admits it, so the document this
                // build writes back is one the schema accepts.
                if !wire_name(&entry.provider)
                    || !wire_name(entry.state.as_str())
                    || unknown
                        .iter()
                        .any(|UnknownEntry(kept)| kept.provider == entry.provider)
                {
                    return None;
                }
                recorded_from_wire(entry.clone())?;
                unknown.push(UnknownEntry(entry));
            }
        }
        if unknown.len() + AgentLoginProvider::ALL.len() > ENTRY_LIMIT {
            return None;
        }
        Some(Self {
            updated_at: parse_rfc3339(&wire.updated_at)?,
            logins,
            unknown,
        })
    }
}

/// Whether `name` is a provider or state name as the schema admits one
/// (`^[a-z][a-z0-9_]{0,63}$`).
fn wire_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    name.len() <= 64
        && bytes.next().is_some_and(|first| first.is_ascii_lowercase())
        && bytes.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

/// What an installable entry records besides its provider, state and
/// selection.
struct Recorded {
    checked_at: Option<OffsetDateTime>,
    login: Option<RecordedLogin>,
    last_use: Option<LastUse>,
}

/// An installable entry's check time, the agent login it records and, only
/// beside an install, its last use.
fn recorded_from_wire(entry: WireEntry) -> Option<Recorded> {
    if entry
        .agent_login_id
        .as_deref()
        .is_some_and(|id| !um_support::valid_typed_id(id, "agl_"))
    {
        return None;
    }
    let login = match (
        entry.agent_login_id,
        entry.generation,
        entry.installed_at,
        entry.installed_path,
    ) {
        (None, None, None, None) => None,
        (Some(agent_login_id), None, None, None) => Some(RecordedLogin::Named(agent_login_id)),
        (Some(agent_login_id), Some(generation), Some(installed_at), Some(installed_path)) => {
            let installed_path = PathBuf::from(installed_path);
            if generation == 0 || !installed_path.is_absolute() {
                return None;
            }
            Some(RecordedLogin::Installed(Installation {
                agent_login_id,
                generation,
                installed_at: parse_rfc3339(&installed_at)?,
                installed_path,
            }))
        }
        _ => return None,
    };
    let checked_at = match entry.checked_at.as_deref() {
        Some(checked_at) => Some(parse_rfc3339(checked_at)?),
        None => None,
    };
    let last_use = match (entry.last_use, entry.last_use_at.as_deref(), &login) {
        (None, None, _) => None,
        (Some(outcome), Some(at), Some(RecordedLogin::Installed(_))) => Some(LastUse {
            outcome,
            at: parse_rfc3339(at)?,
        }),
        _ => return None,
    };
    Some(Recorded {
        checked_at,
        login,
        last_use,
    })
}

fn format_time(value: OffsetDateTime) -> Result<String, time::error::Format> {
    value.format(&Rfc3339)
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct WireDocument {
    schema_version: u8,
    updated_at: String,
    providers: Vec<WireEntry>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct WireEntry {
    provider: String,
    state: EntryState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    agent_login_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    generation: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    checked_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    installed_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    installed_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    selected: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_use: Option<UseOutcome>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_use_at: Option<String>,
}

impl WireEntry {
    /// The entry as the control socket reports it; `None` for a provider
    /// this build does not know.
    fn reported(self) -> Option<ProviderReadiness> {
        Some(ProviderReadiness {
            provider: AgentLoginProvider::named(&self.provider)?,
            state: match self.state {
                EntryState::Login(state) => state,
                EntryState::Other(_) => return None,
            },
            agent_login_id: self.agent_login_id,
            generation: self.generation,
            checked_at: self.checked_at,
            installed_at: self.installed_at,
            selected: self.selected,
            last_use: self.last_use,
        })
    }
}

/// An entry's state: one this build knows, or one it does not, which only
/// an unknown provider's entry may hold. A brokered provider's entry holds
/// only `not_delivered`, which an installable provider's never does.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(untagged)]
enum EntryState {
    Login(AgentLoginState),
    Other(String),
}

impl EntryState {
    fn as_str(&self) -> &str {
        match self {
            Self::Login(state) => state.as_str(),
            Self::Other(state) => state,
        }
    }
}

#[cfg(test)]
mod tests;
