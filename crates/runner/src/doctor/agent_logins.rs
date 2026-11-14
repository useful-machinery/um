use std::collections::BTreeMap;
use std::path::PathBuf;

use super::{CheckDescriptor, DoctorCheck, Outcome, Status};
use crate::agent_login::readiness::{ReadinessDocument, readiness_path};
use crate::control_protocol::{AgentLoginReadiness, AgentLoginState};
use crate::enrollment::{RunnerHome, load_runner_state_path};
use crate::telemetry::io_error_class;

/// Where a doctor check that needs the runner operator configuration finds it
/// when it runs (REQ-RUNNERENROLL-45). Naming one reads nothing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OperatorConfigSource {
    /// The file `--config` names.
    File(PathBuf),
    /// The runner home's configuration, the home's default layout when it
    /// holds none (REQ-RUNNERENROLL-109).
    Home(RunnerHome),
}

impl OperatorConfigSource {
    /// `runnerStatePath`, read without taking the state lock or reading the
    /// state; `None` when the configuration cannot be loaded. Creates nothing.
    fn runner_state_path(&self) -> Option<PathBuf> {
        match self {
            Self::File(path) => load_runner_state_path(path).ok(),
            Self::Home(home) => match home.existing_configuration() {
                Ok(Some(path)) => load_runner_state_path(&path).ok(),
                Ok(None) => Some(home.state_path()),
                Err(_) => None,
            },
        }
    }
}

/// `credentials.agent-logins` (REQ-RUNNERENROLL-86): reads the readiness the
/// serving runner recorded beside its state. It takes no lock, contacts
/// nothing and never reads the state file, so it never sees the runner
/// credential; the readiness document holds no login material.
pub(super) struct AgentLoginsCheck {
    config: Option<OperatorConfigSource>,
}

impl AgentLoginsCheck {
    pub(super) const fn new(config: Option<OperatorConfigSource>) -> Self {
        Self { config }
    }
}

impl DoctorCheck for AgentLoginsCheck {
    fn descriptor(&self) -> CheckDescriptor {
        CheckDescriptor {
            id: "credentials.agent-logins",
            title: "Agent logins",
            default: false,
        }
    }

    fn run(&self) -> Outcome {
        let Some(config) = &self.config else {
            return Outcome::fail(
                "config_required",
                "This check reads the runner's recorded readiness; set UM_HOME or HOME to locate the runner home, or name the runner operator configuration with --config PATH.",
                BTreeMap::new(),
            );
        };
        let Some(state_path) = config.runner_state_path() else {
            return Outcome::fail(
                "config_invalid",
                "The runner operator configuration is invalid.",
                BTreeMap::new(),
            );
        };
        let document = match ReadinessDocument::load(&readiness_path(&state_path)) {
            Ok(Some(document)) => document,
            Ok(None) => {
                return Outcome::fail(
                    "readiness_unknown",
                    "The runner has recorded no agent-login readiness; start `um runner serve` or run `um runner grant sync`.",
                    BTreeMap::new(),
                );
            }
            // The class of the failure, never a path: `permission_denied`
            // for a document that is not the runner's private file,
            // `invalid_data` for one that is not this document.
            Err(error) => {
                return Outcome::fail(
                    "readiness_unknown",
                    "The runner's agent-login readiness document cannot be read.",
                    BTreeMap::from([("cause".to_owned(), io_error_class(error.kind()).to_owned())]),
                );
            }
        };

        // The readiness as the control socket reports it: every provider
        // this build knows, without the path of an install.
        let Some(readiness) = document.reported() else {
            return Outcome::fail(
                "readiness_unknown",
                "The runner's agent-login readiness document cannot be read.",
                BTreeMap::from([(
                    "cause".to_owned(),
                    io_error_class(std::io::ErrorKind::InvalidData).to_owned(),
                )]),
            );
        };
        readiness_outcome(&readiness)
    }
}

/// The check's outcome for the readiness the runner recorded: it fails for
/// the first agent login Cloud authorized that is not ready to use, and never
/// for an OAuth provider's entry, which records one assignment's outcome
/// (REQ-GRANT-88).
fn readiness_outcome(readiness: &AgentLoginReadiness) -> Outcome {
    let mut details = BTreeMap::new();
    for entry in &readiness.providers {
        let provider = entry.provider.as_str();
        // An OAuth provider's `not_delivered` is implied, as before.
        if entry.state != AgentLoginState::NotDelivered {
            details.insert(provider.to_owned(), entry.state.as_str().to_owned());
        }
        let status = entry.credential_status();
        details.insert(
            format!("{provider}_availability"),
            status.availability.as_str().to_owned(),
        );
        details.insert(
            format!("{provider}_next_action"),
            status.next_action.as_str().to_owned(),
        );
        // An OAuth provider's entry of an ended assignment names its run and
        // that end where it records them (REQ-GRANT-88).
        for (suffix, value) in [("run_id", &entry.run_id), ("ended_at", &entry.ended_at)] {
            if let Some(value) = value {
                details.insert(format!("{provider}_{suffix}"), value.clone());
            }
        }
    }
    if let Some((entry, code)) = readiness
        .providers
        .iter()
        .find_map(|entry| Some((entry, entry.failure_code()?)))
    {
        details.insert("provider".to_owned(), entry.provider.as_str().to_owned());
        let message = if entry.state == AgentLoginState::Ready {
            format!(
                "The {} login Cloud authorized for the runner was refused at its last use.",
                entry.provider.as_str()
            )
        } else {
            format!(
                "The {} login Cloud authorized for the runner is not ready.",
                entry.provider.as_str()
            )
        };
        return Outcome {
            status: Status::Fail,
            code,
            message,
            details,
        };
    }
    let message = if readiness
        .providers
        .iter()
        .any(|entry| !entry.provider.is_brokered() && entry.state == AgentLoginState::Ready)
    {
        "Every agent login Cloud authorized for the runner is ready."
    } else {
        "Cloud has authorized no agent login for the runner."
    };
    Outcome {
        status: Status::Pass,
        code: "ok",
        message: message.to_owned(),
        details,
    }
}

#[cfg(test)]
mod tests {
    use super::readiness_outcome;
    use crate::control_protocol::{
        AgentLoginProvider, AgentLoginReadiness, AgentLoginState, ProviderReadiness,
    };
    use crate::doctor::Status;

    fn entry(provider: AgentLoginProvider, state: AgentLoginState) -> ProviderReadiness {
        ProviderReadiness {
            provider,
            state,
            agent_login_id: None,
            generation: None,
            checked_at: Some("2026-11-12T01:00:00Z".to_owned()),
            installed_at: None,
            grant_id: None,
            expires_at: None,
            selected: Some(true),
            last_use: None,
            last_use_at: None,
            run_id: None,
            ended_at: None,
        }
    }

    // REQ-GRANT-88, REQ-RUNNERENROLL-86
    #[test]
    fn the_check_passes_with_an_oauth_entry_cloud_did_not_answer() {
        let readiness = AgentLoginReadiness {
            updated_at: "2026-11-12T01:00:00Z".to_owned(),
            providers: vec![
                ProviderReadiness {
                    selected: Some(false),
                    ..entry(AgentLoginProvider::Codex, AgentLoginState::NotAuthorized)
                },
                entry(
                    AgentLoginProvider::Github,
                    AgentLoginState::CloudUnavailable,
                ),
                entry(
                    AgentLoginProvider::Gitlab,
                    AgentLoginState::ReconnectRequired,
                ),
                entry(AgentLoginProvider::Linear, AgentLoginState::InstallFailed),
            ],
        };

        let outcome = readiness_outcome(&readiness);

        assert_eq!(outcome.status, Status::Pass, "{outcome:?}");
        assert_eq!(outcome.code, "ok");
        // Each OAuth entry is reported with its state, availability and
        // next action (REQ-GRANT-53).
        for (provider, state, availability, next_action) in [
            (
                "github",
                "cloud_unavailable",
                "delivery_failed",
                "sync_runner",
            ),
            (
                "gitlab",
                "reconnect_required",
                "reconnect_required",
                "reconnect",
            ),
            ("linear", "install_failed", "delivery_failed", "sync_runner"),
        ] {
            let detail = |suffix: &str| {
                outcome
                    .details
                    .get(&format!("{provider}{suffix}"))
                    .map(String::as_str)
            };
            assert_eq!(detail(""), Some(state), "{outcome:?}");
            assert_eq!(detail("_availability"), Some(availability), "{outcome:?}");
            assert_eq!(detail("_next_action"), Some(next_action), "{outcome:?}");
        }

        // An installable provider in the same state still fails it.
        let mut failing = readiness;
        failing.providers[0] = entry(AgentLoginProvider::Codex, AgentLoginState::CloudUnavailable);
        let outcome = readiness_outcome(&failing);
        assert_eq!(outcome.status, Status::Fail, "{outcome:?}");
        assert_eq!(outcome.code, "cloud_unavailable");
        assert_eq!(
            outcome.details.get("provider").map(String::as_str),
            Some("codex")
        );
    }

    // REQ-GRANT-88, REQ-RUNNERENROLL-86
    #[test]
    fn the_check_names_an_ended_oauth_entrys_run_and_end_and_still_passes() {
        let run_id = "run_01k0z6r1w8f4jy2m7q9v3x5abc";
        let ended_at = "2026-11-12T02:00:00Z";
        let readiness = AgentLoginReadiness {
            updated_at: ended_at.to_owned(),
            providers: vec![
                ProviderReadiness {
                    run_id: Some(run_id.to_owned()),
                    ended_at: Some(ended_at.to_owned()),
                    ..entry(AgentLoginProvider::Github, AgentLoginState::Ready)
                },
                entry(AgentLoginProvider::Gitlab, AgentLoginState::Revoked),
            ],
        };

        let outcome = readiness_outcome(&readiness);

        assert_eq!(outcome.status, Status::Pass, "{outcome:?}");
        assert_eq!(outcome.code, "ok");
        let detail = |key: &str| outcome.details.get(key).map(String::as_str);
        assert_eq!(detail("github_run_id"), Some(run_id), "{outcome:?}");
        assert_eq!(detail("github_ended_at"), Some(ended_at), "{outcome:?}");
        assert_eq!(detail("github_availability"), Some("unverified"));
        // An entry that records neither names neither.
        assert_eq!(detail("gitlab_run_id"), None, "{outcome:?}");
        assert_eq!(detail("gitlab_ended_at"), None, "{outcome:?}");
    }
}
