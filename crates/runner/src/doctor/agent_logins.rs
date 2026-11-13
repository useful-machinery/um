use std::collections::BTreeMap;
use std::path::PathBuf;

use super::{CheckDescriptor, DoctorCheck, Outcome, Status};
use crate::agent_login::readiness::{ReadinessDocument, readiness_path};
use crate::control_protocol::AgentLoginState;
use crate::enrollment::load_runner_state_path;
use crate::telemetry::io_error_class;

/// `credentials.agent-logins` (REQ-RUNNERENROLL-86): reads the readiness the
/// serving runner recorded beside its state. It takes no lock, contacts
/// nothing and never reads the state file, so it never sees the runner
/// credential; the readiness document holds no login material.
pub(super) struct AgentLoginsCheck {
    config: Option<PathBuf>,
}

impl AgentLoginsCheck {
    pub(super) const fn new(config: Option<PathBuf>) -> Self {
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
                "This check reads the runner's recorded readiness; name the runner operator configuration with --config PATH.",
                BTreeMap::new(),
            );
        };
        let Ok(state_path) = load_runner_state_path(config) else {
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
        let mut details = BTreeMap::new();
        for entry in &readiness.providers {
            let provider = entry.provider.as_str();
            // The brokered providers' `not_delivered` is implied, as before.
            if !entry.provider.is_brokered() {
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
        }
        // The brokered providers are `not_delivered` and never fail the check.
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
            .any(|entry| entry.state == AgentLoginState::Ready)
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
}
