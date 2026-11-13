mod agent_logins;
mod claude_code;
mod codex;
mod git;
mod pi;
mod workflow_credentials;

use std::collections::BTreeSet;
use std::fmt;
use std::path::PathBuf;

use um_execution::{HarnessQualification, UntestedHarnessAdvisory};

use agent_logins::AgentLoginsCheck;
use claude_code::ClaudeCodeCheck;
use codex::CodexCheck;
use git::GitCheck;
use pi::PiCheck;

pub use workflow_credentials::{
    CredentialKind, CredentialOrigin, CredentialRequirement, CredentialSummary, Declaration,
    DeclarationKind, Node, NodeKind, OriginDeclaration, ReadinessCode, RepositorySource,
    UnknownCredential, UnknownDeclaration, WorkflowCredentials, read_workflow_credentials,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CheckDescriptor {
    pub id: &'static str,
    pub title: &'static str,
    pub default: bool,
}

pub(crate) trait DoctorCheck: Send + Sync {
    fn descriptor(&self) -> CheckDescriptor;
    fn run(&self) -> Outcome;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Status {
    Pass,
    Fail,
}

impl Status {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pass => "pass",
            Self::Fail => "fail",
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct Outcome {
    pub status: Status,
    pub code: &'static str,
    pub message: String,
    pub details: std::collections::BTreeMap<String, String>,
}

impl Outcome {
    pub(crate) fn fail(
        code: &'static str,
        message: &'static str,
        details: std::collections::BTreeMap<String, String>,
    ) -> Self {
        Self {
            status: Status::Fail,
            code,
            message: message.to_owned(),
            details,
        }
    }
}

fn capability_failure_details(
    mut details: std::collections::BTreeMap<String, String>,
    capability: &str,
    version: String,
) -> std::collections::BTreeMap<String, String> {
    details.insert("capability".to_owned(), capability.to_owned());
    details.insert("version".to_owned(), version);
    details
}

/// The facts a doctor check reports about an installation that admission accepted.
struct CompatibleHarness<'a> {
    harness: &'static str,
    version: &'a str,
    profile: &'a str,
    capabilities: Vec<&'static str>,
    executable: &'a std::path::Path,
    qualification: HarnessQualification,
    untested: Option<UntestedHarnessAdvisory>,
}

fn compatible_harness_outcome(
    installation: CompatibleHarness<'_>,
    mut details: std::collections::BTreeMap<String, String>,
) -> Outcome {
    let CompatibleHarness {
        harness,
        version,
        profile,
        capabilities,
        executable,
        qualification,
        untested,
    } = installation;
    details.insert("capabilities".to_owned(), capabilities.join(","));
    details.insert("profile".to_owned(), profile.to_owned());
    details.insert("version".to_owned(), version.to_owned());
    if let Some(executable) = executable.to_str() {
        details.insert("executablePath".to_owned(), executable.to_owned());
    }
    details.insert(
        "qualification".to_owned(),
        qualification.as_str().to_owned(),
    );
    let message = match untested {
        Some(advisory) => format!("{advisory}."),
        None => format!("{harness} {version} is compatible with {profile}."),
    };
    Outcome {
        status: Status::Pass,
        code: "ok",
        message,
        details,
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct CheckResult {
    pub descriptor: CheckDescriptor,
    pub outcome: Outcome,
}

#[derive(Debug, Eq, PartialEq)]
pub struct Summary {
    pub passed: usize,
    pub failed: usize,
}

#[derive(Debug, Eq, PartialEq)]
pub struct Report {
    pub results: Vec<CheckResult>,
}

impl Report {
    pub fn summary(&self) -> Summary {
        self.results.iter().fold(
            Summary {
                passed: 0,
                failed: 0,
            },
            |mut summary, result| {
                match result.outcome.status {
                    Status::Pass => summary.passed += 1,
                    Status::Fail => summary.failed += 1,
                }
                summary
            },
        )
    }

    pub fn has_failures(&self) -> bool {
        self.results
            .iter()
            .any(|result| result.outcome.status == Status::Fail)
    }
}

pub struct Registry {
    checks: Vec<Box<dyn DoctorCheck>>,
}

impl Registry {
    pub(crate) fn new() -> Self {
        Self { checks: Vec::new() }
    }

    pub(crate) fn register(&mut self, check: Box<dyn DoctorCheck>) -> Result<(), RegistryError> {
        let descriptor = check.descriptor();
        validate_descriptor(descriptor)?;
        if self
            .checks
            .iter()
            .any(|registered| registered.descriptor().id == descriptor.id)
        {
            return Err(RegistryError::DuplicateId(descriptor.id));
        }
        self.checks.push(check);
        Ok(())
    }

    pub fn descriptors(&self) -> Vec<CheckDescriptor> {
        self.checks.iter().map(|check| check.descriptor()).collect()
    }

    pub fn run(&self, requested: &[String]) -> Result<Report, SelectionError> {
        for id in requested {
            if !self.checks.iter().any(|check| check.descriptor().id == id) {
                return Err(SelectionError::UnknownId(id.clone()));
            }
        }

        let requested_ids = requested
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        let selected = |descriptor: CheckDescriptor| {
            if requested.is_empty() {
                descriptor.default
            } else {
                requested_ids.contains(descriptor.id)
            }
        };
        let results = self
            .checks
            .iter()
            .filter_map(|check| {
                let descriptor = check.descriptor();
                selected(descriptor).then(|| CheckResult {
                    descriptor,
                    outcome: check.run(),
                })
            })
            .collect();

        Ok(Report { results })
    }
}

/// The checks compiled into this executable. `config` is the runner operator
/// configuration a check that needs it reads when it runs; no check reads it
/// unless it is selected (REQ-RUNNERENROLL-45).
pub fn built_in_registry(config: Option<PathBuf>) -> Result<Registry, RegistryError> {
    let mut registry = Registry::new();
    registry.register(Box::new(GitCheck::system()))?;
    registry.register(Box::new(PiCheck))?;
    registry.register(Box::new(ClaudeCodeCheck))?;
    registry.register(Box::new(CodexCheck))?;
    registry.register(Box::new(AgentLoginsCheck::new(config)))?;
    Ok(registry)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RegistryError {
    InvalidId(&'static str),
    EmptyTitle(&'static str),
    DuplicateId(&'static str),
}

impl fmt::Display for RegistryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidId(id) => write!(formatter, "runner doctor check ID is invalid: {id}"),
            Self::EmptyTitle(id) => write!(formatter, "runner doctor check title is empty: {id}"),
            Self::DuplicateId(id) => {
                write!(formatter, "runner doctor check ID is duplicated: {id}")
            }
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum SelectionError {
    UnknownId(String),
}

impl fmt::Display for SelectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownId(id) => write!(
                formatter,
                "unknown runner doctor check '{id}'; use 'um runner doctor --list-checks' to list available checks"
            ),
        }
    }
}

fn validate_descriptor(descriptor: CheckDescriptor) -> Result<(), RegistryError> {
    if !is_valid_check_id(descriptor.id) {
        return Err(RegistryError::InvalidId(descriptor.id));
    }
    if descriptor.title.is_empty() {
        return Err(RegistryError::EmptyTitle(descriptor.id));
    }
    Ok(())
}

fn is_valid_check_id(id: &str) -> bool {
    let mut segments = id.split('.');
    let mut count = 0;

    for segment in &mut segments {
        count += 1;
        let bytes = segment.as_bytes();
        let Some((&first, rest)) = bytes.split_first() else {
            return false;
        };
        if !first.is_ascii_lowercase() {
            return false;
        }

        let mut previous_hyphen = false;
        for byte in rest {
            if byte.is_ascii_lowercase() || byte.is_ascii_digit() {
                previous_hyphen = false;
            } else if *byte == b'-' && !previous_hyphen {
                previous_hyphen = true;
            } else {
                return false;
            }
        }
        if previous_hyphen {
            return false;
        }
    }

    count >= 2
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::{
        CheckDescriptor, DoctorCheck, Outcome, Registry, RegistryError, SelectionError, Status,
        built_in_registry,
    };

    struct FakeCheck {
        descriptor: CheckDescriptor,
        outcome: Outcome,
        runs: Arc<AtomicUsize>,
    }

    impl DoctorCheck for FakeCheck {
        fn descriptor(&self) -> CheckDescriptor {
            self.descriptor
        }

        fn run(&self) -> Outcome {
            self.runs.fetch_add(1, Ordering::SeqCst);
            Outcome {
                status: self.outcome.status,
                code: self.outcome.code,
                message: self.outcome.message.clone(),
                details: self.outcome.details.clone(),
            }
        }
    }

    fn fake_check(
        id: &'static str,
        default: bool,
        status: Status,
        runs: Arc<AtomicUsize>,
    ) -> FakeCheck {
        FakeCheck {
            descriptor: CheckDescriptor {
                id,
                title: "Fixture check",
                default,
            },
            outcome: Outcome {
                status,
                code: "fixture",
                message: "Fixture result".to_owned(),
                details: BTreeMap::new(),
            },
            runs,
        }
    }

    #[test]
    fn registration_preserves_order_and_rejects_duplicate_ids() {
        let mut registry = Registry::new();
        registry
            .register(Box::new(fake_check(
                "extension.fixture.check",
                true,
                Status::Pass,
                Arc::new(AtomicUsize::new(0)),
            )))
            .unwrap();
        registry
            .register(Box::new(fake_check(
                "environment.command.git-lfs",
                false,
                Status::Pass,
                Arc::new(AtomicUsize::new(0)),
            )))
            .unwrap();

        assert_eq!(
            registry
                .descriptors()
                .into_iter()
                .map(|descriptor| descriptor.id)
                .collect::<Vec<_>>(),
            ["extension.fixture.check", "environment.command.git-lfs"]
        );
        assert_eq!(
            registry.register(Box::new(fake_check(
                "extension.fixture.check",
                true,
                Status::Pass,
                Arc::new(AtomicUsize::new(0)),
            ))),
            Err(RegistryError::DuplicateId("extension.fixture.check"))
        );
    }

    #[test]
    fn registration_rejects_invalid_namespaced_ids() {
        for id in [
            "git",
            "Environment.git",
            "environment..git",
            "environment.command.git-",
            "environment.command.a--b",
        ] {
            let mut registry = Registry::new();
            assert_eq!(
                registry.register(Box::new(fake_check(
                    id,
                    true,
                    Status::Pass,
                    Arc::new(AtomicUsize::new(0)),
                ))),
                Err(RegistryError::InvalidId(id))
            );
        }
    }

    // REQ-RUNNERENROLL-46
    #[test]
    fn empty_selection_runs_only_default_checks() {
        let default_runs = Arc::new(AtomicUsize::new(0));
        let non_default_runs = Arc::new(AtomicUsize::new(0));
        let mut registry = Registry::new();
        registry
            .register(Box::new(fake_check(
                "extension.fixture.default",
                true,
                Status::Pass,
                Arc::clone(&default_runs),
            )))
            .unwrap();
        registry
            .register(Box::new(fake_check(
                "extension.fixture.extra",
                false,
                Status::Pass,
                Arc::clone(&non_default_runs),
            )))
            .unwrap();

        let report = registry.run(&[]).unwrap();

        assert_eq!(report.results.len(), 1);
        assert_eq!(report.results[0].descriptor.id, "extension.fixture.default");
        assert_eq!(default_runs.load(Ordering::SeqCst), 1);
        assert_eq!(non_default_runs.load(Ordering::SeqCst), 0);
    }

    // REQ-RUNNERENROLL-46
    #[test]
    fn explicit_selection_deduplicates_and_uses_registry_order() {
        let first_runs = Arc::new(AtomicUsize::new(0));
        let second_runs = Arc::new(AtomicUsize::new(0));
        let mut registry = Registry::new();
        registry
            .register(Box::new(fake_check(
                "extension.fixture.first",
                false,
                Status::Pass,
                Arc::clone(&first_runs),
            )))
            .unwrap();
        registry
            .register(Box::new(fake_check(
                "extension.fixture.second",
                false,
                Status::Pass,
                Arc::clone(&second_runs),
            )))
            .unwrap();

        let report = registry
            .run(&[
                "extension.fixture.second".to_owned(),
                "extension.fixture.first".to_owned(),
                "extension.fixture.second".to_owned(),
            ])
            .unwrap();

        assert_eq!(
            report
                .results
                .iter()
                .map(|result| result.descriptor.id)
                .collect::<Vec<_>>(),
            ["extension.fixture.first", "extension.fixture.second"]
        );
        assert_eq!(first_runs.load(Ordering::SeqCst), 1);
        assert_eq!(second_runs.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn unknown_selection_is_rejected_without_running_checks() {
        let runs = Arc::new(AtomicUsize::new(0));
        let mut registry = Registry::new();
        registry
            .register(Box::new(fake_check(
                "extension.fixture.check",
                true,
                Status::Pass,
                Arc::clone(&runs),
            )))
            .unwrap();

        let error = registry
            .run(&["extension.fixture.missing".to_owned()])
            .unwrap_err();

        assert_eq!(
            error,
            SelectionError::UnknownId("extension.fixture.missing".to_owned())
        );
        assert_eq!(runs.load(Ordering::SeqCst), 0);
    }

    // REQ-AGENT-47
    #[test]
    fn built_in_registry_contains_the_default_git_check() {
        let registry = built_in_registry(None).unwrap();

        assert_eq!(
            registry.descriptors(),
            vec![
                CheckDescriptor {
                    id: "environment.command.git",
                    title: "Git",
                    default: true,
                },
                CheckDescriptor {
                    id: "execution.harness.pi-json-v1",
                    title: "PiJsonV1 installation",
                    default: false,
                },
                CheckDescriptor {
                    id: "execution.harness.claude-code-stream-json-v1",
                    title: "ClaudeCodeStreamJsonV1 installation",
                    default: false,
                },
                CheckDescriptor {
                    id: "execution.harness.codex-app-server-v1",
                    title: "CodexAppServerV1 installation",
                    default: false,
                },
                CheckDescriptor {
                    id: "credentials.agent-logins",
                    title: "Agent logins",
                    default: false,
                },
            ]
        );
    }

    /// A runner host as enrollment leaves it: a private state directory and
    /// the operator configuration naming the state inside it.
    struct AgentLoginHost {
        root: tempfile::TempDir,
        config: std::path::PathBuf,
    }

    impl AgentLoginHost {
        fn new() -> Self {
            use std::os::unix::fs::PermissionsExt as _;

            let root = tempfile::tempdir().unwrap();
            let state = root.path().join("state");
            std::fs::create_dir(&state).unwrap();
            std::fs::set_permissions(&state, std::fs::Permissions::from_mode(0o700)).unwrap();
            let config = root.path().join("runner.json");
            std::fs::write(
                &config,
                serde_json::to_vec(&serde_json::json!({
                    "schemaVersion": 1,
                    "deploymentMode": "development",
                    "runnerStatePath": state.join("runner-state.json"),
                    "controlSocketPath": root.path().join("runner.sock"),
                    "workRoot": root.path().join("work"),
                }))
                .unwrap(),
            )
            .unwrap();
            Self { root, config }
        }

        fn record_codex(&self, state: &str) {
            self.record_codex_entry(serde_json::json!({"provider": "codex", "state": state}));
        }

        /// Records a `ready` `codex` install whose last use was `last_use`.
        fn record_ready_codex(&self, last_use: Option<&str>) {
            let mut entry = serde_json::json!({
                "provider": "codex",
                "state": "ready",
                "agentLoginId": "agl_01k0z6r1w8f4jy2m7q9v3x5abc",
                "generation": 2,
                "installedAt": "2026-11-12T00:55:00Z",
                "installedPath": self.root.path().join("state/.runner-agent-login/codex/auth.json"),
                "selected": true,
            });
            if let Some(last_use) = last_use {
                entry["lastUse"] = last_use.into();
                entry["lastUseAt"] = "2026-11-12T00:58:00Z".into();
            }
            self.record_codex_entry(entry);
        }

        fn record_codex_entry(&self, codex: serde_json::Value) {
            use std::os::unix::fs::OpenOptionsExt as _;

            let document = serde_json::json!({
                "schemaVersion": 1,
                "updatedAt": "2026-11-12T01:00:00Z",
                "providers": [
                    codex,
                    {"provider": "github", "state": "not_delivered"},
                    {"provider": "gitlab", "state": "not_delivered"},
                    {"provider": "linear", "state": "not_delivered"},
                ],
            });
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(
                    self.root
                        .path()
                        .join("state")
                        .join(".runner-agent-logins.json"),
                )
                .unwrap();
            std::io::Write::write_all(&mut file, &serde_json::to_vec(&document).unwrap()).unwrap();
        }

        fn run(&self) -> Outcome {
            run_agent_logins(Some(self.config.clone()))
        }
    }

    fn run_agent_logins(config: Option<std::path::PathBuf>) -> Outcome {
        let report = built_in_registry(config)
            .unwrap()
            .run(&["credentials.agent-logins".to_owned()])
            .unwrap();
        let [result] = <[_; 1]>::try_from(report.results).unwrap();
        result.outcome
    }

    // REQ-RUNNERENROLL-86
    #[test]
    fn agent_logins_check_passes_when_every_authorized_provider_is_ready() {
        let host = AgentLoginHost::new();
        host.record_codex("ready");

        let outcome = host.run();

        assert_eq!((outcome.status, outcome.code), (Status::Pass, "ok"));
        assert_eq!(
            outcome.details.get("codex").map(String::as_str),
            Some("ready")
        );
    }

    // REQ-RUNNERENROLL-86
    #[test]
    fn agent_logins_check_fails_with_the_state_of_an_authorized_provider_that_is_not_ready() {
        let host = AgentLoginHost::new();
        host.record_codex("foreign_login");

        let outcome = host.run();

        assert_eq!(
            (outcome.status, outcome.code),
            (Status::Fail, "foreign_login")
        );
        assert_eq!(
            outcome.details.get("provider").map(String::as_str),
            Some("codex")
        );
    }

    // REQ-RUNNERENROLL-86
    #[test]
    fn agent_logins_check_passes_when_cloud_authorized_no_provider() {
        for state in ["not_authorized", "revoked"] {
            let host = AgentLoginHost::new();
            host.record_codex(state);

            let outcome = host.run();

            assert_eq!(
                (outcome.status, outcome.code),
                (Status::Pass, "ok"),
                "{state}"
            );
        }
    }

    // REQ-RUNNERENROLL-86, REQ-AGENTLOGIN-41
    #[test]
    fn agent_logins_check_fails_a_selected_provider_cloud_refuses() {
        let host = AgentLoginHost::new();
        host.record_codex_entry(serde_json::json!({
            "provider": "codex",
            "state": "not_authorized",
            "agentLoginId": "agl_01k0z6r1w8f4jy2m7q9v3x5abc",
            "checkedAt": "2026-11-12T01:00:00Z",
            "selected": true,
        }));

        let outcome = host.run();

        assert_eq!(
            (outcome.status, outcome.code),
            (Status::Fail, "not_authorized")
        );
        assert_eq!(
            outcome.details.get("provider").map(String::as_str),
            Some("codex")
        );
    }

    // REQ-RUNNERENROLL-86
    #[test]
    fn agent_logins_check_fails_a_ready_login_refused_at_its_last_use() {
        for (last_use, status, code) in [
            (Some("refused"), Status::Fail, "reconnect_required"),
            (Some("accepted"), Status::Pass, "ok"),
            (None, Status::Pass, "ok"),
        ] {
            let host = AgentLoginHost::new();
            host.record_ready_codex(last_use);

            let outcome = host.run();

            assert_eq!(
                (outcome.status, outcome.code),
                (status, code),
                "{last_use:?}"
            );
            assert_eq!(
                outcome.details.get("codex").map(String::as_str),
                Some("ready")
            );
        }
    }

    // REQ-RUNNERENROLL-86, REQ-AGENTLOGIN-43
    #[test]
    fn agent_logins_check_names_every_providers_availability_and_next_action() {
        for (record, availability, next_action) in [
            (Some("refused"), "reconnect_required", "reconnect"),
            (Some("accepted"), "available", "none"),
            (None, "unverified", "none"),
        ] {
            let host = AgentLoginHost::new();
            host.record_ready_codex(record);
            let outcome = host.run();
            let detail = |key: &str| outcome.details.get(key).map(String::as_str);
            assert_eq!(detail("codex_availability"), Some(availability));
            assert_eq!(detail("codex_next_action"), Some(next_action));
            for brokered in ["github", "gitlab", "linear"] {
                assert_eq!(
                    detail(&format!("{brokered}_availability")),
                    Some("not_delivered")
                );
                assert_eq!(detail(&format!("{brokered}_next_action")), Some("none"));
            }
        }

        // A failing result names them too.
        let host = AgentLoginHost::new();
        host.record_codex("unsupported_target");
        let outcome = host.run();
        assert_eq!(
            (outcome.status, outcome.code),
            (Status::Fail, "unsupported_target")
        );
        assert_eq!(
            outcome
                .details
                .get("codex_availability")
                .map(String::as_str),
            Some("delivery_failed")
        );
        assert_eq!(
            outcome.details.get("codex_next_action").map(String::as_str),
            Some("check_target")
        );
    }

    // REQ-RUNNERENROLL-86
    #[test]
    fn agent_logins_check_fails_without_recorded_readiness_or_configuration() {
        let host = AgentLoginHost::new();
        let outcome = host.run();
        assert_eq!(
            (outcome.status, outcome.code),
            (Status::Fail, "readiness_unknown")
        );

        // A document the check cannot load says why, as a closed class.
        host.record_codex("ready");
        let document = host.root.path().join("state/.runner-agent-logins.json");
        std::fs::set_permissions(
            &document,
            <std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o644),
        )
        .unwrap();
        let outcome = host.run();
        assert_eq!(
            (outcome.status, outcome.code),
            (Status::Fail, "readiness_unknown")
        );
        assert_eq!(
            outcome.details.get("cause").map(String::as_str),
            Some("permission_denied")
        );

        let outcome = run_agent_logins(None);
        assert_eq!(
            (outcome.status, outcome.code),
            (Status::Fail, "config_required")
        );

        let absent = host.config.with_file_name("no-such-runner.toml");
        let outcome = run_agent_logins(Some(absent));
        assert_eq!(
            (outcome.status, outcome.code),
            (Status::Fail, "config_invalid")
        );
    }

    // REQ-RUNNERENROLL-45
    #[test]
    fn a_default_run_with_configuration_runs_only_the_git_check() {
        let host = AgentLoginHost::new();
        // A readiness document the default run would fail on if it read it.
        host.record_codex("install_failed");

        let report = built_in_registry(Some(host.config.clone()))
            .unwrap()
            .run(&[])
            .unwrap();

        assert_eq!(
            report
                .results
                .iter()
                .map(|result| result.descriptor.id)
                .collect::<Vec<_>>(),
            ["environment.command.git"]
        );
    }
}
