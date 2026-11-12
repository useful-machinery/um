use std::ffi::OsString;
use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::*;
use crate::agent_login::cloud::{CloudFailure, LoginStatus, ProviderLogin};
use crate::agent_login::readiness::readiness_path;
use crate::service::{
    CountedGate, FAKE_AGENT_LOGIN_ID, FAKE_AGENT_LOGIN_SENTINEL, FakeAgentLoginCloud,
    fake_agent_login_material, fake_codex_authorized,
};
use crate::telemetry::{TestCapture, test_recorder};

fn now() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_794_450_000).unwrap()
}

/// Setup for a service whose `HOME`, `CODEX_HOME` when it sets one, and
/// state directory are under a temporary root.
struct Fixture {
    root: tempfile::TempDir,
    cloud: Arc<FakeAgentLoginCloud>,
    gate: Arc<CountedGate>,
    setup: Arc<AgentLoginSetup>,
    recorder: Arc<Recorder>,
    capture: TestCapture,
}

impl Fixture {
    fn new(answer: Result<Vec<ProviderLogin>, CloudFailure>) -> Self {
        Self::serving(answer, false)
    }

    /// Setup for a service that sets `CODEX_HOME` to `service-codex` under
    /// the root when `codex_home` holds.
    fn serving(answer: Result<Vec<ProviderLogin>, CloudFailure>, codex_home: bool) -> Self {
        let root = tempfile::tempdir().unwrap();
        for directory in ["home", "service-codex", "state"] {
            fs::create_dir(root.path().join(directory)).unwrap();
        }
        let state = root.path().join("state");
        fs::set_permissions(&state, fs::Permissions::from_mode(0o700)).unwrap();
        let cloud = FakeAgentLoginCloud::new(answer);
        let gate = Arc::new(CountedGate::idle());
        let (recorder, capture) = test_recorder("rbt_01k0z6r1w8f4jy2m7q9v3x5abc");
        let home = OsString::from(root.path().join("home"));
        let service_codex = OsString::from(root.path().join("service-codex"));
        let environment = ServiceEnvironment::from_lookup(|name| match name {
            "HOME" => Some(home.clone()),
            "CODEX_HOME" if codex_home => Some(service_codex.clone()),
            _ => None,
        });
        let setup = Arc::new(AgentLoginSetup::new(
            SetupDependencies::new(
                Arc::clone(&cloud) as Arc<dyn AgentLoginCloud>,
                environment,
                &state.join("runner-state.json"),
                Arc::clone(&recorder),
            ),
            Arc::clone(&gate) as Arc<dyn AssignmentGate>,
            now,
        ));
        Self {
            root,
            cloud,
            gate,
            setup,
            recorder,
            capture,
        }
    }

    /// The service environment the fixture's setup was given, without a
    /// `CODEX_HOME`.
    fn service(&self) -> ServiceEnvironment {
        let home = OsString::from(self.root.path().join("home"));
        ServiceEnvironment::from_lookup(|name| (name == "HOME").then(|| home.clone()))
    }

    fn directory(&self) -> LoginDirectory {
        LoginDirectory::beside(&self.root.path().join("state/runner-state.json"))
    }

    fn auth_path(&self) -> PathBuf {
        codex::auth_path(&self.directory())
    }

    fn token_path(&self) -> PathBuf {
        self.directory().claude_code_token()
    }

    /// The names under the service's own `HOME` and `CODEX_HOME`, which the
    /// runner never writes.
    fn service_entries(&self) -> usize {
        ["home", "service-codex"]
            .into_iter()
            .map(|directory| {
                fs::read_dir(self.root.path().join(directory))
                    .unwrap()
                    .count()
            })
            .sum()
    }

    fn recorded(&self) -> Option<ReadinessDocument> {
        ReadinessDocument::load(&readiness_path(
            &self.root.path().join("state/runner-state.json"),
        ))
        .unwrap()
    }

    async fn pass(&self) -> ReadinessDocument {
        match self.setup.run_pass().await {
            SetupOutcome::Recorded(document) => document,
            other => panic!("setup did not record readiness: {other:?}"),
        }
    }

    fn setup_records(&self) -> Vec<serde_json::Map<String, serde_json::Value>> {
        self.capture
            .events()
            .into_iter()
            .filter(|event| event["event.name"] == SETUP_EVENT)
            .collect()
    }
}

fn authorized(provider: AgentLoginProvider, generation: u64) -> ProviderLogin {
    ProviderLogin {
        provider,
        agent_login_id: FAKE_AGENT_LOGIN_ID.to_owned(),
        status: LoginStatus::Authorized {
            generation,
            record_schema_version: 1,
        },
    }
}

fn state_of(document: &ReadinessDocument, provider: AgentLoginProvider) -> AgentLoginState {
    document.login(provider).expect("provider readiness").state
}

// REQ-AGENTLOGIN-26, REQ-AGENTLOGIN-30, REQ-TELEMETRY-45, INV-RUNNER-15
#[tokio::test]
async fn an_install_made_before_an_assignment_starts_is_recorded_at_once() {
    let fixture = Fixture::new(Ok(vec![
        authorized(AgentLoginProvider::Codex, 1),
        authorized(AgentLoginProvider::ClaudeCode, 1),
    ]));
    // The pass finds the runner idle, asks for the Codex login and installs
    // it; an assignment starts before it asks for the Claude Code token,
    // which Cloud is therefore not asked to deliver, and so does not record
    // as delivered.
    fixture.gate.idle_calls.store(3, Ordering::SeqCst);

    let outcome = fixture.setup.run_pass().await;
    assert_eq!(outcome, SetupOutcome::Busy(fixture.recorded()));
    assert_eq!(fixture.cloud.deliveries(), 1);

    assert_eq!(
        fs::read(fixture.auth_path()).unwrap(),
        fake_agent_login_material(1)
    );
    assert!(!fixture.token_path().exists());
    let document = fixture.recorded().expect("the install is recorded");
    assert_eq!(
        state_of(&document, AgentLoginProvider::Codex),
        AgentLoginState::Ready
    );
    let installation = document
        .login(AgentLoginProvider::Codex)
        .and_then(LoginReadiness::installation)
        .expect("the Codex installation");
    assert_eq!(installation.installed_path, fixture.auth_path());
    assert_eq!(installation.generation, 1);
    assert!(document.login(AgentLoginProvider::ClaudeCode).is_none());
    assert_eq!(fixture.service_entries(), 0);
    let records = fixture.setup_records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["um.agent_login.codex.state"], "ready");
    assert!(records[0].get("um.agent_login.claude_code.state").is_none());

    // Codex refreshes the login during the assignment. Once the runner is
    // idle again the record vouches for the file: it is not fetched and
    // written again.
    fs::write(fixture.auth_path(), b"{\"refreshed\":true}").unwrap();
    fixture.gate.idle_calls.store(usize::MAX, Ordering::SeqCst);
    let document = fixture.pass().await;

    assert_eq!(
        state_of(&document, AgentLoginProvider::Codex),
        AgentLoginState::Ready
    );
    assert_eq!(
        state_of(&document, AgentLoginProvider::ClaudeCode),
        AgentLoginState::Ready
    );
    assert_eq!(
        fs::read(fixture.auth_path()).unwrap(),
        b"{\"refreshed\":true}"
    );
    // Each login was asked for once: Codex before the assignment, Claude
    // Code after it.
    assert_eq!(fixture.cloud.deliveries(), 2);

    // The owner revokes: the runner removes what it installed.
    fixture.cloud.answer(Ok(vec![
        ProviderLogin {
            status: LoginStatus::Revoked,
            ..authorized(AgentLoginProvider::Codex, 1)
        },
        ProviderLogin {
            status: LoginStatus::Revoked,
            ..authorized(AgentLoginProvider::ClaudeCode, 1)
        },
    ]));
    let document = fixture.pass().await;
    assert_eq!(
        state_of(&document, AgentLoginProvider::Codex),
        AgentLoginState::Revoked
    );
    assert!(!fixture.auth_path().exists());
    assert!(!fixture.token_path().exists());
}

fn write_private(path: &Path, bytes: &[u8]) {
    fs::write(path, bytes).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
}

// REQ-AGENTLOGIN-27
#[tokio::test]
async fn a_service_that_sets_codex_home_is_never_sent_a_codex_login() {
    let fixture = Fixture::serving(fake_codex_authorized(1), true);

    for _pass in 0..2 {
        let document = fixture.pass().await;
        assert_eq!(
            state_of(&document, AgentLoginProvider::Codex),
            AgentLoginState::ForeignLogin
        );
    }

    // The first pass also reports the `foreign_login` it recorded.
    assert_eq!(fixture.cloud.status_requests(), 3);
    assert_eq!(fixture.cloud.deliveries(), 0);
    assert!(!fixture.auth_path().exists());
    assert_eq!(fixture.service_entries(), 0);
}

// REQ-AGENTLOGIN-27, REQ-AGENTLOGIN-30
#[tokio::test]
async fn a_codex_login_the_runner_holds_is_removed_once_the_service_sets_codex_home() {
    let fixture = Fixture::new(fake_codex_authorized(1));
    fixture.pass().await;
    assert!(fixture.auth_path().is_file());

    // The service restarts with a `CODEX_HOME` of its own, over the same
    // state.
    let (recorder, _capture) = test_recorder("rbt_01k0z6r1w8f4jy2m7q9v3x5abc");
    let service_codex = OsString::from(fixture.root.path().join("service-codex"));
    let restarted = Arc::new(AgentLoginSetup::new(
        SetupDependencies::new(
            Arc::clone(&fixture.cloud) as Arc<dyn AgentLoginCloud>,
            ServiceEnvironment::from_lookup(|name| {
                (name == "CODEX_HOME").then(|| service_codex.clone())
            }),
            &fixture.root.path().join("state/runner-state.json"),
            recorder,
        ),
        Arc::new(CountedGate::idle()),
        now,
    ));
    let SetupOutcome::Recorded(document) = restarted.run_pass().await else {
        panic!("setup did not record readiness");
    };

    let codex = document.login(AgentLoginProvider::Codex).unwrap();
    assert_eq!(codex.state, AgentLoginState::ForeignLogin);
    assert_eq!(codex.installation(), None);
    assert!(!fixture.auth_path().exists());
    assert_eq!(fixture.cloud.deliveries(), 1);
    assert_eq!(fixture.service_entries(), 0);
}

// REQ-AGENTLOGIN-26, REQ-AGENTLOGIN-27
#[tokio::test]
async fn a_codex_login_under_the_service_home_changes_nothing() {
    let fixture = Fixture::new(fake_codex_authorized(1));
    let own = fixture.root.path().join("home/.codex/auth.json");
    fs::create_dir(own.parent().unwrap()).unwrap();
    write_private(&own, b"{\"operator\":true}");

    let document = fixture.pass().await;

    assert_eq!(
        state_of(&document, AgentLoginProvider::Codex),
        AgentLoginState::Ready
    );
    assert_eq!(
        fs::read(fixture.auth_path()).unwrap(),
        fake_agent_login_material(1)
    );
    assert_eq!(fs::read(&own).unwrap(), b"{\"operator\":true}");
}

// REQ-AGENTLOGIN-26, REQ-AGENTLOGIN-35
#[tokio::test]
async fn a_login_without_a_record_is_removed_when_the_login_ends() {
    for answer in [
        Ok(vec![
            ProviderLogin {
                status: LoginStatus::Revoked,
                ..authorized(AgentLoginProvider::Codex, 1)
            },
            ProviderLogin {
                status: LoginStatus::Revoked,
                ..authorized(AgentLoginProvider::ClaudeCode, 1)
            },
        ]),
        Ok(Vec::new()),
    ] {
        // The runner kept both logins and stopped before it recorded them.
        let fixture = Fixture::new(answer);
        let directory = fixture.directory();
        fs::create_dir_all(directory.codex_home()).unwrap();
        write_private(&fixture.token_path(), b"sk-ant-oat01-kept-and-unrecorded");
        write_private(&fixture.auth_path(), b"{\"kept\":true}");

        let document = fixture.pass().await;

        assert!(!fixture.token_path().exists());
        assert!(!fixture.auth_path().exists());
        assert_ne!(
            state_of(&document, AgentLoginProvider::Codex),
            AgentLoginState::InstallFailed
        );
    }
}

impl Fixture {
    fn readiness_path(&self) -> PathBuf {
        readiness_path(&self.root.path().join("state/runner-state.json"))
    }

    /// The pass's one `runner.agent_login_setup` record, checked to carry
    /// no path under the fixture's root and no material.
    fn only_record(&self) -> serde_json::Map<String, serde_json::Value> {
        let mut records = self.setup_records();
        assert_eq!(records.len(), 1, "{records:?}");
        let record = records.remove(0);
        let root = self.root.path().to_str().unwrap();
        for value in record.values() {
            let value = value.to_string();
            assert!(!value.contains(root), "{value}");
            assert!(!value.contains(FAKE_AGENT_LOGIN_SENTINEL), "{value}");
        }
        record
    }

    /// Checks that the pass's record names the provider's failed stage and
    /// closed cause, and Cloud's HTTP status where there was one.
    fn assert_failure(
        &self,
        provider: AgentLoginProvider,
        stage: &str,
        cause: &str,
        status: Option<u16>,
    ) {
        let record = self.only_record();
        let key = |name: &str| format!("um.agent_login.{}.{name}", provider.as_str());
        assert_eq!(record[&key("failure_stage")], stage, "{record:?}");
        assert_eq!(record[&key("failure_cause")], cause, "{record:?}");
        assert_eq!(
            record
                .get(&key("http_status"))
                .and_then(serde_json::Value::as_u64),
            status.map(u64::from),
            "{record:?}"
        );
    }

    /// Checks that the pass failed and its record names why.
    fn assert_pass_failure(&self, error_type: &str, cause: &str) {
        let record = self.only_record();
        assert_eq!(record["um.outcome"], "failure", "{record:?}");
        assert_eq!(record["error.type"], error_type, "{record:?}");
        assert_eq!(record["um.failure.cause_type"], cause, "{record:?}");
    }
}

fn set_mode(path: &Path, mode: u32) {
    fs::set_permissions(path, fs::Permissions::from_mode(mode)).unwrap();
}

// REQ-TELEMETRY-45
#[tokio::test]
async fn a_failed_install_or_removal_records_its_stage_and_cause() {
    // Creating the runner's directory: a file occupies it.
    let fixture = Fixture::new(fake_codex_authorized(1));
    fs::write(fixture.directory().path(), b"occupied").unwrap();
    let document = fixture.pass().await;
    assert_eq!(
        state_of(&document, AgentLoginProvider::Codex),
        AgentLoginState::InstallFailed
    );
    fixture.assert_failure(
        AgentLoginProvider::Codex,
        "directory",
        "not_a_directory",
        None,
    );

    // Writing the provider's file: a directory occupies the token's place.
    let fixture = Fixture::new(Ok(vec![authorized(AgentLoginProvider::ClaudeCode, 1)]));
    fs::create_dir_all(fixture.token_path().join("occupied")).unwrap();
    let document = fixture.pass().await;
    assert_eq!(
        state_of(&document, AgentLoginProvider::ClaudeCode),
        AgentLoginState::InstallFailed
    );
    fixture.assert_failure(
        AgentLoginProvider::ClaudeCode,
        "write",
        "is_a_directory",
        None,
    );

    // Removing it: the runner's directory is read-only.
    let fixture = Fixture::new(Ok(Vec::new()));
    fs::create_dir(fixture.directory().path()).unwrap();
    write_private(&fixture.token_path(), b"sk-ant-oat01-kept");
    set_mode(fixture.directory().path(), 0o500);
    let document = fixture.pass().await;
    set_mode(fixture.directory().path(), 0o700);
    assert_eq!(
        state_of(&document, AgentLoginProvider::ClaudeCode),
        AgentLoginState::InstallFailed
    );
    assert!(fixture.token_path().exists());
    fixture.assert_failure(
        AgentLoginProvider::ClaudeCode,
        "remove",
        "permission_denied",
        None,
    );
}

impl Fixture {
    /// Records `logins` as the readiness of an earlier pass.
    fn record(&self, logins: Vec<LoginReadiness>) {
        ReadinessDocument {
            updated_at: now() - time::Duration::minutes(5),
            logins,
            unknown: Vec::new(),
        }
        .write(&self.readiness_path())
        .unwrap();
    }
}

/// An earlier pass's entry for `provider` in `state`, naming the fake agent
/// login when `named` holds.
fn earlier(provider: AgentLoginProvider, state: AgentLoginState, named: bool) -> LoginReadiness {
    LoginReadiness {
        provider,
        state,
        checked_at: Some(now() - time::Duration::minutes(5)),
        login: named.then(|| RecordedLogin::Named(FAKE_AGENT_LOGIN_ID.to_owned())),
    }
}

// REQ-AGENTLOGIN-28, REQ-AGENTLOGIN-31
#[tokio::test]
async fn a_failed_status_request_keeps_a_provider_cloud_did_not_authorize() {
    for state in [
        None,
        Some(AgentLoginState::NotAuthorized),
        Some(AgentLoginState::Revoked),
    ] {
        let fixture = Fixture::new(Err(CloudFailure::Status(404)));
        let logins: Vec<_> = state
            .into_iter()
            .flat_map(|state| {
                AgentLoginProvider::INSTALLABLE
                    .map(|provider| earlier(provider, state, state == AgentLoginState::Revoked))
            })
            .collect();
        fixture.record(logins.clone());

        let document = fixture.pass().await;
        assert_eq!(document.logins, logins, "{state:?}");
        assert_eq!(fixture.recorded().unwrap().logins, logins, "{state:?}");
        assert_eq!(fixture.cloud.status_requests(), 1);
    }

    // With nothing recorded at all, the pass records nothing for either.
    let fixture = Fixture::new(Err(CloudFailure::Transport));
    assert!(fixture.pass().await.logins.is_empty());
}

// REQ-AGENTLOGIN-28
#[tokio::test]
async fn a_failed_status_request_marks_a_provider_cloud_authorized_cloud_unavailable() {
    // An install is kept as it was, and marked `cloud_unavailable`.
    let fixture = Fixture::new(naming(FAKE_AGENT_LOGIN_ID, 1));
    let installed = fixture.pass().await;
    fixture.cloud.answer(Err(CloudFailure::Transport));
    let document = fixture.pass().await;
    for provider in AgentLoginProvider::INSTALLABLE {
        let login = document.login(provider).unwrap();
        assert_eq!(login.state, AgentLoginState::CloudUnavailable);
        assert_eq!(
            login.installation(),
            installed.login(provider).unwrap().installation()
        );
    }
    assert!(fixture.auth_path().is_file());
    assert!(fixture.token_path().is_file());

    // A login Cloud authorized that the runner does not hold is marked
    // `cloud_unavailable`; beside it, one Cloud did not authorize keeps
    // its state.
    for state in [
        AgentLoginState::UnsupportedTarget,
        AgentLoginState::StaleGeneration,
        AgentLoginState::ForeignLogin,
        AgentLoginState::InstallFailed,
        AgentLoginState::CloudUnavailable,
    ] {
        let fixture = Fixture::new(Err(CloudFailure::Transport));
        let not_authorized = earlier(
            AgentLoginProvider::ClaudeCode,
            AgentLoginState::NotAuthorized,
            false,
        );
        fixture.record(vec![
            earlier(AgentLoginProvider::Codex, state, true),
            not_authorized.clone(),
        ]);
        let document = fixture.pass().await;
        let codex = document.login(AgentLoginProvider::Codex).unwrap();
        assert_eq!(codex.state, AgentLoginState::CloudUnavailable, "{state:?}");
        assert_eq!(
            codex.login,
            Some(RecordedLogin::Named(FAKE_AGENT_LOGIN_ID.to_owned()))
        );
        assert_eq!(
            document.login(AgentLoginProvider::ClaudeCode),
            Some(&not_authorized)
        );
    }
}

// REQ-TELEMETRY-45
#[tokio::test]
async fn a_failed_cloud_request_records_its_stage_and_cause() {
    for (failure, cause, status) in [
        (CloudFailure::Status(503), "http_status", Some(503)),
        (CloudFailure::Transport, "transport", None),
        (CloudFailure::InvalidResponse, "invalid_response", None),
    ] {
        let fixture = Fixture::new(Err(failure));
        fixture.record(
            AgentLoginProvider::INSTALLABLE
                .map(|provider| earlier(provider, AgentLoginState::StaleGeneration, true))
                .to_vec(),
        );
        let document = fixture.pass().await;
        for provider in AgentLoginProvider::INSTALLABLE {
            assert_eq!(
                state_of(&document, provider),
                AgentLoginState::CloudUnavailable
            );
            fixture.assert_failure(provider, "status", cause, status);
        }

        let fixture = Fixture::new(fake_codex_authorized(1));
        fixture.cloud.fail_deliveries(Some(failure));
        let document = fixture.pass().await;
        assert_eq!(
            state_of(&document, AgentLoginProvider::Codex),
            AgentLoginState::CloudUnavailable
        );
        fixture.assert_failure(AgentLoginProvider::Codex, "delivery", cause, status);
        assert!(
            fixture
                .only_record()
                .keys()
                .all(|key| !key.starts_with("um.agent_login.claude_code.failure"))
        );
    }
}

// REQ-TELEMETRY-45
#[tokio::test]
async fn a_pass_that_cannot_read_or_replace_its_record_or_start_records_why() {
    // Loading the readiness document: a directory the runner cannot remove
    // stands in its place.
    let fixture = Fixture::new(Ok(Vec::new()));
    fs::create_dir_all(fixture.readiness_path().join("occupied")).unwrap();
    assert_eq!(fixture.setup.run_pass().await, SetupOutcome::Failed);
    assert_eq!(fixture.cloud.status_requests(), 0);
    fixture.assert_pass_failure("agent_login_readiness_load_failed", "permission_denied");

    // Writing it: the state directory is read-only.
    let fixture = Fixture::new(Ok(Vec::new()));
    let state = fixture.root.path().join("state");
    set_mode(&state, 0o500);
    let outcome = fixture.setup.run_pass().await;
    set_mode(&state, 0o700);
    assert_eq!(outcome, SetupOutcome::Failed);
    fixture.assert_pass_failure("agent_login_readiness_write_failed", "permission_denied");

    // Spawning the setup thread.
    let fixture = Fixture::new(Ok(Vec::new()));
    let outcome = fixture
        .setup
        .run_pass_on(|_| Err(std::io::ErrorKind::WouldBlock.into()))
        .await;
    assert_eq!(outcome, SetupOutcome::Failed);
    assert_eq!(fixture.cloud.status_requests(), 0);
    fixture.assert_pass_failure("agent_login_setup_spawn_failed", "io_error");
}

// REQ-AGENTLOGIN-28, REQ-AGENTLOGIN-30, REQ-TELEMETRY-45
#[tokio::test]
async fn a_pass_over_a_record_it_cannot_load_proceeds_as_if_nothing_were_recorded() {
    let revoked = || {
        Ok(AgentLoginProvider::INSTALLABLE
            .into_iter()
            .map(|provider| ProviderLogin {
                status: LoginStatus::Revoked,
                ..authorized(provider, 1)
            })
            .collect())
    };
    let valid = serde_json::to_vec(&serde_json::json!({
        "schemaVersion": 1,
        "updatedAt": "2026-11-12T01:00:00Z",
        "providers": [{"provider": "codex", "state": "ready"}],
    }))
    .unwrap();
    let newer = serde_json::to_vec(&serde_json::json!({
        "schemaVersion": 2,
        "updatedAt": "2026-11-12T01:00:00Z",
        "providers": [],
    }))
    .unwrap();
    for (mode, contents, cause) in [
        (0o644, valid, "permission_denied"),
        (0o600, b"{\"schemaVersion\": 1,".to_vec(), "invalid_data"),
        (0o600, newer, "invalid_data"),
    ] {
        // The runner holds both logins; its record of them cannot be loaded.
        let fixture = Fixture::new(revoked());
        let directory = fixture.directory();
        fs::create_dir_all(directory.codex_home()).unwrap();
        write_private(&fixture.auth_path(), b"{\"kept\":true}");
        write_private(&fixture.token_path(), b"sk-ant-oat01-kept");
        write_private(&fixture.readiness_path(), &contents);
        set_mode(&fixture.readiness_path(), mode);

        let document = fixture.pass().await;

        assert!(!fixture.auth_path().exists(), "{cause}");
        assert!(!fixture.token_path().exists(), "{cause}");
        for provider in AgentLoginProvider::INSTALLABLE {
            assert_eq!(state_of(&document, provider), AgentLoginState::Revoked);
        }
        assert_eq!(fixture.recorded().as_ref(), Some(&document), "{cause}");
        let failures = fixture
            .setup_records()
            .into_iter()
            .filter(|record| record.contains_key("error.type"))
            .collect::<Vec<_>>();
        assert_eq!(failures.len(), 1, "{failures:?}");
        assert_eq!(
            failures[0]["error.type"],
            "agent_login_readiness_load_failed"
        );
        assert_eq!(failures[0]["um.failure.cause_type"], cause);

        // The next pass reads the record it wrote and fails nothing.
        let records = fixture.setup_records().len();
        assert_eq!(fixture.pass().await, document, "{cause}");
        assert_eq!(fixture.setup_records().len(), records, "{cause}");
    }
}

// REQ-AGENTLOGIN-28
#[tokio::test]
async fn a_pass_keeps_the_entry_of_a_provider_this_build_does_not_know() {
    let fixture = Fixture::new(fake_codex_authorized(1));
    let gemini = serde_json::json!({
        "provider": "gemini",
        "state": "ready",
        "agentLoginId": FAKE_AGENT_LOGIN_ID,
    });
    write_private(
        &fixture.readiness_path(),
        &serde_json::to_vec(&serde_json::json!({
            "schemaVersion": 1,
            "updatedAt": "2026-11-12T01:00:00Z",
            "providers": [gemini],
        }))
        .unwrap(),
    );

    let document = fixture.pass().await;

    assert_eq!(
        state_of(&document, AgentLoginProvider::Codex),
        AgentLoginState::Ready
    );
    let written: serde_json::Value =
        serde_json::from_slice(&fs::read(fixture.readiness_path()).unwrap()).unwrap();
    assert_eq!(
        written["providers"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|entry| entry["provider"] == "gemini")
            .collect::<Vec<_>>(),
        [&gemini]
    );
}

// REQ-AGENTLOGIN-26
#[tokio::test]
async fn an_install_whose_file_is_gone_is_installed_again() {
    let fixture = Fixture::new(Ok(vec![
        authorized(AgentLoginProvider::Codex, 1),
        authorized(AgentLoginProvider::ClaudeCode, 1),
    ]));
    fixture.pass().await;
    assert_eq!(fixture.cloud.deliveries(), 2);

    // While both files are there, a pass asks for no delivery.
    let document = fixture.pass().await;
    for provider in AgentLoginProvider::INSTALLABLE {
        assert_eq!(state_of(&document, provider), AgentLoginState::Ready);
    }
    assert_eq!(fixture.cloud.deliveries(), 2);

    // A file deleted under the runner, as `codex logout` with the runner's
    // Codex home deletes `auth.json`, is fetched and installed again.
    for (provider, path) in [
        (AgentLoginProvider::Codex, fixture.auth_path()),
        (AgentLoginProvider::ClaudeCode, fixture.token_path()),
    ] {
        fs::remove_file(&path).unwrap();
        let deliveries = fixture.cloud.deliveries();

        let document = fixture.pass().await;

        assert_eq!(state_of(&document, provider), AgentLoginState::Ready);
        assert_eq!(fixture.cloud.deliveries(), deliveries + 1);
        assert_eq!(fs::read(&path).unwrap(), fake_agent_login_material(1));
        let installation = document
            .login(provider)
            .and_then(LoginReadiness::installation)
            .expect("the installation");
        assert_eq!(installation.generation, 1);
        assert_eq!(installation.installed_path, path);
    }
}

/// A second agent login of the same providers.
const OTHER_AGENT_LOGIN_ID: &str = "agl_01k0z6r1w8f4jy2m7q9v3x5xyz";

/// Cloud's answer authorizing `agent_login_id` for every installable
/// provider at `generation`.
fn naming(agent_login_id: &str, generation: u64) -> Result<Vec<ProviderLogin>, CloudFailure> {
    Ok(AgentLoginProvider::INSTALLABLE
        .into_iter()
        .map(|provider| ProviderLogin {
            agent_login_id: agent_login_id.to_owned(),
            ..authorized(provider, generation)
        })
        .collect())
}

/// What the runner held when it asked for a delivery.
#[derive(Debug, Eq, PartialEq)]
struct AtDelivery {
    provider: AgentLoginProvider,
    /// Whether the file watched for the provider was there.
    held: bool,
    /// The agent login the readiness document recorded as installed.
    installed: Option<String>,
}

/// The shared fake Cloud, noting each status request and at each delivery
/// what the runner held then, failing each delivery with `failure` and
/// each status request after the first with `later_status_failure` when
/// they are set.
struct ObservingCloud {
    inner: Arc<FakeAgentLoginCloud>,
    watched: Vec<(AgentLoginProvider, PathBuf)>,
    readiness: PathBuf,
    failure: Option<DeliveryFailure>,
    later_status_failure: Option<CloudFailure>,
    seen: Mutex<Vec<AtDelivery>>,
    reports: Mutex<Vec<StatusRequest>>,
}

impl AgentLoginCloud for ObservingCloud {
    fn status(&self, request: &StatusRequest) -> Result<Vec<ProviderLogin>, CloudFailure> {
        let mut reports = self.reports.lock().unwrap();
        reports.push(request.clone());
        match self.later_status_failure {
            Some(failure) if reports.len() > 1 => Err(failure),
            _ => self.inner.status(request),
        }
    }

    fn delivery(&self, request: &DeliveryRequest) -> Result<Delivery, DeliveryFailure> {
        let held = self
            .watched
            .iter()
            .filter(|(provider, _)| *provider == request.provider)
            .any(|(_, path)| path.symlink_metadata().is_ok());
        let installed = ReadinessDocument::load(&self.readiness)
            .unwrap()
            .and_then(|document| {
                document
                    .login(request.provider)
                    .and_then(LoginReadiness::installation)
                    .map(|installation| installation.agent_login_id.clone())
            });
        self.seen.lock().unwrap().push(AtDelivery {
            provider: request.provider,
            held,
            installed,
        });
        match self.failure {
            Some(failure) => Err(failure),
            None => self.inner.delivery(request),
        }
    }

    fn follow_credential(&self, _endpoint: &Url, _runner_credential: &Credential) {}
}

impl Fixture {
    /// Watches `watched` at each delivery of a pass over the fixture's
    /// state, failing each delivery with `failure` when it is set.
    fn observing(
        &self,
        watched: Vec<(AgentLoginProvider, PathBuf)>,
        failure: Option<DeliveryFailure>,
    ) -> Arc<ObservingCloud> {
        Arc::new(ObservingCloud {
            watched,
            failure,
            ..self.reporting(None)
        })
    }

    /// Notes each status request of a pass over the fixture's state,
    /// failing each after the first with `later_status_failure` when it is
    /// set.
    fn reporting(&self, later_status_failure: Option<CloudFailure>) -> ObservingCloud {
        ObservingCloud {
            inner: Arc::clone(&self.cloud),
            watched: Vec::new(),
            readiness: self.readiness_path(),
            failure: None,
            later_status_failure,
            seen: Mutex::new(Vec::new()),
            reports: Mutex::new(Vec::new()),
        }
    }

    /// One pass over the fixture's service and state that asks `cloud`.
    async fn pass_asking(&self, cloud: Arc<ObservingCloud>) -> ReadinessDocument {
        let home = OsString::from(self.root.path().join("home"));
        let setup = Arc::new(AgentLoginSetup::new(
            SetupDependencies::new(
                cloud,
                ServiceEnvironment::from_lookup(|name| (name == "HOME").then(|| home.clone())),
                &self.root.path().join("state/runner-state.json"),
                Arc::clone(&self.recorder),
            ),
            Arc::clone(&self.gate) as Arc<dyn AssignmentGate>,
            now,
        ));
        match setup.run_pass().await {
            SetupOutcome::Recorded(document) => document,
            other => panic!("setup did not record readiness: {other:?}"),
        }
    }
}

// REQ-AGENTLOGIN-30, INV-RUNNER-15
#[tokio::test]
async fn cloud_naming_another_agent_login_ends_the_earlier_install_before_it_asks() {
    for (failure, state) in [
        (
            DeliveryFailure::Refused(DeliveryRefusal::Unauthorized),
            AgentLoginState::NotAuthorized,
        ),
        (
            DeliveryFailure::Refused(DeliveryRefusal::Revoked),
            AgentLoginState::Revoked,
        ),
        (
            DeliveryFailure::Cloud(CloudFailure::Transport),
            AgentLoginState::CloudUnavailable,
        ),
    ] {
        let fixture = Fixture::new(naming(FAKE_AGENT_LOGIN_ID, 1));
        fixture.pass().await;
        assert!(fixture.auth_path().is_file());
        assert!(fixture.token_path().is_file());

        // Cloud now names another agent login for each provider, whose
        // delivery fails.
        fixture.cloud.answer(naming(OTHER_AGENT_LOGIN_ID, 1));
        let cloud = fixture.observing(
            vec![
                (AgentLoginProvider::Codex, fixture.auth_path()),
                (AgentLoginProvider::ClaudeCode, fixture.token_path()),
            ],
            Some(failure),
        );
        let document = fixture.pass_asking(Arc::clone(&cloud)).await;

        // The earlier login's file was gone, and recorded gone, before the
        // other was asked for.
        assert_eq!(
            *cloud.seen.lock().unwrap(),
            AgentLoginProvider::INSTALLABLE
                .into_iter()
                .map(|provider| AtDelivery {
                    provider,
                    held: false,
                    installed: None,
                })
                .collect::<Vec<_>>(),
            "{failure:?}"
        );
        assert!(!fixture.auth_path().exists(), "{failure:?}");
        assert!(!fixture.token_path().exists(), "{failure:?}");
        assert_eq!(fixture.recorded().as_ref(), Some(&document));
        for provider in AgentLoginProvider::INSTALLABLE {
            let login = document.login(provider).unwrap();
            assert_eq!(login.state, state, "{failure:?}");
            assert_eq!(login.installation(), None, "{failure:?}");
            assert_eq!(agent_login_id(login), Some(OTHER_AGENT_LOGIN_ID));
        }
    }
}

// REQ-AGENTLOGIN-30, REQ-AGENTLOGIN-26
#[tokio::test]
async fn an_install_recorded_elsewhere_is_ended_before_the_login_is_installed_again() {
    let fixture = Fixture::new(fake_codex_authorized(1));
    fixture.pass().await;
    // An install recorded at another file in the runner's directory is
    // removed; one recorded outside it, as an older build's, is forgotten
    // and left.
    for (earlier, removed) in [
        (fixture.directory().path().join("earlier-auth.json"), true),
        (fixture.root.path().join("home/auth.json"), false),
    ] {
        write_private(&earlier, b"{\"earlier\":true}");
        let mut document = fixture.recorded().unwrap();
        for login in &mut document.logins {
            if let Some(RecordedLogin::Installed(installation)) = &mut login.login {
                installation.installed_path.clone_from(&earlier);
            }
        }
        document.write(&fixture.readiness_path()).unwrap();
        let cloud = fixture.observing(vec![(AgentLoginProvider::Codex, earlier.clone())], None);

        let document = fixture.pass_asking(Arc::clone(&cloud)).await;

        assert_eq!(
            *cloud.seen.lock().unwrap(),
            [AtDelivery {
                provider: AgentLoginProvider::Codex,
                held: !removed,
                installed: None,
            }],
            "{earlier:?}"
        );
        assert_eq!(earlier.exists(), !removed);
        assert_eq!(
            state_of(&document, AgentLoginProvider::Codex),
            AgentLoginState::Ready
        );
        let installation = document
            .login(AgentLoginProvider::Codex)
            .and_then(LoginReadiness::installation)
            .expect("the Codex installation");
        assert_eq!(installation.installed_path, fixture.auth_path());
        assert_eq!(
            fs::read(fixture.auth_path()).unwrap(),
            fake_agent_login_material(1)
        );
    }
}

// REQ-AGENTLOGIN-32, INV-RUNNER-15
#[tokio::test]
async fn a_pass_first_removes_the_temporary_files_its_killed_writes_left() {
    let fixture = Fixture::new(Ok(vec![
        authorized(AgentLoginProvider::Codex, 1),
        authorized(AgentLoginProvider::ClaudeCode, 1),
    ]));
    let directory = fixture.directory();
    let state = fixture.root.path().join("state");
    fs::create_dir_all(directory.codex_home()).unwrap();
    // What a write killed between creating its temporary file and the
    // rename leaves, as `<prefix>.<pid>.<sequence>`.
    let leftovers = [
        directory.codex_home().join(".um-auth.json.tmp.4242.0"),
        directory.codex_home().join(".um-config.toml.tmp.4242.1"),
        directory.path().join(".um-claude-code.token.tmp.4242.2"),
        state.join(".runner-agent-logins.json.tmp.4242.3"),
    ];
    for leftover in &leftovers {
        write_private(leftover, FAKE_AGENT_LOGIN_SENTINEL.as_bytes());
    }
    // Enrollment's own temporary file, files of other names, and a link of
    // a swept name are not the pass's to remove; the link is not followed.
    let outside = fixture.root.path().join("home/outside");
    write_private(&outside, b"outside");
    let link = directory.path().join(".um-claude-code.token.tmp.4242.4");
    std::os::unix::fs::symlink(&outside, &link).unwrap();
    let kept = [
        state.join(".runner-state.tmp.4242.5"),
        state.join(".runner-agent-logins.json.tmp.notes"),
        directory.path().join("notes"),
        directory.codex_home().join(".um-auth.json.tmp"),
    ];
    for other in &kept {
        write_private(other, b"other");
    }

    // An assignment starts before the pass installs anything: the leftovers
    // are already gone.
    fixture.gate.idle_calls.store(1, Ordering::SeqCst);
    assert_eq!(fixture.setup.run_pass().await, SetupOutcome::Busy(None));
    assert_eq!(fixture.cloud.deliveries(), 0);
    assert!(!fixture.auth_path().exists());
    assert!(!fixture.token_path().exists());
    for leftover in &leftovers {
        assert!(!leftover.exists(), "{leftover:?}");
    }
    for other in kept.iter().chain([&outside]) {
        assert!(other.is_file(), "{other:?}");
    }
    assert!(
        fs::symlink_metadata(&link)
            .unwrap()
            .file_type()
            .is_symlink()
    );

    // Once idle the pass installs both logins and leaves the other files.
    fixture.gate.idle_calls.store(usize::MAX, Ordering::SeqCst);
    let document = fixture.pass().await;
    for provider in AgentLoginProvider::INSTALLABLE {
        assert_eq!(state_of(&document, provider), AgentLoginState::Ready);
    }
    for other in kept.iter().chain([&outside]) {
        assert!(other.is_file(), "{other:?}");
    }
}

impl ObservingCloud {
    fn reports(&self) -> Vec<StatusRequest> {
        self.reports.lock().unwrap().clone()
    }
}

/// Asserts that `report` names, for every installable provider, `state`
/// and the fake agent login installed at `generation` when there is one.
fn assert_reported(
    report: &StatusRequest,
    state: Option<AgentLoginState>,
    generation: Option<u64>,
) {
    let installed = generation.map(|generation| InstalledLogin {
        agent_login_id: FAKE_AGENT_LOGIN_ID.to_owned(),
        generation,
    });
    assert_eq!(
        report.providers,
        AgentLoginProvider::INSTALLABLE
            .into_iter()
            .map(|provider| ProviderReport {
                provider,
                installed: installed.clone(),
                state,
            })
            .collect::<Vec<_>>()
    );
}

// REQ-AGENTLOGIN-17, SC-AGENTLOGIN-1
#[tokio::test]
async fn a_pass_reports_the_readiness_it_changed_before_it_ends() {
    let fixture = Fixture::new(naming(FAKE_AGENT_LOGIN_ID, 1));
    let cloud = Arc::new(fixture.reporting(None));
    fixture.pass_asking(Arc::clone(&cloud)).await;

    // The pass asked holding nothing, and then told Cloud what it installed.
    let reports = cloud.reports();
    assert_eq!(reports.len(), 2, "{reports:?}");
    assert_reported(&reports[0], None, None);
    assert_reported(&reports[1], Some(AgentLoginState::Ready), Some(1));

    // A pass that changes nothing asks once, reporting the same.
    let cloud = Arc::new(fixture.reporting(None));
    fixture.pass_asking(Arc::clone(&cloud)).await;
    assert_eq!(cloud.reports(), [reports[1].clone()]);
}

// REQ-AGENTLOGIN-17, REQ-AGENTLOGIN-28
#[tokio::test]
async fn the_first_pass_to_reach_cloud_after_an_outage_reports_the_state_it_reaches() {
    let fixture = Fixture::new(naming(FAKE_AGENT_LOGIN_ID, 1));
    fixture.pass().await;

    // Cloud did not answer, so the pass tells it nothing more.
    fixture.cloud.answer(Err(CloudFailure::Transport));
    let cloud = Arc::new(fixture.reporting(None));
    let document = fixture.pass_asking(Arc::clone(&cloud)).await;
    for provider in AgentLoginProvider::INSTALLABLE {
        assert_eq!(
            state_of(&document, provider),
            AgentLoginState::CloudUnavailable
        );
    }
    assert_eq!(cloud.reports().len(), 1);

    fixture.cloud.answer(naming(FAKE_AGENT_LOGIN_ID, 1));
    let cloud = Arc::new(fixture.reporting(None));
    fixture.pass_asking(Arc::clone(&cloud)).await;
    let reports = cloud.reports();
    assert_eq!(reports.len(), 2, "{reports:?}");
    assert_reported(&reports[1], Some(AgentLoginState::Ready), Some(1));
}

// REQ-AGENTLOGIN-17, REQ-TELEMETRY-45
#[tokio::test]
async fn a_report_cloud_does_not_take_changes_nothing_the_pass_recorded() {
    for (failure, cause, status) in [
        (CloudFailure::Status(503), "http_status", Some(503)),
        (CloudFailure::Transport, "transport", None),
    ] {
        let fixture = Fixture::new(naming(FAKE_AGENT_LOGIN_ID, 1));
        let cloud = Arc::new(fixture.reporting(Some(failure)));
        let document = fixture.pass_asking(Arc::clone(&cloud)).await;

        assert_eq!(cloud.reports().len(), 2);
        for provider in AgentLoginProvider::INSTALLABLE {
            assert_eq!(state_of(&document, provider), AgentLoginState::Ready);
        }
        assert_eq!(fixture.recorded().as_ref(), Some(&document));
        // The pass's one record names the report's failure beside the
        // state it recorded.
        fixture.assert_failure(AgentLoginProvider::Codex, "status", cause, status);
        assert_eq!(fixture.only_record()["um.agent_login.codex.state"], "ready");
    }
}

// REQ-AGENTLOGIN-26, REQ-AGENTLOGIN-34
#[tokio::test]
async fn a_token_file_the_runner_would_not_offer_is_installed_again() {
    let fixture = Fixture::new(Ok(vec![authorized(AgentLoginProvider::ClaudeCode, 1)]));
    fixture.pass().await;
    assert_eq!(fixture.cloud.deliveries(), 1);
    let directory = fixture.directory();
    assert!(claude_code::offered_token(&fixture.service(), &directory).is_some());

    // Something widens the file's mode: admission reads no token from it, so
    // the install is no longer held and `ready` would promise a login no
    // step receives.
    set_mode(&fixture.token_path(), 0o644);
    assert!(claude_code::offered_token(&fixture.service(), &directory).is_none());

    let document = fixture.pass().await;

    assert_eq!(fixture.cloud.deliveries(), 2);
    assert_eq!(
        state_of(&document, AgentLoginProvider::ClaudeCode),
        AgentLoginState::Ready
    );
    assert_eq!(
        fs::metadata(fixture.token_path())
            .unwrap()
            .permissions()
            .mode()
            & 0o7777,
        0o600
    );
    assert!(claude_code::offered_token(&fixture.service(), &directory).is_some());
}

// REQ-AGENTLOGIN-30, REQ-RUNNERENROLL-86
#[tokio::test]
async fn a_file_where_the_runners_directory_belongs_holds_no_login_to_end() {
    for answer in [
        Ok(Vec::new()),
        Ok(vec![ProviderLogin {
            status: LoginStatus::Revoked,
            ..authorized(AgentLoginProvider::Codex, 1)
        }]),
    ] {
        // Cloud authorizes nothing for the runner, and a file sits where its
        // directory belongs: there is no login in it to remove, so nothing
        // fails and the runner does not fail readiness.
        let fixture = Fixture::new(answer);
        write_private(fixture.directory().path(), b"not a directory");

        let document = fixture.pass().await;

        for provider in AgentLoginProvider::INSTALLABLE {
            assert!(
                !state_of(&document, provider).fails_readiness(),
                "{provider:?}: {:?}",
                state_of(&document, provider)
            );
        }
        assert!(
            fixture.setup_records().iter().all(|record| !record
                .contains_key("um.agent_login.codex.failure_stage")
                && !record.contains_key("um.agent_login.claude_code.failure_stage")),
            "{:?}",
            fixture.setup_records()
        );
    }
}
