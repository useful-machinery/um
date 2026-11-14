use std::ffi::OsString;
use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use super::*;
use crate::agent_login::check::LoginCheckOutcome;
use crate::agent_login::cloud::{
    CloudFailure, GrantTokenFailure, GrantTokenRequest, IssuedToken, LoginStatus, ProviderLogin,
};
use crate::agent_login::readiness::readiness_path;
use crate::control_protocol::AgentLoginCheckCause;
use crate::service::{
    CountedGate, FAKE_AGENT_LOGIN_ID, FAKE_AGENT_LOGIN_SENTINEL, FakeAgentLoginCloud,
    FakeLoginCheck, fake_agent_login_material, fake_codex_authorized,
};
use crate::telemetry::{TestCapture, test_recorder};

/// The boot and the assignment an admission's setup is made for.
const BOOT: &str = "rbt_01k0z6r1w8f4jy2m7q9v3x5abe";
const ASSIGNMENT: &str = "asn_01k0z6r1w8f4jy2m7q9v3x5abc";

fn now() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_794_450_000).unwrap()
}

/// Setup for a service whose `HOME`, `CODEX_HOME` when it sets one, and
/// state directory are under a temporary root.
struct Fixture {
    root: tempfile::TempDir,
    cloud: Arc<FakeAgentLoginCloud>,
    check: Arc<FakeLoginCheck>,
    gate: Arc<CountedGate>,
    setup: Arc<AgentLoginSetup>,
    recorder: Arc<Recorder>,
    capture: TestCapture,
    /// Seconds the setup's clock reads after `now()`.
    clock: Arc<AtomicI64>,
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
        let check = FakeLoginCheck::new();
        let gate = Arc::new(CountedGate::idle());
        let (recorder, capture) = test_recorder("rbt_01k0z6r1w8f4jy2m7q9v3x5abc");
        let clock = Arc::new(AtomicI64::new(0));
        let elapsed = Arc::clone(&clock);
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
                Arc::clone(&check) as Arc<dyn LoginCheck>,
                environment,
                &state.join("runner-state.json"),
                Arc::clone(&recorder),
            ),
            Arc::clone(&gate) as Arc<dyn AssignmentGate>,
            move || now() + time::Duration::seconds(elapsed.load(Ordering::SeqCst)),
        ));
        Self {
            root,
            cloud,
            check,
            gate,
            setup,
            recorder,
            capture,
            clock,
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

// REQ-GRANT-36, REQ-GRANT-40, REQ-TELEMETRY-45, INV-RUNNER-15
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

// REQ-GRANT-37
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

// REQ-GRANT-37, REQ-GRANT-40
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
            Arc::clone(&fixture.check) as Arc<dyn LoginCheck>,
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
    assert!(!fixture.directory().codex_home().exists());
    assert_eq!(fixture.cloud.deliveries(), 1);
    assert_eq!(fixture.service_entries(), 0);
}

// REQ-GRANT-36, REQ-GRANT-37
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

// REQ-GRANT-36, REQ-GRANT-45
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
        selected: readiness::selected_by_state(state),
        last_use: None,
    }
}

// REQ-GRANT-38, REQ-GRANT-41
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

// REQ-GRANT-38
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

// REQ-GRANT-38
#[tokio::test]
async fn a_pass_records_whether_cloud_names_an_authorization_in_force() {
    for (answer, selected) in [
        (
            codex_answer(authorized(AgentLoginProvider::Codex, 1).status),
            true,
        ),
        (codex_answer(LoginStatus::Unauthorized), true),
        (codex_answer(LoginStatus::UnsupportedTarget), true),
        (codex_answer(LoginStatus::Revoked), false),
        (Ok(Vec::new()), false),
    ] {
        let fixture = Fixture::new(answer.clone());
        let document = fixture.pass().await;
        let codex = document.login(AgentLoginProvider::Codex).unwrap();
        assert_eq!(codex.selected, selected, "{answer:?}");
        assert_eq!(fixture.recorded(), Some(document), "{answer:?}");
    }

    // A delivery refused as `revoked` ends the authorization Cloud named;
    // one refused as `unauthorized` does not.
    for (refusal, selected) in [
        (DeliveryRefusal::Revoked, false),
        (DeliveryRefusal::Unauthorized, true),
    ] {
        let fixture = Fixture::new(codex_answer(
            authorized(AgentLoginProvider::Codex, 1).status,
        ));
        let cloud = fixture.observing(Vec::new(), Some(DeliveryFailure::Refused(refusal)));
        let document = fixture.pass_asking(cloud).await;
        let codex = document.login(AgentLoginProvider::Codex).unwrap();
        assert_eq!(codex.selected, selected, "{refusal:?}");
    }
}

// REQ-GRANT-38
#[tokio::test]
async fn a_pass_that_cloud_does_not_answer_keeps_whether_the_provider_is_selected() {
    for state in [
        AgentLoginState::NotAuthorized,
        AgentLoginState::InstallFailed,
    ] {
        for selected in [true, false] {
            let fixture = Fixture::new(Err(CloudFailure::Transport));
            fixture.record(vec![LoginReadiness {
                selected,
                ..earlier(AgentLoginProvider::Codex, state, true)
            }]);
            let document = fixture.pass().await;
            let codex = document.login(AgentLoginProvider::Codex).unwrap();
            assert_eq!(codex.selected, selected, "{state:?}");
            assert_eq!(fixture.recorded(), Some(document), "{state:?}");
        }
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
        .set_up_on(SetupCaller::Schedule, |_| {
            Err(std::io::ErrorKind::WouldBlock.into())
        })
        .await;
    assert_eq!(outcome.outcome, SetupOutcome::Failed);
    assert_eq!(fixture.cloud.status_requests(), 0);
    fixture.assert_pass_failure("agent_login_setup_spawn_failed", "io_error");
}

// REQ-GRANT-38, REQ-GRANT-40, REQ-TELEMETRY-45
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

// REQ-GRANT-38
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

// REQ-GRANT-36
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

    fn grant_token(&self, request: &GrantTokenRequest) -> Result<IssuedToken, GrantTokenFailure> {
        self.inner.grant_token(request)
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
                Arc::clone(&self.check) as Arc<dyn LoginCheck>,
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

// REQ-GRANT-40, INV-RUNNER-15
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

// REQ-GRANT-40, REQ-GRANT-36
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

// REQ-GRANT-42, INV-RUNNER-15
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
                last_use: None,
            })
            .collect::<Vec<_>>()
    );
}

// REQ-GRANT-27, SC-GRANT-2
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

// REQ-GRANT-27, REQ-GRANT-38
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

// REQ-GRANT-27, REQ-TELEMETRY-45
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

// REQ-GRANT-36, REQ-GRANT-44
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

// REQ-GRANT-40, REQ-RUNNERENROLL-86
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

/// What `report` says of the runner's Codex login.
fn codex_report(report: &StatusRequest) -> &ProviderReport {
    report
        .providers
        .iter()
        .find(|provider| provider.provider == AgentLoginProvider::Codex)
        .expect("the Codex report")
}

// REQ-GRANT-50, REQ-GRANT-27
#[tokio::test]
async fn a_login_the_closing_answer_no_longer_authorizes_is_removed_before_the_pass_ends() {
    let codex = |status| ProviderLogin {
        status,
        ..authorized(AgentLoginProvider::Codex, 3)
    };
    for (closing, state, named) in [
        (
            Ok(vec![codex(LoginStatus::Revoked)]),
            AgentLoginState::Revoked,
            Some(FAKE_AGENT_LOGIN_ID),
        ),
        (Ok(Vec::new()), AgentLoginState::NotAuthorized, None),
        (
            Ok(vec![codex(LoginStatus::Unauthorized)]),
            AgentLoginState::NotAuthorized,
            Some(FAKE_AGENT_LOGIN_ID),
        ),
        (
            Ok(vec![ProviderLogin {
                agent_login_id: OTHER_AGENT_LOGIN_ID.to_owned(),
                ..authorized(AgentLoginProvider::Codex, 3)
            }]),
            AgentLoginState::NotAuthorized,
            Some(OTHER_AGENT_LOGIN_ID),
        ),
    ] {
        // Cloud authorizes generation 3 when the pass asks, and has ended
        // the authorization by the time the pass reports installing it.
        let fixture = Fixture::new(Ok(Vec::new()));
        fixture
            .cloud
            .answer_next_then(fake_codex_authorized(3), closing.clone());
        let cloud = Arc::new(fixture.reporting(None));

        let document = fixture.pass_asking(Arc::clone(&cloud)).await;

        assert!(!fixture.auth_path().exists(), "{closing:?}");
        let login = document.login(AgentLoginProvider::Codex).unwrap();
        assert_eq!(login.state, state, "{closing:?}");
        assert_eq!(login.installation(), None, "{closing:?}");
        assert_eq!(agent_login_id(login), named, "{closing:?}");
        assert_eq!(fixture.recorded().as_ref(), Some(&document));
        // The closing answer asked for no delivery, and the removal was
        // reported in a third request whose answer is not acted on.
        assert_eq!(fixture.cloud.deliveries(), 1, "{closing:?}");
        let reports = cloud.reports();
        assert_eq!(reports.len(), 3, "{reports:?}");
        assert_eq!(
            codex_report(&reports[1]).installed,
            Some(InstalledLogin {
                agent_login_id: FAKE_AGENT_LOGIN_ID.to_owned(),
                generation: 3,
            })
        );
        assert_eq!(
            *codex_report(&reports[2]),
            ProviderReport {
                provider: AgentLoginProvider::Codex,
                installed: None,
                state: Some(state),
                last_use: None,
            },
            "{closing:?}"
        );
        assert_eq!(
            fixture.only_record()["um.agent_login.codex.state"],
            state.as_str()
        );
    }
}

// REQ-GRANT-50, REQ-GRANT-27
#[tokio::test]
async fn a_closing_answer_that_still_authorizes_the_login_leaves_it_installed() {
    // A newer generation waits for the next pass; the same generation
    // changes nothing, so the pass reports nothing more.
    for closing in [3, 4] {
        let fixture = Fixture::new(Ok(Vec::new()));
        fixture
            .cloud
            .answer_next_then(fake_codex_authorized(3), fake_codex_authorized(closing));
        let cloud = Arc::new(fixture.reporting(None));

        let document = fixture.pass_asking(Arc::clone(&cloud)).await;

        assert_eq!(
            fs::read(fixture.auth_path()).unwrap(),
            fake_agent_login_material(3)
        );
        assert_eq!(
            state_of(&document, AgentLoginProvider::Codex),
            AgentLoginState::Ready
        );
        let installation = document
            .login(AgentLoginProvider::Codex)
            .and_then(LoginReadiness::installation)
            .expect("the Codex installation");
        assert_eq!(installation.generation, 3);
        assert_eq!(fixture.cloud.deliveries(), 1);
        assert_eq!(cloud.reports().len(), 2, "{closing}");
    }
}

// REQ-GRANT-50, REQ-GRANT-27, INV-RUNNER-15
#[tokio::test]
async fn a_closing_answer_never_reached_or_never_asked_for_changes_nothing() {
    let revoked = Ok(vec![ProviderLogin {
        status: LoginStatus::Revoked,
        ..authorized(AgentLoginProvider::Codex, 3)
    }]);
    // The closing request fails.
    let fixture = Fixture::new(Ok(Vec::new()));
    fixture
        .cloud
        .answer_next_then(fake_codex_authorized(3), revoked.clone());
    let cloud = Arc::new(fixture.reporting(Some(CloudFailure::Transport)));
    let document = fixture.pass_asking(Arc::clone(&cloud)).await;
    assert_eq!(cloud.reports().len(), 2);
    assert_eq!(
        state_of(&document, AgentLoginProvider::Codex),
        AgentLoginState::Ready
    );
    assert!(fixture.auth_path().is_file());
    assert_eq!(fixture.recorded().as_ref(), Some(&document));

    // An assignment starts after the Codex install, before the Claude Code
    // delivery: the pass makes no closing request.
    let fixture = Fixture::new(Ok(Vec::new()));
    fixture.cloud.answer_next_then(
        Ok(vec![
            authorized(AgentLoginProvider::Codex, 3),
            authorized(AgentLoginProvider::ClaudeCode, 3),
        ]),
        revoked,
    );
    fixture.gate.idle_calls.store(3, Ordering::SeqCst);
    let outcome = fixture.setup.run_pass().await;
    assert_eq!(outcome, SetupOutcome::Busy(fixture.recorded()));
    assert_eq!(fixture.cloud.status_requests(), 1);
    assert!(fixture.auth_path().is_file());
    assert_eq!(
        state_of(&fixture.recorded().unwrap(), AgentLoginProvider::Codex),
        AgentLoginState::Ready
    );
}

impl Fixture {
    fn codex_home(&self) -> PathBuf {
        self.directory().codex_home()
    }

    /// Writes into the runner's Codex home, beside `auth.json`, a file and a
    /// directory holding one, as Codex writes its state and sessions there.
    fn write_as_codex(&self) {
        let home = self.codex_home();
        write_private(&home.join("state_5.sqlite"), b"codex state");
        fs::create_dir_all(home.join("sessions/2026")).unwrap();
        write_private(&home.join("sessions/2026/rollout.jsonl"), b"{}");
    }

    /// The names in the runner's Codex home.
    fn codex_entries(&self) -> Vec<String> {
        let mut names = fs::read_dir(self.codex_home())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect::<Vec<_>>();
        names.sort();
        names
    }
}

/// Cloud's answer naming the fake agent login for Codex with `status`.
fn codex_answer(status: LoginStatus) -> Result<Vec<ProviderLogin>, CloudFailure> {
    Ok(vec![ProviderLogin {
        status,
        ..authorized(AgentLoginProvider::Codex, 1)
    }])
}

// REQ-GRANT-40, REQ-GRANT-36
#[tokio::test]
async fn ending_a_codex_login_removes_everything_in_the_runners_codex_home() {
    for (answer, state) in [
        (codex_answer(LoginStatus::Revoked), AgentLoginState::Revoked),
        (
            codex_answer(LoginStatus::Unauthorized),
            AgentLoginState::NotAuthorized,
        ),
        (Ok(Vec::new()), AgentLoginState::NotAuthorized),
    ] {
        let fixture = Fixture::new(fake_codex_authorized(1));
        fixture.pass().await;
        fixture.write_as_codex();
        fixture.cloud.answer(answer.clone());

        let document = fixture.pass().await;

        assert!(!fixture.codex_home().exists(), "{answer:?}");
        assert!(fixture.directory().path().is_dir(), "{answer:?}");
        let login = document.login(AgentLoginProvider::Codex).unwrap();
        assert_eq!(login.state, state, "{answer:?}");
        assert_eq!(login.installation(), None, "{answer:?}");
    }

    // Another agent login: the earlier one's home is gone before the other
    // is asked for, and the other is installed into a fresh home.
    let fixture = Fixture::new(fake_codex_authorized(1));
    fixture.pass().await;
    fixture.write_as_codex();
    fixture.cloud.answer(Ok(vec![ProviderLogin {
        agent_login_id: OTHER_AGENT_LOGIN_ID.to_owned(),
        ..authorized(AgentLoginProvider::Codex, 1)
    }]));
    let cloud = fixture.observing(
        vec![(AgentLoginProvider::Codex, fixture.codex_home())],
        None,
    );

    let document = fixture.pass_asking(Arc::clone(&cloud)).await;

    assert_eq!(
        *cloud.seen.lock().unwrap(),
        [AtDelivery {
            provider: AgentLoginProvider::Codex,
            held: false,
            installed: None,
        }]
    );
    assert_eq!(fixture.codex_entries(), ["auth.json", "config.toml"]);
    let installation = document
        .login(AgentLoginProvider::Codex)
        .and_then(LoginReadiness::installation)
        .expect("the other login's installation");
    assert_eq!(installation.agent_login_id, OTHER_AGENT_LOGIN_ID);
}

// REQ-GRANT-40, REQ-GRANT-36
#[tokio::test]
async fn a_newer_generation_replaces_auth_json_alone() {
    let fixture = Fixture::new(fake_codex_authorized(1));
    fixture.pass().await;
    fixture.write_as_codex();
    fixture.cloud.answer(fake_codex_authorized(2));

    let document = fixture.pass().await;

    assert_eq!(
        state_of(&document, AgentLoginProvider::Codex),
        AgentLoginState::Ready
    );
    assert_eq!(
        fs::read(fixture.auth_path()).unwrap(),
        fake_agent_login_material(2)
    );
    assert_eq!(
        fixture.codex_entries(),
        ["auth.json", "config.toml", "sessions", "state_5.sqlite"]
    );
    assert!(
        fixture
            .codex_home()
            .join("sessions/2026/rollout.jsonl")
            .is_file()
    );
}

// REQ-GRANT-40, REQ-GRANT-45
#[tokio::test]
async fn ending_a_claude_code_login_removes_its_token_file_alone() {
    let fixture = Fixture::new(Ok(vec![
        authorized(AgentLoginProvider::Codex, 1),
        authorized(AgentLoginProvider::ClaudeCode, 1),
    ]));
    fixture.pass().await;
    fixture.write_as_codex();
    fixture.cloud.answer(Ok(vec![
        authorized(AgentLoginProvider::Codex, 1),
        ProviderLogin {
            status: LoginStatus::Revoked,
            ..authorized(AgentLoginProvider::ClaudeCode, 1)
        },
    ]));

    let document = fixture.pass().await;

    assert_eq!(
        state_of(&document, AgentLoginProvider::ClaudeCode),
        AgentLoginState::Revoked
    );
    assert!(!fixture.token_path().exists());
    assert_eq!(
        fixture.codex_entries(),
        ["auth.json", "config.toml", "sessions", "state_5.sqlite"]
    );
    assert_eq!(
        fs::read(fixture.auth_path()).unwrap(),
        fake_agent_login_material(1)
    );
}

// REQ-GRANT-40, REQ-GRANT-36
#[tokio::test]
async fn a_link_in_the_codex_home_is_removed_without_touching_its_target() {
    let fixture = Fixture::new(fake_codex_authorized(1));
    fixture.pass().await;
    fixture.write_as_codex();
    // A directory and a file outside the runner's directory, linked from
    // the home and from a directory in it.
    let outside = fixture.root.path().join("outside");
    fs::create_dir(&outside).unwrap();
    write_private(&outside.join("kept.json"), b"{\"kept\":true}");
    std::os::unix::fs::symlink(&outside, fixture.codex_home().join("linked")).unwrap();
    std::os::unix::fs::symlink(
        outside.join("kept.json"),
        fixture.codex_home().join("sessions/2026/linked.json"),
    )
    .unwrap();
    fixture.cloud.answer(codex_answer(LoginStatus::Revoked));

    let document = fixture.pass().await;

    assert_eq!(
        state_of(&document, AgentLoginProvider::Codex),
        AgentLoginState::Revoked
    );
    assert!(!fixture.codex_home().exists());
    assert_eq!(
        fs::read(outside.join("kept.json")).unwrap(),
        b"{\"kept\":true}"
    );
}

// REQ-GRANT-40, REQ-TELEMETRY-45
#[tokio::test]
async fn a_codex_home_removal_that_fails_is_finished_by_the_next_pass() {
    let fixture = Fixture::new(fake_codex_authorized(1));
    fixture.pass().await;
    fixture.write_as_codex();
    fixture.cloud.answer(codex_answer(LoginStatus::Revoked));
    // A directory in the home whose entries cannot be removed.
    let sessions = fixture.codex_home().join("sessions");
    set_mode(&sessions, 0o500);

    let document = fixture.pass().await;
    set_mode(&sessions, 0o700);

    assert_eq!(
        state_of(&document, AgentLoginProvider::Codex),
        AgentLoginState::InstallFailed
    );
    // The login itself is gone whatever else the removal could not take, so
    // no later assignment is offered the revoked login (REQ-GRANT-49).
    assert!(!fixture.codex_home().join("auth.json").exists());
    let records = fixture.setup_records();
    let record = records.last().expect("the failed removal's record");
    assert_eq!(record["um.agent_login.codex.failure_stage"], "remove");
    assert_eq!(
        record["um.agent_login.codex.failure_cause"],
        "permission_denied"
    );
    assert!(fixture.codex_home().exists());

    let document = fixture.pass().await;

    assert_eq!(
        state_of(&document, AgentLoginProvider::Codex),
        AgentLoginState::Revoked
    );
    assert!(!fixture.codex_home().exists());
}

// REQ-GRANT-49, REQ-GRANT-50, INV-RUNNER-15
#[tokio::test]
async fn an_admissions_pass_installs_and_ends_without_consulting_the_assignment_gate() {
    let fixture = Fixture::new(fake_codex_authorized(1));
    fixture.pass().await;
    // The assignment being admitted holds the slot, so the periodic gate
    // refuses; the admission's own pass installs generation 2, and ends it
    // when its closing answer has revoked the login.
    fixture.gate.idle_calls.store(0, Ordering::SeqCst);
    fixture
        .cloud
        .answer_next_then(fake_codex_authorized(2), codex_answer(LoginStatus::Revoked));
    assert_eq!(fixture.setup.run_pass().await, SetupOutcome::Busy(None));

    let Some(SetupOutcome::Recorded(document)) = fixture
        .setup
        .set_up_admission(BOOT, ASSIGNMENT)
        .agent_logins
    else {
        panic!("the admission's pass did not record readiness");
    };

    assert_eq!(fixture.cloud.deliveries(), 2);
    assert_eq!(
        state_of(&document, AgentLoginProvider::Codex),
        AgentLoginState::Revoked
    );
    assert!(!fixture.codex_home().exists());
    assert_eq!(fixture.recorded().unwrap().logins, document.logins);
}

// REQ-GRANT-49
#[tokio::test]
async fn an_admissions_pass_waits_for_a_pass_in_flight_and_then_makes_its_own() {
    let fixture = Fixture::new(fake_codex_authorized(1));
    fixture.pass().await;
    let before = fixture.cloud.status_requests();
    fixture.cloud.answer(fake_codex_authorized(2));
    let (held, arrival) = fixture.cloud.hold_next_status_arrival();
    let periodic = tokio::spawn({
        let setup = Arc::clone(&fixture.setup);
        async move { setup.run_pass().await }
    });
    // The periodic pass waits on its status request.
    tokio::task::spawn_blocking(move || arrival.recv())
        .await
        .unwrap()
        .expect("the periodic pass asked Cloud");
    let admission = tokio::task::spawn_blocking({
        let setup = Arc::clone(&fixture.setup);
        move || setup.set_up_admission(BOOT, ASSIGNMENT).agent_logins
    });
    drop(held);

    let periodic = periodic.await.unwrap();
    let admission = admission.await.unwrap();

    // The periodic pass installed generation 2 and reported it; the
    // admission's, made after it, found it installed and asked once.
    assert!(
        matches!(periodic, SetupOutcome::Recorded(_)),
        "{periodic:?}"
    );
    let Some(SetupOutcome::Recorded(document)) = admission else {
        panic!("the admission's pass did not record readiness: {admission:?}");
    };
    assert_eq!(fixture.cloud.deliveries(), 2);
    assert_eq!(fixture.cloud.status_requests(), before + 3);
    assert_eq!(
        document
            .login(AgentLoginProvider::Codex)
            .and_then(LoginReadiness::installation)
            .map(|installation| installation.generation),
        Some(2)
    );
}

/// What an admission offered of the fake agent login for `provider` at
/// `generation`, under the variable its coding agent reads.
fn offered(provider: AgentLoginProvider, generation: u64) -> Vec<OfferedLogin> {
    vec![OfferedLogin {
        variable: match provider {
            AgentLoginProvider::ClaudeCode => um_execution::CLAUDE_CODE_LOGIN_VARIABLE,
            _ => um_execution::CODEX_LOGIN_VARIABLE,
        },
        provider,
        agent_login_id: FAKE_AGENT_LOGIN_ID.to_owned(),
        generation,
    }]
}

impl Fixture {
    /// Notes a use with `outcome` of the fake Codex login at `generation`,
    /// as the engine tells an admission's observer.
    fn note_codex_use(&self, generation: u64, outcome: HarnessLoginOutcome) {
        self.setup
            .observer(offered(AgentLoginProvider::Codex, generation))
            .expect("an observer of what was offered")
            .observe(um_execution::CODEX_LOGIN_VARIABLE, outcome);
    }

    /// Moves the setup's clock `seconds` later.
    fn advance_clock(&self, seconds: i64) {
        self.clock.fetch_add(seconds, Ordering::SeqCst);
    }

    /// The Codex last use each status request since the `since`th stated.
    fn stated_codex_uses(&self, since: usize) -> Vec<Option<LastUse>> {
        self.cloud.reports()[since..]
            .iter()
            .map(|report| codex_report(report).last_use)
            .collect()
    }
}

fn codex_last_use(document: &ReadinessDocument) -> Option<LastUse> {
    document
        .login(AgentLoginProvider::Codex)
        .and_then(|login| login.last_use)
}

fn used(outcome: UseOutcome) -> Option<LastUse> {
    Some(LastUse { outcome, at: now() })
}

// REQ-GRANT-52, REQ-GRANT-25, REQ-GRANT-27, REQ-TELEMETRY-45
#[tokio::test]
async fn a_noted_use_is_recorded_beside_its_install_and_stated_to_cloud() {
    let fixture = Fixture::new(fake_codex_authorized(2));
    fixture.pass().await;
    let records = fixture.setup_records().len();
    fixture.note_codex_use(2, HarnessLoginOutcome::Refused);
    let asked = fixture.cloud.reports().len();

    let document = fixture.pass().await;

    // A refused login stays installed, offered and `ready`.
    let codex = document.login(AgentLoginProvider::Codex).unwrap();
    assert_eq!(codex.state, AgentLoginState::Ready);
    assert_eq!(codex.last_use, used(UseOutcome::Refused));
    assert!(fixture.auth_path().exists());
    assert_eq!(fixture.recorded().as_ref(), Some(&document));
    // The opening request carried what was recorded; the changed last use
    // alone made the pass report again.
    assert_eq!(
        fixture.stated_codex_uses(asked),
        [None, used(UseOutcome::Refused)]
    );
    assert_eq!(fixture.cloud.deliveries(), 1);
    // A changed last use writes no record.
    assert_eq!(fixture.setup_records().len(), records);

    // A pass with nothing new noted keeps it, and asks once.
    let asked = fixture.cloud.reports().len();
    assert_eq!(
        codex_last_use(&fixture.pass().await),
        used(UseOutcome::Refused)
    );
    assert_eq!(
        fixture.stated_codex_uses(asked),
        [used(UseOutcome::Refused)]
    );

    // An accepted use noted afterwards replaces it.
    fixture.note_codex_use(2, HarnessLoginOutcome::Accepted);
    let asked = fixture.cloud.reports().len();
    assert_eq!(
        codex_last_use(&fixture.pass().await),
        used(UseOutcome::Accepted)
    );
    assert_eq!(
        fixture.stated_codex_uses(asked),
        [used(UseOutcome::Refused), used(UseOutcome::Accepted)]
    );
}

// REQ-GRANT-52
#[tokio::test]
async fn a_later_use_with_the_same_outcome_is_reported_with_its_time() {
    let fixture = Fixture::new(fake_codex_authorized(2));
    fixture.pass().await;
    fixture.note_codex_use(2, HarnessLoginOutcome::Accepted);
    fixture.pass().await;

    // A second accepted use, noted later, changes only the last use's time,
    // and that alone makes the pass report again.
    fixture.advance_clock(90);
    fixture.note_codex_use(2, HarnessLoginOutcome::Accepted);
    let asked = fixture.cloud.reports().len();
    let later = Some(LastUse {
        outcome: UseOutcome::Accepted,
        at: now() + time::Duration::seconds(90),
    });
    assert_eq!(codex_last_use(&fixture.pass().await), later);
    assert_eq!(
        fixture.stated_codex_uses(asked),
        [used(UseOutcome::Accepted), later]
    );
}

// REQ-GRANT-52
#[tokio::test]
async fn a_use_of_an_earlier_generation_is_dropped_and_an_ending_clears_the_last_use() {
    let fixture = Fixture::new(fake_codex_authorized(2));
    fixture.pass().await;
    fixture.note_codex_use(2, HarnessLoginOutcome::Accepted);
    assert_eq!(
        codex_last_use(&fixture.pass().await),
        used(UseOutcome::Accepted)
    );

    // Generation 3 is installed: what was recorded of generation 2, and a
    // use noted for it, are forgotten.
    fixture.note_codex_use(2, HarnessLoginOutcome::Refused);
    fixture.cloud.answer(fake_codex_authorized(3));
    let asked = fixture.cloud.reports().len();
    let document = fixture.pass().await;
    let codex = document.login(AgentLoginProvider::Codex).unwrap();
    assert_eq!(codex.installation().map(|login| login.generation), Some(3));
    assert_eq!(codex.last_use, None);
    assert_eq!(
        fixture.stated_codex_uses(asked),
        [used(UseOutcome::Accepted), None]
    );
    assert_eq!(codex_last_use(&fixture.pass().await), None);

    // Ending the login clears it.
    fixture.note_codex_use(3, HarnessLoginOutcome::Refused);
    assert_eq!(
        codex_last_use(&fixture.pass().await),
        used(UseOutcome::Refused)
    );
    fixture.cloud.answer(codex_answer(LoginStatus::Revoked));
    let asked = fixture.cloud.reports().len();
    let document = fixture.pass().await;
    assert_eq!(
        state_of(&document, AgentLoginProvider::Codex),
        AgentLoginState::Revoked
    );
    assert_eq!(codex_last_use(&document), None);
    assert_eq!(
        fixture.stated_codex_uses(asked),
        [used(UseOutcome::Refused), None]
    );
}

// REQ-GRANT-52, REQ-GRANT-25
#[tokio::test]
async fn a_use_is_stated_to_cloud_only_beside_a_ready_install() {
    let fixture = Fixture::new(fake_codex_authorized(2));
    fixture.pass().await;

    // Cloud names generation 3 and refuses its delivery as stale: the
    // install is kept, not `ready`, and its last use is not stated.
    fixture.note_codex_use(2, HarnessLoginOutcome::Refused);
    fixture.cloud.answer(fake_codex_authorized(3));
    fixture
        .cloud
        .refuse_deliveries(DeliveryRefusal::StaleGeneration);
    let asked = fixture.cloud.reports().len();
    let document = fixture.pass().await;
    let codex = document.login(AgentLoginProvider::Codex).unwrap();
    assert_eq!(codex.state, AgentLoginState::StaleGeneration);
    assert_eq!(codex.installation().map(|login| login.generation), Some(2));
    assert_eq!(codex.last_use, used(UseOutcome::Refused));
    assert_eq!(fixture.stated_codex_uses(asked), [None, None]);

    // A use noted for a provider the runner installed nothing of is
    // neither recorded nor stated.
    fixture
        .setup
        .observer(offered(AgentLoginProvider::ClaudeCode, 1))
        .unwrap()
        .observe(
            um_execution::CLAUDE_CODE_LOGIN_VARIABLE,
            HarnessLoginOutcome::Refused,
        );
    let asked = fixture.cloud.reports().len();
    let document = fixture.pass().await;
    assert!(
        document
            .logins
            .iter()
            .all(|login| login.provider != AgentLoginProvider::ClaudeCode
                || login.last_use.is_none()),
        "{document:?}"
    );
    assert!(
        fixture.cloud.reports()[asked..]
            .iter()
            .flat_map(|report| &report.providers)
            .all(|report| report.provider != AgentLoginProvider::ClaudeCode
                || report.last_use.is_none())
    );
    assert!(fixture.setup.observer(Vec::new()).is_none());
}

impl Fixture {
    /// A setup `um runner grant sync` asked for.
    async fn sync(&self) -> SetupAnswer {
        self.setup.set_up(SetupCaller::Sync).await
    }

    /// A setup that holds a `ready` Claude Code login at generation 1 with
    /// no last use, installed by a setup whose check gave no answer.
    async fn holding_claude_code() -> Self {
        let fixture = Self::new(Ok(vec![authorized(AgentLoginProvider::ClaudeCode, 1)]));
        let document = fixture.pass().await;
        let claude_code = document.login(AgentLoginProvider::ClaudeCode).unwrap();
        assert_eq!(claude_code.state, AgentLoginState::Ready);
        assert_eq!(claude_code.last_use, None);
        fixture
    }

    /// The logins checked since the `since`th check.
    fn checked_since(&self, since: usize) -> Vec<(AgentLoginProvider, u64)> {
        self.check.checked()[since..].to_vec()
    }

    /// The Claude Code last use each status request since the `since`th
    /// stated.
    fn stated_claude_code_uses(&self, since: usize) -> Vec<Option<LastUse>> {
        self.cloud.reports()[since..]
            .iter()
            .map(|report| {
                report
                    .providers
                    .iter()
                    .find(|provider| provider.provider == AgentLoginProvider::ClaudeCode)
                    .expect("the Claude Code report")
                    .last_use
            })
            .collect()
    }
}

fn claude_code_last_use(document: &ReadinessDocument) -> Option<LastUse> {
    document
        .login(AgentLoginProvider::ClaudeCode)
        .and_then(|login| login.last_use)
}

/// The document a setup recorded and answered.
fn recorded_by(answer: &SetupAnswer) -> &ReadinessDocument {
    match &answer.outcome {
        SetupOutcome::Recorded(document) => document,
        other => panic!("setup did not record readiness: {other:?}"),
    }
}

// REQ-GRANT-90, REQ-GRANT-52, REQ-GRANT-41, REQ-GRANT-25
#[tokio::test]
async fn a_sync_checks_each_ready_login_and_records_an_accepted_check_as_its_last_use() {
    let fixture = Fixture::holding_claude_code().await;
    fixture.check.answer(
        AgentLoginProvider::ClaudeCode,
        LoginCheckOutcome::Answered(UseOutcome::Accepted),
    );
    let (checks, asked) = (fixture.check.checked().len(), fixture.cloud.reports().len());

    let answer = fixture.sync().await;

    assert_eq!(
        fixture.checked_since(checks),
        [(AgentLoginProvider::ClaudeCode, 1)]
    );
    let document = recorded_by(&answer);
    let accepted = Some(LastUse {
        outcome: UseOutcome::Accepted,
        at: now(),
    });
    assert_eq!(claude_code_last_use(document), accepted);
    assert_eq!(fixture.recorded().as_ref(), Some(document));
    assert!(answer.unanswered.is_empty(), "{answer:?}");
    // The setup told Cloud before it ended: the second pass asked with what
    // the first recorded and then reported the last use.
    assert_eq!(
        fixture.stated_claude_code_uses(asked).last(),
        Some(&accepted)
    );
    let reported = document.reported().unwrap();
    let entry = reported
        .providers
        .iter()
        .find(|entry| entry.provider == AgentLoginProvider::ClaudeCode)
        .unwrap();
    assert_eq!(entry.last_use, Some(UseOutcome::Accepted));
    assert!(entry.last_use_at.is_some());
    assert_eq!(entry.failure_code(), None);
}

// REQ-GRANT-90, REQ-GRANT-52, REQ-GRANT-53
#[tokio::test]
async fn a_refused_check_is_recorded_and_the_login_reads_reconnect_required() {
    let fixture = Fixture::holding_claude_code().await;
    fixture.check.answer(
        AgentLoginProvider::ClaudeCode,
        LoginCheckOutcome::Answered(UseOutcome::Refused),
    );

    let answer = fixture.sync().await;

    let document = recorded_by(&answer);
    let claude_code = document.login(AgentLoginProvider::ClaudeCode).unwrap();
    // A refused login stays installed and `ready` (REQ-GRANT-52).
    assert_eq!(claude_code.state, AgentLoginState::Ready);
    assert_eq!(claude_code.last_use, used(UseOutcome::Refused));
    assert!(fixture.token_path().exists());
    assert_eq!(fixture.recorded().as_ref(), Some(document));
    let reported = document.reported().unwrap();
    let entry = reported
        .providers
        .iter()
        .find(|entry| entry.provider == AgentLoginProvider::ClaudeCode)
        .unwrap();
    assert_eq!(entry.failure_code(), Some("reconnect_required"));
}

// REQ-GRANT-90, REQ-GRANT-41
#[tokio::test]
async fn a_check_with_no_answer_changes_nothing_and_is_named_in_the_setups_answer() {
    let fixture = Fixture::holding_claude_code().await;
    fixture.check.answer(
        AgentLoginProvider::ClaudeCode,
        LoginCheckOutcome::Answered(UseOutcome::Accepted),
    );
    fixture.sync().await;
    let earlier = fixture.recorded().unwrap();
    assert_eq!(claude_code_last_use(&earlier), used(UseOutcome::Accepted));

    for cause in [
        AgentLoginCheckCause::ProgramUnavailable,
        AgentLoginCheckCause::StartFailed,
        AgentLoginCheckCause::TimedOut,
        AgentLoginCheckCause::HarnessError,
    ] {
        fixture.advance_clock(90);
        fixture.check.answer(
            AgentLoginProvider::ClaudeCode,
            LoginCheckOutcome::NoAnswer(cause),
        );
        let asked = fixture.cloud.reports().len();

        let answer = fixture.sync().await;

        let document = recorded_by(&answer);
        // The last use and its time are exactly as they were, and the setup
        // made no second pass.
        assert_eq!(
            claude_code_last_use(document),
            claude_code_last_use(&earlier),
            "{cause:?}"
        );
        assert_eq!(fixture.cloud.reports().len(), asked + 1, "{cause:?}");
        assert_eq!(
            answer.unanswered,
            [UnansweredCheck {
                provider: AgentLoginProvider::ClaudeCode,
                cause,
            }]
        );
    }
}

// REQ-GRANT-90
#[tokio::test]
async fn a_scheduled_setup_checks_only_a_login_it_installed() {
    let fixture = Fixture::new(fake_codex_authorized(2));
    fixture.pass().await;
    assert_eq!(fixture.check.checked(), [(AgentLoginProvider::Codex, 2)]);

    // Cloud authorizes a Claude Code login too: the setup that installs it
    // checks it, and not the Codex login it already held.
    fixture.cloud.answer(Ok(vec![
        authorized(AgentLoginProvider::Codex, 2),
        authorized(AgentLoginProvider::ClaudeCode, 1),
    ]));
    let document = fixture.pass().await;
    assert_eq!(
        state_of(&document, AgentLoginProvider::ClaudeCode),
        AgentLoginState::Ready
    );
    assert_eq!(
        fixture.checked_since(1),
        [(AgentLoginProvider::ClaudeCode, 1)]
    );

    // A scheduled setup that installs nothing checks nothing.
    fixture.pass().await;
    assert_eq!(fixture.check.checked().len(), 2);
}

// REQ-GRANT-90, REQ-GRANT-49, INV-RUNNER-29
#[tokio::test]
async fn an_admissions_pass_checks_nothing_even_when_it_installs() {
    let fixture = Fixture::new(fake_codex_authorized(1));
    fixture.pass().await;
    fixture.check.answer(
        AgentLoginProvider::Codex,
        LoginCheckOutcome::Answered(UseOutcome::Accepted),
    );
    let checks = fixture.check.checked().len();
    fixture.cloud.answer(fake_codex_authorized(2));

    let Some(SetupOutcome::Recorded(document)) = fixture
        .setup
        .set_up_admission(BOOT, ASSIGNMENT)
        .agent_logins
    else {
        panic!("the admission's pass did not record readiness");
    };

    assert_eq!(
        document
            .login(AgentLoginProvider::Codex)
            .and_then(LoginReadiness::installation)
            .map(|installation| installation.generation),
        Some(2)
    );
    assert_eq!(codex_last_use(&document), None);
    assert_eq!(fixture.check.checked().len(), checks);
}

// REQ-GRANT-90, INV-RUNNER-29
#[tokio::test]
async fn no_check_starts_while_the_runner_holds_an_assignment() {
    let fixture = Fixture::holding_claude_code().await;
    fixture.check.answer(
        AgentLoginProvider::ClaudeCode,
        LoginCheckOutcome::Answered(UseOutcome::Accepted),
    );
    let checks = fixture.check.checked().len();

    // An assignment starts once the sync's pass has finished: the pass is
    // recorded, and no check starts.
    fixture.gate.idle_calls.store(1, Ordering::SeqCst);
    let answer = fixture.sync().await;
    assert_eq!(claude_code_last_use(recorded_by(&answer)), None);
    assert!(answer.unanswered.is_empty(), "{answer:?}");
    assert_eq!(fixture.check.checked().len(), checks);

    // While one is held throughout, as `CountedGate::busy()` is, the setup
    // answers busy and checks nothing either.
    fixture.gate.idle_calls.store(0, Ordering::SeqCst);
    let answer = fixture.sync().await;
    assert_eq!(answer.outcome, SetupOutcome::Busy(None));
    assert_eq!(fixture.check.checked().len(), checks);
}

// REQ-GRANT-90, REQ-GRANT-52
#[tokio::test]
async fn a_check_noted_for_a_login_the_second_pass_ends_is_dropped() {
    let fixture = Fixture::holding_claude_code().await;
    fixture.check.answer(
        AgentLoginProvider::ClaudeCode,
        LoginCheckOutcome::Answered(UseOutcome::Accepted),
    );
    // Cloud still authorizes the login when the sync's pass asks, and has
    // revoked it when its second pass does.
    fixture.cloud.answer_next_then(
        Ok(vec![authorized(AgentLoginProvider::ClaudeCode, 1)]),
        Ok(vec![ProviderLogin {
            status: LoginStatus::Revoked,
            ..authorized(AgentLoginProvider::ClaudeCode, 1)
        }]),
    );

    let answer = fixture.sync().await;

    let document = recorded_by(&answer);
    assert_eq!(
        state_of(document, AgentLoginProvider::ClaudeCode),
        AgentLoginState::Revoked
    );
    assert_eq!(claude_code_last_use(document), None);
    assert!(!fixture.token_path().exists());

    // Authorized again at the same generation, the login is installed
    // anew without the dropped check's outcome.
    fixture.check.answer(
        AgentLoginProvider::ClaudeCode,
        LoginCheckOutcome::NoAnswer(AgentLoginCheckCause::TimedOut),
    );
    fixture
        .cloud
        .answer(Ok(vec![authorized(AgentLoginProvider::ClaudeCode, 1)]));
    let document = fixture.pass().await;
    assert_eq!(
        state_of(&document, AgentLoginProvider::ClaudeCode),
        AgentLoginState::Ready
    );
    assert_eq!(claude_code_last_use(&document), None);
}

// REQ-TELEMETRY-45, REQ-GRANT-90
#[tokio::test]
async fn a_check_with_no_answer_is_recorded_with_its_stage_and_cause() {
    let fixture = Fixture::holding_claude_code().await;
    for (cause, name) in [
        (
            AgentLoginCheckCause::ProgramUnavailable,
            "program_unavailable",
        ),
        (AgentLoginCheckCause::StartFailed, "start_failed"),
        (AgentLoginCheckCause::TimedOut, "timed_out"),
        (AgentLoginCheckCause::HarnessError, "harness_error"),
    ] {
        fixture.check.answer(
            AgentLoginProvider::ClaudeCode,
            LoginCheckOutcome::NoAnswer(cause),
        );
        let records = fixture.setup_records().len();

        fixture.sync().await;

        // The readiness did not change, and the setup's one record names
        // the provider's check and its cause beside its state.
        let new = &fixture.setup_records()[records..];
        assert_eq!(new.len(), 1, "{new:?}");
        let record = &new[0];
        assert_eq!(record["um.agent_login.claude_code.state"], "ready");
        assert_eq!(record["um.agent_login.claude_code.failure_stage"], "check");
        assert_eq!(record["um.agent_login.claude_code.failure_cause"], name);
        assert!(
            record
                .keys()
                .all(|key| !key.starts_with("um.agent_login.codex.")),
            "{record:?}"
        );
    }

    // An accepted or refused check adds nothing, and neither does a setup
    // that makes no check.
    let records = fixture.setup_records().len();
    for outcome in [UseOutcome::Accepted, UseOutcome::Refused] {
        fixture.check.answer(
            AgentLoginProvider::ClaudeCode,
            LoginCheckOutcome::Answered(outcome),
        );
        fixture.sync().await;
    }
    fixture.pass().await;
    assert_eq!(fixture.setup_records().len(), records);
}

// REQ-TELEMETRY-45
#[tokio::test]
async fn a_step_that_failed_in_the_setup_is_named_before_its_unanswered_check() {
    // The setup installs the login, its closing status request fails, and the
    // check of the login it installed gives no answer.
    let fixture = Fixture::new(fake_codex_authorized(3));
    let cloud = Arc::new(fixture.reporting(Some(CloudFailure::Transport)));
    let records = fixture.setup_records().len();

    let document = fixture.pass_asking(Arc::clone(&cloud)).await;

    assert_eq!(
        state_of(&document, AgentLoginProvider::Codex),
        AgentLoginState::Ready
    );
    assert_eq!(cloud.reports().len(), 2);
    assert_eq!(fixture.check.checked(), [(AgentLoginProvider::Codex, 3)]);
    // The provider has one stage and cause in the record: the step that
    // failed first, not the check.
    let new = &fixture.setup_records()[records..];
    assert_eq!(new.len(), 1, "{new:?}");
    let record = &new[0];
    assert_eq!(record["um.agent_login.codex.state"], "ready");
    assert_eq!(record["um.agent_login.codex.failure_stage"], "status");
    assert_eq!(record["um.agent_login.codex.failure_cause"], "transport");
}
