use std::os::unix::fs::{DirBuilderExt as _, OpenOptionsExt as _};

use super::*;
use useful_execution::{
    CLAUDE_CODE_LOGIN_VARIABLE as TOKEN_VARIABLE, CODEX_LOGIN_VARIABLE as CODEX_HOME,
};

use crate::agent_login::cloud::{
    CloudFailure, CredentialSourcesFailure, CredentialSourcesRequest, DeliveryRefusal,
    GrantProvider, GrantProvidersFailure, GrantTokenFailure, GrantTokenRefusal, ListedProvider,
    LoginStatus, ProviderLogin, ReportedSource, ReportedTokenService, TokenServiceSource,
};
use crate::agent_login::directory::LoginDirectory;
use crate::agent_login::readiness::{
    GrantReadiness, LoginReadiness, ReadinessDocument, UseOutcome, readiness_path,
};
use crate::agent_login::setup::{AgentLoginSetup, SetupOutcome};
use crate::control_protocol::{AgentLoginProvider, AgentLoginState, CredentialSource};
use crate::service::HeldRunGate;
use crate::service::control::{AdmissionAgentLogins, LiveStatus};
use crate::service::test_support::{
    CountedGate, FAKE_AGENT_LOGIN_ID, FAKE_GRANT_ID, FAKE_INTEGRATION_TOKEN_ID,
    FakeAgentLoginCloud, FakeGrantToken, SleepRelease, controlled_sleeper,
    fake_agent_login_material, fake_agent_login_setup, fake_agent_login_setup_serving,
    fake_codex_answer, fake_codex_authorized, fake_grant_expiry, fake_grant_token,
    preparation_teardown_holding_sleeper,
};
use crate::telemetry::test_recorder;

const DELIVERED: &str = "sk-ant-oat01-delivered-claude-code-token";

/// A workflow that passes `CLAUDE_CODE_OAUTH_TOKEN`: `kept` must see the
/// delivered token, and `narrowed`, whose own list omits the name, nothing.
const PASSING_WORKFLOW: &str = r#"schemaVersion: 1
environmentPassthrough: [CLAUDE_CODE_OAUTH_TOKEN]
steps:
  kept:
    kind: cmd
    command:
      argv: [sh, -c, 'test "$CLAUDE_CODE_OAUTH_TOKEN" = sk-ant-oat01-delivered-claude-code-token']
  narrowed:
    kind: cmd
    environmentPassthrough: []
    command:
      argv: [sh, -c, 'test -z "${CLAUDE_CODE_OAUTH_TOKEN+x}"']
"#;

/// A workflow that passes `CODEX_HOME`: `kept` must find the runner's Codex
/// login through it, and `narrowed`, whose own list omits the name, nothing.
const PASSING_CODEX_WORKFLOW: &str = r#"schemaVersion: 1
environmentPassthrough: [CODEX_HOME]
steps:
  kept:
    kind: cmd
    command:
      argv: [sh, -c, 'test -f "$CODEX_HOME/auth.json"']
  narrowed:
    kind: cmd
    environmentPassthrough: []
    command:
      argv: [sh, -c, 'test -z "${CODEX_HOME+x}"']
"#;

/// A manager for `workflow` whose process environment has no Claude Code or
/// Codex login of its own: no token variable, no `CLAUDE_CONFIG_DIR`, no
/// `CODEX_HOME`, and an empty home. Its agent-login directory is beside a
/// state file in the returned directory.
fn manager_without_agent_logins(workflow: &str) -> (tempfile::TempDir, AssignmentManager) {
    let (temporary, mut manager) = manager_fixture(workflow);
    let home = temporary.path().join("service-home");
    fs::create_dir(&home).unwrap();
    let mut variables = manager.environment.variables().clone();
    for name in [TOKEN_VARIABLE, "CLAUDE_CONFIG_DIR", CODEX_HOME] {
        variables.remove(OsStr::new(name));
    }
    variables.insert(OsString::from("HOME"), home.into_os_string());
    manager.environment = EnvironmentSnapshot::new(variables);
    manager.agent_login_directory = Some(LoginDirectory::beside(
        &temporary.path().join("runner-state.json"),
    ));
    (temporary, manager)
}

fn login_directory(manager: &AssignmentManager) -> &LoginDirectory {
    manager
        .agent_login_directory
        .as_ref()
        .expect("the manager has an agent-login directory")
}

fn create_private_directory(path: &Path) {
    fs::DirBuilder::new().mode(0o700).create(path).unwrap();
}

fn write_private(path: &Path, bytes: &[u8]) {
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .unwrap();
    file.write_all(bytes).unwrap();
}

/// Keeps `DELIVERED` as the runner's Claude Code token, as setup leaves it.
fn keep_token(directory: &LoginDirectory) -> PathBuf {
    create_private_directory(directory.path());
    let path = directory.claude_code_token();
    write_private(&path, DELIVERED.as_bytes());
    path
}

/// Keeps a Codex login in the runner's Codex home, as setup leaves it.
fn keep_codex_login(directory: &LoginDirectory) -> PathBuf {
    create_private_directory(directory.path());
    create_private_directory(&directory.codex_home());
    let path = directory.codex_home().join("auth.json");
    write_private(&path, b"{\"tokens\":{}}");
    path
}

fn accepted_variable(manager: &AssignmentManager, name: &str) -> Option<OsString> {
    match held_assignment(manager) {
        Some(HeldAssignment::Accepted(accepted)) => engine_admission(accepted)
            .execution()
            .environment()
            .variable(OsStr::new(name))
            .map(OsStr::to_os_string),
        _ => panic!("assignment should be accepted"),
    }
}

fn accepted_token(manager: &AssignmentManager) -> Option<OsString> {
    accepted_variable(manager, TOKEN_VARIABLE)
}

/// Releases the accepted assignment `offered` before it runs and waits for
/// its cleanup, so the manager admits the next one.
async fn release_unrun(manager: &mut AssignmentManager, offered: &AssignmentOffer) {
    manager
        .handle_release(release_for(offered, "br", "stale_or_invalid_acceptance"))
        .unwrap();
    settle_cleanup(manager).await;
    assert!(held_assignment(manager).is_none());
}

// REQ-GRANT-44, REQ-WFEXEC-71
#[tokio::test]
async fn a_kept_token_reaches_only_the_steps_of_a_workflow_that_passes_it() {
    let (_temporary, mut manager) = manager_without_agent_logins(PASSING_WORKFLOW);
    keep_token(login_directory(&manager));
    let offered = offer("bg");

    offer_then_prepare(&mut manager, &offered).await;

    assert_eq!(accepted_token(&manager), Some(OsString::from(DELIVERED)));
    assert_eq!(
        manager.environment.variable(OsStr::new(TOKEN_VARIABLE)),
        None
    );
    assert_succeeded(&execute_to_terminal(&mut manager, &offered).await);
}

// REQ-GRANT-44, REQ-WFEXEC-71
#[tokio::test]
async fn a_workflow_that_does_not_pass_the_name_receives_no_token() {
    let workflow = r#"schemaVersion: 1
environmentPassthrough: [OTHER_NAME]
steps:
  check:
    kind: cmd
    command:
      argv: [sh, -c, 'test -z "${CLAUDE_CODE_OAUTH_TOKEN+x}"']
"#;
    let (_temporary, mut manager) = manager_without_agent_logins(workflow);
    keep_token(login_directory(&manager));
    let offered = offer("bg");

    offer_then_prepare(&mut manager, &offered).await;

    assert_eq!(accepted_token(&manager), None);
    assert_succeeded(&execute_to_terminal(&mut manager, &offered).await);
}

// REQ-GRANT-44, REQ-GRANT-45
#[tokio::test]
async fn a_token_in_the_process_environment_passes_through_unchanged() {
    let workflow = PASSING_WORKFLOW.replace(DELIVERED, "service-token");
    let (_temporary, mut manager) = manager_without_agent_logins(&workflow);
    keep_token(login_directory(&manager));
    let mut variables = manager.environment.variables().clone();
    variables.insert(
        OsString::from(TOKEN_VARIABLE),
        OsString::from("service-token"),
    );
    manager.environment = EnvironmentSnapshot::new(variables);
    let offered = offer("bg");

    offer_then_prepare(&mut manager, &offered).await;

    assert_eq!(
        accepted_token(&manager),
        Some(OsString::from("service-token"))
    );
    assert_succeeded(&execute_to_terminal(&mut manager, &offered).await);
}

// REQ-GRANT-44, REQ-GRANT-45
#[tokio::test]
async fn each_admission_reads_the_token_the_runner_keeps_then() {
    let (_temporary, mut manager) = manager_without_agent_logins(PASSING_WORKFLOW);
    let path = keep_token(login_directory(&manager));
    let first = offer("bg");
    offer_then_prepare(&mut manager, &first).await;
    assert_eq!(accepted_token(&manager), Some(OsString::from(DELIVERED)));
    release_unrun(&mut manager, &first).await;

    fs::remove_file(&path).unwrap();
    let next = offer("bh");
    offer_then_prepare(&mut manager, &next).await;

    assert_eq!(accepted_token(&manager), None);
}

// REQ-GRANT-37, REQ-WFEXEC-71
#[tokio::test]
async fn a_kept_codex_login_reaches_only_the_steps_of_a_workflow_that_passes_codex_home() {
    let (_temporary, mut manager) = manager_without_agent_logins(PASSING_CODEX_WORKFLOW);
    keep_codex_login(login_directory(&manager));
    let codex_home = login_directory(&manager).codex_home();
    let offered = offer("bg");

    offer_then_prepare(&mut manager, &offered).await;

    assert_eq!(
        accepted_variable(&manager, CODEX_HOME),
        Some(codex_home.into_os_string())
    );
    assert_eq!(manager.environment.variable(OsStr::new(CODEX_HOME)), None);
    assert_succeeded(&execute_to_terminal(&mut manager, &offered).await);
}

// REQ-GRANT-37, REQ-WFEXEC-71
#[tokio::test]
async fn a_workflow_that_does_not_pass_codex_home_receives_none() {
    let workflow = r#"schemaVersion: 1
environmentPassthrough: [OTHER_NAME]
steps:
  check:
    kind: cmd
    command:
      argv: [sh, -c, 'test -z "${CODEX_HOME+x}"']
"#;
    let (_temporary, mut manager) = manager_without_agent_logins(workflow);
    keep_codex_login(login_directory(&manager));
    let offered = offer("bg");

    offer_then_prepare(&mut manager, &offered).await;

    assert_eq!(accepted_variable(&manager, CODEX_HOME), None);
    assert_succeeded(&execute_to_terminal(&mut manager, &offered).await);
}

// REQ-GRANT-37
#[tokio::test]
async fn a_codex_home_in_the_process_environment_passes_through_unchanged() {
    let (temporary, mut manager) = manager_without_agent_logins(PASSING_CODEX_WORKFLOW);
    keep_codex_login(login_directory(&manager));
    let own = temporary.path().join("service-codex");
    create_private_directory(&own);
    write_private(&own.join("auth.json"), b"{\"operator\":true}");
    let mut variables = manager.environment.variables().clone();
    variables.insert(OsString::from(CODEX_HOME), own.clone().into_os_string());
    manager.environment = EnvironmentSnapshot::new(variables);
    let offered = offer("bg");

    offer_then_prepare(&mut manager, &offered).await;

    assert_eq!(
        accepted_variable(&manager, CODEX_HOME),
        Some(own.into_os_string())
    );
    assert_succeeded(&execute_to_terminal(&mut manager, &offered).await);
}

// REQ-GRANT-37
#[tokio::test]
async fn each_admission_looks_for_the_codex_login_the_runner_keeps_then() {
    let (_temporary, mut manager) = manager_without_agent_logins(PASSING_CODEX_WORKFLOW);
    let path = keep_codex_login(login_directory(&manager));
    let first = offer("bg");
    offer_then_prepare(&mut manager, &first).await;
    assert!(accepted_variable(&manager, CODEX_HOME).is_some());
    release_unrun(&mut manager, &first).await;

    fs::remove_file(&path).unwrap();
    let next = offer("bh");
    offer_then_prepare(&mut manager, &next).await;

    assert_eq!(accepted_variable(&manager, CODEX_HOME), None);
}

/// A manager for `PASSING_CODEX_WORKFLOW` whose admissions set agent logins up
/// and ask for their grant tokens against a fake Cloud (REQ-GRANT-49,
/// REQ-GRANT-86), behind the lock the periodic pass's gate takes, and that
/// setup, which keeps its readiness and agent-login directory beside the
/// manager's state.
struct SettingUp {
    temporary: tempfile::TempDir,
    manager: Arc<Mutex<AssignmentManager>>,
    setup: Arc<AgentLoginSetup>,
    cloud: Arc<FakeAgentLoginCloud>,
    status: Arc<LiveStatus>,
}

impl SettingUp {
    fn new(cloud: Arc<FakeAgentLoginCloud>) -> Self {
        Self::running(PASSING_CODEX_WORKFLOW, cloud)
    }

    /// `new`, for a manager whose assignments run `workflow`.
    fn running(workflow: &str, cloud: Arc<FakeAgentLoginCloud>) -> Self {
        Self::running_serving(workflow, cloud, &[])
    }

    /// `running`, for a service whose environment also carries `service`.
    fn running_serving(
        workflow: &str,
        cloud: Arc<FakeAgentLoginCloud>,
        service: &[(&str, &str)],
    ) -> Self {
        let (temporary, manager) = manager_without_agent_logins(workflow);
        Self::serving(temporary, manager, cloud, service)
    }

    /// `new`, for `manager`, whose agent-login directory and service home
    /// are in `temporary`, and whose service environment, the admission's
    /// and the setup's alike, also carries `service`.
    fn serving(
        temporary: tempfile::TempDir,
        mut manager: AssignmentManager,
        cloud: Arc<FakeAgentLoginCloud>,
        service: &[(&str, &str)],
    ) -> Self {
        let mut environment = manager.environment.variables().clone();
        for (name, value) in service {
            environment.insert(OsString::from(name), OsString::from(value));
        }
        manager.environment = EnvironmentSnapshot::new(environment);
        let manager = Arc::new(Mutex::new(manager));
        let (recorder, _capture) = test_recorder("rbt_01k0z6r1w8f4jy2m7q9v3x5abc");
        let setup = Arc::new(AgentLoginSetup::new(
            fake_agent_login_setup_serving(
                Arc::clone(&cloud),
                &temporary.path().join("service-home"),
                service,
                temporary.path(),
                recorder,
            ),
            Arc::new(HeldRunGate::new(Arc::clone(&manager))),
            || time::OffsetDateTime::from_unix_timestamp(1_794_450_000).unwrap(),
        ));
        let status = Arc::new(LiveStatus::new(
            "rbt_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            "rrc_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            None,
        ));
        manager
            .lock()
            .unwrap()
            .set_up_agent_logins_with(AdmissionAgentLogins::new(&setup, Arc::clone(&status)));
        Self {
            temporary,
            manager,
            setup,
            cloud,
            status,
        }
    }

    /// Has each later admission keep its grant token files current
    /// (REQ-GRANT-89) on a controlled sleeper, whose sleeps it answers.
    fn refreshing(&self) -> tokio::sync::mpsc::UnboundedReceiver<(Duration, SleepRelease)> {
        let (sleeper, sleeps) = controlled_sleeper();
        self.manager
            .lock()
            .unwrap()
            .refresh_grant_tokens_with(sleeper, Arc::clone(&self.status));
        sleeps
    }

    /// Admits `offered` and answers the `CODEX_HOME` it was offered. The
    /// manager's lock is held throughout, so an admission whose setup took
    /// it would never finish.
    #[expect(
        clippy::await_holding_lock,
        reason = "the admission's setup must not take the manager's lock"
    )]
    async fn admit(&self, offered: &AssignmentOffer) -> Option<OsString> {
        let mut manager = self.manager.lock().unwrap();
        offer_then_prepare(&mut manager, offered).await;
        accepted_variable(&manager, CODEX_HOME)
    }

    /// Admits `offered`, runs it and answers its reports.
    #[expect(
        clippy::await_holding_lock,
        reason = "the admission's setup must not take the manager's lock"
    )]
    async fn run(&self, offered: &AssignmentOffer) -> Vec<ExecutionReport> {
        let mut manager = self.manager.lock().unwrap();
        offer_then_prepare(&mut manager, offered).await;
        execute_to_terminal(&mut manager, offered).await
    }

    #[expect(
        clippy::await_holding_lock,
        reason = "nothing else takes the manager's lock while it releases"
    )]
    async fn release(&self, offered: &AssignmentOffer) {
        let mut manager = self.manager.lock().unwrap();
        release_unrun(&mut manager, offered).await;
    }

    /// A pass that falls due, as the five-minute schedule makes one.
    async fn periodic_pass(&self) -> SetupOutcome {
        self.setup.run_pass().await
    }

    fn codex_home(&self) -> PathBuf {
        LoginDirectory::beside(&self.temporary.path().join("runner-state.json")).codex_home()
    }

    /// The `auth.json` in the runner's Codex home.
    fn installed(&self) -> Option<Vec<u8>> {
        fs::read(self.codex_home().join("auth.json")).ok()
    }

    fn readiness(&self) -> Option<ReadinessDocument> {
        ReadinessDocument::load(&readiness_path(
            &self.temporary.path().join("runner-state.json"),
        ))
        .unwrap()
    }

    fn codex(&self) -> LoginReadiness {
        self.readiness()
            .and_then(|document| document.login(AgentLoginProvider::Codex).cloned())
            .expect("codex readiness")
    }

    fn grant(&self, provider: AgentLoginProvider) -> GrantReadiness {
        self.readiness()
            .and_then(|document| document.grant(provider).cloned())
            .expect("grant readiness")
    }

    fn requests(&self) -> (u64, u64) {
        (self.cloud.status_requests(), self.cloud.deliveries())
    }

    /// A setup that holds `codex` generation 2, which Cloud still authorizes.
    async fn holding_generation_two() -> Self {
        let fixture = Self::new(FakeAgentLoginCloud::new(fake_codex_authorized(2)));
        assert!(matches!(
            fixture.periodic_pass().await,
            SetupOutcome::Recorded(_)
        ));
        assert_eq!(fixture.installed(), Some(fake_agent_login_material(2)));
        fixture
    }
}

fn auth_json_in(codex_home: &OsStr) -> Option<Vec<u8>> {
    fs::read(Path::new(codex_home).join("auth.json")).ok()
}

// REQ-GRANT-49, INV-RUNNER-15
#[tokio::test]
async fn an_admission_after_a_replacement_is_offered_the_new_generation() {
    let fixture = SettingUp::holding_generation_two().await;
    let first = offer("bg");
    let offered = fixture.admit(&first).await.expect("CODEX_HOME offered");
    assert_eq!(offered, fixture.codex_home().into_os_string());
    assert_eq!(auth_json_in(&offered), Some(fake_agent_login_material(2)));
    assert_eq!(fixture.cloud.deliveries(), 1);

    // Cloud replaces the login while the assignment is held: a pass that
    // falls due then changes nothing.
    fixture.cloud.answer(fake_codex_authorized(3));
    let held = fixture.requests();
    assert_eq!(fixture.periodic_pass().await, SetupOutcome::Busy(None));
    assert_eq!(fixture.requests(), held);
    assert_eq!(auth_json_in(&offered), Some(fake_agent_login_material(2)));

    // The next admission, with no idle pass in between, sets up generation
    // 3 for itself.
    fixture.release(&first).await;
    let offered = fixture
        .admit(&offer("bh"))
        .await
        .expect("CODEX_HOME offered");
    assert_eq!(auth_json_in(&offered), Some(fake_agent_login_material(3)));
    assert_eq!(fixture.cloud.deliveries(), 2);
    let codex = fixture.codex();
    assert_eq!(codex.state, AgentLoginState::Ready);
    assert_eq!(codex.installation().map(|login| login.generation), Some(3));
}

// REQ-GRANT-49, REQ-GRANT-40
#[tokio::test]
async fn an_admission_after_a_revocation_is_offered_no_codex_home_and_the_home_is_gone() {
    let fixture = SettingUp::holding_generation_two().await;
    fixture
        .cloud
        .answer(fake_codex_answer(LoginStatus::Revoked));

    assert_eq!(fixture.admit(&offer("bg")).await, None);

    assert!(fixture.codex_home().symlink_metadata().is_err());
    assert_eq!(fixture.codex().state, AgentLoginState::Revoked);
}

// REQ-GRANT-49
#[tokio::test]
async fn an_admission_whose_status_request_fails_is_offered_the_login_the_runner_holds() {
    let fixture = SettingUp::holding_generation_two().await;
    fixture.cloud.answer(Err(CloudFailure::Transport));
    let (status_requests, deliveries) = fixture.requests();

    let offered = fixture
        .admit(&offer("bg"))
        .await
        .expect("CODEX_HOME offered");

    assert_eq!(auth_json_in(&offered), Some(fake_agent_login_material(2)));
    assert_eq!(fixture.requests(), (status_requests + 1, deliveries));
    assert_eq!(fixture.codex().state, AgentLoginState::CloudUnavailable);
}

// REQ-GRANT-49
#[tokio::test]
async fn an_admission_whose_delivery_is_refused_stale_is_offered_the_generation_it_holds() {
    let fixture = SettingUp::holding_generation_two().await;
    fixture.cloud.answer(fake_codex_authorized(3));
    fixture
        .cloud
        .refuse_deliveries(DeliveryRefusal::StaleGeneration);

    let offered = fixture
        .admit(&offer("bg"))
        .await
        .expect("CODEX_HOME offered");

    assert_eq!(auth_json_in(&offered), Some(fake_agent_login_material(2)));
    assert_eq!(fixture.cloud.deliveries(), 2);
    let codex = fixture.codex();
    assert_eq!(codex.state, AgentLoginState::StaleGeneration);
    assert_eq!(codex.installation().map(|login| login.generation), Some(2));
}

// REQ-GRANT-49
#[tokio::test]
async fn an_admission_asks_nothing_while_the_readiness_names_no_login() {
    let fixture = SettingUp::new(FakeAgentLoginCloud::new(fake_codex_authorized(1)));

    // Nothing is recorded.
    let first = offer("bg");
    assert_eq!(fixture.admit(&first).await, None);
    assert_eq!(fixture.requests(), (0, 0));
    fixture.release(&first).await;

    // What is recorded names nothing installed or authorized.
    fixture.cloud.answer(Ok(Vec::new()));
    assert!(matches!(
        fixture.periodic_pass().await,
        SetupOutcome::Recorded(_)
    ));
    assert_eq!(fixture.codex().state, AgentLoginState::NotAuthorized);
    let recorded = fixture.requests();
    assert_eq!(fixture.admit(&offer("bh")).await, None);
    assert_eq!(fixture.requests(), recorded);
}

// REQ-GRANT-49, REQ-GRANT-51
#[tokio::test]
async fn an_admission_asks_nothing_for_a_provider_cloud_refuses() {
    // Cloud names an authorization in force whose owner it refuses: the
    // provider is `not_authorized` and holds nothing, so no login is
    // authorized and an admission asks nothing, even once Cloud would
    // authorize it again; a periodic pass asks.
    let fixture = SettingUp::new(FakeAgentLoginCloud::new(fake_codex_answer(
        LoginStatus::Unauthorized,
    )));
    assert!(matches!(
        fixture.periodic_pass().await,
        SetupOutcome::Recorded(_)
    ));
    assert_eq!(fixture.codex().state, AgentLoginState::NotAuthorized);

    fixture.cloud.answer(fake_codex_authorized(1));
    let recorded = fixture.requests();
    assert_eq!(fixture.admit(&offer("bg")).await, None);
    assert_eq!(fixture.requests(), recorded);
}

// INV-RUNNER-15
#[tokio::test]
async fn nothing_is_asked_for_installed_or_removed_between_an_offer_and_the_release() {
    let fixture = SettingUp::holding_generation_two().await;
    let first = offer("bg");
    assert!(fixture.admit(&first).await.is_some());
    let admitted = fixture.requests();

    fixture
        .cloud
        .answer(fake_codex_answer(LoginStatus::Revoked));
    assert_eq!(fixture.periodic_pass().await, SetupOutcome::Busy(None));
    fixture.release(&first).await;

    assert_eq!(fixture.requests(), admitted);
    assert_eq!(fixture.installed(), Some(fake_agent_login_material(2)));
    assert_eq!(fixture.codex().state, AgentLoginState::Ready);
}

/// A readiness document as an earlier release left it for a runner that
/// holds no login: each provider in `state` and recorded as selected.
fn record_selected(state_file: &Path, entries: &[(AgentLoginProvider, AgentLoginState)]) {
    let document = readiness_path(state_file);
    fs::write(
        &document,
        serde_json::to_vec(&serde_json::json!({
            "schemaVersion": 1,
            "updatedAt": "2026-11-12T01:00:00Z",
            "providers": entries
                .iter()
                .map(|&(provider, state)| serde_json::json!({
                    "provider": provider.as_str(),
                    "state": state.as_str(),
                    "selected": true,
                }))
                .collect::<Vec<_>>(),
        }))
        .unwrap(),
    )
    .unwrap();
    fs::set_permissions(&document, fs::Permissions::from_mode(0o600)).unwrap();
    assert!(ReadinessDocument::load(&document).unwrap().is_some());
}

// REQ-GRANT-51, REQ-GRANT-37, INV-RUNNER-17
#[test]
fn a_provider_recorded_as_selected_has_nothing_removed_or_replaced() {
    for state in [
        AgentLoginState::InstallFailed,
        AgentLoginState::NotAuthorized,
        AgentLoginState::CloudUnavailable,
    ] {
        let temporary = tempfile::tempdir().unwrap();
        let state_file = temporary.path().join("runner-state.json");
        let home = temporary.path().join("service-home");
        fs::create_dir(&home).unwrap();
        record_selected(
            &state_file,
            &[
                (AgentLoginProvider::Codex, state),
                (AgentLoginProvider::ClaudeCode, state),
            ],
        );
        let directory = LoginDirectory::beside(&state_file);
        let without_codex_home =
            EnvironmentSnapshot::new([(OsString::from("HOME"), home.clone().into_os_string())]);
        let own_codex_home = EnvironmentSnapshot::new([
            (OsString::from("HOME"), home.into_os_string()),
            (
                OsString::from(CODEX_HOME),
                temporary.path().join("service-codex").into_os_string(),
            ),
        ]);

        for service in [without_codex_home, own_codex_home] {
            let offered = crate::agent_login::offer(&service, Some(&directory));
            assert_eq!(offered.environment, service, "{state:?}");
            assert!(offered.offered.is_empty(), "{state:?}");
        }
    }
}

// REQ-GRANT-51, REQ-GRANT-44, INV-RUNNER-17
#[tokio::test]
async fn an_agent_step_with_no_login_to_offer_starts_with_the_variable_unset() {
    // A root list keeps only the names it declares, so it also passes the
    // fixture's own `CLAUDE_CONFIG_DIR` rather than leave the harness to the
    // service `HOME`.
    let workflow = CLAUDE_CODE_ONLY_WORKFLOW.replacen(
        "schemaVersion: 1\n",
        "schemaVersion: 1\nenvironmentPassthrough: [CLAUDE_CODE_OAUTH_TOKEN, CLAUDE_CONFIG_DIR]\n",
        1,
    );
    // The fixture notes the token it was started with, or that it had none.
    let claude_code = SUCCESSFUL_CLAUDE_CODE.replacen(
        "set -eu\n",
        "set -eu\nprintf '%s' \"${CLAUDE_CODE_OAUTH_TOKEN-unset}\" > \"${0%/*}/claude.token\"\n",
        1,
    );
    let (temporary, mut manager) = super::harness_executable_tests::manager_fixture_with_harnesses(
        &workflow,
        None,
        Some(&claude_code),
        None,
    );
    let mut variables = manager.environment.variables().clone();
    variables.remove(OsStr::new(TOKEN_VARIABLE));
    manager.environment = EnvironmentSnapshot::new(variables);
    let state = temporary.path().join("runner-state.json");
    manager.agent_login_directory = Some(LoginDirectory::beside(&state));
    // Cloud named an authorization in force whose owner it refused, and an
    // earlier release recorded the provider as selected.
    record_selected(
        &state,
        &[(
            AgentLoginProvider::ClaudeCode,
            AgentLoginState::NotAuthorized,
        )],
    );
    let offered = offer("bg");

    offer_then_prepare(&mut manager, &offered).await;
    assert_eq!(accepted_variable(&manager, TOKEN_VARIABLE), None);
    let reports = execute_to_terminal(&mut manager, &offered).await;

    assert_succeeded(&reports);
    assert!(
        !format!("{reports:?}").contains("harness_start_failed"),
        "{reports:#?}"
    );
    assert_eq!(
        fs::read_to_string(temporary.path().join("claude.token")).unwrap(),
        "unset"
    );
}

/// The last use the readiness records of the runner's Codex login once an
/// admission whose Codex step succeeded has been followed by a pass, for a
/// service whose environment carries `CODEX_HOME` of its own when
/// `service_codex_home` holds (REQ-GRANT-52).
async fn codex_last_use_after_a_codex_step(service_codex_home: bool) -> Option<UseOutcome> {
    let (temporary, mut manager) = super::harness_executable_tests::manager_fixture_with_harnesses(
        CODEX_ONLY_WORKFLOW,
        None,
        None,
        Some(SUCCESSFUL_CODEX),
    );
    let service_home = temporary.path().join("service-home");
    fs::create_dir(&service_home).unwrap();
    // The admission's `HOME` is the service's empty one, which holds no Codex login.
    let mut variables = manager.environment.variables().clone();
    variables.insert(
        OsString::from("HOME"),
        service_home.clone().into_os_string(),
    );
    if !service_codex_home {
        variables.remove(OsStr::new(CODEX_HOME));
    }
    manager.environment = EnvironmentSnapshot::new(variables);
    let state = temporary.path().join("runner-state.json");
    manager.agent_login_directory = Some(LoginDirectory::beside(&state));
    let (recorder, _capture) = test_recorder("rbt_01k0z6r1w8f4jy2m7q9v3x5abc");
    // The setup's own environment has no `CODEX_HOME`, so it installs the
    // login whichever environment the admission draws from.
    let setup = Arc::new(AgentLoginSetup::new(
        fake_agent_login_setup(
            FakeAgentLoginCloud::new(fake_codex_authorized(2)),
            &service_home,
            temporary.path(),
            recorder,
        ),
        Arc::new(CountedGate::idle()),
        || time::OffsetDateTime::from_unix_timestamp(1_794_450_000).unwrap(),
    ));
    assert!(matches!(setup.run_pass().await, SetupOutcome::Recorded(_)));
    let status = Arc::new(LiveStatus::new(
        "rbt_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
        "rrc_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
        None,
    ));
    manager.set_up_agent_logins_with(AdmissionAgentLogins::new(&setup, status));
    let offered = offer("bg");

    offer_then_prepare(&mut manager, &offered).await;
    let runner_codex_home = LoginDirectory::beside(&state).codex_home();
    assert_eq!(
        accepted_variable(&manager, CODEX_HOME) == Some(runner_codex_home.into_os_string()),
        !service_codex_home
    );
    assert_succeeded(&execute_to_terminal(&mut manager, &offered).await);

    let Some(SetupOutcome::Recorded(document)) = setup
        .set_up_admission(
            "rbt_01k0z6r1w8f4jy2m7q9v3x5abe",
            "run_01k0z6r1w8f4jy2m7q9v3x5abz",
            "asn_01k0z6r1w8f4jy2m7q9v3x5abz",
        )
        .agent_logins
    else {
        panic!("the next pass did not record readiness");
    };
    let codex = document.login(AgentLoginProvider::Codex).unwrap();
    assert_eq!(codex.state, AgentLoginState::Ready);
    codex.last_use.map(|last_use| last_use.outcome)
}

// REQ-GRANT-52
#[tokio::test]
async fn an_admissions_observer_notes_a_use_of_the_codex_home_the_runner_added() {
    assert_eq!(
        codex_last_use_after_a_codex_step(false).await,
        Some(UseOutcome::Accepted)
    );
}

// REQ-GRANT-52, REQ-GRANT-37
#[tokio::test]
async fn a_codex_home_the_service_carried_notes_no_use() {
    assert_eq!(codex_last_use_after_a_codex_step(true).await, None);
}

/// The names each OAuth provider's grant token is offered as: the token's
/// variable and the variable naming its file (REQ-GRANT-87).
const GRANT_NAMES: [(AgentLoginProvider, &str, &str); 3] = [
    (AgentLoginProvider::Github, "GH_TOKEN", "GH_TOKEN_FILE"),
    (
        AgentLoginProvider::Gitlab,
        "GITLAB_TOKEN",
        "GITLAB_TOKEN_FILE",
    ),
    (
        AgentLoginProvider::Linear,
        "USEFUL_LINEAR_ACCESS_TOKEN",
        "USEFUL_LINEAR_ACCESS_TOKEN_FILE",
    ),
];

/// The service's own value of every grant name.
const SERVICE_GRANT_VALUES: [(&str, &str); 6] = [
    ("GH_TOKEN", "service-gh-token"),
    ("GH_TOKEN_FILE", "/service/gh-token"),
    ("GITLAB_TOKEN", "service-gitlab-token"),
    ("GITLAB_TOKEN_FILE", "/service/gitlab-token"),
    ("USEFUL_LINEAR_ACCESS_TOKEN", "service-linear-token"),
    ("USEFUL_LINEAR_ACCESS_TOKEN_FILE", "/service/linear-token"),
];

/// A workflow that passes `variable` and `file_variable`: `kept` must see
/// `token` in the one and in the file the other names, and `narrowed`,
/// whose own list omits both, neither.
fn workflow_passing_a_grant(variable: &str, file_variable: &str, token: &str) -> String {
    format!(
        r#"schemaVersion: 1
environmentPassthrough: [{variable}, {file_variable}]
steps:
  kept:
    kind: cmd
    command:
      argv: [sh, -c, 'test "${variable}" = {token} && test "$(cat "${file_variable}")" = {token}']
  narrowed:
    kind: cmd
    environmentPassthrough: []
    command:
      argv: [sh, -c, 'test -z "${{{variable}+x}}" && test -z "${{{file_variable}+x}}"']
"#
    )
}

/// A workflow whose root passes `passed` and whose one step must succeed
/// at `check`.
fn workflow_checking(passed: &str, check: &str) -> String {
    format!(
        r#"schemaVersion: 1
environmentPassthrough: [{passed}]
steps:
  check:
    kind: cmd
    command:
      argv: [sh, -c, '{check}']
"#
    )
}

/// A shell test that none of `names` is set.
fn unset(names: &[&str]) -> String {
    names
        .iter()
        .map(|name| format!(r#"test -z "${{{name}+x}}""#))
        .collect::<Vec<_>>()
        .join(" && ")
}

fn every_grant_name() -> Vec<&'static str> {
    SERVICE_GRANT_VALUES.iter().map(|(name, _)| *name).collect()
}

fn refused(refusal: GrantTokenRefusal) -> FakeGrantToken {
    FakeGrantToken::Fail(GrantTokenFailure::Refused(refusal))
}

// REQ-GRANT-87, REQ-WFEXEC-71
#[tokio::test]
async fn an_issued_token_and_its_file_reach_only_the_steps_that_pass_them() {
    for (provider, variable, file_variable) in GRANT_NAMES {
        let token = fake_grant_token(provider, 4);
        let fixture = SettingUp::running(
            &workflow_passing_a_grant(variable, file_variable, &token),
            FakeAgentLoginCloud::new(Ok(Vec::new())),
        );
        fixture
            .cloud
            .answer_grant_token(provider, FakeGrantToken::Issue(4));

        assert_succeeded(&fixture.run(&offer("bg")).await);
    }
}

// REQ-GRANT-87, REQ-WFEXEC-71
#[tokio::test]
async fn a_workflow_that_passes_no_grant_name_receives_no_token() {
    let fixture = SettingUp::running(
        &workflow_checking("OTHER_NAME", &unset(&every_grant_name())),
        FakeAgentLoginCloud::new(Ok(Vec::new())),
    );
    for (provider, _, _) in GRANT_NAMES {
        fixture
            .cloud
            .answer_grant_token(provider, FakeGrantToken::Issue(1));
    }

    assert_succeeded(&fixture.run(&offer("bg")).await);
}

/// The `requires.integrations` id that declares `provider` (REQ-WFDEF-34).
fn declared_id(provider: AgentLoginProvider) -> &'static str {
    match provider {
        AgentLoginProvider::Github => "github",
        AgentLoginProvider::Gitlab => "gitlab.com",
        AgentLoginProvider::Linear => "linear",
        other => panic!("{other:?} has no grant names"),
    }
}

/// A workflow that declares the integration `id` and lists no name of it:
/// `plain` and `narrowed`, whose own list is empty, must both succeed at
/// `check`.
fn workflow_declaring(id: &str, check: &str) -> String {
    format!(
        r#"schemaVersion: 1
requires:
  integrations:
    {id}: {{}}
steps:
  plain:
    kind: cmd
    command:
      argv: [sh, -c, '{check}']
  narrowed:
    kind: cmd
    environmentPassthrough: []
    command:
      argv: [sh, -c, '{check}']
"#
    )
}

// REQ-GRANT-87, REQ-WFEXEC-78
#[tokio::test]
async fn a_declared_providers_token_and_its_file_reach_every_step() {
    for (provider, variable, file_variable) in GRANT_NAMES {
        let token = fake_grant_token(provider, 4);
        let fixture = SettingUp::running(
            &workflow_declaring(
                declared_id(provider),
                &format!(
                    r#"test "${variable}" = {token} && test "$(cat "${file_variable}")" = {token}"#
                ),
            ),
            FakeAgentLoginCloud::new(Ok(Vec::new())),
        );
        fixture
            .cloud
            .answer_grant_token(provider, FakeGrantToken::Issue(4));
        assert_succeeded(&fixture.run(&offer("bg")).await);

        let service = |name: &str| {
            SERVICE_GRANT_VALUES
                .iter()
                .find(|(service, _)| *service == name)
                .map(|(_, value)| *value)
                .unwrap()
        };
        let fixture = SettingUp::running_serving(
            &workflow_declaring(
                declared_id(provider),
                &format!(
                    r#"test "${variable}" = {} && test "${file_variable}" = {}"#,
                    service(variable),
                    service(file_variable)
                ),
            ),
            FakeAgentLoginCloud::new(Ok(Vec::new())),
            &SERVICE_GRANT_VALUES,
        );
        fixture
            .cloud
            .answer_grant_token(provider, FakeGrantToken::Issue(1));
        assert_succeeded(&fixture.run(&offer("bg")).await);
    }
}

/// A workflow that declares no integration and lists no name: its one
/// step must succeed at `check`.
fn workflow_declaring_nothing(check: &str) -> String {
    format!(
        r#"schemaVersion: 1
steps:
  check:
    kind: cmd
    command:
      argv: [sh, -c, '{check}']
"#
    )
}

/// A fake Cloud that lists `mergify` and `buildkite` as token services and
/// issues Mergify's token from an integration token.
fn cloud_issuing_mergify() -> Arc<FakeAgentLoginCloud> {
    let cloud = FakeAgentLoginCloud::new(Ok(Vec::new()));
    cloud.answer_token_services(Ok(mergify_and_buildkite()));
    cloud.answer_grant_token(mergify(), FakeGrantToken::IssueIntegrationToken(1));
    cloud
}

// REQ-WFEXEC-78, REQ-INTEGRATION-12
#[tokio::test]
async fn a_declared_token_services_token_and_its_file_reach_every_step() {
    let token = fake_grant_token(mergify(), 1);
    let fixture = SettingUp::running(
        &workflow_declaring(
            "mergify",
            &format!(
                r#"test "$MERGIFY_TOKEN" = {token} && test "$(cat "$MERGIFY_TOKEN_FILE")" = {token}"#
            ),
        ),
        cloud_issuing_mergify(),
    );
    assert_succeeded(&fixture.run(&offer("bg")).await);

    // The host provides Mergify: its two values reach every step, and the
    // runner asks Cloud for no token of it.
    let fixture = SettingUp::running_serving(
        &workflow_declaring(
            "mergify",
            r#"test "$MERGIFY_TOKEN" = service-mergify-token && test "$MERGIFY_TOKEN_FILE" = /service/mergify-token"#,
        ),
        cloud_issuing_mergify(),
        &[
            ("MERGIFY_TOKEN", "service-mergify-token"),
            ("MERGIFY_TOKEN_FILE", "/service/mergify-token"),
        ],
    );
    assert_succeeded(&fixture.run(&offer("bg")).await);
    assert!(
        fixture
            .cloud
            .grant_requests()
            .iter()
            .all(|request| request.provider != mergify())
    );
}

// REQ-WFEXEC-78, REQ-INTEGRATION-12
#[tokio::test]
async fn a_workflow_that_declares_no_token_service_receives_none_of_its_names() {
    let fixture = SettingUp::running(
        &workflow_declaring_nothing(&unset(&["MERGIFY_TOKEN", "MERGIFY_TOKEN_FILE"])),
        cloud_issuing_mergify(),
    );

    assert_succeeded(&fixture.run(&offer("bg")).await);
}

// REQ-GRANT-44, REQ-WFEXEC-78
#[tokio::test]
async fn a_kept_token_reaches_every_step_of_a_workflow_that_declares_claude_code() {
    let (_temporary, mut manager) = manager_without_agent_logins(&workflow_declaring(
        "claude_code",
        &format!(r#"test "${TOKEN_VARIABLE}" = {DELIVERED}"#),
    ));
    keep_token(login_directory(&manager));
    let offered = offer("bg");

    offer_then_prepare(&mut manager, &offered).await;

    assert_eq!(accepted_token(&manager), Some(OsString::from(DELIVERED)));
    assert_succeeded(&execute_to_terminal(&mut manager, &offered).await);
}

// REQ-GRANT-37, REQ-WFEXEC-78
#[tokio::test]
async fn a_kept_codex_login_reaches_every_step_of_a_workflow_that_declares_codex() {
    let (_temporary, mut manager) = manager_without_agent_logins(&workflow_declaring(
        "codex",
        &format!(r#"test -f "${CODEX_HOME}/auth.json""#),
    ));
    keep_codex_login(login_directory(&manager));
    let codex_home = login_directory(&manager).codex_home();
    let offered = offer("bg");

    offer_then_prepare(&mut manager, &offered).await;

    assert_eq!(
        accepted_variable(&manager, CODEX_HOME),
        Some(codex_home.into_os_string())
    );
    assert_succeeded(&execute_to_terminal(&mut manager, &offered).await);
}

/// A fake Cloud that lists `example`, a provider of its configuration, with
/// `EXAMPLE_ACCESS_TOKEN` and `EXAMPLE_ACCESS_TOKEN_FILE` for each
/// assignment, and issues its token at generation 2.
fn cloud_listing_example() -> Arc<FakeAgentLoginCloud> {
    let cloud = FakeAgentLoginCloud::new(Ok(Vec::new()));
    cloud.answer_grant_providers(Ok(vec![ListedProvider {
        provider: "example".to_owned(),
        token: "EXAMPLE_ACCESS_TOKEN".to_owned(),
        token_file: "EXAMPLE_ACCESS_TOKEN_FILE".to_owned(),
    }]));
    cloud.answer_grant_token(example(), FakeGrantToken::Issue(2));
    cloud
}

fn example() -> GrantProvider {
    GrantProvider::listed("example").unwrap()
}

// REQ-GRANT-87, REQ-WFEXEC-71
#[tokio::test]
async fn a_configured_providers_token_and_its_file_reach_only_the_steps_that_pass_them() {
    let token = fake_grant_token(example(), 2);
    let fixture = SettingUp::running(
        &workflow_passing_a_grant("EXAMPLE_ACCESS_TOKEN", "EXAMPLE_ACCESS_TOKEN_FILE", &token),
        cloud_listing_example(),
    );

    assert_succeeded(&fixture.run(&offer("bg")).await);
    assert_eq!(
        fixture
            .cloud
            .grant_requests()
            .iter()
            .map(|request| request.provider.clone())
            .collect::<Vec<_>>(),
        [example()]
    );
}

// REQ-GRANT-87, REQ-WFEXEC-71
#[tokio::test]
async fn a_workflow_that_passes_no_configured_providers_name_receives_nothing() {
    let fixture = SettingUp::running(
        &workflow_checking(
            "OTHER_NAME",
            &unset(&["EXAMPLE_ACCESS_TOKEN", "EXAMPLE_ACCESS_TOKEN_FILE"]),
        ),
        cloud_listing_example(),
    );

    assert_succeeded(&fixture.run(&offer("bg")).await);
    assert_eq!(fixture.cloud.grant_requests().len(), 1);
}

/// The providers the fake Cloud was asked for grant tokens of, in the
/// document's order.
fn asked_for(cloud: &FakeAgentLoginCloud) -> Vec<AgentLoginProvider> {
    let asked: Vec<AgentLoginProvider> = cloud
        .grant_requests()
        .iter()
        .filter_map(|request| request.provider.agent_login_provider())
        .collect();
    GRANT_NAMES
        .iter()
        .map(|(provider, _, _)| *provider)
        .filter(|provider| asked.contains(provider))
        .collect()
}

// REQ-GRANT-87, REQ-GRANT-86, REQ-WORKSPACE-18
#[tokio::test]
async fn a_gh_token_the_service_sets_reaches_the_step_and_a_grant_fills_only_its_absence() {
    let issuing_every_grant = || {
        let cloud = FakeAgentLoginCloud::new(Ok(Vec::new()));
        for (provider, _, _) in GRANT_NAMES {
            cloud.answer_grant_token(provider, FakeGrantToken::Issue(1));
        }
        cloud
    };
    let offered = offer("bg");

    // The host provides `github`: its value reaches the step, no file is
    // added, and Cloud is asked for no GitHub token, only the other two.
    let fixture = SettingUp::running_serving(
        &workflow_checking(
            "GH_TOKEN, GH_TOKEN_FILE",
            &format!(
                r#"test "$GH_TOKEN" = service-gh-token && {}"#,
                unset(&["GH_TOKEN_FILE"])
            ),
        ),
        issuing_every_grant(),
        &[("GH_TOKEN", "service-gh-token")],
    );
    assert_succeeded(&fixture.run(&offered).await);
    assert_eq!(
        asked_for(&fixture.cloud),
        [AgentLoginProvider::Gitlab, AgentLoginProvider::Linear]
    );
    let github = fixture.grant(AgentLoginProvider::Github);
    assert_eq!(
        (
            github.state,
            github.issued,
            github.checked_at.is_some(),
            github.run_id.as_deref()
        ),
        (
            AgentLoginState::NotDelivered,
            None,
            true,
            Some(offered.run_id.as_str())
        )
    );

    // The host sets none of the six names: the token issued, and its file,
    // reach the step.
    let token = fake_grant_token(AgentLoginProvider::Github, 1);
    let fixture = SettingUp::running(
        &workflow_checking(
            "GH_TOKEN, GH_TOKEN_FILE",
            &format!(r#"test "$GH_TOKEN" = {token} && test "$(cat "$GH_TOKEN_FILE")" = {token}"#),
        ),
        issuing_every_grant(),
    );
    assert_succeeded(&fixture.run(&offered).await);
    assert_eq!(
        asked_for(&fixture.cloud),
        GRANT_NAMES.map(|(provider, _, _)| provider)
    );
}

// REQ-GRANT-87, REQ-GRANT-86, REQ-WORKSPACE-18
#[tokio::test]
async fn every_provider_the_host_provides_keeps_the_services_names_whatever_cloud_holds() {
    let check = SERVICE_GRANT_VALUES
        .iter()
        .map(|(name, value)| format!(r#"test "${name}" = {value}"#))
        .collect::<Vec<_>>()
        .join(" && ");
    let fixture = SettingUp::running_serving(
        &workflow_checking(&every_grant_name().join(", "), &check),
        FakeAgentLoginCloud::new(Ok(Vec::new())),
        &SERVICE_GRANT_VALUES,
    );
    for (provider, _, _) in GRANT_NAMES {
        fixture
            .cloud
            .answer_grant_token(provider, FakeGrantToken::Issue(1));
    }

    assert_succeeded(&fixture.run(&offer("bg")).await);
    assert!(fixture.cloud.grant_requests().is_empty());
    for (provider, _, _) in GRANT_NAMES {
        assert_eq!(
            fixture.grant(provider).state,
            AgentLoginState::NotDelivered,
            "{provider:?}"
        );
    }
}

// REQ-GRANT-87, REQ-GRANT-86, REQ-WORKSPACE-18
#[tokio::test]
async fn a_provider_an_earlier_release_recorded_as_selected_with_no_token_leaves_its_names_unset() {
    let fixture = SettingUp::running(
        &workflow_checking(
            "GH_TOKEN, GH_TOKEN_FILE",
            &unset(&["GH_TOKEN", "GH_TOKEN_FILE"]),
        ),
        FakeAgentLoginCloud::new(Ok(Vec::new())),
    );
    // An earlier release recorded `github` as selected: Cloud named a grant
    // in force that could not issue a token.
    let document = readiness_path(&fixture.temporary.path().join("runner-state.json"));
    fs::write(
        &document,
        serde_json::to_vec(&serde_json::json!({
            "schemaVersion": 1,
            "updatedAt": "2026-11-12T01:00:00Z",
            "providers": [{
                "provider": "github",
                "state": "reconnect_required",
                "checkedAt": "2026-11-12T01:00:00Z",
                "selected": true,
                "runId": "run_01k0z6r1w8f4jy2m7q9v3x5abz",
            }],
        }))
        .unwrap(),
    )
    .unwrap();
    fs::set_permissions(&document, fs::Permissions::from_mode(0o600)).unwrap();
    fixture.cloud.answer_grant_token(
        AgentLoginProvider::Github,
        refused(GrantTokenRefusal::ReconnectRequired),
    );

    assert_succeeded(&fixture.run(&offer("bg")).await);
    assert_eq!(
        fixture.grant(AgentLoginProvider::Github).state,
        AgentLoginState::ReconnectRequired
    );
}

// REQ-GRANT-87, REQ-GRANT-51
#[tokio::test]
async fn an_agent_step_starts_with_a_grant_name_unset() {
    // The names the Codex fixture reads, and the GitLab grant's.
    let workflow = CODEX_ONLY_WORKFLOW.replacen(
        "schemaVersion: 1\n",
        "schemaVersion: 1\nenvironmentPassthrough: [GITLAB_TOKEN, GITLAB_TOKEN_FILE, CODEX_HOME, CODEX_FIXTURE_HELPER, CODEX_FIXTURE_ARGUMENTS, CODEX_FIXTURE_REQUESTS, CODEX_FIXTURE_PROCESS, CODEX_FIXTURE_READY, CODEX_FIXTURE_PROCEED, CODEX_FIXTURE_DESCENDANT, CODEX_FIXTURE_SCENARIO, CODEX_FIXTURE_VERSION, CODEX_FIXTURE_RESPONSE]\n",
        1,
    );
    let (temporary, mut manager) = super::harness_executable_tests::manager_fixture_with_harnesses(
        &workflow,
        None,
        None,
        Some(SUCCESSFUL_CODEX),
    );
    fs::create_dir(temporary.path().join("service-home")).unwrap();
    manager.agent_login_directory = Some(LoginDirectory::beside(
        &temporary.path().join("runner-state.json"),
    ));
    let fixture = SettingUp::serving(
        temporary,
        manager,
        FakeAgentLoginCloud::new(Ok(Vec::new())),
        &[],
    );
    fixture.cloud.answer_grant_token(
        AgentLoginProvider::Gitlab,
        refused(GrantTokenRefusal::ReconnectRequired),
    );

    assert_succeeded(&fixture.run(&offer("bg")).await);
    assert!(fixture.temporary.path().join("codex.calls").exists());
}

// REQ-GRANT-89, INV-RUNNER-28
#[tokio::test]
async fn a_running_assignment_has_its_token_file_replaced_and_an_ended_one_does_not() {
    let wait = Duration::from_secs(60);
    let one = fake_grant_token(AgentLoginProvider::Github, 1);
    let two = fake_grant_token(AgentLoginProvider::Github, 2);
    let marker = tempfile::tempdir().unwrap();
    let started = marker.path().join("started");
    // The step says it has started, then waits for the file to hold
    // generation 2; its variable is still generation 1's.
    let workflow = workflow_checking(
        "GH_TOKEN, GH_TOKEN_FILE",
        &format!(
            r#": > {started} && i=0 && until test "$(cat "$GH_TOKEN_FILE")" = {two}; do i=$((i+1)); test $i -lt 600 || exit 1; sleep 0.1; done && test "$GH_TOKEN" = {one}"#,
            started = started.display(),
        ),
    );
    let fixture = SettingUp::running(&workflow, FakeAgentLoginCloud::new(Ok(Vec::new())));
    let mut sleeps = fixture.refreshing();
    fixture
        .cloud
        .issue_grant_tokens_with(wait, fake_grant_expiry());
    fixture
        .cloud
        .answer_grant_token(AgentLoginProvider::Github, FakeGrantToken::Issue(1));

    let refresh = async {
        let (asked, release) = sleeps.recv().await.unwrap();
        assert_eq!(asked, wait);
        wait_for_fixture_path(&started).await;
        fixture
            .cloud
            .answer_grant_token(AgentLoginProvider::Github, FakeGrantToken::Issue(2));
        release.release();
    };
    let running = offer("bg");
    let (reports, ()) = tokio::join!(fixture.run(&running), refresh);

    assert_succeeded(&reports);
    assert_eq!(
        fixture
            .grant(AgentLoginProvider::Github)
            .issued
            .unwrap()
            .generation,
        2
    );

    // An assignment that has ended asks nothing when its wait passes, even
    // while the worker that admitted it is still ending (INV-RUNNER-28).
    let fixture = SettingUp::running(&workflow, FakeAgentLoginCloud::new(Ok(Vec::new())));
    let mut sleeps = fixture.refreshing();
    let (sleeper, teardown) = preparation_teardown_holding_sleeper();
    fixture.manager.lock().unwrap().sleeper = sleeper;
    fixture
        .cloud
        .answer_grant_token(AgentLoginProvider::Github, FakeGrantToken::Issue(1));
    let ended = offer("bh");
    fixture.admit(&ended).await;
    let tokens = LoginDirectory::beside(&fixture.temporary.path().join("runner-state.json"))
        .assignment(&ended.assignment_id)
        .expect("the assignment has a token directory");
    assert!(tokens.is_dir());
    // Only the assignment's task and the held worker keep the sleeper now,
    // so its sleeps end with both.
    fixture.manager.lock().unwrap().grant_token_refresh = None;
    let (_, release) = sleeps.recv().await.unwrap();
    fixture.release(&ended).await;
    fixture
        .cloud
        .answer_grant_token(AgentLoginProvider::Github, FakeGrantToken::Issue(2));

    release.release();

    // The assignment's hold ended with it: its directory is removed and
    // its task asks for no further sleep and nothing of Cloud.
    wait_for_fixture_path_removal(&tokens).await;
    assert!(matches!(
        sleeps.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
    assert_eq!(fixture.cloud.grant_requests().len(), 3);
    teardown.let_go();

    assert!(
        with_watchdog(sleeps.recv())
            .await
            .expect("the ended assignment's task stops")
            .is_none()
    );
    assert_eq!(fixture.cloud.grant_requests().len(), 3);
    assert_eq!(
        fixture
            .grant(AgentLoginProvider::Github)
            .issued
            .unwrap()
            .generation,
        1
    );
}

// REQ-GRANT-89, SC-GRANT-13
#[tokio::test]
async fn a_running_assignment_has_its_configured_providers_token_file_replaced() {
    let wait = Duration::from_secs(60);
    let one = fake_grant_token(example(), 1);
    let two = fake_grant_token(example(), 2);
    let marker = tempfile::tempdir().unwrap();
    let started = marker.path().join("started");
    // The step says it has started, then waits for the file to hold
    // generation 2; its variable is still generation 1's.
    let workflow = workflow_checking(
        "EXAMPLE_ACCESS_TOKEN, EXAMPLE_ACCESS_TOKEN_FILE",
        &format!(
            r#": > {started} && i=0 && until test "$(cat "$EXAMPLE_ACCESS_TOKEN_FILE")" = {two}; do i=$((i+1)); test $i -lt 600 || exit 1; sleep 0.1; done && test "$EXAMPLE_ACCESS_TOKEN" = {one}"#,
            started = started.display(),
        ),
    );
    let fixture = SettingUp::running(&workflow, cloud_listing_example());
    let mut sleeps = fixture.refreshing();
    fixture
        .cloud
        .issue_grant_tokens_with(wait, fake_grant_expiry());
    fixture
        .cloud
        .answer_grant_token(example(), FakeGrantToken::Issue(1));

    let refresh = async {
        let (asked, release) = sleeps.recv().await.unwrap();
        assert_eq!(asked, wait);
        wait_for_fixture_path(&started).await;
        fixture
            .cloud
            .answer_grant_token(example(), FakeGrantToken::Issue(2));
        release.release();
    };
    let running = offer("bg");
    let (reports, ()) = tokio::join!(fixture.run(&running), refresh);

    assert_succeeded(&reports);
    assert_eq!(
        fixture
            .cloud
            .grant_requests()
            .iter()
            .map(|request| request.provider.clone())
            .collect::<Vec<_>>(),
        [example(), example()]
    );
}

/// The boot `SettingUp`'s live status serves as.
const SETTING_UP_BOOT: &str = "rbt_01k0z6r1w8f4jy2m7q9v3x5abc";

/// The entry of `provider` reported as `source`, with `grant_id` or
/// `agent_login_id` where it names one.
fn reported(
    provider: AgentLoginProvider,
    source: CredentialSource,
    grant_id: Option<&str>,
    agent_login_id: Option<&str>,
) -> ReportedSource {
    ReportedSource {
        provider,
        source,
        grant_id: grant_id.map(str::to_owned),
        agent_login_id: agent_login_id.map(str::to_owned),
    }
}

/// A fake Cloud that authorizes the fake Claude Code login, issues a
/// Linear token and records each credential-source report.
fn cloud_issuing_linear_and_claude_code() -> Arc<FakeAgentLoginCloud> {
    let cloud = FakeAgentLoginCloud::new(Ok(vec![ProviderLogin {
        provider: AgentLoginProvider::ClaudeCode,
        agent_login_id: FAKE_AGENT_LOGIN_ID.to_owned(),
        status: LoginStatus::Authorized {
            generation: 1,
            record_schema_version: 1,
        },
    }]));
    cloud.answer_grant_token(AgentLoginProvider::Linear, FakeGrantToken::Issue(1));
    cloud.answer_credential_sources(Ok(()));
    cloud
}

/// The entries of the five `cloud_issuing_linear_and_claude_code` and a
/// service carrying `GH_TOKEN` report.
fn reported_five() -> Vec<ReportedSource> {
    vec![
        reported(
            AgentLoginProvider::ClaudeCode,
            CredentialSource::Grant,
            None,
            Some(FAKE_AGENT_LOGIN_ID),
        ),
        reported(
            AgentLoginProvider::Github,
            CredentialSource::Host,
            None,
            None,
        ),
        reported(
            AgentLoginProvider::Linear,
            CredentialSource::Grant,
            Some(FAKE_GRANT_ID),
            None,
        ),
    ]
}

/// Admits an assignment against `cloud`, after a pass installs its Claude
/// Code login, for a service whose environment carries `GH_TOKEN` and
/// `service`, and answers the reports Cloud took and the assignment's id.
async fn admission_reports(
    cloud: Arc<FakeAgentLoginCloud>,
    service: &[(&str, &str)],
) -> (Vec<CredentialSourcesRequest>, String) {
    let service: Vec<(&str, &str)> = [("GH_TOKEN", "service-gh-token")]
        .into_iter()
        .chain(service.iter().copied())
        .collect();
    let fixture =
        SettingUp::running_serving(&workflow_checking("OTHER_NAME", "true"), cloud, &service);
    assert!(matches!(
        fixture.periodic_pass().await,
        SetupOutcome::Recorded(_)
    ));
    assert!(fixture.cloud.credential_source_reports().is_empty());
    let offered = offer("bg");

    fixture.admit(&offered).await;

    (
        fixture.cloud.credential_source_reports(),
        offered.assignment_id,
    )
}

// REQ-GRANT-93, REQ-GRANT-92
#[tokio::test]
async fn an_admission_reports_each_providers_source_once_before_it_is_accepted() {
    // The host provides GitHub, Cloud issues a Linear token, and the runner
    // keeps a Claude Code login its readiness records as installed; it has
    // no source of GitLab or Codex.
    let (reports, assignment_id) =
        admission_reports(cloud_issuing_linear_and_claude_code(), &[]).await;

    // The report was made on the admission's preparation, so it is there
    // once the assignment is accepted, and only once.
    assert_eq!(
        reports,
        [CredentialSourcesRequest {
            boot_id: SETTING_UP_BOOT.to_owned(),
            assignment_id,
            sources: reported_five(),
            token_services: Vec::new(),
        }]
    );
}

/// Cloud's token-service list naming `mergify` and `buildkite`, each with
/// its two names.
fn mergify_and_buildkite() -> Vec<ListedProvider> {
    [
        ("mergify", "MERGIFY_TOKEN", "MERGIFY_TOKEN_FILE"),
        (
            "buildkite",
            "BUILDKITE_API_TOKEN",
            "BUILDKITE_API_TOKEN_FILE",
        ),
    ]
    .into_iter()
    .map(|(provider, token, token_file)| ListedProvider {
        provider: provider.to_owned(),
        token: token.to_owned(),
        token_file: token_file.to_owned(),
    })
    .collect()
}

fn mergify() -> GrantProvider {
    GrantProvider::token_service("mergify").unwrap()
}

/// The service environment of the token-service report tests: it carries
/// Buildkite's token and not Mergify's.
const SERVICE_BUILDKITE: [(&str, &str); 1] = [("BUILDKITE_API_TOKEN", "service-buildkite-token")];

// REQ-INTEGRATION-12, REQ-GRANT-93
#[tokio::test]
async fn an_admissions_one_report_names_each_listed_token_service_with_a_source_after_the_five() {
    // Cloud lists both token services and issues Mergify's token from an
    // integration token; the service carries Buildkite's.
    let cloud = cloud_issuing_linear_and_claude_code();
    cloud.answer_token_services(Ok(mergify_and_buildkite()));
    cloud.answer_grant_token(mergify(), FakeGrantToken::IssueIntegrationToken(1));

    let (reports, assignment_id) = admission_reports(cloud, &SERVICE_BUILDKITE).await;

    assert_eq!(
        reports,
        [CredentialSourcesRequest {
            boot_id: SETTING_UP_BOOT.to_owned(),
            assignment_id,
            sources: reported_five(),
            token_services: vec![
                ReportedTokenService {
                    provider: "mergify".to_owned(),
                    source: TokenServiceSource::Token {
                        integration_token_id: FAKE_INTEGRATION_TOKEN_ID.to_owned(),
                    },
                },
                ReportedTokenService {
                    provider: "buildkite".to_owned(),
                    source: TokenServiceSource::Host,
                },
            ],
        }]
    );
}

// REQ-INTEGRATION-12, REQ-COMPAT-17
#[tokio::test]
async fn a_report_names_no_token_service_cloud_did_not_list_or_the_runner_has_no_source_of() {
    // A Cloud without the token-service list: the report is today's,
    // although the service carries Buildkite's token.
    let cloud = cloud_issuing_linear_and_claude_code();
    cloud.answer_token_services(Err(GrantProvidersFailure::NoRoute));
    cloud.answer_grant_token(mergify(), FakeGrantToken::IssueIntegrationToken(1));
    let (reports, _) = admission_reports(cloud, &SERVICE_BUILDKITE).await;
    assert_eq!(
        reports
            .iter()
            .map(|report| (report.sources.clone(), report.token_services.clone()))
            .collect::<Vec<_>>(),
        [(reported_five(), Vec::new())]
    );

    // Cloud refuses Mergify's token, which the host does not provide: only
    // Buildkite, which it does, is named.
    let cloud = cloud_issuing_linear_and_claude_code();
    cloud.answer_token_services(Ok(mergify_and_buildkite()));
    cloud.answer_grant_token(mergify(), refused(GrantTokenRefusal::Unauthorized));
    let (reports, _) = admission_reports(cloud, &SERVICE_BUILDKITE).await;
    assert_eq!(
        reports
            .iter()
            .map(|report| (report.sources.clone(), report.token_services.clone()))
            .collect::<Vec<_>>(),
        [(
            reported_five(),
            vec![ReportedTokenService {
                provider: "buildkite".to_owned(),
                source: TokenServiceSource::Host,
            }]
        )]
    );
}

// REQ-GRANT-93
#[tokio::test]
async fn an_admission_whose_report_cloud_does_not_take_is_accepted_and_runs_all_the_same() {
    for answer in [
        Err(CredentialSourcesFailure::Cloud(CloudFailure::Status(409))),
        Err(CredentialSourcesFailure::Cloud(CloudFailure::Transport)),
        Err(CredentialSourcesFailure::NoRoute),
    ] {
        let fixture = SettingUp::running_serving(
            &workflow_checking("GH_TOKEN", r#"test "$GH_TOKEN" = service-gh-token"#),
            FakeAgentLoginCloud::new(Ok(Vec::new())),
            &[("GH_TOKEN", "service-gh-token")],
        );
        fixture.cloud.answer_credential_sources(answer);

        assert_succeeded(&fixture.run(&offer("bg")).await);
        assert_eq!(
            fixture
                .cloud
                .credential_source_reports()
                .iter()
                .map(|report| report.sources.clone())
                .collect::<Vec<_>>(),
            [vec![reported(
                AgentLoginProvider::Github,
                CredentialSource::Host,
                None,
                None,
            )]],
            "{answer:?}"
        );
    }
}

// REQ-GRANT-93
#[tokio::test]
#[expect(
    clippy::await_holding_lock,
    reason = "the admission's setup must not take the manager's lock"
)]
async fn a_continuations_admission_reports_its_own_sources() {
    let (temporary, manager, initial, workspace, _baseline) =
        super::git_continuation_tests::retained_initial_attempt(false).await;
    let fixture = SettingUp::serving(
        temporary,
        manager,
        FakeAgentLoginCloud::new(Ok(Vec::new())),
        &[("GH_TOKEN", "service-gh-token")],
    );
    let mut manager = fixture.manager.lock().unwrap();
    let successor =
        super::git_continuation_tests::continuation_successor(&manager, &initial, &workspace);
    manager.handle_offer(successor.clone()).unwrap();

    super::git_continuation_tests::prepare_retained_continuation(&mut manager, &successor).await;

    assert!(matches!(
        held_assignment(&manager),
        Some(HeldAssignment::Accepted(_))
    ));
    // The fixture's own coding-agent logins may add host entries; the
    // continuation's one report is its own assignment's.
    let reports = fixture.cloud.credential_source_reports();
    assert_eq!(
        reports
            .iter()
            .map(|report| (report.boot_id.as_str(), report.assignment_id.as_str()))
            .collect::<Vec<_>>(),
        [(SETTING_UP_BOOT, successor.assignment_id.as_str())]
    );
    assert!(
        reports[0].sources.contains(&reported(
            AgentLoginProvider::Github,
            CredentialSource::Host,
            None,
            None,
        )),
        "{reports:?}"
    );
}
