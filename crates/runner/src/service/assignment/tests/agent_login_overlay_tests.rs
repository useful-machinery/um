use std::os::unix::fs::{DirBuilderExt as _, OpenOptionsExt as _};

use super::*;
use um_execution::{
    CLAUDE_CODE_LOGIN_VARIABLE as TOKEN_VARIABLE, CODEX_LOGIN_VARIABLE as CODEX_HOME,
};

use crate::agent_login::cloud::{
    CloudFailure, DeliveryRefusal, GrantTokenFailure, GrantTokenRefusal, LoginStatus,
};
use crate::agent_login::directory::LoginDirectory;
use crate::agent_login::readiness::{
    GrantReadiness, LoginReadiness, ReadinessDocument, UseOutcome, readiness_path,
};
use crate::agent_login::setup::{AgentLoginSetup, SetupOutcome};
use crate::control_protocol::{AgentLoginProvider, AgentLoginState};
use crate::service::IdleAssignmentGate;
use crate::service::control::{AdmissionAgentLogins, LiveStatus};
use crate::service::test_support::{
    CountedGate, FakeAgentLoginCloud, FakeGrantToken, SleepRelease, controlled_sleeper,
    fake_agent_login_material, fake_agent_login_setup, fake_codex_answer, fake_codex_authorized,
    fake_grant_expiry, fake_grant_token,
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
    match &manager.slot {
        Some(LocalSlot::Accepted(accepted)) => engine_admission(accepted)
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
    assert!(manager.slot.is_none());
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
        let (temporary, manager) = manager_without_agent_logins(workflow);
        Self::serving(temporary, manager, cloud)
    }

    /// `new`, for `manager`, whose agent-login directory and service home
    /// are in `temporary`.
    fn serving(
        temporary: tempfile::TempDir,
        manager: AssignmentManager,
        cloud: Arc<FakeAgentLoginCloud>,
    ) -> Self {
        let manager = Arc::new(Mutex::new(manager));
        let (recorder, _capture) = test_recorder("rbt_01k0z6r1w8f4jy2m7q9v3x5abc");
        let setup = Arc::new(AgentLoginSetup::new(
            fake_agent_login_setup(
                Arc::clone(&cloud),
                &temporary.path().join("service-home"),
                temporary.path(),
                recorder,
            ),
            Arc::new(IdleAssignmentGate::new(Arc::clone(&manager))),
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

    /// Adds `variables` to the service's environment.
    fn serve_with(&self, variables: &[(&str, &str)]) {
        let mut manager = self.manager.lock().unwrap();
        let mut environment = manager.environment.variables().clone();
        for (name, value) in variables {
            environment.insert(OsString::from(name), OsString::from(value));
        }
        manager.environment = EnvironmentSnapshot::new(environment);
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
async fn an_admission_sets_up_a_selected_provider_that_holds_no_login() {
    // Cloud names an authorization in force whose owner it refuses: the
    // provider is selected, `not_authorized`, and holds nothing.
    let fixture = SettingUp::new(FakeAgentLoginCloud::new(fake_codex_answer(
        LoginStatus::Unauthorized,
    )));
    assert!(matches!(
        fixture.periodic_pass().await,
        SetupOutcome::Recorded(_)
    ));
    let codex = fixture.codex();
    assert_eq!(codex.state, AgentLoginState::NotAuthorized);
    assert!(codex.selected);

    // Cloud authorizes it again; the next admission asks, rather than
    // withhold the login until a periodic pass finds the runner idle.
    fixture.cloud.answer(fake_codex_authorized(1));
    assert!(fixture.admit(&offer("bg")).await.is_some());
    assert_eq!(fixture.installed(), Some(fake_agent_login_material(1)));
    assert_eq!(fixture.codex().state, AgentLoginState::Ready);
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

/// A runner whose agent-login directory and readiness document are beside a
/// state file in a fresh directory, served from an environment with no
/// Claude Code or Codex login of its own: an empty home and nothing else.
struct Selecting {
    temporary: tempfile::TempDir,
    directory: LoginDirectory,
    service: BTreeMap<OsString, OsString>,
}

impl Selecting {
    fn new() -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let state = temporary.path().join("runner-state.json");
        let home = temporary.path().join("service-home");
        fs::create_dir(&home).unwrap();
        Self {
            directory: LoginDirectory::beside(&state),
            service: BTreeMap::from([(OsString::from("HOME"), home.into_os_string())]),
            temporary,
        }
    }

    fn home(&self) -> &Path {
        Path::new(&self.service[OsStr::new("HOME")])
    }

    fn readiness(&self) -> PathBuf {
        readiness_path(&self.temporary.path().join("runner-state.json"))
    }

    /// Records one entry per `(provider, state, selected)`, as a pass leaves
    /// the document.
    fn record(&self, entries: &[(AgentLoginProvider, AgentLoginState, bool)]) {
        let _ = fs::remove_file(self.readiness());
        ReadinessDocument {
            updated_at: time::OffsetDateTime::from_unix_timestamp(1_794_450_000).unwrap(),
            logins: entries
                .iter()
                .map(|&(provider, state, selected)| LoginReadiness {
                    provider,
                    state,
                    checked_at: None,
                    login: None,
                    selected,
                    last_use: None,
                })
                .collect(),
            unknown: Vec::new(),
        }
        .write(&self.readiness())
        .unwrap();
    }

    /// What an admission is offered now, with the service's environment.
    fn admit(&self) -> crate::agent_login::Offer {
        crate::agent_login::offer(
            &EnvironmentSnapshot::new(self.service.clone()),
            Some(&self.directory),
        )
    }

    fn withheld(&self) -> Vec<&'static str> {
        self.admit().withheld
    }

    fn service_environment(&self) -> EnvironmentSnapshot {
        EnvironmentSnapshot::new(self.service.clone())
    }
}

// REQ-GRANT-51, INV-RUNNER-17
#[test]
fn a_selected_codex_login_the_runner_holds_none_of_is_withheld() {
    for state in [
        AgentLoginState::InstallFailed,
        // Cloud answered `unauthorized`: an authorization in force whose
        // owner is not an active member.
        AgentLoginState::NotAuthorized,
        AgentLoginState::CloudUnavailable,
    ] {
        let runner = Selecting::new();
        runner.record(&[(AgentLoginProvider::Codex, state, true)]);

        let offered = runner.admit();

        assert_eq!(offered.withheld, vec![CODEX_HOME], "{state:?}");
        assert_eq!(
            offered.environment,
            runner.service_environment(),
            "{state:?}"
        );
        assert_eq!(
            offered.environment.variable(OsStr::new(CODEX_HOME)),
            None,
            "{state:?}"
        );
    }
}

// REQ-GRANT-51, REQ-GRANT-37
#[test]
fn a_selected_codex_login_the_runner_still_holds_is_offered_and_nothing_withheld() {
    let runner = Selecting::new();
    keep_codex_login(&runner.directory);
    runner.record(&[(
        AgentLoginProvider::Codex,
        AgentLoginState::CloudUnavailable,
        true,
    )]);

    let offered = runner.admit();

    assert!(offered.withheld.is_empty());
    assert_eq!(
        offered.environment.variable(OsStr::new(CODEX_HOME)),
        Some(runner.directory.codex_home().as_os_str())
    );
}

// REQ-GRANT-51
#[test]
fn a_revoked_or_unselected_provider_leaves_the_service_environment() {
    for (state, selected) in [
        (AgentLoginState::Revoked, false),
        (AgentLoginState::NotAuthorized, false),
    ] {
        let runner = Selecting::new();
        runner.record(&[
            (AgentLoginProvider::Codex, state, selected),
            (AgentLoginProvider::ClaudeCode, state, selected),
        ]);

        let offered = runner.admit();

        assert!(offered.withheld.is_empty(), "{state:?}");
        assert_eq!(
            offered.environment,
            runner.service_environment(),
            "{state:?}"
        );
    }
}

// REQ-GRANT-51, REQ-GRANT-37, INV-RUNNER-17
#[test]
fn a_service_codex_home_is_never_withheld_or_replaced() {
    for (state, kept) in [
        (AgentLoginState::InstallFailed, false),
        (AgentLoginState::ForeignLogin, false),
        (AgentLoginState::Ready, true),
        (AgentLoginState::CloudUnavailable, true),
    ] {
        let mut runner = Selecting::new();
        let own = runner.temporary.path().join("service-codex");
        runner
            .service
            .insert(OsString::from(CODEX_HOME), own.clone().into_os_string());
        if kept {
            keep_codex_login(&runner.directory);
        }
        runner.record(&[(AgentLoginProvider::Codex, state, true)]);

        let offered = runner.admit();

        assert!(offered.withheld.is_empty(), "{state:?}");
        assert_eq!(
            offered.environment.variable(OsStr::new(CODEX_HOME)),
            Some(own.as_os_str()),
            "{state:?}"
        );
    }
}

// REQ-GRANT-51, REQ-GRANT-44, REQ-GRANT-45
#[test]
fn a_selected_claude_code_login_without_a_token_is_withheld_unless_the_service_has_its_own() {
    let runner = Selecting::new();
    runner.record(&[(
        AgentLoginProvider::ClaudeCode,
        AgentLoginState::InstallFailed,
        true,
    )]);
    assert_eq!(runner.withheld(), vec![TOKEN_VARIABLE]);

    let configuration = runner.home().join(".claude");
    fs::create_dir(&configuration).unwrap();
    fs::write(configuration.join(".credentials.json"), "{}").unwrap();

    let offered = runner.admit();
    assert!(offered.withheld.is_empty());
    assert_eq!(offered.environment, runner.service_environment());
}

// REQ-GRANT-51
#[test]
fn no_readiness_document_or_an_unreadable_one_withholds_nothing() {
    let runner = Selecting::new();
    assert!(runner.withheld().is_empty());

    write_private(&runner.readiness(), b"not a readiness document");
    let offered = runner.admit();
    assert!(offered.withheld.is_empty());
    assert_eq!(offered.environment, runner.service_environment());
}

// REQ-GRANT-51, REQ-DIAG-41, INV-RUNNER-17
#[tokio::test]
async fn an_agent_step_whose_codex_login_was_withheld_fails_before_codex_starts() {
    let workflow = CODEX_ONLY_WORKFLOW.replacen(
        "schemaVersion: 1\n",
        "schemaVersion: 1\nenvironmentPassthrough: [CODEX_HOME]\n",
        1,
    );
    let (temporary, mut manager) = super::harness_executable_tests::manager_fixture_with_harnesses(
        &workflow,
        None,
        None,
        Some(SUCCESSFUL_CODEX),
    );
    let mut variables = manager.environment.variables().clone();
    variables.remove(OsStr::new(CODEX_HOME));
    manager.environment = EnvironmentSnapshot::new(variables);
    let state = temporary.path().join("runner-state.json");
    manager.agent_login_directory = Some(LoginDirectory::beside(&state));
    ReadinessDocument {
        updated_at: time::OffsetDateTime::from_unix_timestamp(1_794_450_000).unwrap(),
        logins: vec![LoginReadiness {
            provider: AgentLoginProvider::Codex,
            state: AgentLoginState::InstallFailed,
            checked_at: None,
            login: None,
            selected: true,
            last_use: None,
        }],
        unknown: Vec::new(),
    }
    .write(&readiness_path(&state))
    .unwrap();
    let offered = offer("bg");

    offer_then_prepare(&mut manager, &offered).await;
    assert_eq!(accepted_variable(&manager, CODEX_HOME), None);
    let reports = execute_to_terminal(&mut manager, &offered).await;

    let Some(ExecutionReport::Finished {
        outcome,
        diagnostic,
        ..
    }) = reports.last()
    else {
        panic!("unexpected reports: {reports:#?}");
    };
    assert_eq!(outcome["outcome"], "failed", "{outcome:#}");
    assert_eq!(
        outcome["primaryIssue"]["detail"]["code"], "harness_start_failed",
        "{outcome:#}"
    );
    assert_eq!(
        diagnostic,
        &Some(json!({ "launchFailure": "agent_login_unavailable" }))
    );
    assert!(!temporary.path().join("codex.calls").exists());
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
    if !service_codex_home {
        let mut variables = manager.environment.variables().clone();
        variables.remove(OsStr::new(CODEX_HOME));
        manager.environment = EnvironmentSnapshot::new(variables);
    }
    let state = temporary.path().join("runner-state.json");
    manager.agent_login_directory = Some(LoginDirectory::beside(&state));
    let service_home = temporary.path().join("service-home");
    fs::create_dir(&service_home).unwrap();
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
        "UM_LINEAR_ACCESS_TOKEN",
        "UM_LINEAR_ACCESS_TOKEN_FILE",
    ),
];

/// The service's own value of every grant name.
const SERVICE_GRANT_VALUES: [(&str, &str); 6] = [
    ("GH_TOKEN", "service-gh-token"),
    ("GH_TOKEN_FILE", "/service/gh-token"),
    ("GITLAB_TOKEN", "service-gitlab-token"),
    ("GITLAB_TOKEN_FILE", "/service/gitlab-token"),
    ("UM_LINEAR_ACCESS_TOKEN", "service-linear-token"),
    ("UM_LINEAR_ACCESS_TOKEN_FILE", "/service/linear-token"),
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
async fn an_issued_token_and_its_file_reach_only_the_steps_that_pass_them_over_the_services() {
    for (provider, variable, file_variable) in GRANT_NAMES {
        let token = fake_grant_token(provider, 4);
        let fixture = SettingUp::running(
            &workflow_passing_a_grant(variable, file_variable, &token),
            FakeAgentLoginCloud::new(Ok(Vec::new())),
        );
        fixture.serve_with(&SERVICE_GRANT_VALUES);
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

// REQ-GRANT-87, REQ-WORKSPACE-18
#[tokio::test]
async fn an_unselected_provider_leaves_the_services_gitlab_token_and_never_its_gh_token() {
    let check = format!(
        r#"test "$GITLAB_TOKEN" = service-gitlab-token && test "$GITLAB_TOKEN_FILE" = /service/gitlab-token && test "$GH_TOKEN_FILE" = /service/gh-token && {}"#,
        unset(&[
            "GH_TOKEN",
            "UM_LINEAR_ACCESS_TOKEN",
            "UM_LINEAR_ACCESS_TOKEN_FILE"
        ])
    );
    let fixture = SettingUp::running(
        &workflow_checking(&every_grant_name().join(", "), &check),
        FakeAgentLoginCloud::new(Ok(Vec::new())),
    );
    fixture.serve_with(&SERVICE_GRANT_VALUES);
    for (provider, _, _) in GRANT_NAMES {
        fixture
            .cloud
            .answer_grant_token(provider, refused(GrantTokenRefusal::Unauthorized));
    }

    assert_succeeded(&fixture.run(&offer("bg")).await);
    assert!(!fixture.grant(AgentLoginProvider::Gitlab).selected);
}

// REQ-GRANT-87, REQ-GRANT-10
#[tokio::test]
async fn a_selected_provider_without_a_token_withholds_the_services_gitlab_token() {
    let workflow = workflow_checking(
        "GITLAB_TOKEN, GITLAB_TOKEN_FILE",
        &unset(&["GITLAB_TOKEN", "GITLAB_TOKEN_FILE"]),
    );
    let serving = || {
        let fixture = SettingUp::running(&workflow, FakeAgentLoginCloud::new(Ok(Vec::new())));
        fixture.serve_with(&SERVICE_GRANT_VALUES);
        fixture
    };

    // Cloud names a grant in force that cannot issue a token.
    let fixture = serving();
    fixture.cloud.answer_grant_token(
        AgentLoginProvider::Gitlab,
        refused(GrantTokenRefusal::ReconnectRequired),
    );
    assert_succeeded(&fixture.run(&offer("bg")).await);
    assert!(fixture.grant(AgentLoginProvider::Gitlab).selected);

    // Cloud does not answer for a provider an earlier admission was issued
    // a token of.
    let fixture = serving();
    fixture
        .cloud
        .answer_grant_token(AgentLoginProvider::Gitlab, FakeGrantToken::Issue(1));
    let first = offer("bg");
    fixture.admit(&first).await;
    fixture.release(&first).await;
    fixture.cloud.answer_grant_token(
        AgentLoginProvider::Gitlab,
        FakeGrantToken::Fail(GrantTokenFailure::Cloud(CloudFailure::Transport)),
    );
    assert_succeeded(&fixture.run(&offer("bh")).await);
    let gitlab = fixture.grant(AgentLoginProvider::Gitlab);
    assert_eq!(gitlab.state, AgentLoginState::CloudUnavailable);
    assert!(gitlab.selected);
}

// REQ-GRANT-87, REQ-GRANT-51
#[tokio::test]
async fn an_agent_step_is_not_refused_over_a_withheld_grant_name() {
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
    let fixture = SettingUp::serving(temporary, manager, FakeAgentLoginCloud::new(Ok(Vec::new())));
    fixture.serve_with(&SERVICE_GRANT_VALUES);
    fixture.cloud.answer_grant_token(
        AgentLoginProvider::Gitlab,
        refused(GrantTokenRefusal::ReconnectRequired),
    );

    assert_succeeded(&fixture.run(&offer("bg")).await);
    assert!(fixture.temporary.path().join("codex.calls").exists());
}

// REQ-GRANT-89
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

    // An assignment that has ended asks nothing when its wait passes.
    let fixture = SettingUp::running(&workflow, FakeAgentLoginCloud::new(Ok(Vec::new())));
    let mut sleeps = fixture.refreshing();
    fixture
        .cloud
        .answer_grant_token(AgentLoginProvider::Github, FakeGrantToken::Issue(1));
    let ended = offer("bh");
    fixture.admit(&ended).await;
    // Only the assignment's task keeps the sleeper now, so its sleeps end
    // with it.
    fixture.manager.lock().unwrap().grant_token_refresh = None;
    let (_, release) = sleeps.recv().await.unwrap();
    fixture.release(&ended).await;
    fixture
        .cloud
        .answer_grant_token(AgentLoginProvider::Github, FakeGrantToken::Issue(2));

    release.release();

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
