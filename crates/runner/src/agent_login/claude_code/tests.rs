use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
use std::sync::Arc;

use um_execution::EnvironmentSnapshot;

use super::*;
use crate::agent_login::cloud::{
    AgentLoginCloud, CloudFailure, LoginMaterial, LoginStatus, ProviderLogin,
};
use crate::agent_login::readiness::{ReadinessDocument, readiness_path};
use crate::agent_login::setup::{AgentLoginSetup, SetupDependencies, SetupOutcome};
use crate::agent_login::{FailureCause, FailureStage, StepFailure, offer};
use crate::control_protocol::{AgentLoginProvider, AgentLoginState};
use crate::service::{
    CountedGate, FAKE_AGENT_LOGIN_ID, FAKE_AGENT_LOGIN_SENTINEL, FakeAgentLoginCloud,
    FakeLoginCheck, fake_agent_login_material,
};
use crate::telemetry::{TestCapture, test_recorder};

const SENTINEL: &str = "sk-ant-oat01-claude-code-token-sentinel";

fn token(generation: u64) -> String {
    format!("{SENTINEL}-{generation}")
}

fn now() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_794_445_200).unwrap()
}

fn delivery(generation: u64) -> Delivery {
    Delivery {
        provider: AgentLoginProvider::ClaudeCode,
        agent_login_id: FAKE_AGENT_LOGIN_ID.to_owned(),
        generation,
        record_schema_version: 1,
        material: LoginMaterial::from_bytes(token(generation).as_bytes()),
    }
}

fn state_directory() -> tempfile::TempDir {
    let directory = tempfile::tempdir().unwrap();
    fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
    directory
}

fn environment(variables: &[(&str, &OsStr)]) -> ServiceEnvironment {
    let variables = variables
        .iter()
        .map(|(name, value)| ((*name).to_owned(), value.to_os_string()))
        .collect::<BTreeMap<_, _>>();
    ServiceEnvironment::from_lookup(|name| variables.get(name).cloned())
}

fn claude_code(status: LoginStatus) -> Result<Vec<ProviderLogin>, CloudFailure> {
    Ok(vec![ProviderLogin {
        provider: AgentLoginProvider::ClaudeCode,
        agent_login_id: FAKE_AGENT_LOGIN_ID.to_owned(),
        status,
    }])
}

fn authorized(generation: u64) -> Result<Vec<ProviderLogin>, CloudFailure> {
    claude_code(LoginStatus::Authorized {
        generation,
        record_schema_version: 1,
    })
}

/// Setup over a temporary state directory with `environment` as the
/// service's.
struct Fixture {
    state: tempfile::TempDir,
    cloud: Arc<FakeAgentLoginCloud>,
    setup: Arc<AgentLoginSetup>,
    capture: TestCapture,
}

impl Fixture {
    fn new(environment: ServiceEnvironment, cloud: Arc<FakeAgentLoginCloud>) -> Self {
        let state = state_directory();
        let (recorder, capture) = test_recorder("rbt_01k0z6r1w8f4jy2m7q9v3x5abc");
        let dependencies = SetupDependencies::new(
            Arc::clone(&cloud) as Arc<dyn AgentLoginCloud>,
            FakeLoginCheck::new(),
            environment,
            &state.path().join("runner-state.json"),
            recorder,
        );
        let setup = Arc::new(AgentLoginSetup::new(
            dependencies,
            Arc::new(CountedGate::idle()),
            now,
        ));
        Self {
            state,
            cloud,
            setup,
            capture,
        }
    }

    fn token_path(&self) -> PathBuf {
        LoginDirectory::beside(&self.state.path().join("runner-state.json")).claude_code_token()
    }

    async fn pass(&self) -> ReadinessDocument {
        match self.setup.run_pass().await {
            SetupOutcome::Recorded(document) => document,
            other => panic!("setup did not record readiness: {other:?}"),
        }
    }

    async fn claude_code_state(&self) -> AgentLoginState {
        self.pass()
            .await
            .login(AgentLoginProvider::ClaudeCode)
            .expect("claude_code readiness")
            .state
    }

    fn assert_no_token_outside_its_file(&self) {
        let readiness =
            fs::read_to_string(readiness_path(&self.state.path().join("runner-state.json")))
                .unwrap();
        assert!(!readiness.contains(FAKE_AGENT_LOGIN_SENTINEL));
        let records = serde_json::to_string(&self.capture.records()).unwrap();
        assert!(!records.contains(FAKE_AGENT_LOGIN_SENTINEL));
    }
}

// REQ-GRANT-36, REQ-GRANT-44
#[test]
fn a_delivered_token_is_kept_privately_in_the_runners_directory_and_replaced_in_one_step() {
    let state = state_directory();
    let directory = LoginDirectory::beside(&state.path().join("runner-state.json"));
    let path = directory.claude_code_token();
    assert_eq!(
        path,
        state.path().join(".runner-agent-login/claude-code.token")
    );

    let first = install_claude_code(&directory, None, &delivery(1), now()).unwrap();
    assert_eq!(fs::read(&path).unwrap(), token(1).as_bytes());
    let metadata = fs::symlink_metadata(&path).unwrap();
    assert_eq!(metadata.permissions().mode() & 0o7777, 0o600);
    assert_eq!(metadata.nlink(), 1);
    assert_eq!(first.installed_path, path);
    assert_eq!(first.generation, 1);
    assert_eq!(
        fs::symlink_metadata(directory.path()).unwrap().mode() & 0o7777,
        0o700
    );

    let second = install_claude_code(&directory, Some(&first), &delivery(2), now()).unwrap();
    assert_eq!(second.generation, 2);
    assert_eq!(fs::read(&path).unwrap(), token(2).as_bytes());
    let entries = fs::read_dir(directory.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<Vec<_>>();
    assert_eq!(entries, ["claude-code.token"]);

    assert_eq!(
        install_claude_code(&directory, Some(&second), &delivery(1), now()),
        Err(InstallRefusal::StaleGeneration)
    );
    assert_eq!(fs::read(&path).unwrap(), token(2).as_bytes());

    let mut unusable = delivery(3);
    unusable.material = LoginMaterial::from_bytes(b"two words\n");
    assert_eq!(
        install_claude_code(&directory, Some(&second), &unusable, now()),
        Err(InstallRefusal::InstallFailed(StepFailure {
            stage: FailureStage::Delivery,
            cause: FailureCause::InvalidResponse,
        }))
    );
    assert_eq!(fs::read(&path).unwrap(), token(2).as_bytes());
}

// REQ-GRANT-44, REQ-GRANT-42
#[tokio::test]
async fn setup_asks_for_claude_code_and_keeps_each_newer_generation() {
    let home = tempfile::tempdir().unwrap();
    let fixture = Fixture::new(
        environment(&[("HOME", home.path().as_os_str())]),
        FakeAgentLoginCloud::new(authorized(1)),
    );

    assert_eq!(fixture.claude_code_state().await, AgentLoginState::Ready);
    assert_eq!(AgentLoginProvider::ClaudeCode.record_schema_versions(), [1]);
    assert_eq!(
        fs::read(fixture.token_path()).unwrap(),
        fake_agent_login_material(1)
    );

    fixture.cloud.answer(authorized(2));
    let document = fixture.pass().await;
    let login = document.login(AgentLoginProvider::ClaudeCode).unwrap();
    assert_eq!(login.state, AgentLoginState::Ready);
    assert_eq!(login.installation().unwrap().generation, 2);
    assert_eq!(
        login.installation().unwrap().installed_path,
        fixture.token_path()
    );
    assert_eq!(
        fs::read(fixture.token_path()).unwrap(),
        fake_agent_login_material(2)
    );
    assert_eq!(fixture.cloud.deliveries(), 2);
    let records = fixture.capture.events();
    assert_eq!(
        records.last().unwrap()["um.agent_login.claude_code.state"],
        "ready"
    );
    assert_eq!(
        records.last().unwrap()["um.agent_login.claude_code.generation"],
        2
    );
    fixture.assert_no_token_outside_its_file();
}

// REQ-GRANT-45
#[tokio::test]
async fn a_service_with_its_own_claude_code_login_gets_no_token_and_asks_for_none() {
    let token_home = tempfile::tempdir().unwrap();
    let configured = tempfile::tempdir().unwrap();
    fs::write(configured.path().join(".credentials.json"), "{}").unwrap();
    let logged_in_home = tempfile::tempdir().unwrap();
    fs::create_dir(logged_in_home.path().join(".claude")).unwrap();
    fs::write(
        logged_in_home.path().join(".claude/.credentials.json"),
        "{}",
    )
    .unwrap();

    for (case, service) in [
        (
            "token in the service environment",
            environment(&[
                ("HOME", token_home.path().as_os_str()),
                (CLAUDE_CODE_LOGIN_VARIABLE, OsStr::new("service-token")),
            ]),
        ),
        (
            "credentials under CLAUDE_CONFIG_DIR",
            environment(&[
                ("HOME", token_home.path().as_os_str()),
                ("CLAUDE_CONFIG_DIR", configured.path().as_os_str()),
            ]),
        ),
        (
            "credentials under .claude in the home",
            environment(&[("HOME", logged_in_home.path().as_os_str())]),
        ),
    ] {
        let fixture = Fixture::new(service, FakeAgentLoginCloud::new(authorized(1)));
        assert_eq!(
            fixture.claude_code_state().await,
            AgentLoginState::ForeignLogin,
            "{case}"
        );
        assert_eq!(fixture.cloud.deliveries(), 0, "{case}");
        assert!(!fixture.token_path().exists(), "{case}");
    }

    // A credentials file elsewhere is not the service's.
    let fixture = Fixture::new(
        environment(&[
            ("HOME", logged_in_home.path().as_os_str()),
            ("CLAUDE_CONFIG_DIR", token_home.path().as_os_str()),
        ]),
        FakeAgentLoginCloud::new(authorized(1)),
    );
    assert_eq!(fixture.claude_code_state().await, AgentLoginState::Ready);
}

// REQ-GRANT-45
#[tokio::test]
async fn a_token_the_runner_kept_is_removed_once_ended_or_outranked() {
    let home = tempfile::tempdir().unwrap();
    for (answer, state) in [
        (claude_code(LoginStatus::Revoked), AgentLoginState::Revoked),
        (
            claude_code(LoginStatus::Unauthorized),
            AgentLoginState::NotAuthorized,
        ),
        (Ok(Vec::new()), AgentLoginState::NotAuthorized),
    ] {
        let fixture = Fixture::new(
            environment(&[("HOME", home.path().as_os_str())]),
            FakeAgentLoginCloud::new(authorized(1)),
        );
        assert_eq!(fixture.claude_code_state().await, AgentLoginState::Ready);
        assert!(fixture.token_path().is_file());

        fixture.cloud.answer(answer);
        let document = fixture.pass().await;
        let login = document.login(AgentLoginProvider::ClaudeCode).unwrap();
        assert_eq!(login.state, state);
        assert_eq!(login.installation(), None);
        assert!(!fixture.token_path().exists(), "{state:?}");
    }

    // A login the service gains later outranks the kept token, which goes.
    let fixture = Fixture::new(
        environment(&[("HOME", home.path().as_os_str())]),
        FakeAgentLoginCloud::new(authorized(1)),
    );
    assert_eq!(fixture.claude_code_state().await, AgentLoginState::Ready);
    fs::create_dir(home.path().join(".claude")).unwrap();
    fs::write(home.path().join(".claude/.credentials.json"), "{}").unwrap();
    assert_eq!(
        fixture.claude_code_state().await,
        AgentLoginState::ForeignLogin
    );
    assert!(!fixture.token_path().exists());
    assert_eq!(fixture.cloud.deliveries(), 1);
}

// REQ-GRANT-44, REQ-GRANT-45
#[test]
fn the_token_is_offered_only_where_the_process_environment_has_no_claude_code_login() {
    let state = state_directory();
    let directory = LoginDirectory::beside(&state.path().join("runner-state.json"));
    let path = directory.claude_code_token();
    let home = tempfile::tempdir().unwrap();
    let process = EnvironmentSnapshot::new([
        ("HOME", home.path().as_os_str()),
        ("PATH", OsStr::new("/usr/bin:/bin")),
    ]);
    let offered = |process: &EnvironmentSnapshot| {
        offer(process, Some(&directory))
            .environment
            .variable(OsStr::new(CLAUDE_CODE_LOGIN_VARIABLE))
            .map(OsStr::to_os_string)
    };

    assert_eq!(offered(&process), None);
    assert_eq!(offer(&process, Some(&directory)).environment, process);
    install_claude_code(&directory, None, &delivery(1), now()).unwrap();
    assert_eq!(offer(&process, None).environment, process);
    assert_eq!(offered(&process), Some(OsString::from(token(1))));
    let overlaid = offer(&process, Some(&directory)).environment;
    assert_eq!(
        overlaid.variable(OsStr::new("PATH")),
        Some(OsStr::new("/usr/bin:/bin"))
    );
    assert_eq!(overlaid.variables().len(), 3);
    assert_eq!(
        process.variable(OsStr::new(CLAUDE_CODE_LOGIN_VARIABLE)),
        None
    );

    let carrying = EnvironmentSnapshot::new([
        ("HOME", home.path().as_os_str()),
        (CLAUDE_CODE_LOGIN_VARIABLE, OsStr::new("service-token")),
    ]);
    assert_eq!(offer(&carrying, Some(&directory)).environment, carrying);

    fs::create_dir(home.path().join(".claude")).unwrap();
    fs::write(home.path().join(".claude/.credentials.json"), "{}").unwrap();
    assert_eq!(offered(&process), None);
    fs::remove_file(home.path().join(".claude/.credentials.json")).unwrap();

    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
    assert_eq!(offered(&process), None);
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    assert_eq!(offered(&process), Some(OsString::from(token(1))));
}

// REQ-GRANT-42
#[test]
fn a_read_token_never_appears_in_debug() {
    let state = state_directory();
    let directory = LoginDirectory::beside(&state.path().join("runner-state.json"));
    install_claude_code(&directory, None, &delivery(1), now()).unwrap();
    let read = read_token(&directory.claude_code_token()).unwrap();
    assert!(!format!("{read:?}").contains(SENTINEL));
}
