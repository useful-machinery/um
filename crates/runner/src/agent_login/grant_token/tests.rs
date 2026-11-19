use std::ffi::OsString;
use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use time::OffsetDateTime;

use super::*;
use crate::agent_login::check::LoginCheck;
use crate::agent_login::cloud::{
    AgentLoginCloud, CloudFailure, GrantProvidersFailure, GrantProvidersRequest, GrantTokenFailure,
    GrantTokenRefusal, ListedProvider, ReportedTokenService, TokenServiceSource,
};
use crate::agent_login::readiness::{ReadinessDocument, readiness_path};
use crate::agent_login::setup::{
    AdmissionSetup, AgentLoginSetup, AssignmentGate, SetupDependencies, SetupOutcome,
};
use crate::agent_login::{ProviderSource, ServiceEnvironment};
use crate::control_protocol::{AgentLoginProvider, AgentLoginState, CredentialSource};
use crate::service::{
    CountedGate, FAKE_GRANT_ID, FAKE_GRANT_TOKEN_SENTINEL, FakeAgentLoginCloud, FakeGrantToken,
    FakeLoginCheck, SleepRelease, controlled_sleeper, fake_grant_expiry, fake_grant_token,
    with_watchdog,
};
use crate::telemetry::{TestCapture, test_recorder};

const BOOT: &str = "rbt_01k0z6r1w8f4jy2m7q9v3x5abe";
const ASSIGNMENT: &str = "asn_01k0z6r1w8f4jy2m7q9v3x5abc";
const NEXT_ASSIGNMENT: &str = "asn_01k0z6r1w8f4jy2m7q9v3x5abd";
const RUN: &str = "run_01k0z6r1w8f4jy2m7q9v3x5abc";
const NEXT_RUN: &str = "run_01k0z6r1w8f4jy2m7q9v3x5abd";
/// An assignment the runner no longer holds, whose directory an
/// interrupted runner left.
const ENDED_ASSIGNMENT: &str = "asn_01k0z6r1w8f4jy2m7q9v3x5abf";

/// When the fixture's setup starts, and every time it reads unless a test
/// moves its clock on.
const START: i64 = 1_794_450_000;

fn now() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(START).unwrap()
}

/// The expiry of every token the fake Cloud issues unless a test sets
/// another.
fn expiry() -> OffsetDateTime {
    fake_grant_expiry()
}

/// Setup for a service whose state directory is under a temporary root,
/// against a fake Cloud that authorizes no agent login.
struct Fixture {
    root: tempfile::TempDir,
    cloud: Arc<FakeAgentLoginCloud>,
    setup: Arc<AgentLoginSetup>,
    capture: TestCapture,
    /// The setup's clock, in seconds since the epoch.
    clock: Arc<AtomicI64>,
}

impl Fixture {
    fn new() -> Self {
        Self::serving(&[])
    }

    /// `new`, for a service whose environment also carries `variables`.
    fn serving(variables: &[(&str, &str)]) -> Self {
        let root = tempfile::tempdir().unwrap();
        for directory in ["home", "state"] {
            fs::create_dir(root.path().join(directory)).unwrap();
        }
        let state = root.path().join("state");
        fs::set_permissions(&state, fs::Permissions::from_mode(0o700)).unwrap();
        let cloud = FakeAgentLoginCloud::new(Ok(Vec::new()));
        let (recorder, capture) = test_recorder(BOOT);
        let home = OsString::from(root.path().join("home"));
        let clock = Arc::new(AtomicI64::new(START));
        let reading = Arc::clone(&clock);
        let setup = Arc::new(AgentLoginSetup::new(
            SetupDependencies::new(
                Arc::clone(&cloud) as Arc<dyn AgentLoginCloud>,
                FakeLoginCheck::new() as Arc<dyn LoginCheck>,
                ServiceEnvironment::from_variables(
                    variables
                        .iter()
                        .map(|(name, value)| (OsString::from(name), OsString::from(value)))
                        .chain([(OsString::from("HOME"), home)]),
                ),
                &state.join("runner-state.json"),
                recorder,
            ),
            Arc::new(CountedGate::idle()) as Arc<dyn AssignmentGate>,
            move || OffsetDateTime::from_unix_timestamp(reading.load(Ordering::SeqCst)).unwrap(),
        ));
        Self {
            root,
            cloud,
            setup,
            capture,
            clock,
        }
    }

    /// Moves the setup's clock to `at`.
    fn set_clock(&self, at: OffsetDateTime) {
        self.clock.store(at.unix_timestamp(), Ordering::SeqCst);
    }

    fn state_path(&self) -> PathBuf {
        self.root.path().join("state/runner-state.json")
    }

    /// The file of `provider`'s token of the assignment `ASSIGNMENT`.
    fn token_file(&self, provider: AgentLoginProvider) -> PathBuf {
        self.token_file_of(ASSIGNMENT, provider)
    }

    fn token_file_of(&self, assignment_id: &str, provider: AgentLoginProvider) -> PathBuf {
        LoginDirectory::beside(&self.state_path())
            .grant_token(assignment_id, provider)
            .unwrap()
            .0
    }

    /// Leaves a token file in the directory of the assignment
    /// `assignment_id`, as its admission would.
    fn leave_token_file_of(&self, assignment_id: &str, provider: AgentLoginProvider, token: &str) {
        let directory = LoginDirectory::beside(&self.state_path());
        for private in [
            directory.path().to_owned(),
            directory.assignments(),
            directory.assignment(assignment_id).unwrap(),
        ] {
            crate::agent_login::directory::ensure_private_directory(&private).unwrap();
        }
        let (path, temporary_prefix) = directory.grant_token(assignment_id, provider).unwrap();
        crate::agent_login::directory::write_file(&path, token.as_bytes(), temporary_prefix)
            .unwrap();
    }

    /// Admits the assignment `assignment_id` of the run `RUN`.
    fn admit(&self, assignment_id: &str) -> AdmissionSetup {
        self.admit_of(RUN, assignment_id)
    }

    fn admit_of(&self, run_id: &str, assignment_id: &str) -> AdmissionSetup {
        self.setup.set_up_admission(BOOT, run_id, assignment_id)
    }

    /// Each OAuth provider's recorded entry, in the document's order.
    fn grants(&self) -> Vec<GrantReadiness> {
        let document = self.recorded();
        brokered_providers()
            .map(|provider| document.grant(provider).cloned().unwrap())
            .collect()
    }

    fn recorded(&self) -> ReadinessDocument {
        ReadinessDocument::load(&readiness_path(&self.state_path()))
            .unwrap()
            .unwrap()
    }

    fn records(&self) -> Vec<serde_json::Map<String, serde_json::Value>> {
        self.capture
            .events()
            .into_iter()
            .filter(|event| event["event.name"] == GRANT_TOKEN_EVENT)
            .collect()
    }

    fn answer_all(&self, answer: FakeGrantToken) {
        for provider in brokered_providers() {
            self.cloud.answer_grant_token(provider, answer);
        }
    }

    /// Lets Cloud list the providers of Cloud's `ids` for each assignment.
    fn list(&self, ids: &[&str]) {
        self.cloud.answer_grant_providers(Ok(ids
            .iter()
            .map(|id| ListedProvider {
                provider: (*id).to_owned(),
                token: "LISTED_TOKEN".to_owned(),
                token_file: "LISTED_TOKEN_FILE".to_owned(),
            })
            .collect()));
    }

    /// The providers Cloud was asked for the token of, in the document's
    /// order.
    fn asked(&self) -> Vec<AgentLoginProvider> {
        let asked: Vec<_> = self
            .cloud
            .grant_requests()
            .iter()
            .filter_map(|request| request.provider.agent_login_provider())
            .collect();
        brokered_providers()
            .filter(|provider| asked.contains(provider))
            .collect()
    }

    /// Asserts that Cloud was asked for the assignment's providers once,
    /// before any token.
    fn assert_listed_once_first(&self) {
        assert_eq!(
            self.cloud.grant_providers_requests(),
            [(
                0,
                GrantProvidersRequest {
                    boot_id: BOOT.to_owned(),
                    assignment_id: ASSIGNMENT.to_owned(),
                }
            )]
        );
    }

    /// The attributes of the admission's record of `provider`.
    fn admission_record(&self, provider: &str) -> serde_json::Map<String, serde_json::Value> {
        self.records()
            .iter()
            .map(grant_attributes)
            .find(|record| record["um.grant_token.provider"] == provider)
            .unwrap()
    }

    /// Lets Cloud list each of `entries`, a provider's id with the names its
    /// token and the path of its file are to be offered under, for each
    /// assignment.
    fn list_named(&self, entries: &[(&str, &str, &str)]) {
        self.cloud
            .answer_grant_providers(Ok(listed_entries(entries)));
    }

    /// Where the runner keeps the token of the configured provider `id` for
    /// the assignment `ASSIGNMENT`.
    fn configured_file(&self, id: &str) -> PathBuf {
        LoginDirectory::beside(&self.state_path())
            .configured_grant_token(ASSIGNMENT, id)
            .unwrap()
            .0
    }

    /// Cloud's id of each provider Cloud was asked for the token of, in
    /// order of the id.
    fn asked_ids(&self) -> Vec<String> {
        let mut asked: Vec<String> = self
            .cloud
            .grant_requests()
            .iter()
            .map(|request| request.provider.cloud_id().to_owned())
            .collect();
        asked.sort();
        asked
    }

    /// The directory the runner keeps the grant tokens of the assignment
    /// `assignment_id` in.
    fn assignment_directory(&self, assignment_id: &str) -> PathBuf {
        LoginDirectory::beside(&self.state_path())
            .path()
            .join("assignments")
            .join(assignment_id)
    }

    /// Leaves what no assignment the runner holds owns: a token file in the
    /// directory of an assignment an interrupted runner held, and the token
    /// files an earlier release kept at fixed names. Answers each location.
    fn leave_leftovers(&self) -> Vec<PathBuf> {
        let directory = LoginDirectory::beside(&self.state_path()).path().to_owned();
        let ended = self.assignment_directory(ENDED_ASSIGNMENT);
        for private in [
            directory.clone(),
            directory.join("assignments"),
            ended.clone(),
            directory.join("grant-tokens"),
        ] {
            crate::agent_login::directory::ensure_private_directory(&private).unwrap();
        }
        let left = vec![
            ended.join("github.token"),
            directory.join("github.token"),
            directory.join("grant-tokens/example.token"),
        ];
        for file in &left {
            fs::write(file, "left-token").unwrap();
        }
        vec![ended, left[1].clone(), directory.join("grant-tokens")]
    }

    /// The attributes of each record that names `provider`.
    fn records_naming(&self, provider: &str) -> Vec<serde_json::Map<String, serde_json::Value>> {
        self.records()
            .iter()
            .map(grant_attributes)
            .filter(|record| record["um.grant_token.provider"] == provider)
            .collect()
    }

    /// Lets Cloud list each of `entries` as a token service, with the names
    /// its token and the path of its file are to be offered under, for each
    /// assignment.
    fn list_token_services(&self, entries: &[(&str, &str, &str)]) {
        self.cloud
            .answer_token_services(Ok(listed_entries(entries)));
    }
}

/// Each of `entries`, a provider's id with the names its token and the path
/// of its file are to be offered under, as Cloud lists it.
fn listed_entries(entries: &[(&str, &str, &str)]) -> Vec<ListedProvider> {
    entries
        .iter()
        .map(|(id, token, token_file)| ListedProvider {
            provider: (*id).to_owned(),
            token: (*token).to_owned(),
            token_file: (*token_file).to_owned(),
        })
        .collect()
}

/// The token-service list entry of Mergify.
const MERGIFY_ENTRY: (&str, &str, &str) = ("mergify", "MERGIFY_TOKEN", "MERGIFY_TOKEN_FILE");

/// The token service Cloud lists as `id`.
fn token_service(id: &str) -> GrantProvider {
    GrantProvider::token_service(id).unwrap()
}

/// The list entry of `github`, with the names the runner offers it under.
const GITHUB_ENTRY: (&str, &str, &str) = ("github", "GH_TOKEN", "GH_TOKEN_FILE");

/// The list entry of a provider of Cloud's configuration.
const EXAMPLE_ENTRY: (&str, &str, &str) = (
    "example",
    "EXAMPLE_ACCESS_TOKEN",
    "EXAMPLE_ACCESS_TOKEN_FILE",
);

/// The configured provider Cloud lists as `id`.
fn configured(id: &str) -> GrantProvider {
    GrantProvider::listed(id).unwrap()
}

/// What an admission holding `tokens` offers a step where the service
/// carries nothing.
fn offered_alone(tokens: &AssignmentGrantTokens) -> EnvironmentSnapshot {
    offer(
        &EnvironmentSnapshot::new(Vec::<(OsString, OsString)>::new()),
        Some(tokens),
    )
}

fn variable<'a>(environment: &'a EnvironmentSnapshot, name: &str) -> Option<&'a std::ffi::OsStr> {
    environment.variable(std::ffi::OsStr::new(name))
}

/// `record` as the attributes a test compares.
fn attributes(record: &serde_json::Value) -> serde_json::Map<String, serde_json::Value> {
    record.as_object().unwrap().clone()
}

fn mode(path: &Path) -> u32 {
    fs::symlink_metadata(path).unwrap().permissions().mode() & 0o777
}

// INV-ENGINE-43
#[test]
fn a_declared_integration_names_exactly_the_variables_the_runner_offers_it_under() {
    use useful_execution::IntegrationProvider;

    assert_eq!(
        OFFERED_VARIABLES.map(|(_, variable, file_variable)| [variable, file_variable]),
        [
            IntegrationProvider::Github,
            IntegrationProvider::GitlabCom,
            IntegrationProvider::Linear,
        ]
        .map(|provider| <[&str; 2]>::try_from(provider.environment_names()).unwrap())
    );
    assert_eq!(
        OFFERED_VARIABLES.map(|(provider, _, _)| provider),
        [
            AgentLoginProvider::Github,
            AgentLoginProvider::Gitlab,
            AgentLoginProvider::Linear,
        ]
    );
    assert_eq!(
        IntegrationProvider::ClaudeCode.environment_names(),
        [CLAUDE_CODE_LOGIN_VARIABLE]
    );
    assert_eq!(
        IntegrationProvider::Codex.environment_names(),
        [CODEX_LOGIN_VARIABLE]
    );
}

// INV-ENGINE-43
#[test]
fn a_declared_token_service_names_exactly_the_variables_cloud_lists_for_it() {
    use useful_execution::IntegrationProvider;

    let path = format!(
        "{}/../../tests/fixtures/runner-grant-providers/v1/token-services.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let fixture: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap();
    let mut ids = Vec::new();
    for entry in fixture["providers"].as_array().unwrap() {
        let id = entry["provider"].as_str().unwrap();
        let provider: IntegrationProvider =
            serde_json::from_value(entry["provider"].clone()).unwrap();
        assert_eq!(
            provider.environment_names(),
            [
                entry["environment"]["token"].as_str().unwrap(),
                entry["environment"]["tokenFile"].as_str().unwrap(),
            ],
            "{id}"
        );
        ids.push(id);
    }
    assert_eq!(ids, ["mergify", "buildkite"]);
}

// REQ-GRANT-86, REQ-GRANT-88
#[test]
fn an_admission_asks_for_each_provider_and_keeps_each_issued_token_in_its_own_private_file() {
    let fixture = Fixture::new();
    fixture
        .cloud
        .answer_grant_token(AgentLoginProvider::Github, FakeGrantToken::Issue(4));
    fixture
        .cloud
        .answer_grant_token(AgentLoginProvider::Gitlab, FakeGrantToken::Issue(2));
    fixture.cloud.answer_grant_token(
        AgentLoginProvider::Linear,
        FakeGrantToken::Fail(GrantTokenFailure::Refused(GrantTokenRefusal::Unauthorized)),
    );

    let admitted = fixture.admit(ASSIGNMENT);

    let mut requests = fixture.cloud.grant_requests();
    requests.sort_by_key(|request| request.provider.agent_login_provider());
    assert_eq!(
        requests
            .iter()
            .map(|request| (
                request.boot_id.as_str(),
                request.assignment_id.as_str(),
                request.provider.agent_login_provider()
            ))
            .collect::<Vec<_>>(),
        [
            (BOOT, ASSIGNMENT, Some(AgentLoginProvider::Github)),
            (BOOT, ASSIGNMENT, Some(AgentLoginProvider::Gitlab)),
            (BOOT, ASSIGNMENT, Some(AgentLoginProvider::Linear)),
        ]
    );
    for (provider, generation) in [
        (AgentLoginProvider::Github, 4),
        (AgentLoginProvider::Gitlab, 2),
    ] {
        let path = fixture.token_file(provider);
        assert_eq!(
            fs::read_to_string(&path).unwrap(),
            fake_grant_token(provider, generation)
        );
        assert_eq!(mode(&path), 0o600);
        assert_eq!(
            fixture.recorded().grant(provider).cloned(),
            Some(GrantReadiness {
                provider,
                state: AgentLoginState::Ready,
                checked_at: Some(now()),
                issued: Some(IssuedGrant {
                    grant_id: FAKE_GRANT_ID.to_owned(),
                    generation,
                    expires_at: expiry(),
                }),
                run_id: Some(RUN.to_owned()),
                ended_at: None,
                source: Some(CredentialSource::Grant),
            })
        );
    }
    assert!(!fixture.token_file(AgentLoginProvider::Linear).exists());
    assert_eq!(
        fixture
            .recorded()
            .grant(AgentLoginProvider::Linear)
            .cloned(),
        Some(GrantReadiness {
            provider: AgentLoginProvider::Linear,
            state: AgentLoginState::NotAuthorized,
            checked_at: Some(now()),
            issued: None,
            run_id: Some(RUN.to_owned()),
            ended_at: None,
            source: None,
        })
    );
    // What the admission recorded is what it publishes.
    assert_eq!(admitted.grants, Some(fixture.recorded()));
    assert_eq!(admitted.agent_logins, None);
}

// REQ-GRANT-87, REQ-GRANT-86, REQ-GRANT-88
#[test]
fn an_admission_asks_for_no_token_of_a_provider_the_host_provides() {
    // Either of a provider's two names is the host's providing it.
    let service = [
        ("GH_TOKEN_FILE", "/service/gh-token"),
        ("USEFUL_LINEAR_ACCESS_TOKEN", "service-linear-token"),
    ];
    let fixture = Fixture::serving(&service);
    fixture.answer_all(FakeGrantToken::Issue(2));
    // An earlier release's admission was issued a `github` token.
    let mut earlier = ReadinessDocument {
        updated_at: now(),
        logins: Vec::new(),
        unknown: Vec::new(),
    };
    earlier.record_grant(GrantReadiness {
        run_id: Some(NEXT_RUN.to_owned()),
        ..ready(1, expiry())
    });
    earlier
        .write(&readiness_path(&fixture.state_path()))
        .unwrap();

    let admitted = fixture.admit(ASSIGNMENT);

    assert_eq!(
        fixture
            .cloud
            .grant_requests()
            .iter()
            .map(|request| request.provider.agent_login_provider())
            .collect::<Vec<_>>(),
        [Some(AgentLoginProvider::Gitlab)]
    );
    for provider in [AgentLoginProvider::Github, AgentLoginProvider::Linear] {
        assert!(!fixture.token_file(provider).exists(), "{provider:?}");
        assert_eq!(
            fixture.recorded().grant(provider).cloned(),
            Some(GrantReadiness {
                provider,
                source: Some(CredentialSource::Host),
                ..not_ready(AgentLoginState::NotDelivered)
            }),
            "{provider:?}"
        );
    }
    assert_eq!(
        fixture
            .recorded()
            .grant(AgentLoginProvider::Gitlab)
            .map(|grant| grant.state),
        Some(AgentLoginState::Ready)
    );
    // The provider whose state changed is recorded.
    let github = fixture
        .records()
        .iter()
        .map(grant_attributes)
        .find(|record| record["um.grant_token.provider"] == "github")
        .unwrap();
    assert_eq!(github["um.grant_token.state"], "not_delivered");
    // The service's names stay as it has them; only `gitlab` is offered.
    let environment = EnvironmentSnapshot::new(service);
    let offered = offer(&environment, Some(&admitted.tokens));
    for (name, value) in service {
        assert_eq!(
            offered.variable(std::ffi::OsStr::new(name)),
            Some(std::ffi::OsStr::new(value))
        );
    }
    for name in ["GH_TOKEN", "USEFUL_LINEAR_ACCESS_TOKEN_FILE"] {
        assert_eq!(offered.variable(std::ffi::OsStr::new(name)), None, "{name}");
    }
    assert_eq!(
        offered.variable(std::ffi::OsStr::new("GITLAB_TOKEN")),
        Some(std::ffi::OsStr::new(&fake_grant_token(
            AgentLoginProvider::Gitlab,
            2
        )))
    );
}

// REQ-GRANT-86, REQ-GRANT-88, REQ-TELEMETRY-46
#[test]
fn an_admission_lists_the_assignments_providers_first_and_asks_for_the_token_of_each_listed() {
    let fixture = Fixture::new();
    fixture.answer_all(FakeGrantToken::Issue(1));
    // Cloud lists the runner's GitLab by its id `gitlab.com`; `gitlab` and
    // `example` are providers of Cloud's configuration.
    fixture.list(&["linear", "gitlab", "example", "github"]);

    let admitted = fixture.admit(ASSIGNMENT);

    fixture.assert_listed_once_first();
    assert_eq!(
        fixture.asked(),
        [AgentLoginProvider::Github, AgentLoginProvider::Linear]
    );
    for provider in [AgentLoginProvider::Github, AgentLoginProvider::Linear] {
        assert!(admitted.tokens.holds(provider), "{provider:?}");
    }
    assert!(!fixture.token_file(AgentLoginProvider::Gitlab).exists());
    assert_eq!(
        fixture
            .recorded()
            .grant(AgentLoginProvider::Gitlab)
            .cloned(),
        Some(GrantReadiness {
            provider: AgentLoginProvider::Gitlab,
            ..not_ready(AgentLoginState::NotAuthorized)
        })
    );
    assert_eq!(
        fixture.admission_record("gitlab"),
        serde_json::json!({
            "um.grant_token.provider": "gitlab",
            "um.grant_token.phase": "admission",
            "um.grant_token.state": "not_authorized",
            "um.assignment.id": ASSIGNMENT,
        })
        .as_object()
        .unwrap()
        .clone()
    );

    // An empty list asks for no token.
    let fixture = Fixture::new();
    fixture.answer_all(FakeGrantToken::Issue(1));
    fixture.list(&[]);

    drop(fixture.admit(ASSIGNMENT));

    fixture.assert_listed_once_first();
    assert_eq!(fixture.asked(), []);
    for grant in fixture.grants() {
        assert_eq!(grant.state, AgentLoginState::NotAuthorized, "{grant:?}");
        assert!(!fixture.token_file(grant.provider).exists());
    }
}

// REQ-GRANT-86, REQ-GRANT-88, REQ-TELEMETRY-46
#[test]
fn a_list_cloud_did_not_answer_usefully_asks_for_no_token_and_is_a_failure_of_stage_list() {
    for (failure, cause, status) in [
        (
            GrantProvidersFailure::Cloud(CloudFailure::Transport),
            "transport",
            None,
        ),
        (
            GrantProvidersFailure::Cloud(CloudFailure::Status(500)),
            "http_status",
            Some(500),
        ),
        (
            GrantProvidersFailure::Cloud(CloudFailure::InvalidResponse),
            "invalid_response",
            None,
        ),
        (
            GrantProvidersFailure::AssignmentNotCurrent,
            "http_status",
            Some(409),
        ),
    ] {
        // The host provides GitHub.
        let fixture = Fixture::serving(&[("GH_TOKEN", "service-github-token")]);
        fixture.answer_all(FakeGrantToken::Issue(1));
        fixture.cloud.answer_grant_providers(Err(failure));

        let _admitted = fixture.admit(ASSIGNMENT);

        fixture.assert_listed_once_first();
        assert_eq!(fixture.asked(), [], "{failure:?}");
        for provider in ["gitlab", "linear"] {
            let mut expected = serde_json::json!({
                "um.grant_token.provider": provider,
                "um.grant_token.phase": "admission",
                "um.grant_token.state": "cloud_unavailable",
                "um.grant_token.failure_stage": "list",
                "um.grant_token.failure_cause": cause,
                "um.assignment.id": ASSIGNMENT,
            });
            if let Some(status) = status {
                expected["um.grant_token.http_status"] = status.into();
            }
            assert_eq!(
                serde_json::Value::Object(fixture.admission_record(provider)),
                expected,
                "{failure:?}"
            );
        }
        for provider in [AgentLoginProvider::Gitlab, AgentLoginProvider::Linear] {
            assert_eq!(
                fixture.recorded().grant(provider).map(|grant| grant.state),
                Some(AgentLoginState::CloudUnavailable),
                "{failure:?}"
            );
        }
        assert_eq!(
            fixture
                .recorded()
                .grant(AgentLoginProvider::Github)
                .cloned(),
            Some(GrantReadiness {
                source: Some(CredentialSource::Host),
                ..not_ready(AgentLoginState::NotDelivered)
            }),
            "{failure:?}"
        );
        // The host's provider is not a failure of the list.
        assert!(
            fixture
                .records()
                .iter()
                .all(|record| record["um.grant_token.provider"] != "github"),
            "{failure:?}"
        );
    }
}

// REQ-GRANT-86
#[test]
fn a_cloud_without_the_list_is_asked_for_each_of_the_three_as_before() {
    let fixture = Fixture::new();
    fixture.answer_all(FakeGrantToken::Issue(1));

    let admitted = fixture.admit(ASSIGNMENT);

    fixture.assert_listed_once_first();
    assert_eq!(fixture.asked(), brokered_providers().collect::<Vec<_>>());
    for provider in brokered_providers() {
        assert!(admitted.tokens.holds(provider), "{provider:?}");
    }
}

// REQ-GRANT-92, REQ-GRANT-87, REQ-GRANT-88, INV-RUNNER-31
#[test]
fn an_admission_records_each_oauth_providers_source_with_its_run() {
    let service = [("GH_TOKEN", "service-github-token")];
    let fixture = Fixture::serving(&service);
    fixture
        .cloud
        .answer_grant_token(AgentLoginProvider::Linear, FakeGrantToken::Issue(1));
    fixture.cloud.answer_grant_token(
        AgentLoginProvider::Gitlab,
        FakeGrantToken::Fail(GrantTokenFailure::Refused(GrantTokenRefusal::Unauthorized)),
    );

    let admitted = fixture.admit(ASSIGNMENT);

    // The host's `github` is its source whatever the runner asked, a token
    // issued and kept is the grant's, and a refused one has none.
    let expected = [
        (
            AgentLoginProvider::Github,
            AgentLoginState::NotDelivered,
            Some(CredentialSource::Host),
        ),
        (
            AgentLoginProvider::Gitlab,
            AgentLoginState::NotAuthorized,
            None,
        ),
        (
            AgentLoginProvider::Linear,
            AgentLoginState::Ready,
            Some(CredentialSource::Grant),
        ),
    ];
    let grants = fixture.grants();
    assert_eq!(
        grants
            .iter()
            .map(|grant| (grant.provider, grant.state, grant.source))
            .collect::<Vec<_>>(),
        expected
    );
    assert!(
        grants
            .iter()
            .all(|grant| grant.run_id.as_deref() == Some(RUN)),
        "{grants:?}"
    );
    // What the admission is offered settles the same sources.
    let offered = crate::agent_login::offer_with_grant_tokens(
        &EnvironmentSnapshot::new(service),
        Some(&LoginDirectory::beside(&fixture.state_path())),
        Some(&admitted.tokens),
    );
    assert_eq!(
        offered.sources,
        expected
            .into_iter()
            .filter_map(|(provider, _, source)| Some(ProviderSource {
                provider,
                source: source?,
            }))
            .collect::<Vec<_>>()
    );
    // The control socket reports each source beside its state.
    let reported = fixture.recorded().reported().unwrap();
    assert_eq!(
        reported
            .providers
            .iter()
            .map(|entry| (entry.provider, entry.state, entry.source))
            .collect::<Vec<_>>(),
        expected
    );
}

// REQ-GRANT-86, REQ-GRANT-88
#[test]
fn each_answer_without_a_token_writes_no_file_and_records_its_state() {
    let unavailable = |failure| FakeGrantToken::Fail(GrantTokenFailure::Cloud(failure));
    let refused = |refusal| FakeGrantToken::Fail(GrantTokenFailure::Refused(refusal));
    // Each answer with the state it records, whatever the answer before it.
    for (answer, state) in [
        (
            refused(GrantTokenRefusal::Unauthorized),
            AgentLoginState::NotAuthorized,
        ),
        (
            FakeGrantToken::Fail(GrantTokenFailure::NoRoute),
            AgentLoginState::NotAuthorized,
        ),
        (
            refused(GrantTokenRefusal::Revoked),
            AgentLoginState::Revoked,
        ),
        (
            refused(GrantTokenRefusal::ReconnectRequired),
            AgentLoginState::ReconnectRequired,
        ),
        (
            refused(GrantTokenRefusal::ProviderUnavailable),
            AgentLoginState::UnsupportedTarget,
        ),
        (
            refused(GrantTokenRefusal::AssignmentNotCurrent),
            AgentLoginState::CloudUnavailable,
        ),
        (
            unavailable(CloudFailure::Status(503)),
            AgentLoginState::CloudUnavailable,
        ),
        (
            unavailable(CloudFailure::Transport),
            AgentLoginState::CloudUnavailable,
        ),
        (
            unavailable(CloudFailure::InvalidResponse),
            AgentLoginState::CloudUnavailable,
        ),
    ] {
        for before in [
            FakeGrantToken::Fail(GrantTokenFailure::NoRoute),
            FakeGrantToken::Issue(1),
        ] {
            let fixture = Fixture::new();
            fixture.answer_all(before);
            drop(fixture.admit(ASSIGNMENT));
            fixture.answer_all(answer);

            let _admitted = fixture.admit(NEXT_ASSIGNMENT);

            for provider in brokered_providers() {
                assert!(!fixture.token_file(provider).exists(), "{answer:?}");
                assert_eq!(
                    fixture.recorded().grant(provider).cloned(),
                    Some(GrantReadiness {
                        provider,
                        state,
                        checked_at: Some(now()),
                        issued: None,
                        run_id: Some(RUN.to_owned()),
                        ended_at: None,
                        source: None,
                    }),
                    "{answer:?} after {before:?}"
                );
            }
        }
    }
}

// REQ-GRANT-86, INV-RUNNER-28
#[test]
fn a_file_an_earlier_assignment_left_is_gone_before_any_answer_is_used() {
    for answer in [
        FakeGrantToken::Issue(7),
        FakeGrantToken::Fail(GrantTokenFailure::Refused(GrantTokenRefusal::Revoked)),
        FakeGrantToken::Fail(GrantTokenFailure::NoRoute),
        FakeGrantToken::Fail(GrantTokenFailure::Cloud(CloudFailure::Transport)),
    ] {
        let fixture = Fixture::new();
        // An earlier assignment's files, and what an interrupted runner left
        // in the admitted assignment's own directory.
        for (assignment_id, provider) in [ENDED_ASSIGNMENT, ASSIGNMENT]
            .into_iter()
            .flat_map(|assignment_id| brokered_providers().map(move |p| (assignment_id, p)))
        {
            fixture.leave_token_file_of(assignment_id, provider, "earlier-assignment-token");
        }
        let files: Vec<PathBuf> = [ENDED_ASSIGNMENT, ASSIGNMENT]
            .into_iter()
            .flat_map(|assignment_id| {
                brokered_providers().map(|provider| fixture.token_file_of(assignment_id, provider))
            })
            .collect();
        let present_when_asked = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&present_when_asked);
        fixture.cloud.on_grant_request(Box::new(move |_| {
            seen.lock()
                .unwrap()
                .extend(files.iter().filter(|file| file.exists()).cloned());
        }));
        fixture.answer_all(answer);

        let _admitted = fixture.admit(ASSIGNMENT);

        assert_eq!(*present_when_asked.lock().unwrap(), Vec::<PathBuf>::new());
        assert!(!fixture.assignment_directory(ENDED_ASSIGNMENT).exists());
        for provider in brokered_providers() {
            let file = fixture.token_file(provider);
            match answer {
                FakeGrantToken::Issue(generation)
                | FakeGrantToken::IssueIntegrationToken(generation)
                | FakeGrantToken::IssueFrom(_, generation) => assert_eq!(
                    fs::read_to_string(&file).unwrap(),
                    fake_grant_token(provider, generation)
                ),
                FakeGrantToken::Fail(_) => assert!(!file.exists(), "{answer:?}"),
            }
        }
    }
}

// REQ-GRANT-86, REQ-GRANT-88, INV-RUNNER-28
#[test]
fn when_the_assignment_ends_its_files_are_gone_and_its_entries_stay() {
    let fixture = Fixture::new();
    fixture.answer_all(FakeGrantToken::Issue(3));
    let admitted = fixture.admit(ASSIGNMENT);
    for provider in brokered_providers() {
        assert!(fixture.token_file(provider).exists());
    }

    drop(admitted.tokens);

    for provider in brokered_providers() {
        assert!(!fixture.token_file(provider).exists());
        assert_eq!(
            fixture.recorded().grant(provider).map(|grant| grant.state),
            Some(AgentLoginState::Ready)
        );
    }

    // The end of an earlier assignment never takes the files of the one
    // admitted after it.
    let earlier = fixture.admit(ASSIGNMENT);
    fixture.answer_all(FakeGrantToken::Issue(4));
    let later = fixture.admit(NEXT_ASSIGNMENT);
    drop(earlier.tokens);
    for provider in brokered_providers() {
        assert_eq!(
            fs::read_to_string(fixture.token_file_of(NEXT_ASSIGNMENT, provider)).unwrap(),
            fake_grant_token(provider, 4)
        );
    }
    drop(later.tokens);
    for assignment_id in [ASSIGNMENT, NEXT_ASSIGNMENT] {
        assert!(!fixture.assignment_directory(assignment_id).exists());
    }
}

// REQ-GRANT-88, INV-RUNNER-30, REQ-TELEMETRY-46
#[test]
fn an_admissions_entries_name_its_run_and_are_marked_ended_when_its_assignment_ends() {
    let refused = |refusal| FakeGrantToken::Fail(GrantTokenFailure::Refused(refusal));
    // With a token kept, and with none kept, when the files hold nothing of
    // the assignment's.
    for answers in [
        [
            FakeGrantToken::Issue(3),
            refused(GrantTokenRefusal::ReconnectRequired),
            refused(GrantTokenRefusal::Unauthorized),
        ],
        [
            refused(GrantTokenRefusal::Revoked),
            refused(GrantTokenRefusal::ReconnectRequired),
            FakeGrantToken::Fail(GrantTokenFailure::Cloud(CloudFailure::Transport)),
        ],
    ] {
        let fixture = Fixture::new();
        for (provider, answer) in brokered_providers().zip(answers) {
            fixture.cloud.answer_grant_token(provider, answer);
        }
        let admitted = fixture.admit(ASSIGNMENT);
        let recorded = fixture.grants();
        for entry in &recorded {
            assert_eq!(entry.run_id.as_deref(), Some(RUN), "{answers:?}");
            assert_eq!(entry.ended_at, None, "{answers:?}");
        }
        let records = fixture.records().len();
        let ended_at = now() + time::Duration::minutes(7);
        fixture.set_clock(ended_at);

        drop(admitted.tokens);

        // Only the end is added: the state, grant, generation, check time,
        // run and selection stay as recorded.
        assert_eq!(
            fixture.grants(),
            recorded
                .into_iter()
                .map(|entry| GrantReadiness {
                    ended_at: Some(ended_at),
                    ..entry
                })
                .collect::<Vec<_>>(),
            "{answers:?}"
        );
        // Marking changes no state, so it writes no record.
        assert_eq!(fixture.records().len(), records, "{answers:?}");
    }

    // The end of an earlier assignment never marks what a later admission
    // recorded.
    let fixture = Fixture::new();
    fixture.answer_all(FakeGrantToken::Issue(3));
    let earlier = fixture.admit(ASSIGNMENT);
    fixture.answer_all(refused(GrantTokenRefusal::Revoked));
    let later = fixture.admit_of(NEXT_RUN, NEXT_ASSIGNMENT);
    drop(earlier.tokens);
    for entry in fixture.grants() {
        assert_eq!(
            (entry.run_id.as_deref(), entry.ended_at),
            (Some(NEXT_RUN), None),
            "{entry:?}"
        );
    }
    drop(later.tokens);
    for entry in fixture.grants() {
        assert_eq!(
            (entry.run_id.as_deref(), entry.ended_at),
            (Some(NEXT_RUN), Some(now())),
            "{entry:?}"
        );
    }
}

// REQ-GRANT-86, INV-RUNNER-28
#[test]
fn an_idle_pass_removes_a_leftover_token_file_and_asks_cloud_for_none() {
    let fixture = Fixture::new();
    let left = fixture.leave_leftovers();
    let directory = LoginDirectory::beside(&fixture.state_path());
    let temporaries = crate::agent_login::directory::GRANT_TOKEN_TEMPORARY_PREFIXES
        .map(|prefix| directory.path().join(format!("{prefix}.4242.1")));
    for temporary in &temporaries {
        fs::write(temporary, "interrupted-write").unwrap();
    }

    let outcome = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(fixture.setup.run_pass());

    assert!(matches!(outcome, SetupOutcome::Recorded(_)), "{outcome:?}");
    for path in &left {
        assert!(fs::symlink_metadata(path).is_err(), "{}", path.display());
    }
    for temporary in &temporaries {
        assert!(!temporary.exists(), "{}", temporary.display());
    }
    assert_eq!(fixture.cloud.grant_requests(), []);
    assert_eq!(fixture.records(), []);
}

// REQ-GRANT-88, INV-RUNNER-30
#[test]
fn an_idle_pass_marks_ended_each_entry_an_interrupted_runner_left_and_asks_cloud_for_none() {
    let fixture = Fixture::new();
    // An interrupted runner's entries: each names its run and has no end.
    let mut interrupted = ReadinessDocument {
        updated_at: now(),
        logins: Vec::new(),
        unknown: Vec::new(),
    };
    for (provider, entry) in brokered_providers().zip([
        ready(3, expiry()),
        not_ready(AgentLoginState::ReconnectRequired),
        not_ready(AgentLoginState::NotAuthorized),
    ]) {
        interrupted.record_grant(GrantReadiness { provider, ..entry });
    }
    interrupted
        .write(&readiness_path(&fixture.state_path()))
        .unwrap();
    let left = fixture.grants();
    let passed_at = now() + time::Duration::hours(2);
    fixture.set_clock(passed_at);

    let outcome = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(fixture.setup.run_pass());

    assert!(matches!(outcome, SetupOutcome::Recorded(_)), "{outcome:?}");
    assert_eq!(
        fixture.grants(),
        left.into_iter()
            .map(|entry| GrantReadiness {
                ended_at: Some(passed_at),
                ..entry
            })
            .collect::<Vec<_>>()
    );
    assert_eq!(fixture.cloud.grant_requests(), []);
    assert_eq!(fixture.records(), []);
}

// INV-RUNNER-28, INV-RUNNER-30
#[test]
fn a_setup_while_an_assignment_is_held_removes_what_no_held_assignment_owns_and_keeps_its_own() {
    let fixture = Fixture::new();
    fixture
        .cloud
        .answer_grant_token(AgentLoginProvider::Github, FakeGrantToken::Issue(1));
    let held = fixture.admit(ASSIGNMENT);
    let own = fixture.assignment_directory(ASSIGNMENT);
    let left = fixture.leave_leftovers();
    // The temporary files a killed write left in the held assignment's
    // directory.
    fs::create_dir(own.join("grant-tokens")).unwrap();
    let temporaries = [
        own.join(".useful-github.token.tmp.4242.1"),
        own.join("grant-tokens/.useful-grant-token.tmp.4242.2"),
    ];
    for temporary in &temporaries {
        fs::write(temporary, "interrupted-write").unwrap();
    }
    let entries = fixture.grants();
    let records = fixture.records().len();

    let outcome = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(fixture.setup.run_pass());

    assert!(matches!(outcome, SetupOutcome::Recorded(_)), "{outcome:?}");
    for path in left.iter().chain(&temporaries) {
        assert!(fs::symlink_metadata(path).is_err(), "{}", path.display());
    }
    assert_eq!(
        fs::read_to_string(own.join("github.token")).unwrap(),
        fake_grant_token(AgentLoginProvider::Github, 1)
    );
    assert_eq!(fixture.grants(), entries);
    for entry in &entries {
        assert_eq!((entry.run_id.as_deref(), entry.ended_at), (Some(RUN), None));
    }
    assert_eq!(fixture.records().len(), records);
    drop(held.tokens);
}

// REQ-GRANT-86, INV-RUNNER-28
#[test]
fn an_admission_removes_what_no_held_assignment_owns_before_it_asks_cloud() {
    let fixture = Fixture::new();
    fixture
        .cloud
        .answer_grant_token(AgentLoginProvider::Github, FakeGrantToken::Issue(1));
    let held = fixture.admit(ASSIGNMENT);
    let own = fixture
        .assignment_directory(ASSIGNMENT)
        .join("github.token");
    let left = fixture.leave_leftovers();
    let watched: Vec<PathBuf> = left.iter().chain([&own]).cloned().collect();
    let present_when_asked = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&present_when_asked);
    fixture.cloud.on_grant_request(Box::new(move |_| {
        seen.lock().unwrap().extend(
            watched
                .iter()
                .filter(|path| fs::symlink_metadata(path).is_ok())
                .cloned(),
        );
    }));
    fixture
        .cloud
        .answer_grant_token(AgentLoginProvider::Github, FakeGrantToken::Issue(2));

    let next = fixture.admit_of(NEXT_RUN, NEXT_ASSIGNMENT);

    // Each request saw A's file and nothing else.
    let mut present = present_when_asked.lock().unwrap().clone();
    present.dedup();
    assert_eq!(present, std::slice::from_ref(&own));
    assert_eq!(
        fs::read_to_string(&own).unwrap(),
        fake_grant_token(AgentLoginProvider::Github, 1)
    );
    assert_eq!(
        fs::read_to_string(
            fixture
                .assignment_directory(NEXT_ASSIGNMENT)
                .join("github.token")
        )
        .unwrap(),
        fake_grant_token(AgentLoginProvider::Github, 2)
    );
    drop(next.tokens);
    drop(held.tokens);
}

// REQ-GRANT-86, REQ-TELEMETRY-46, INV-RUNNER-32
#[test]
fn no_token_is_in_the_readiness_document_its_report_or_any_record() {
    let fixture = Fixture::new();
    fixture.answer_all(FakeGrantToken::Issue(5));
    fixture
        .cloud
        .answer_grant_token(configured("example"), FakeGrantToken::Issue(5));
    fixture.list_named(&[
        GITHUB_ENTRY,
        ("gitlab.com", "GITLAB_TOKEN", "GITLAB_TOKEN_FILE"),
        (
            "linear",
            "USEFUL_LINEAR_ACCESS_TOKEN",
            "USEFUL_LINEAR_ACCESS_TOKEN_FILE",
        ),
        EXAMPLE_ENTRY,
    ]);

    let admitted = fixture.admit(ASSIGNMENT);

    assert!(fixture.configured_file("example").exists());
    let document = fs::read_to_string(readiness_path(&fixture.state_path())).unwrap();
    let reported = serde_json::to_string(&admitted.grants.unwrap().reported().unwrap()).unwrap();
    let records = serde_json::to_string(&fixture.capture.events()).unwrap();
    for (name, text) in [
        ("readiness document", document),
        ("reported readiness", reported),
        ("telemetry", records),
    ] {
        assert!(!text.contains(FAKE_GRANT_TOKEN_SENTINEL), "{name}: {text}");
        assert!(
            !text.contains(".token"),
            "{name} names a token file: {text}"
        );
        assert!(
            !text.contains("EXAMPLE_ACCESS_TOKEN"),
            "{name} names an offered variable: {text}"
        );
    }
    // The hold names its files and their names, never a token.
    let hold = format!("{:?}", admitted.tokens);
    assert!(!hold.contains(FAKE_GRANT_TOKEN_SENTINEL), "{hold}");
}

/// The record's attributes other than those every record carries.
fn grant_attributes(
    record: &serde_json::Map<String, serde_json::Value>,
) -> serde_json::Map<String, serde_json::Value> {
    record
        .iter()
        .filter(|(key, _)| key.starts_with("um.grant_token.") || *key == "um.assignment.id")
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

// REQ-TELEMETRY-46
#[test]
fn one_record_is_written_per_changed_state_or_failed_request() {
    let fixture = Fixture::new();
    fixture
        .cloud
        .answer_grant_token(AgentLoginProvider::Github, FakeGrantToken::Issue(4));
    fixture.cloud.answer_grant_token(
        AgentLoginProvider::Gitlab,
        FakeGrantToken::Fail(GrantTokenFailure::Cloud(CloudFailure::Status(503))),
    );

    drop(fixture.admit(ASSIGNMENT));

    let mut records: Vec<_> = fixture.records().iter().map(grant_attributes).collect();
    records.sort_by_key(|record| record["um.grant_token.provider"].to_string());
    assert_eq!(
        records,
        [
            serde_json::json!({
                "um.grant_token.provider": "github",
                "um.grant_token.phase": "admission",
                "um.grant_token.state": "ready",
                "um.grant_token.generation": 4,
                "um.assignment.id": ASSIGNMENT,
            }),
            serde_json::json!({
                "um.grant_token.provider": "gitlab",
                "um.grant_token.phase": "admission",
                "um.grant_token.state": "cloud_unavailable",
                "um.grant_token.failure_stage": "issue",
                "um.grant_token.failure_cause": "http_status",
                "um.grant_token.http_status": 503,
                "um.assignment.id": ASSIGNMENT,
            }),
            serde_json::json!({
                "um.grant_token.provider": "linear",
                "um.grant_token.phase": "admission",
                "um.grant_token.state": "not_authorized",
                "um.assignment.id": ASSIGNMENT,
            }),
        ]
        .map(|record| record.as_object().unwrap().clone())
    );

    // The next admission is answered alike: only the failed request is
    // recorded again, since nothing else changed.
    drop(fixture.admit(NEXT_ASSIGNMENT));
    let records: Vec<_> = fixture.records().iter().map(grant_attributes).collect();
    assert_eq!(records.len(), 4);
    assert_eq!(records[3]["um.grant_token.provider"], "gitlab");
    assert_eq!(records[3]["um.grant_token.failure_cause"], "http_status");
    assert_eq!(records[3]["um.assignment.id"], NEXT_ASSIGNMENT);

    // A token that cannot be kept is a failed write of the provider.
    fixture
        .cloud
        .answer_grant_token(AgentLoginProvider::Linear, FakeGrantToken::Issue(1));
    let directory = LoginDirectory::beside(&fixture.state_path());
    let _ = fs::remove_dir_all(directory.path());
    fs::write(directory.path(), "not a directory").unwrap();
    let _admitted = fixture.admit(ASSIGNMENT);
    assert!(!fixture.token_file(AgentLoginProvider::Linear).exists());
    assert_eq!(
        fixture
            .recorded()
            .grant(AgentLoginProvider::Linear)
            .map(|grant| grant.state),
        Some(AgentLoginState::InstallFailed)
    );
    let linear = fixture
        .records()
        .iter()
        .map(grant_attributes)
        .rfind(|record| record["um.grant_token.provider"] == "linear")
        .unwrap();
    assert_eq!(linear["um.grant_token.state"], "install_failed");
    assert_eq!(linear["um.grant_token.failure_stage"], "write");
    assert!(linear.contains_key("um.grant_token.failure_cause"));
}

// REQ-GRANT-87, INV-RUNNER-17
#[test]
fn a_kept_token_never_replaces_a_name_the_offers_own_environment_carries() {
    // The setup's service environment carries none of the names, so the
    // admission holds a token of each provider.
    let fixture = Fixture::new();
    fixture.answer_all(FakeGrantToken::Issue(3));
    let admitted = fixture.admit(ASSIGNMENT);
    // The environment the offer is built from carries one name of `github`
    // and the other of `linear`.
    let service = EnvironmentSnapshot::new([
        ("GH_TOKEN", "service-github-token"),
        ("USEFUL_LINEAR_ACCESS_TOKEN_FILE", "/srv/linear-token"),
    ]);

    let offered = offer(&service, Some(&admitted.tokens));

    let variable = |name: &str| offered.variable(std::ffi::OsStr::new(name));
    assert_eq!(
        variable("GH_TOKEN"),
        Some(std::ffi::OsStr::new("service-github-token"))
    );
    assert_eq!(
        variable("USEFUL_LINEAR_ACCESS_TOKEN_FILE"),
        Some(std::ffi::OsStr::new("/srv/linear-token"))
    );
    for name in ["GH_TOKEN_FILE", "USEFUL_LINEAR_ACCESS_TOKEN"] {
        assert_eq!(variable(name), None, "{name}");
    }
    let gitlab = fake_grant_token(AgentLoginProvider::Gitlab, 3);
    assert_eq!(
        variable("GITLAB_TOKEN"),
        Some(std::ffi::OsStr::new(&gitlab))
    );
}

// REQ-GRANT-87
#[test]
fn a_kept_token_whose_file_no_longer_holds_one_is_not_offered() {
    let fixture = Fixture::new();
    fixture.answer_all(FakeGrantToken::Issue(3));
    let admitted = fixture.admit(ASSIGNMENT);
    let service = EnvironmentSnapshot::new([("OTHER_NAME", "kept")]);
    // The file of `gitlab` is replaced by one that is not a token's.
    fixture.leave_token_file_of(ASSIGNMENT, AgentLoginProvider::Gitlab, "two\nlines");

    let offered = offer(&service, Some(&admitted.tokens));

    let variable = |name: &str| offered.variable(std::ffi::OsStr::new(name));
    for name in ["GITLAB_TOKEN", "GITLAB_TOKEN_FILE"] {
        assert_eq!(variable(name), None, "{name}");
    }
    let github = fake_grant_token(AgentLoginProvider::Github, 3);
    assert_eq!(variable("GH_TOKEN"), Some(std::ffi::OsStr::new(&github)));
    assert_eq!(
        variable("GH_TOKEN_FILE"),
        Some(fixture.token_file(AgentLoginProvider::Github).as_os_str())
    );
    assert_eq!(variable("OTHER_NAME"), Some(std::ffi::OsStr::new("kept")));
}

// REQ-GRANT-86, REQ-GRANT-87, INV-RUNNER-28, INV-RUNNER-32
#[test]
fn a_listed_configured_providers_token_is_kept_in_a_file_of_its_own_and_offered_under_its_names() {
    let fixture = Fixture::new();
    fixture.list_named(&[GITHUB_ENTRY, EXAMPLE_ENTRY]);
    fixture
        .cloud
        .answer_grant_token(AgentLoginProvider::Github, FakeGrantToken::Issue(1));
    fixture
        .cloud
        .answer_grant_token(configured("example"), FakeGrantToken::Issue(3));

    let admitted = fixture.admit(ASSIGNMENT);

    fixture.assert_listed_once_first();
    let mut requests: Vec<_> = fixture
        .cloud
        .grant_requests()
        .iter()
        .map(|request| {
            (
                request.boot_id.clone(),
                request.assignment_id.clone(),
                request.provider.cloud_id().to_owned(),
            )
        })
        .collect();
    requests.sort();
    assert_eq!(
        requests,
        ["example", "github"].map(|id| (BOOT.to_owned(), ASSIGNMENT.to_owned(), id.to_owned()))
    );
    let file = fixture.configured_file("example");
    let token = fake_grant_token(configured("example"), 3);
    assert_eq!(fs::read_to_string(&file).unwrap(), token);
    assert_eq!(mode(&file), 0o600);
    assert_eq!(mode(file.parent().unwrap()), 0o700);
    let offered = offered_alone(&admitted.tokens);
    assert_eq!(
        variable(&offered, "EXAMPLE_ACCESS_TOKEN"),
        Some(std::ffi::OsStr::new(&token))
    );
    assert_eq!(
        variable(&offered, "EXAMPLE_ACCESS_TOKEN_FILE"),
        Some(file.as_os_str())
    );
    let github = fake_grant_token(AgentLoginProvider::Github, 1);
    assert_eq!(
        variable(&offered, "GH_TOKEN"),
        Some(std::ffi::OsStr::new(&github))
    );
    assert_eq!(
        variable(&offered, "GH_TOKEN_FILE"),
        Some(fixture.token_file(AgentLoginProvider::Github).as_os_str())
    );
    assert_eq!(
        fixture.records_naming("example"),
        [attributes(&serde_json::json!({
            "um.grant_token.provider": "example",
            "um.grant_token.phase": "admission",
            "um.grant_token.state": "ready",
            "um.grant_token.generation": 3,
            "um.assignment.id": ASSIGNMENT,
        }))]
    );
    // The readiness document holds an entry of the runner's own providers
    // alone, and the offer settles no source of `example`.
    let document: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(readiness_path(&fixture.state_path())).unwrap())
            .unwrap();
    assert!(
        document["providers"]
            .as_array()
            .unwrap()
            .iter()
            .all(|entry| entry["provider"] != "example"),
        "{document}"
    );
    let sources = crate::agent_login::offer_with_grant_tokens(
        &EnvironmentSnapshot::new(Vec::<(OsString, OsString)>::new()),
        Some(&LoginDirectory::beside(&fixture.state_path())),
        Some(&admitted.tokens),
    )
    .sources;
    assert_eq!(
        sources,
        [ProviderSource {
            provider: AgentLoginProvider::Github,
            source: CredentialSource::Grant,
        }]
    );
}

// REQ-GRANT-87, INV-RUNNER-17, INV-RUNNER-32
#[test]
fn a_configured_provider_the_host_provides_is_asked_for_nothing_and_keeps_the_services_names() {
    let service = [("EXAMPLE_ACCESS_TOKEN_FILE", "/srv/example-token")];
    let fixture = Fixture::serving(&service);
    fixture.list_named(&[GITHUB_ENTRY, EXAMPLE_ENTRY]);
    fixture
        .cloud
        .answer_grant_token(AgentLoginProvider::Github, FakeGrantToken::Issue(1));
    fixture
        .cloud
        .answer_grant_token(configured("example"), FakeGrantToken::Issue(1));

    let admitted = fixture.admit(ASSIGNMENT);

    assert_eq!(fixture.asked_ids(), ["github"]);
    assert!(!fixture.configured_file("example").exists());
    assert_eq!(fixture.records_naming("example"), []);
    let offered = offer(&EnvironmentSnapshot::new(service), Some(&admitted.tokens));
    assert_eq!(
        variable(&offered, "EXAMPLE_ACCESS_TOKEN_FILE"),
        Some(std::ffi::OsStr::new("/srv/example-token"))
    );
    assert_eq!(variable(&offered, "EXAMPLE_ACCESS_TOKEN"), None);
}

// REQ-GRANT-87, INV-RUNNER-17, INV-RUNNER-32
#[test]
fn a_kept_configured_token_never_replaces_a_name_the_offers_own_environment_carries() {
    // The setup's service environment carries neither name, so the
    // admission holds a token of `example`.
    let fixture = Fixture::new();
    fixture.list_named(&[EXAMPLE_ENTRY]);
    fixture
        .cloud
        .answer_grant_token(configured("example"), FakeGrantToken::Issue(3));
    let admitted = fixture.admit(ASSIGNMENT);
    assert!(fixture.configured_file("example").exists());

    // The environment the offer is built from carries one of the names.
    for (carried, value, other) in [
        (
            "EXAMPLE_ACCESS_TOKEN",
            "service-example-token",
            "EXAMPLE_ACCESS_TOKEN_FILE",
        ),
        (
            "EXAMPLE_ACCESS_TOKEN_FILE",
            "/srv/example-token",
            "EXAMPLE_ACCESS_TOKEN",
        ),
    ] {
        let offered = offer(
            &EnvironmentSnapshot::new([(carried, value)]),
            Some(&admitted.tokens),
        );

        assert_eq!(
            variable(&offered, carried),
            Some(std::ffi::OsStr::new(value))
        );
        assert_eq!(variable(&offered, other), None, "{carried}");
    }
}

// REQ-GRANT-87, INV-RUNNER-32, REQ-TELEMETRY-46
#[test]
fn a_listed_entry_whose_names_the_runner_does_not_use_is_asked_for_nothing_and_recorded() {
    let refused = |token, token_file| vec![GITHUB_ENTRY, ("example", token, token_file)];
    for entries in [
        refused("EXAMPLE_ACCESS_TOKEN", "EXAMPLE_ACCESS_TOKEN"),
        refused("PATH", "EXAMPLE_ACCESS_TOKEN_FILE"),
        refused("EXAMPLE_ACCESS_TOKEN", "TMPDIR"),
        refused("USEFUL_HOME", "EXAMPLE_ACCESS_TOKEN_FILE"),
        refused("GITHUB_TOKEN", "EXAMPLE_ACCESS_TOKEN_FILE"),
        refused("EXAMPLE_ACCESS_TOKEN", "GIT_CONFIG_KEY_0"),
        refused("CODEX_HOME", "EXAMPLE_ACCESS_TOKEN_FILE"),
        refused("EXAMPLE_ACCESS_TOKEN", "CLAUDE_CODE_OAUTH_TOKEN"),
        refused("", "EXAMPLE_ACCESS_TOKEN_FILE"),
        refused("A-B", "EXAMPLE_ACCESS_TOKEN_FILE"),
        refused("GH_TOKEN", "EXAMPLE_ACCESS_TOKEN_FILE"),
        refused("EXAMPLE_ACCESS_TOKEN", "GITLAB_TOKEN_FILE"),
        refused("USEFUL_LINEAR_ACCESS_TOKEN", "EXAMPLE_ACCESS_TOKEN_FILE"),
        // A name of the configured entry before it: the first is used.
        vec![
            GITHUB_ENTRY,
            ("first", "FIRST_TOKEN", "SHARED_TOKEN_FILE"),
            ("example", "EXAMPLE_ACCESS_TOKEN", "SHARED_TOKEN_FILE"),
        ],
    ] {
        let fixture = Fixture::new();
        fixture.list_named(&entries);
        fixture
            .cloud
            .answer_grant_token(AgentLoginProvider::Github, FakeGrantToken::Issue(1));
        for id in ["first", "example"] {
            fixture
                .cloud
                .answer_grant_token(configured(id), FakeGrantToken::Issue(2));
        }
        let first_listed = entries.iter().any(|(id, _, _)| *id == "first");

        let admitted = fixture.admit(ASSIGNMENT);

        let mut asked = vec!["github"];
        if first_listed {
            asked.insert(0, "first");
        }
        assert_eq!(fixture.asked_ids(), asked, "{entries:?}");
        assert!(!fixture.configured_file("example").exists(), "{entries:?}");
        // The token `example` would have had is offered under no name; the
        // runner's `github`, and a configured entry it used, are offered.
        let offered = offered_alone(&admitted.tokens);
        let mut names = vec!["GH_TOKEN", "GH_TOKEN_FILE"];
        if first_listed {
            names.extend(["FIRST_TOKEN", "SHARED_TOKEN_FILE"]);
        }
        names.sort_unstable();
        assert_eq!(
            offered.variables().keys().collect::<Vec<_>>(),
            names
                .iter()
                .map(OsString::from)
                .collect::<Vec<_>>()
                .iter()
                .collect::<Vec<_>>(),
            "{entries:?}"
        );
        assert_eq!(
            fixture.records_naming("example"),
            [attributes(&serde_json::json!({
                "um.grant_token.provider": "example",
                "um.grant_token.phase": "admission",
                "um.grant_token.state": "cloud_unavailable",
                "um.grant_token.failure_stage": "list",
                "um.grant_token.failure_cause": "invalid_response",
                "um.assignment.id": ASSIGNMENT,
            }))],
            "{entries:?}"
        );
        assert_eq!(
            fixture
                .recorded()
                .grant(AgentLoginProvider::Github)
                .map(|grant| grant.state),
            Some(AgentLoginState::Ready),
            "{entries:?}"
        );
    }
}

// REQ-GRANT-87, INV-RUNNER-32
#[test]
fn the_runners_own_providers_keep_their_names_whatever_the_list_gives_them() {
    let fixture = Fixture::new();
    fixture.list_named(&[("github", "OTHER", "OTHER_FILE")]);
    fixture
        .cloud
        .answer_grant_token(AgentLoginProvider::Github, FakeGrantToken::Issue(2));

    let admitted = fixture.admit(ASSIGNMENT);

    let file = fixture.token_file(AgentLoginProvider::Github);
    assert!(file.ends_with(format!(
        ".runner-agent-login/assignments/{ASSIGNMENT}/github.token"
    )));
    let token = fake_grant_token(AgentLoginProvider::Github, 2);
    assert_eq!(fs::read_to_string(&file).unwrap(), token);
    let offered = offered_alone(&admitted.tokens);
    assert_eq!(
        variable(&offered, "GH_TOKEN"),
        Some(std::ffi::OsStr::new(&token))
    );
    assert_eq!(variable(&offered, "GH_TOKEN_FILE"), Some(file.as_os_str()));
    for name in ["OTHER", "OTHER_FILE"] {
        assert_eq!(variable(&offered, name), None, "{name}");
    }
}

// REQ-GRANT-86, REQ-GRANT-87, INV-RUNNER-32
#[test]
fn a_listed_gitlab_is_a_configured_provider_apart_from_the_runners_gitlab() {
    let fixture = Fixture::new();
    fixture.list_named(&[("gitlab", "ACME_TOKEN", "ACME_TOKEN_FILE")]);
    fixture
        .cloud
        .answer_grant_token(configured("gitlab"), FakeGrantToken::Issue(1));
    fixture
        .cloud
        .answer_grant_token(AgentLoginProvider::Gitlab, FakeGrantToken::Issue(1));

    let admitted = fixture.admit(ASSIGNMENT);

    assert_eq!(fixture.asked_ids(), ["gitlab"]);
    let file = fixture.configured_file("gitlab");
    let token = fake_grant_token(configured("gitlab"), 1);
    assert_eq!(fs::read_to_string(&file).unwrap(), token);
    assert!(!fixture.token_file(AgentLoginProvider::Gitlab).exists());
    assert_eq!(
        fixture
            .recorded()
            .grant(AgentLoginProvider::Gitlab)
            .map(|grant| grant.state),
        Some(AgentLoginState::NotAuthorized)
    );
    let offered = offered_alone(&admitted.tokens);
    assert_eq!(
        variable(&offered, "ACME_TOKEN"),
        Some(std::ffi::OsStr::new(&token))
    );
    assert_eq!(
        variable(&offered, "ACME_TOKEN_FILE"),
        Some(file.as_os_str())
    );
    for name in ["GITLAB_TOKEN", "GITLAB_TOKEN_FILE"] {
        assert_eq!(variable(&offered, name), None, "{name}");
    }
}

// REQ-GRANT-86, INV-RUNNER-28
#[test]
fn a_configured_file_an_earlier_assignment_left_is_gone_before_any_answer_is_used() {
    let fixture = Fixture::new();
    let directory = LoginDirectory::beside(&fixture.state_path());
    let (left, _) = directory
        .configured_grant_token(ENDED_ASSIGNMENT, "old")
        .unwrap();
    for private in [
        directory.path().to_owned(),
        directory.assignments(),
        directory.assignment(ENDED_ASSIGNMENT).unwrap(),
        directory.configured_grant_tokens(ENDED_ASSIGNMENT).unwrap(),
    ] {
        crate::agent_login::directory::ensure_private_directory(&private).unwrap();
    }
    fs::write(&left, "earlier-assignment-token").unwrap();
    let present_when_asked = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&present_when_asked);
    let watched = left.clone();
    fixture.cloud.on_grant_request(Box::new(move |request| {
        seen.lock()
            .unwrap()
            .push((request.provider.cloud_id().to_owned(), watched.exists()));
    }));
    fixture.list_named(&[GITHUB_ENTRY, EXAMPLE_ENTRY]);
    fixture
        .cloud
        .answer_grant_token(AgentLoginProvider::Github, FakeGrantToken::Issue(1));
    fixture
        .cloud
        .answer_grant_token(configured("example"), FakeGrantToken::Issue(1));

    let admitted = fixture.admit(ASSIGNMENT);

    let mut seen = present_when_asked.lock().unwrap().clone();
    seen.sort();
    assert_eq!(
        seen,
        [("example".to_owned(), false), ("github".to_owned(), false)]
    );
    assert!(!fixture.assignment_directory(ENDED_ASSIGNMENT).exists());
    assert!(fixture.configured_file("example").exists());

    // When the assignment ends its directory is gone.
    drop(admitted.tokens);
    assert!(!fixture.assignment_directory(ASSIGNMENT).exists());
}

// REQ-GRANT-86, INV-RUNNER-28
#[test]
fn an_idle_pass_removes_a_leftover_configured_token_file_and_asks_cloud_for_none() {
    let fixture = Fixture::new();
    // The configured providers' directory an earlier release kept.
    let directory = LoginDirectory::beside(&fixture.state_path())
        .path()
        .join("grant-tokens");
    let left = directory.join("old.token");
    crate::agent_login::directory::ensure_private_directory(directory.parent().unwrap()).unwrap();
    crate::agent_login::directory::ensure_private_directory(&directory).unwrap();
    fs::write(&left, "interrupted-runner-token").unwrap();
    let temporary = directory.join(".useful-grant-token.tmp.4242.1");
    fs::write(&temporary, "interrupted-write").unwrap();

    let outcome = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(fixture.setup.run_pass());

    assert!(matches!(outcome, SetupOutcome::Recorded(_)), "{outcome:?}");
    assert!(!left.exists());
    assert!(!temporary.exists());
    assert!(!directory.exists());
    assert_eq!(fixture.cloud.grant_requests(), []);
    assert_eq!(fixture.records(), []);
}

// REQ-GRANT-86, REQ-GRANT-88, REQ-TELEMETRY-46, INV-RUNNER-28
#[test]
fn an_assignments_directory_that_cannot_be_cleared_asks_for_none_of_its_providers() {
    let fixture = Fixture::new();
    // What an interrupted runner left in the admitted assignment's own
    // directory, in a directory that cannot be written, so that it cannot be
    // removed.
    let left = fixture.configured_file("old");
    let directory = left.parent().unwrap().to_owned();
    let logins = LoginDirectory::beside(&fixture.state_path());
    for private in [
        logins.path().to_owned(),
        logins.assignments(),
        logins.assignment(ASSIGNMENT).unwrap(),
        directory.clone(),
    ] {
        crate::agent_login::directory::ensure_private_directory(&private).unwrap();
    }
    fs::write(&left, "earlier-assignment-token").unwrap();
    fs::set_permissions(&directory, fs::Permissions::from_mode(0o500)).unwrap();
    fixture.list_named(&[GITHUB_ENTRY, EXAMPLE_ENTRY]);
    fixture
        .cloud
        .answer_grant_token(AgentLoginProvider::Github, FakeGrantToken::Issue(1));
    fixture
        .cloud
        .answer_grant_token(configured("example"), FakeGrantToken::Issue(1));

    let admitted = fixture.admit(ASSIGNMENT);

    assert_eq!(fixture.asked_ids(), Vec::<String>::new());
    assert!(!admitted.tokens.holds(AgentLoginProvider::Github));
    for provider in ["github", "example"] {
        assert_eq!(
            fixture.records_naming(provider),
            [attributes(&serde_json::json!({
                "um.grant_token.provider": provider,
                "um.grant_token.phase": "admission",
                "um.grant_token.state": "install_failed",
                "um.grant_token.failure_stage": "remove",
                "um.grant_token.failure_cause": "permission_denied",
                "um.assignment.id": ASSIGNMENT,
            }))]
        );
    }
    assert_eq!(
        fixture
            .recorded()
            .grant(AgentLoginProvider::Github)
            .unwrap()
            .state,
        AgentLoginState::InstallFailed
    );

    // The end of the assignment cannot remove its directory either, and
    // records it with the assignment and no provider.
    let records = fixture.records().len();
    drop(admitted.tokens);
    let removal = |from: usize| -> Vec<_> {
        fixture.records()[from..]
            .iter()
            .map(grant_attributes)
            .collect()
    };
    assert_eq!(
        removal(records),
        [attributes(&serde_json::json!({
            "um.grant_token.failure_stage": "remove",
            "um.grant_token.failure_cause": "permission_denied",
            "um.assignment.id": ASSIGNMENT,
        }))]
    );

    // A setup made once it is no longer held names neither.
    let records = fixture.records().len();
    let outcome = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(fixture.setup.run_pass());
    fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
    assert!(matches!(outcome, SetupOutcome::Recorded(_)), "{outcome:?}");
    assert_eq!(
        removal(records),
        [attributes(&serde_json::json!({
            "um.grant_token.failure_stage": "remove",
            "um.grant_token.failure_cause": "permission_denied",
        }))]
    );
}

/// The request each admission of `ASSIGNMENT` lists its providers with.
fn listing_request() -> GrantProvidersRequest {
    GrantProvidersRequest {
        boot_id: BOOT.to_owned(),
        assignment_id: ASSIGNMENT.to_owned(),
    }
}

/// The attributes of each record of the fixture, sorted by the provider
/// each names.
fn sorted_records(fixture: &Fixture) -> Vec<serde_json::Map<String, serde_json::Value>> {
    let mut records: Vec<_> = fixture.records().iter().map(grant_attributes).collect();
    records.sort_by_key(|record| {
        record
            .get("um.grant_token.provider")
            .map(ToString::to_string)
    });
    records
}

// REQ-INTEGRATION-12, REQ-GRANT-86, REQ-TELEMETRY-46
#[test]
fn a_listed_token_services_token_is_asked_for_beside_the_others_kept_and_offered_under_its_names() {
    let fixture = Fixture::new();
    fixture.answer_all(FakeGrantToken::Issue(1));
    fixture.list_token_services(&[MERGIFY_ENTRY]);
    fixture.cloud.answer_grant_token(
        token_service("mergify"),
        FakeGrantToken::IssueIntegrationToken(2),
    );

    let admitted = fixture.admit(ASSIGNMENT);

    fixture.assert_listed_once_first();
    assert_eq!(
        fixture.cloud.token_services_requests(),
        [(0, listing_request())]
    );
    assert_eq!(
        fixture.asked_ids(),
        ["github", "gitlab.com", "linear", "mergify"]
    );
    assert!(
        fixture
            .cloud
            .grant_requests()
            .iter()
            .any(|request| request.provider == token_service("mergify"))
    );
    let file = fixture.configured_file("mergify");
    let token = fake_grant_token(token_service("mergify"), 2);
    assert_eq!(fs::read_to_string(&file).unwrap(), token);
    assert_eq!(mode(&file), 0o600);
    let offered = offered_alone(&admitted.tokens);
    assert_eq!(
        variable(&offered, "MERGIFY_TOKEN"),
        Some(std::ffi::OsStr::new(&token))
    );
    assert_eq!(
        variable(&offered, "MERGIFY_TOKEN_FILE"),
        Some(file.as_os_str())
    );
    assert_eq!(
        fixture.records_naming("mergify"),
        [attributes(&serde_json::json!({
            "um.grant_token.provider": "mergify",
            "um.grant_token.phase": "admission",
            "um.grant_token.state": "ready",
            "um.grant_token.generation": 2,
            "um.assignment.id": ASSIGNMENT,
        }))]
    );
    // The readiness document's entries are the runner's three alone.
    let document: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(readiness_path(&fixture.state_path())).unwrap())
            .unwrap();
    let mut entries: Vec<&str> = document["providers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| entry["provider"].as_str().unwrap())
        .collect();
    entries.sort_unstable();
    assert_eq!(entries, ["github", "gitlab", "linear"], "{document}");
}

// REQ-INTEGRATION-12, INV-RUNNER-31
#[test]
fn a_token_services_source_is_the_token_while_its_kept_file_reads_back_and_none_after() {
    const ISSUED_FROM: &str = "itk_01k0z6r1w8f4jy2m7q9v3x5abe";
    let fixture = Fixture::new();
    fixture.list_token_services(&[MERGIFY_ENTRY]);
    fixture.cloud.answer_grant_token(
        token_service("mergify"),
        FakeGrantToken::IssueFrom(ISSUED_FROM, 1),
    );
    let admitted = fixture.admit(ASSIGNMENT);
    let nothing = EnvironmentSnapshot::new(Vec::<(OsString, OsString)>::new());
    assert_eq!(
        token_service_sources(&nothing, Some(&admitted.tokens)),
        [ReportedTokenService {
            provider: "mergify".to_owned(),
            source: TokenServiceSource::Token {
                integration_token_id: ISSUED_FROM.to_owned(),
            },
        }]
    );

    // The file no longer holds a token of its form: `offer` offers none,
    // and the service has no source.
    fs::write(fixture.configured_file("mergify"), "").unwrap();

    assert_eq!(
        variable(&offered_alone(&admitted.tokens), "MERGIFY_TOKEN"),
        None
    );
    assert_eq!(token_service_sources(&nothing, Some(&admitted.tokens)), []);
}

// REQ-INTEGRATION-12, REQ-GRANT-87, INV-RUNNER-17
#[test]
fn a_token_service_the_host_provides_is_asked_for_nothing_and_keeps_no_file() {
    let service = [("MERGIFY_TOKEN_FILE", "/srv/mergify-token")];
    let fixture = Fixture::serving(&service);
    fixture.answer_all(FakeGrantToken::Issue(1));
    fixture.list_token_services(&[MERGIFY_ENTRY]);
    fixture.cloud.answer_grant_token(
        token_service("mergify"),
        FakeGrantToken::IssueIntegrationToken(1),
    );

    let admitted = fixture.admit(ASSIGNMENT);

    assert_eq!(fixture.asked_ids(), ["github", "gitlab.com", "linear"]);
    assert!(!fixture.configured_file("mergify").exists());
    assert_eq!(fixture.records_naming("mergify"), []);
    let offered = offer(&EnvironmentSnapshot::new(service), Some(&admitted.tokens));
    assert_eq!(
        variable(&offered, "MERGIFY_TOKEN_FILE"),
        Some(std::ffi::OsStr::new("/srv/mergify-token"))
    );
    assert_eq!(variable(&offered, "MERGIFY_TOKEN"), None);
}

// REQ-INTEGRATION-12
#[test]
fn a_cloud_without_the_token_service_list_is_asked_and_recorded_exactly_as_before() {
    let fixture = Fixture::new();
    fixture.answer_all(FakeGrantToken::Issue(1));
    fixture.list_named(&[GITHUB_ENTRY]);
    fixture.cloud.answer_grant_token(
        token_service("mergify"),
        FakeGrantToken::IssueIntegrationToken(1),
    );

    let admitted = fixture.admit(ASSIGNMENT);

    fixture.assert_listed_once_first();
    assert_eq!(
        fixture.cloud.token_services_requests(),
        [(0, listing_request())]
    );
    assert_eq!(fixture.asked_ids(), ["github"]);
    assert!(
        !LoginDirectory::beside(&fixture.state_path())
            .configured_grant_tokens(ASSIGNMENT)
            .unwrap()
            .exists()
    );
    assert_eq!(
        offered_alone(&admitted.tokens)
            .variables()
            .keys()
            .collect::<Vec<_>>(),
        ["GH_TOKEN", "GH_TOKEN_FILE"]
            .map(OsString::from)
            .iter()
            .collect::<Vec<_>>()
    );
    assert_eq!(
        sorted_records(&fixture),
        [
            serde_json::json!({
                "um.grant_token.provider": "github",
                "um.grant_token.phase": "admission",
                "um.grant_token.state": "ready",
                "um.grant_token.generation": 1,
                "um.assignment.id": ASSIGNMENT,
            }),
            serde_json::json!({
                "um.grant_token.provider": "gitlab",
                "um.grant_token.phase": "admission",
                "um.grant_token.state": "not_authorized",
                "um.assignment.id": ASSIGNMENT,
            }),
            serde_json::json!({
                "um.grant_token.provider": "linear",
                "um.grant_token.phase": "admission",
                "um.grant_token.state": "not_authorized",
                "um.assignment.id": ASSIGNMENT,
            }),
        ]
        .map(|record| record.as_object().unwrap().clone())
    );
}

// REQ-INTEGRATION-12, REQ-TELEMETRY-46
#[test]
fn a_token_service_list_cloud_did_not_answer_asks_for_none_and_is_recorded_once_with_no_provider() {
    let admit_with = |services: Result<Vec<ListedProvider>, GrantProvidersFailure>| {
        let fixture = Fixture::new();
        fixture.answer_all(FakeGrantToken::Issue(1));
        fixture.list_named(&[GITHUB_ENTRY]);
        fixture.cloud.answer_grant_token(
            token_service("mergify"),
            FakeGrantToken::IssueIntegrationToken(1),
        );
        fixture.cloud.answer_token_services(services);
        drop(fixture.admit(ASSIGNMENT));
        fixture
    };
    // What the runner asks and records when Cloud lists no token service.
    let empty = admit_with(Ok(Vec::new()));
    assert_eq!(empty.asked_ids(), ["github"]);
    let expected = sorted_records(&empty);
    assert_eq!(expected.len(), 3);

    for (failure, cause, status) in [
        (
            GrantProvidersFailure::Cloud(CloudFailure::Status(503)),
            "http_status",
            Some(503),
        ),
        (
            GrantProvidersFailure::AssignmentNotCurrent,
            "http_status",
            Some(409),
        ),
        (
            GrantProvidersFailure::Cloud(CloudFailure::InvalidResponse),
            "invalid_response",
            None,
        ),
        (
            GrantProvidersFailure::Cloud(CloudFailure::Transport),
            "transport",
            None,
        ),
    ] {
        let fixture = admit_with(Err(failure));

        assert_eq!(fixture.asked_ids(), empty.asked_ids(), "{failure:?}");
        assert!(!fixture.configured_file("mergify").exists(), "{failure:?}");
        let records = sorted_records(&fixture);
        let mut list = serde_json::json!({
            "um.grant_token.phase": "admission",
            "um.grant_token.failure_stage": "list",
            "um.grant_token.failure_cause": cause,
            "um.assignment.id": ASSIGNMENT,
        });
        if let Some(status) = status {
            list["um.grant_token.http_status"] = status.into();
        }
        // The record with no provider sorts first.
        assert_eq!(
            serde_json::Value::Object(records[0].clone()),
            list,
            "{failure:?}"
        );
        assert_eq!(records[1..], expected[..], "{failure:?}");
    }
}

// REQ-INTEGRATION-12
#[test]
fn a_failed_oauth_list_still_asks_for_and_keeps_a_listed_token_services_token() {
    let fixture = Fixture::new();
    fixture.answer_all(FakeGrantToken::Issue(1));
    fixture
        .cloud
        .answer_grant_providers(Err(GrantProvidersFailure::Cloud(CloudFailure::Status(503))));
    fixture.list_token_services(&[MERGIFY_ENTRY]);
    fixture.cloud.answer_grant_token(
        token_service("mergify"),
        FakeGrantToken::IssueIntegrationToken(1),
    );

    let admitted = fixture.admit(ASSIGNMENT);

    assert_eq!(fixture.asked_ids(), ["mergify"]);
    let file = fixture.configured_file("mergify");
    assert_eq!(
        fs::read_to_string(&file).unwrap(),
        fake_grant_token(token_service("mergify"), 1)
    );
    assert_eq!(
        variable(&offered_alone(&admitted.tokens), "MERGIFY_TOKEN_FILE"),
        Some(file.as_os_str())
    );
    for grant in fixture.grants() {
        assert_eq!(grant.state, AgentLoginState::CloudUnavailable, "{grant:?}");
    }
    assert_eq!(
        fixture.records_naming("mergify")[0]["um.grant_token.state"],
        "ready"
    );
}

// REQ-INTEGRATION-12, INV-RUNNER-32, REQ-TELEMETRY-46
#[test]
fn a_token_service_entry_sharing_a_name_or_an_id_with_another_is_asked_for_nothing_and_recorded() {
    for (oauth, services) in [
        // A name of the runner's three.
        (
            vec![GITHUB_ENTRY],
            vec![("mergify", "GH_TOKEN", "MERGIFY_TOKEN_FILE")],
        ),
        // A name of a configured entry of the OAuth list.
        (
            vec![GITHUB_ENTRY, EXAMPLE_ENTRY],
            vec![("mergify", "MERGIFY_TOKEN", "EXAMPLE_ACCESS_TOKEN_FILE")],
        ),
        // An id the OAuth list also names.
        (
            vec![
                GITHUB_ENTRY,
                ("mergify", "OAUTH_MERGIFY_TOKEN", "OAUTH_MERGIFY_TOKEN_FILE"),
            ],
            vec![MERGIFY_ENTRY],
        ),
    ] {
        let fixture = Fixture::new();
        fixture.answer_all(FakeGrantToken::Issue(1));
        fixture.list_named(&oauth);
        fixture.list_token_services(&services);
        fixture.cloud.answer_grant_token(
            token_service("mergify"),
            FakeGrantToken::IssueIntegrationToken(1),
        );

        let admitted = fixture.admit(ASSIGNMENT);

        assert!(
            fixture
                .cloud
                .grant_requests()
                .iter()
                .all(|request| request.provider != token_service("mergify")),
            "{services:?}"
        );
        assert!(!fixture.configured_file("mergify").exists(), "{services:?}");
        let offered = offered_alone(&admitted.tokens);
        assert_eq!(variable(&offered, "MERGIFY_TOKEN"), None, "{services:?}");
        assert!(
            fixture
                .records_naming("mergify")
                .contains(&attributes(&serde_json::json!({
                    "um.grant_token.provider": "mergify",
                    "um.grant_token.phase": "admission",
                    "um.grant_token.state": "cloud_unavailable",
                    "um.grant_token.failure_stage": "list",
                    "um.grant_token.failure_cause": "invalid_response",
                    "um.assignment.id": ASSIGNMENT,
                }))),
            "{services:?}"
        );
        assert_eq!(
            fixture
                .recorded()
                .grant(AgentLoginProvider::Github)
                .map(|grant| grant.state),
            Some(AgentLoginState::Ready),
            "{services:?}"
        );
    }
}

// REQ-INTEGRATION-12, REQ-TELEMETRY-46
#[test]
fn no_token_services_token_is_in_the_readiness_document_or_any_record() {
    let fixture = Fixture::new();
    fixture.list_token_services(&[MERGIFY_ENTRY]);
    fixture.cloud.answer_grant_token(
        token_service("mergify"),
        FakeGrantToken::IssueIntegrationToken(3),
    );
    // Beside it, a failed token-service list's record.
    let failed = Fixture::new();
    failed
        .cloud
        .answer_token_services(Err(GrantProvidersFailure::Cloud(CloudFailure::Transport)));

    let admitted = fixture.admit(ASSIGNMENT);
    drop(failed.admit(ASSIGNMENT));

    assert!(fixture.configured_file("mergify").exists());
    for checked in [&fixture, &failed] {
        let document = fs::read_to_string(readiness_path(&checked.state_path())).unwrap();
        let records = serde_json::to_string(&checked.capture.events()).unwrap();
        for (name, text) in [("readiness document", document), ("telemetry", records)] {
            assert!(!text.contains(FAKE_GRANT_TOKEN_SENTINEL), "{name}: {text}");
            assert!(
                !text.contains(".token"),
                "{name} names a token file: {text}"
            );
            assert!(
                !text.contains("MERGIFY_TOKEN"),
                "{name} names an offered variable: {text}"
            );
        }
    }
    let hold = format!("{:?}", admitted.tokens);
    assert!(!hold.contains(FAKE_GRANT_TOKEN_SENTINEL), "{hold}");
}

/// The wait the fake Cloud names with each token in the refresh tests.
const WAIT: Duration = Duration::from_secs(60);

/// An assignment admitted with the token of generation 1 of `provider`,
/// `github` unless a test lists another, whose hold keeps it current on a
/// controlled sleeper: the sleeps the task asks for, the environment its
/// steps were offered at admission, and each readiness a refresh published.
/// The test keeps no sleeper of its own, so once the task has stopped no
/// sleep can be asked for.
struct Refreshing {
    fixture: Fixture,
    /// The provider whose answer `pass` sets.
    provider: GrantProvider,
    /// How many token requests the admission made.
    admitted: usize,
    tokens: Option<AssignmentGrantTokens>,
    offered: EnvironmentSnapshot,
    sleeps: tokio::sync::mpsc::UnboundedReceiver<(Duration, SleepRelease)>,
    /// The sleep the task waits in, once the test has seen it.
    pending: Option<(Duration, SleepRelease)>,
    published: Arc<Mutex<Vec<ReadinessDocument>>>,
    /// The time on the sleeper's clock when the assignment was admitted.
    started: OffsetDateTime,
}

impl Refreshing {
    /// Admits the assignment with a token that expires `lifetime` after
    /// the sleeper's start, and starts keeping it current.
    fn start(lifetime: time::Duration) -> Self {
        Self::listing(
            lifetime,
            GrantProvider::try_from(AgentLoginProvider::Github).unwrap(),
            &[],
        )
    }

    /// `start` for `provider`, where Cloud lists `entries`, if any, and
    /// issues generation 1 of each provider it lists.
    fn listing(
        lifetime: time::Duration,
        provider: GrantProvider,
        entries: &[(&str, &str, &str)],
    ) -> Self {
        Self::admitted(lifetime, provider, FakeGrantToken::Issue(1), |fixture| {
            if !entries.is_empty() {
                fixture.list_named(entries);
            }
            for (id, _, _) in entries {
                fixture.cloud.answer_grant_token(
                    GrantProvider::listed(id).unwrap(),
                    FakeGrantToken::Issue(1),
                );
            }
        })
    }

    /// `start` for `mergify`, the one token service Cloud lists, whose
    /// token Cloud issues from an integration token at version 1.
    fn mergify(lifetime: time::Duration) -> Self {
        Self::admitted(
            lifetime,
            token_service("mergify"),
            FakeGrantToken::IssueIntegrationToken(1),
            |fixture| fixture.list_token_services(&[MERGIFY_ENTRY]),
        )
    }

    /// `start` for `provider`, issued `issued`, once `prepare` has set what
    /// else Cloud answers.
    fn admitted(
        lifetime: time::Duration,
        provider: GrantProvider,
        issued: FakeGrantToken,
        prepare: impl FnOnce(&Fixture),
    ) -> Self {
        let fixture = Fixture::new();
        let (sleeper, sleeps) = controlled_sleeper();
        let started = sleeper.utc_now();
        fixture
            .cloud
            .issue_grant_tokens_with(WAIT, started + lifetime);
        prepare(&fixture);
        fixture.cloud.answer_grant_token(provider.clone(), issued);
        let mut admitted = fixture.admit(ASSIGNMENT);
        let requests = fixture.cloud.grant_requests().len();
        let offered = offer(
            &EnvironmentSnapshot::new(Vec::<(OsString, OsString)>::new()),
            Some(&admitted.tokens),
        );
        let published = Arc::new(Mutex::new(Vec::new()));
        let publishing = Arc::clone(&published);
        admitted.tokens.publish_with(Arc::new(move |document| {
            publishing.lock().unwrap().push(document.clone());
        }));
        admitted.tokens.keep_current(sleeper);
        Self {
            fixture,
            provider,
            admitted: requests,
            tokens: Some(admitted.tokens),
            offered,
            sleeps,
            pending: None,
            published,
            started,
        }
    }

    /// `start` with a token that outlives every test.
    fn started() -> Self {
        Self::start(time::Duration::days(1))
    }

    /// `start` for `example`, the one provider Cloud lists, with a token
    /// that expires `lifetime` after the sleeper's start.
    fn example(lifetime: time::Duration) -> Self {
        Self::listing(lifetime, configured("example"), &[EXAMPLE_ENTRY])
    }

    /// The wait of the sleep the task asks for once it has done everything
    /// before it; `None` once the task has stopped.
    async fn wait(&mut self) -> Option<Duration> {
        if self.pending.is_none() {
            self.pending = with_watchdog(self.sleeps.recv())
                .await
                .expect("the refresh task neither slept nor stopped");
        }
        self.pending.as_ref().map(|(wait, _)| *wait)
    }

    /// Lets the task's sleep pass with Cloud answering `answer`, and
    /// answers the wait it asks for next; `None` once it has stopped.
    async fn pass(&mut self, answer: FakeGrantToken) -> Option<Duration> {
        self.wait().await.expect("the refresh task sleeps");
        let (_, release) = self.pending.take().unwrap();
        self.fixture
            .cloud
            .answer_grant_token(self.provider.clone(), answer);
        release.release();
        self.wait().await
    }

    /// The file of `provider`'s token.
    fn file(&self) -> PathBuf {
        match self.provider.agent_login_provider() {
            Some(provider) => self.fixture.token_file(provider),
            None => self.fixture.configured_file(self.provider.cloud_id()),
        }
    }

    fn token(&self) -> String {
        fs::read_to_string(self.file()).unwrap()
    }

    fn github(&self) -> GrantReadiness {
        self.fixture
            .recorded()
            .grant(AgentLoginProvider::Github)
            .cloned()
            .unwrap()
    }

    /// The requests Cloud was asked after the admission's.
    fn refresh_requests(&self) -> Vec<GrantTokenRequest> {
        self.fixture
            .cloud
            .grant_requests()
            .into_iter()
            .skip(self.admitted)
            .collect()
    }

    /// The attributes of each record of a refresh.
    fn refresh_records(&self) -> Vec<serde_json::Map<String, serde_json::Value>> {
        self.fixture
            .records()
            .iter()
            .map(grant_attributes)
            .filter(|record| record["um.grant_token.phase"] == "refresh")
            .collect()
    }
}

fn ready(generation: u64, expires_at: OffsetDateTime) -> GrantReadiness {
    GrantReadiness {
        provider: AgentLoginProvider::Github,
        state: AgentLoginState::Ready,
        checked_at: Some(now()),
        issued: Some(IssuedGrant {
            grant_id: FAKE_GRANT_ID.to_owned(),
            generation,
            expires_at,
        }),
        run_id: Some(RUN.to_owned()),
        ended_at: None,
        source: Some(CredentialSource::Grant),
    }
}

/// `not_ready(state)` for a provider whose token the admission kept, whose
/// source a refresh leaves as the admission settled it.
fn held_then(state: AgentLoginState) -> GrantReadiness {
    GrantReadiness {
        source: Some(CredentialSource::Grant),
        ..not_ready(state)
    }
}

fn not_ready(state: AgentLoginState) -> GrantReadiness {
    GrantReadiness {
        provider: AgentLoginProvider::Github,
        state,
        checked_at: Some(now()),
        issued: None,
        run_id: Some(RUN.to_owned()),
        ended_at: None,
        source: None,
    }
}

/// What the shell `script` prints, started with `environment` alone.
fn run_with(environment: &EnvironmentSnapshot, script: &str) -> String {
    let output = std::process::Command::new("/bin/sh")
        .env_clear()
        .envs(environment.variables())
        .args(["-c", script])
        .stdin(std::process::Stdio::null())
        .output()
        .unwrap();
    assert!(output.status.success(), "{script}: {output:?}");
    String::from_utf8(output.stdout).unwrap()
}

/// The password Git's credential fill answers for `https://github.com`
/// through the helper the workflow reference documents, which reads
/// `$GH_TOKEN_FILE`; the first, empty `credential.helper` clears the
/// host's own helpers.
fn git_credential_password(environment: &EnvironmentSnapshot) -> String {
    let mut fill = useful_test_support::fixture_git_command("git");
    fill.envs(environment.variables())
        .args([
            "-c",
            "credential.helper=",
            "-c",
            r#"credential.helper=!f() { echo username=x-access-token; echo "password=$(cat "$GH_TOKEN_FILE")"; }; f"#,
            "credential",
            "fill",
        ])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    let mut child = fill.spawn().unwrap();
    std::io::Write::write_all(
        child.stdin.as_mut().unwrap(),
        b"protocol=https\nhost=github.com\n\n",
    )
    .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success(), "git credential fill: {output:?}");
    let answer = String::from_utf8(output.stdout).unwrap();
    assert!(
        answer.lines().any(|line| line == "username=x-access-token"),
        "{answer}"
    );
    answer
        .lines()
        .find_map(|line| line.strip_prefix("password="))
        .unwrap()
        .to_owned()
}

// REQ-GRANT-89, REQ-GRANT-87
#[tokio::test]
async fn when_the_wait_passes_the_file_is_replaced_in_place_with_the_newer_generation() {
    let mut run = Refreshing::started();
    let file = run.file();
    assert_eq!(run.token(), fake_grant_token(AgentLoginProvider::Github, 1));

    // Nothing is asked before the wait Cloud named has passed.
    assert_eq!(run.wait().await, Some(WAIT));
    assert_eq!(run.refresh_requests(), []);

    let next = run.pass(FakeGrantToken::Issue(2)).await;

    assert_eq!(next, Some(WAIT));
    assert_eq!(
        run.refresh_requests(),
        [GrantTokenRequest {
            boot_id: BOOT.to_owned(),
            assignment_id: ASSIGNMENT.to_owned(),
            provider: GrantProvider::try_from(AgentLoginProvider::Github).unwrap(),
        }]
    );
    assert_eq!(run.file(), file);
    let token = fake_grant_token(AgentLoginProvider::Github, 2);
    assert_eq!(run.token(), token);
    assert_eq!(mode(&file), 0o600);
    let expires_at = run.github().issued.unwrap().expires_at;
    assert_eq!(run.github(), ready(2, expires_at));
    // The live status is given what was recorded.
    assert_eq!(
        run.published.lock().unwrap().last().cloned(),
        Some(run.fixture.recorded())
    );
    // A command started now with the step's environment reads the new token
    // through the file, and Git is given it by the documented helper; the
    // variable is still the token as admitted.
    let offered = run.offered.clone();
    assert_eq!(
        run_with(
            &offered,
            // The file holds the token without a newline, which `read`
            // reads and answers as end of file.
            r#"read -r token < "$GH_TOKEN_FILE"; printf %s "$token""#
        ),
        token
    );
    assert_eq!(git_credential_password(&offered), token);
    assert_eq!(
        run_with(&offered, r#"printf %s "$GH_TOKEN""#),
        fake_grant_token(AgentLoginProvider::Github, 1)
    );
    assert_eq!(run.refresh_records().len(), 1);
    assert_eq!(run.refresh_records()[0]["um.grant_token.state"], "ready");
    assert_eq!(run.refresh_records()[0]["um.grant_token.generation"], 2);
}

// REQ-GRANT-89, REQ-GRANT-86
#[tokio::test]
async fn a_refresh_asks_cloud_for_no_list() {
    let mut run = Refreshing::started();

    assert_eq!(run.pass(FakeGrantToken::Issue(2)).await, Some(WAIT));
    assert_eq!(run.pass(FakeGrantToken::Issue(3)).await, Some(WAIT));

    assert_eq!(run.refresh_requests().len(), 2);
    run.fixture.assert_listed_once_first();
}

// REQ-GRANT-89
#[tokio::test]
async fn an_answer_of_the_held_generation_changes_nothing_and_its_wait_is_followed() {
    let mut run = Refreshing::started();
    let admitted = run.fixture.recorded();
    let published = run.published.lock().unwrap().len();

    let next = run.pass(FakeGrantToken::Issue(1)).await;

    assert_eq!(next, Some(WAIT));
    assert_eq!(run.token(), fake_grant_token(AgentLoginProvider::Github, 1));
    assert_eq!(run.fixture.recorded(), admitted);
    assert_eq!(run.refresh_records(), []);
    assert_eq!(run.published.lock().unwrap().len(), published);

    // A wait under 5 seconds is followed as 5 seconds.
    run.fixture
        .cloud
        .issue_grant_tokens_with(Duration::from_secs(2), fake_grant_expiry());
    assert_eq!(
        run.pass(FakeGrantToken::Issue(1)).await,
        Some(Duration::from_secs(5))
    );
    run.fixture
        .cloud
        .issue_grant_tokens_with(Duration::from_secs(30), fake_grant_expiry());
    assert_eq!(
        run.pass(FakeGrantToken::Issue(1)).await,
        Some(Duration::from_secs(30))
    );
    assert_eq!(run.refresh_requests().len(), 3);
}

// REQ-GRANT-89, REQ-GRANT-88, REQ-TELEMETRY-46
#[tokio::test]
async fn a_failed_refresh_keeps_the_file_is_recorded_and_retried_with_backoff() {
    for (failure, cause, status) in [
        (
            GrantTokenFailure::Cloud(CloudFailure::Status(503)),
            "http_status",
            Some(503),
        ),
        (
            GrantTokenFailure::Cloud(CloudFailure::Transport),
            "transport",
            None,
        ),
        // Cloud's 409 for an assignment it does not count as this runner's
        // is a failed request too, reported with its status.
        (
            GrantTokenFailure::Refused(GrantTokenRefusal::AssignmentNotCurrent),
            "http_status",
            Some(409),
        ),
    ] {
        let mut run = Refreshing::started();
        let held = fake_grant_token(AgentLoginProvider::Github, 1);

        let next = run.pass(FakeGrantToken::Fail(failure)).await;

        assert_eq!(next, Some(Duration::from_secs(5)), "{failure:?}");
        assert_eq!(run.token(), held);
        assert_eq!(run.github(), held_then(AgentLoginState::CloudUnavailable));
        let mut expected = serde_json::json!({
            "um.grant_token.provider": "github",
            "um.grant_token.phase": "refresh",
            "um.grant_token.state": "cloud_unavailable",
            "um.grant_token.failure_stage": "issue",
            "um.grant_token.failure_cause": cause,
            "um.assignment.id": ASSIGNMENT,
        });
        if let Some(status) = status {
            expected["um.grant_token.http_status"] = status.into();
        }
        assert_eq!(
            run.refresh_records(),
            [expected.as_object().unwrap().clone()],
            "{failure:?}"
        );

        // Each failure in a row doubles the wait, up to 5 minutes.
        for wait in [10, 20, 40, 80, 160, 300, 300] {
            assert_eq!(
                run.pass(FakeGrantToken::Fail(failure)).await,
                Some(Duration::from_secs(wait)),
                "{failure:?}"
            );
            assert_eq!(run.token(), held);
        }
        // Each failure is reported, not only the one that changed the state.
        let records = run.refresh_records();
        assert_eq!(records.len(), 8, "{failure:?}");
        assert!(
            records
                .iter()
                .all(|record| record["um.grant_token.failure_cause"] == cause),
            "{failure:?}"
        );

        // A later success is ready again, and waits as Cloud names.
        assert_eq!(run.pass(FakeGrantToken::Issue(2)).await, Some(WAIT));
        assert_eq!(run.token(), fake_grant_token(AgentLoginProvider::Github, 2));
        assert_eq!(run.github().state, AgentLoginState::Ready);
        assert_eq!(
            run.refresh_records().last().unwrap()["um.grant_token.state"],
            "ready"
        );
        assert_eq!(
            run.pass(FakeGrantToken::Fail(failure)).await,
            Some(Duration::from_secs(5)),
            "{failure:?}"
        );
    }
}

// REQ-GRANT-89
#[tokio::test]
async fn once_the_held_token_expires_unrefreshed_its_file_is_removed_and_the_asking_goes_on() {
    // The token expires 70 seconds after the start: the refresh at 60
    // seconds and the retry at 65 keep it, the retry at 75 removes it.
    let mut run = Refreshing::start(time::Duration::seconds(70));
    let unavailable = FakeGrantToken::Fail(GrantTokenFailure::Cloud(CloudFailure::Status(503)));

    assert_eq!(run.pass(unavailable).await, Some(Duration::from_secs(5)));
    assert_eq!(run.pass(unavailable).await, Some(Duration::from_secs(10)));
    assert!(run.file().exists());

    assert_eq!(run.pass(unavailable).await, Some(Duration::from_secs(20)));

    assert!(!run.file().exists());
    assert_eq!(run.github(), held_then(AgentLoginState::CloudUnavailable));
    // The asking goes on. A token of the generation the runner held is kept
    // again, since its file no longer holds it, and so is one issued later.
    assert_eq!(run.pass(FakeGrantToken::Issue(1)).await, Some(WAIT));
    assert_eq!(run.token(), fake_grant_token(AgentLoginProvider::Github, 1));
    assert_eq!(run.pass(FakeGrantToken::Issue(2)).await, Some(WAIT));
    assert_eq!(run.token(), fake_grant_token(AgentLoginProvider::Github, 2));
}

// REQ-GRANT-89, REQ-GRANT-88
#[tokio::test]
async fn a_refusal_removes_the_file_at_once_and_ends_the_asking() {
    let refused = GrantTokenFailure::Refused;
    for (refusal, state) in [
        (
            refused(GrantTokenRefusal::Revoked),
            AgentLoginState::Revoked,
        ),
        (
            refused(GrantTokenRefusal::Unauthorized),
            AgentLoginState::NotAuthorized,
        ),
        (
            refused(GrantTokenRefusal::ReconnectRequired),
            AgentLoginState::ReconnectRequired,
        ),
        (
            refused(GrantTokenRefusal::ProviderUnavailable),
            AgentLoginState::UnsupportedTarget,
        ),
        // A Cloud that no longer has the route authorizes nothing.
        (GrantTokenFailure::NoRoute, AgentLoginState::NotAuthorized),
    ] {
        let mut run = Refreshing::started();

        let next = run.pass(FakeGrantToken::Fail(refusal)).await;

        assert_eq!(next, None, "{refusal:?}");
        assert!(!run.file().exists(), "{refusal:?}");
        assert_eq!(run.github(), held_then(state), "{refusal:?}");
        assert_eq!(
            run.published.lock().unwrap().last().cloned(),
            Some(run.fixture.recorded())
        );
        assert_eq!(run.refresh_requests().len(), 1);
        assert_eq!(run.refresh_records().len(), 1);
        assert_eq!(
            run.refresh_records()[0]["um.grant_token.state"],
            state.as_str()
        );
    }
}

// REQ-GRANT-89, INV-RUNNER-28
#[tokio::test]
async fn when_the_assignment_ends_nothing_more_is_asked() {
    let mut run = Refreshing::started();
    assert_eq!(run.wait().await, Some(WAIT));

    drop(run.tokens.take());

    // The task stopped: its sleep is gone with it, and none follows.
    let (_, release) = run.pending.take().unwrap();
    release.release();
    assert!(run.wait().await.is_none());
    assert_eq!(run.refresh_requests(), []);
}

/// What the fake Cloud answers for a token request it does not answer.
const UNANSWERED: FakeGrantToken =
    FakeGrantToken::Fail(GrantTokenFailure::Cloud(CloudFailure::Transport));

// REQ-GRANT-89, REQ-TELEMETRY-46
#[tokio::test]
async fn a_configured_providers_file_is_replaced_in_place_when_the_wait_passes() {
    let mut run = Refreshing::example(time::Duration::days(1));
    let file = run.fixture.configured_file("example");
    let admitted = fake_grant_token(configured("example"), 1);
    assert_eq!(fs::read_to_string(&file).unwrap(), admitted);
    assert_eq!(run.wait().await, Some(WAIT));
    assert_eq!(run.refresh_requests(), []);

    assert_eq!(run.pass(FakeGrantToken::Issue(2)).await, Some(WAIT));

    assert_eq!(
        run.refresh_requests(),
        [GrantTokenRequest {
            boot_id: BOOT.to_owned(),
            assignment_id: ASSIGNMENT.to_owned(),
            provider: configured("example"),
        }]
    );
    let token = fake_grant_token(configured("example"), 2);
    assert_eq!(fs::read_to_string(&file).unwrap(), token);
    assert_eq!(mode(&file), 0o600);
    // A command started now with the step's environment reads the new token
    // through the file; the variable is still the token as admitted.
    let offered = run.offered.clone();
    assert_eq!(
        run_with(
            &offered,
            r#"read -r token < "$EXAMPLE_ACCESS_TOKEN_FILE"; printf %s "$token""#
        ),
        token
    );
    assert_eq!(
        run_with(&offered, r#"printf %s "$EXAMPLE_ACCESS_TOKEN""#),
        admitted
    );
    assert_eq!(
        run.refresh_records(),
        [attributes(&serde_json::json!({
            "um.grant_token.provider": "example",
            "um.grant_token.phase": "refresh",
            "um.grant_token.state": "ready",
            "um.grant_token.generation": 2,
            "um.assignment.id": ASSIGNMENT,
        }))]
    );
}

// REQ-GRANT-89, REQ-TELEMETRY-46
#[tokio::test]
async fn an_answer_of_a_configured_providers_held_generation_writes_no_record() {
    let mut run = Refreshing::example(time::Duration::days(1));

    assert_eq!(run.pass(FakeGrantToken::Issue(1)).await, Some(WAIT));

    assert_eq!(run.token(), fake_grant_token(configured("example"), 1));
    assert_eq!(run.refresh_records(), []);
    // The next wait is the one Cloud names with it.
    run.fixture
        .cloud
        .issue_grant_tokens_with(Duration::from_secs(30), fake_grant_expiry());
    assert_eq!(
        run.pass(FakeGrantToken::Issue(1)).await,
        Some(Duration::from_secs(30))
    );
    assert_eq!(run.refresh_records(), []);
    assert_eq!(run.refresh_requests().len(), 2);
}

// REQ-GRANT-89, REQ-TELEMETRY-46
#[tokio::test]
async fn a_configured_provider_cloud_does_not_answer_keeps_its_file_is_recorded_and_retried() {
    let mut run = Refreshing::example(time::Duration::days(1));
    let held = fake_grant_token(configured("example"), 1);
    let unavailable = attributes(&serde_json::json!({
        "um.grant_token.provider": "example",
        "um.grant_token.phase": "refresh",
        "um.grant_token.state": "cloud_unavailable",
        "um.grant_token.failure_stage": "issue",
        "um.grant_token.failure_cause": "transport",
        "um.assignment.id": ASSIGNMENT,
    }));

    assert_eq!(run.pass(UNANSWERED).await, Some(Duration::from_secs(5)));

    assert_eq!(run.token(), held);
    assert_eq!(run.refresh_records(), std::slice::from_ref(&unavailable));
    assert_eq!(run.pass(UNANSWERED).await, Some(Duration::from_secs(10)));
    assert_eq!(run.token(), held);
    assert_eq!(run.refresh_records(), [unavailable.clone(), unavailable]);

    // The answer that follows, of the token the file still holds, is
    // `ready` again, since the state the refresh last recorded was not.
    assert_eq!(run.pass(FakeGrantToken::Issue(1)).await, Some(WAIT));
    assert_eq!(run.token(), held);
    assert_eq!(
        run.refresh_records().last().cloned(),
        Some(attributes(&serde_json::json!({
            "um.grant_token.provider": "example",
            "um.grant_token.phase": "refresh",
            "um.grant_token.state": "ready",
            "um.grant_token.generation": 1,
            "um.assignment.id": ASSIGNMENT,
        })))
    );
    assert_eq!(run.pass(UNANSWERED).await, Some(Duration::from_secs(5)));
}

// REQ-GRANT-89
#[tokio::test]
async fn once_a_configured_providers_token_expires_unrefreshed_its_file_is_removed() {
    // The token expires 70 seconds after the start: the refresh at 60
    // seconds and the retry at 65 keep it, the retry at 75 removes it.
    let mut run = Refreshing::example(time::Duration::seconds(70));

    assert_eq!(run.pass(UNANSWERED).await, Some(Duration::from_secs(5)));
    assert_eq!(run.pass(UNANSWERED).await, Some(Duration::from_secs(10)));
    assert!(run.file().exists());

    assert_eq!(run.pass(UNANSWERED).await, Some(Duration::from_secs(20)));

    assert!(!run.file().exists());
    // The asking goes on, and keeps the token of the held generation again.
    assert_eq!(run.pass(FakeGrantToken::Issue(1)).await, Some(WAIT));
    assert_eq!(run.token(), fake_grant_token(configured("example"), 1));
}

// REQ-GRANT-89, REQ-TELEMETRY-46
#[tokio::test]
async fn a_refusal_removes_a_configured_providers_file_and_ends_its_asking_alone() {
    let mut run = Refreshing::listing(
        time::Duration::days(1),
        configured("example"),
        &[GITHUB_ENTRY, EXAMPLE_ENTRY],
    );
    let revoked = FakeGrantToken::Fail(GrantTokenFailure::Refused(GrantTokenRefusal::Revoked));

    assert_eq!(run.pass(revoked).await, Some(WAIT));

    assert!(!run.file().exists());
    assert_eq!(
        run.refresh_records(),
        [attributes(&serde_json::json!({
            "um.grant_token.provider": "example",
            "um.grant_token.phase": "refresh",
            "um.grant_token.state": "revoked",
            "um.assignment.id": ASSIGNMENT,
        }))]
    );
    // `github` is still asked for when its wait passes, and `example` no
    // more.
    run.fixture
        .cloud
        .answer_grant_token(AgentLoginProvider::Github, FakeGrantToken::Issue(2));
    assert_eq!(run.pass(revoked).await, Some(WAIT));
    assert_eq!(
        fs::read_to_string(run.fixture.token_file(AgentLoginProvider::Github)).unwrap(),
        fake_grant_token(AgentLoginProvider::Github, 2)
    );
    assert_eq!(
        run.refresh_requests()
            .iter()
            .map(|request| request.provider.cloud_id())
            .collect::<Vec<_>>(),
        ["github", "example", "github"]
    );
}

// REQ-GRANT-89, REQ-GRANT-88
#[tokio::test]
async fn no_refresh_of_a_configured_provider_writes_or_publishes_the_readiness_document() {
    let mut run = Refreshing::example(time::Duration::days(1));
    let readiness = readiness_path(&run.fixture.state_path());
    let admitted = fs::read(&readiness).unwrap();

    assert_eq!(run.pass(FakeGrantToken::Issue(2)).await, Some(WAIT));
    assert_eq!(run.pass(UNANSWERED).await, Some(Duration::from_secs(5)));
    assert_eq!(
        run.pass(FakeGrantToken::Fail(GrantTokenFailure::Refused(
            GrantTokenRefusal::Revoked
        )))
        .await,
        None
    );

    assert_eq!(run.refresh_records().len(), 3);
    assert_eq!(fs::read(&readiness).unwrap(), admitted);
    assert!(run.published.lock().unwrap().is_empty());
}

// REQ-GRANT-89, INV-RUNNER-28
#[tokio::test]
async fn when_the_assignment_ends_a_configured_provider_is_asked_no_more() {
    let mut run = Refreshing::example(time::Duration::days(1));
    assert_eq!(run.wait().await, Some(WAIT));

    drop(run.tokens.take());

    let (_, release) = run.pending.take().unwrap();
    release.release();
    assert!(run.wait().await.is_none());
    assert_eq!(run.refresh_requests(), []);
}

/// Drops `tokens` on a thread outside any runtime, so that the end of its
/// assignment is made before this returns.
fn end_hold(tokens: AssignmentGrantTokens) {
    std::thread::spawn(move || drop(tokens)).join().unwrap();
}

// REQ-GRANT-86, REQ-GRANT-88, INV-RUNNER-28, INV-RUNNER-30
#[tokio::test]
async fn each_held_assignments_tokens_are_its_own_until_it_ends() {
    // Assignment A holds GitHub's token of generation 1 and keeps it current.
    let mut run = Refreshing::started();
    run.fixture
        .cloud
        .answer_grant_token(AgentLoginProvider::Github, FakeGrantToken::Issue(2));
    // Assignment B, of another run, is admitted while A is held.
    let next = run.fixture.admit_of(NEXT_RUN, NEXT_ASSIGNMENT);
    let assignments = LoginDirectory::beside(&run.fixture.state_path())
        .path()
        .join("assignments");
    let (own, other) = (
        assignments.join(ASSIGNMENT),
        assignments.join(NEXT_ASSIGNMENT),
    );
    let (own_file, other_file) = (own.join("github.token"), other.join("github.token"));
    let token = |generation| fake_grant_token(AgentLoginProvider::Github, generation);

    assert_eq!(fs::read_to_string(&own_file).unwrap(), token(1));
    assert_eq!(fs::read_to_string(&other_file).unwrap(), token(2));
    for (directory, file) in [(&own, &own_file), (&other, &other_file)] {
        assert_eq!((mode(directory), mode(file)), (0o700, 0o600));
    }

    // A's refresh replaces A's file alone and records no entry: the entries
    // are B's admission's.
    assert_eq!(run.pass(FakeGrantToken::Issue(3)).await, Some(WAIT));
    assert_eq!(fs::read_to_string(&own_file).unwrap(), token(3));
    assert_eq!(fs::read_to_string(&other_file).unwrap(), token(2));
    let github = run.github();
    assert_eq!(
        (
            github.run_id.as_deref(),
            github.issued.map(|issued| issued.generation),
            github.ended_at
        ),
        (Some(NEXT_RUN), Some(2), None)
    );

    // A's end removes A's directory alone and marks no entry ended.
    end_hold(run.tokens.take().unwrap());
    assert!(fs::symlink_metadata(&own).is_err());
    assert_eq!(fs::read_to_string(&other_file).unwrap(), token(2));
    for entry in run.fixture.grants() {
        assert_eq!(
            (entry.run_id.as_deref(), entry.ended_at),
            (Some(NEXT_RUN), None)
        );
    }

    // B's end removes B's and marks the entries ended.
    end_hold(next.tokens);
    assert!(fs::symlink_metadata(&other).is_err());
    for entry in run.fixture.grants() {
        assert_eq!(
            (entry.run_id.as_deref(), entry.ended_at),
            (Some(NEXT_RUN), Some(now()))
        );
    }
}

// REQ-INTEGRATION-12, REQ-GRANT-89, REQ-TELEMETRY-46
#[tokio::test]
async fn a_token_services_file_is_replaced_by_a_newer_version_and_removed_at_a_refusal() {
    let mut run = Refreshing::mergify(time::Duration::days(1));
    let file = run.fixture.configured_file("mergify");
    assert_eq!(run.file(), file);
    assert_eq!(run.token(), fake_grant_token(token_service("mergify"), 1));

    assert_eq!(
        run.pass(FakeGrantToken::IssueIntegrationToken(2)).await,
        Some(WAIT)
    );

    assert_eq!(
        run.refresh_requests(),
        [GrantTokenRequest {
            boot_id: BOOT.to_owned(),
            assignment_id: ASSIGNMENT.to_owned(),
            provider: token_service("mergify"),
        }]
    );
    assert_eq!(
        fs::read_to_string(&file).unwrap(),
        fake_grant_token(token_service("mergify"), 2)
    );
    assert_eq!(mode(&file), 0o600);
    assert_eq!(
        run.refresh_records(),
        [attributes(&serde_json::json!({
            "um.grant_token.provider": "mergify",
            "um.grant_token.phase": "refresh",
            "um.grant_token.state": "ready",
            "um.grant_token.generation": 2,
            "um.assignment.id": ASSIGNMENT,
        }))]
    );

    for (refusal, state) in [
        (GrantTokenRefusal::Revoked, "revoked"),
        (GrantTokenRefusal::Unauthorized, "not_authorized"),
    ] {
        let mut run = Refreshing::mergify(time::Duration::days(1));

        // Its asking ends with the one token it held.
        assert_eq!(
            run.pass(FakeGrantToken::Fail(GrantTokenFailure::Refused(refusal)))
                .await,
            None,
            "{refusal:?}"
        );

        assert!(!run.file().exists(), "{refusal:?}");
        assert_eq!(
            run.refresh_records(),
            [attributes(&serde_json::json!({
                "um.grant_token.provider": "mergify",
                "um.grant_token.phase": "refresh",
                "um.grant_token.state": state,
                "um.assignment.id": ASSIGNMENT,
            }))],
            "{refusal:?}"
        );
    }
}

/// Another integration token Cloud issues a token service's tokens from: one
/// revoked and stored again, say.
const OTHER_INTEGRATION_TOKEN_ID: &str = "itk_01k0z6r1w8f4jy2m7q9v3x5abd";
/// Another grant Cloud issues `github`'s tokens from: one revoked and
/// created again, say.
const OTHER_GRANT_ID: &str = "pgr_01k0z6r1w8f4jy2m7q9v3x5abd";

// REQ-GRANT-89, REQ-INTEGRATION-12
#[tokio::test]
async fn each_answer_for_the_held_token_renews_the_expiry_its_file_stands_until() {
    // Admitted at version 1 with a token that expires an hour ahead.
    let mut run = Refreshing::mergify(time::Duration::hours(1));
    let held = fake_grant_token(token_service("mergify"), 1);
    let unavailable = FakeGrantToken::Fail(GrantTokenFailure::Cloud(CloudFailure::Status(503)));
    // The refresh after the wait is answered version 1 again, which expires
    // an hour after that answer and names a wait that ends 10 seconds after
    // the first expiry.
    let answered = run.started + WAIT;
    run.fixture.cloud.issue_grant_tokens_with(
        Duration::from_secs(3550),
        answered + time::Duration::hours(1),
    );

    assert_eq!(
        run.pass(FakeGrantToken::IssueIntegrationToken(1)).await,
        Some(Duration::from_secs(3550))
    );
    assert_eq!(run.refresh_records(), []);

    // The first expiry has passed when the next refresh is not answered,
    // and the file still holds the token.
    assert_eq!(run.pass(unavailable).await, Some(Duration::from_secs(5)));
    assert_eq!(run.token(), held);
    // So it does until the hour from the last answer has passed unanswered:
    // the retries 5, 15 and 35 seconds later keep it, the one 75 seconds
    // later removes it.
    assert_eq!(run.pass(unavailable).await, Some(Duration::from_secs(10)));
    assert_eq!(run.pass(unavailable).await, Some(Duration::from_secs(20)));
    assert_eq!(run.pass(unavailable).await, Some(Duration::from_secs(40)));
    assert_eq!(run.token(), held);

    assert_eq!(run.pass(unavailable).await, Some(Duration::from_secs(80)));

    assert!(!run.file().exists());
}

/// The inode of `path`, which a file written again in one step changes.
fn inode(path: &Path) -> u64 {
    std::os::unix::fs::MetadataExt::ino(&fs::metadata(path).unwrap())
}

// REQ-GRANT-89
#[tokio::test]
async fn an_answer_from_another_grant_or_token_replaces_the_file_whatever_its_generation() {
    let github = GrantProvider::try_from(AgentLoginProvider::Github).unwrap();
    // The generation held is a later one, or the one the other grant or
    // token answers at: a token stored again starts at version 1.
    for (provider, held, other) in [
        (github.clone(), 3, OTHER_GRANT_ID),
        (github, 1, OTHER_GRANT_ID),
        (token_service("mergify"), 3, OTHER_INTEGRATION_TOKEN_ID),
        (token_service("mergify"), 1, OTHER_INTEGRATION_TOKEN_ID),
    ] {
        let issued = if provider.agent_login_provider().is_some() {
            FakeGrantToken::Issue(held)
        } else {
            FakeGrantToken::IssueIntegrationToken(held)
        };
        let mut run = Refreshing::admitted(
            time::Duration::days(1),
            provider.clone(),
            issued,
            |fixture| {
                if provider.agent_login_provider().is_none() {
                    fixture.list_token_services(&[MERGIFY_ENTRY]);
                }
            },
        );
        let file = run.file();
        assert_eq!(run.token(), fake_grant_token(provider.clone(), held));
        let kept = inode(&file);
        let expires_at = run.started + time::Duration::days(2);
        run.fixture.cloud.issue_grant_tokens_with(WAIT, expires_at);

        // Generation 1 of another grant or token than the one held.
        assert_eq!(
            run.pass(FakeGrantToken::IssueFrom(other, 1)).await,
            Some(WAIT),
            "{provider:?} {held}"
        );

        assert_eq!(run.file(), file, "{provider:?} {held}");
        assert_ne!(inode(&file), kept, "{provider:?} {held}");
        assert_eq!(
            run.token(),
            fake_grant_token(provider.clone(), 1),
            "{provider:?} {held}"
        );
        assert_eq!(mode(&file), 0o600, "{provider:?} {held}");
        assert_eq!(
            run.refresh_records(),
            [attributes(&serde_json::json!({
                "um.grant_token.provider": provider.cloud_id(),
                "um.grant_token.phase": "refresh",
                "um.grant_token.state": "ready",
                "um.grant_token.generation": 1,
                "um.assignment.id": ASSIGNMENT,
            }))],
            "{provider:?} {held}"
        );
        if provider.agent_login_provider().is_some() {
            assert_eq!(
                run.github(),
                GrantReadiness {
                    issued: Some(IssuedGrant {
                        grant_id: OTHER_GRANT_ID.to_owned(),
                        generation: 1,
                        expires_at,
                    }),
                    ..ready(1, expires_at)
                },
                "{held}"
            );
        }

        // An answer of the grant or token now held, at the generation held,
        // still writes no file and no record.
        let written = inode(&file);
        let recorded = run.fixture.recorded();
        assert_eq!(
            run.pass(FakeGrantToken::IssueFrom(other, 1)).await,
            Some(WAIT),
            "{provider:?}"
        );
        assert_eq!(inode(&file), written, "{provider:?}");
        assert_eq!(run.refresh_records().len(), 1, "{provider:?}");
        assert_eq!(run.fixture.recorded(), recorded, "{provider:?}");
    }
}
