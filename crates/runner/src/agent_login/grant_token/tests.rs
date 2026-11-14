use std::ffi::OsString;
use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use time::OffsetDateTime;

use super::*;
use crate::agent_login::ServiceEnvironment;
use crate::agent_login::check::LoginCheck;
use crate::agent_login::cloud::{
    AgentLoginCloud, CloudFailure, GrantTokenFailure, GrantTokenRefusal,
};
use crate::agent_login::readiness::{ReadinessDocument, readiness_path};
use crate::agent_login::setup::{
    AdmissionSetup, AgentLoginSetup, AssignmentGate, SetupDependencies, SetupOutcome,
};
use crate::control_protocol::{AgentLoginProvider, AgentLoginState};
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
                ServiceEnvironment::from_lookup(|name| (name == "HOME").then(|| home.clone())),
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

    fn token_file(&self, provider: AgentLoginProvider) -> PathBuf {
        LoginDirectory::beside(&self.state_path())
            .grant_token(provider)
            .unwrap()
            .0
    }

    /// Leaves a token file in the runner's directory, as an earlier
    /// assignment or an interrupted runner would.
    fn leave_token_file(&self, provider: AgentLoginProvider, token: &str) {
        let directory = LoginDirectory::beside(&self.state_path());
        crate::agent_login::directory::ensure_private_directory(directory.path()).unwrap();
        let (path, temporary_prefix) = directory.grant_token(provider).unwrap();
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
}

fn mode(path: &Path) -> u32 {
    fs::symlink_metadata(path).unwrap().permissions().mode() & 0o777
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
    requests.sort_by_key(|request| request.provider.as_str());
    assert_eq!(
        requests
            .iter()
            .map(|request| (
                request.boot_id.as_str(),
                request.assignment_id.as_str(),
                request.provider
            ))
            .collect::<Vec<_>>(),
        [
            (BOOT, ASSIGNMENT, AgentLoginProvider::Github),
            (BOOT, ASSIGNMENT, AgentLoginProvider::Gitlab),
            (BOOT, ASSIGNMENT, AgentLoginProvider::Linear),
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
                selected: true,
                run_id: Some(RUN.to_owned()),
                ended_at: None,
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
            selected: false,
            run_id: Some(RUN.to_owned()),
            ended_at: None,
        })
    );
    // What the admission recorded is what it publishes.
    assert_eq!(admitted.grants, Some(fixture.recorded()));
    assert_eq!(admitted.agent_logins, None);
}

// REQ-GRANT-86, REQ-GRANT-88
#[test]
fn each_answer_without_a_token_writes_no_file_and_records_its_state() {
    let unavailable = |failure| FakeGrantToken::Fail(GrantTokenFailure::Cloud(failure));
    let refused = |refusal| FakeGrantToken::Fail(GrantTokenFailure::Refused(refusal));
    // Each answer with the state it records and whether the provider is
    // selected after it when it was not selected before, and when it was.
    for (answer, state, selected) in [
        (
            refused(GrantTokenRefusal::Unauthorized),
            AgentLoginState::NotAuthorized,
            [false, false],
        ),
        (
            FakeGrantToken::Fail(GrantTokenFailure::NoRoute),
            AgentLoginState::NotAuthorized,
            [false, false],
        ),
        (
            refused(GrantTokenRefusal::Revoked),
            AgentLoginState::Revoked,
            [false, false],
        ),
        (
            refused(GrantTokenRefusal::ReconnectRequired),
            AgentLoginState::ReconnectRequired,
            [true, true],
        ),
        (
            refused(GrantTokenRefusal::ProviderUnavailable),
            AgentLoginState::UnsupportedTarget,
            [false, false],
        ),
        (
            refused(GrantTokenRefusal::AssignmentNotCurrent),
            AgentLoginState::CloudUnavailable,
            [false, true],
        ),
        (
            unavailable(CloudFailure::Status(503)),
            AgentLoginState::CloudUnavailable,
            [false, true],
        ),
        (
            unavailable(CloudFailure::Transport),
            AgentLoginState::CloudUnavailable,
            [false, true],
        ),
        (
            unavailable(CloudFailure::InvalidResponse),
            AgentLoginState::CloudUnavailable,
            [false, true],
        ),
    ] {
        for (before, selected) in [
            FakeGrantToken::Fail(GrantTokenFailure::NoRoute),
            FakeGrantToken::Issue(1),
        ]
        .into_iter()
        .zip(selected)
        {
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
                        selected,
                        run_id: Some(RUN.to_owned()),
                        ended_at: None,
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
        for provider in brokered_providers() {
            fixture.leave_token_file(provider, "earlier-assignment-token");
        }
        let files: Vec<PathBuf> = brokered_providers()
            .map(|provider| fixture.token_file(provider))
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
        for provider in brokered_providers() {
            let file = fixture.token_file(provider);
            match answer {
                FakeGrantToken::Issue(generation) => assert_eq!(
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
            fs::read_to_string(fixture.token_file(provider)).unwrap(),
            fake_grant_token(provider, 4)
        );
    }
    drop(later.tokens);
    for provider in brokered_providers() {
        assert!(!fixture.token_file(provider).exists());
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
    for provider in brokered_providers() {
        fixture.leave_token_file(provider, "interrupted-runner-token");
    }
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
    for provider in brokered_providers() {
        assert!(!fixture.token_file(provider).exists());
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
        not_ready(AgentLoginState::ReconnectRequired, true),
        not_ready(AgentLoginState::NotAuthorized, false),
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

// REQ-GRANT-86, REQ-TELEMETRY-46
#[test]
fn no_token_is_in_the_readiness_document_its_report_or_any_record() {
    let fixture = Fixture::new();
    fixture.answer_all(FakeGrantToken::Issue(5));

    let admitted = fixture.admit(ASSIGNMENT);

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
    }
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

    // A token that cannot be kept is a failed write of the provider, which is
    // selected all the same.
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
            .map(|grant| (grant.state, grant.selected)),
        Some((AgentLoginState::InstallFailed, true))
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

// REQ-GRANT-87, REQ-GRANT-10
#[test]
fn a_kept_token_whose_file_no_longer_holds_one_is_withheld_with_its_file() {
    let fixture = Fixture::new();
    fixture.answer_all(FakeGrantToken::Issue(3));
    let admitted = fixture.admit(ASSIGNMENT);
    let service = EnvironmentSnapshot::new([
        ("GITLAB_TOKEN", "service-gitlab-token"),
        ("GITLAB_TOKEN_FILE", "/service/gitlab-token"),
    ]);
    // The file of `gitlab` is replaced by one that is not a token's.
    fixture.leave_token_file(AgentLoginProvider::Gitlab, "two\nlines");

    // Whatever the readiness document records, or when there is none.
    for document in [None, Some(&fixture.recorded())] {
        let offered = offer(&service, Some(&admitted.tokens), document);

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
    }
}

/// The wait the fake Cloud names with each token in the refresh tests.
const WAIT: Duration = Duration::from_secs(60);

/// An assignment admitted with the `github` token of generation 1, whose
/// hold keeps it current on a controlled sleeper: the sleeps the task asks
/// for, the environment its steps were offered at admission, and each
/// readiness a refresh published. The test keeps no sleeper of its own, so
/// once the task has stopped no sleep can be asked for.
struct Refreshing {
    fixture: Fixture,
    tokens: Option<AssignmentGrantTokens>,
    offered: EnvironmentSnapshot,
    sleeps: tokio::sync::mpsc::UnboundedReceiver<(Duration, SleepRelease)>,
    /// The sleep the task waits in, once the test has seen it.
    pending: Option<(Duration, SleepRelease)>,
    published: Arc<Mutex<Vec<ReadinessDocument>>>,
}

impl Refreshing {
    /// Admits the assignment with a token that expires `lifetime` after
    /// the sleeper's start, and starts keeping it current.
    fn start(lifetime: time::Duration) -> Self {
        let fixture = Fixture::new();
        let (sleeper, sleeps) = controlled_sleeper();
        fixture
            .cloud
            .issue_grant_tokens_with(WAIT, sleeper.utc_now() + lifetime);
        fixture
            .cloud
            .answer_grant_token(AgentLoginProvider::Github, FakeGrantToken::Issue(1));
        let mut admitted = fixture.admit(ASSIGNMENT);
        let offered = offer(
            &EnvironmentSnapshot::new(Vec::<(OsString, OsString)>::new()),
            Some(&admitted.tokens),
            admitted.grants.as_ref(),
        );
        let published = Arc::new(Mutex::new(Vec::new()));
        let publishing = Arc::clone(&published);
        admitted.tokens.publish_with(Arc::new(move |document| {
            publishing.lock().unwrap().push(document.clone());
        }));
        admitted.tokens.keep_current(sleeper);
        Self {
            fixture,
            tokens: Some(admitted.tokens),
            offered,
            sleeps,
            pending: None,
            published,
        }
    }

    /// `start` with a token that outlives every test.
    fn started() -> Self {
        Self::start(time::Duration::days(1))
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
            .answer_grant_token(AgentLoginProvider::Github, answer);
        release.release();
        self.wait().await
    }

    fn file(&self) -> PathBuf {
        self.fixture.token_file(AgentLoginProvider::Github)
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
            .skip(brokered_providers().count())
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
        selected: true,
        run_id: Some(RUN.to_owned()),
        ended_at: None,
    }
}

fn not_ready(state: AgentLoginState, selected: bool) -> GrantReadiness {
    GrantReadiness {
        provider: AgentLoginProvider::Github,
        state,
        checked_at: Some(now()),
        issued: None,
        selected,
        run_id: Some(RUN.to_owned()),
        ended_at: None,
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
    let mut fill = um_test_support::fixture_git_command("git");
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
            provider: AgentLoginProvider::Github,
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
        assert_eq!(
            run.github(),
            not_ready(AgentLoginState::CloudUnavailable, true)
        );
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
    assert_eq!(
        run.github(),
        not_ready(AgentLoginState::CloudUnavailable, true)
    );
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
    for (refusal, state, selected) in [
        (
            refused(GrantTokenRefusal::Revoked),
            AgentLoginState::Revoked,
            false,
        ),
        (
            refused(GrantTokenRefusal::Unauthorized),
            AgentLoginState::NotAuthorized,
            false,
        ),
        (
            refused(GrantTokenRefusal::ReconnectRequired),
            AgentLoginState::ReconnectRequired,
            true,
        ),
        (
            refused(GrantTokenRefusal::ProviderUnavailable),
            AgentLoginState::UnsupportedTarget,
            false,
        ),
        // A Cloud that no longer has the route authorizes nothing.
        (
            GrantTokenFailure::NoRoute,
            AgentLoginState::NotAuthorized,
            false,
        ),
    ] {
        let mut run = Refreshing::started();

        let next = run.pass(FakeGrantToken::Fail(refusal)).await;

        assert_eq!(next, None, "{refusal:?}");
        assert!(!run.file().exists(), "{refusal:?}");
        assert_eq!(run.github(), not_ready(state, selected), "{refusal:?}");
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
