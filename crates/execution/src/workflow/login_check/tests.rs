// INV-HARNESS-18, REQ-AGENT-45: a login check launches the coding agent's program through
// its adapter, in an empty directory of its own that it removes, and answers by the step
// runtime's judgment. The scripted stand-ins record what the program received.
use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::future::Future;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

use rustix::process::Pid;
use serde_json::{Value, json};

use super::*;
use crate::claude_code::{
    CLAUDE_CODE_STREAM_JSON_V1_QUALIFICATION_VERSION, ValidatedClaudeCodeInstallation,
};
use crate::codex::{CODEX_APP_SERVER_V1_QUALIFICATION_VERSION, ValidatedCodexInstallation};
use crate::workflow::claude_code::test_support::FixtureSignal;
use crate::workflow::codex::adapter_tests::FAKE_CODEX;
use crate::workflow::process_group::process_group_is_quiescent;
use crate::workflow::test_support::{ControlledClock, host_scaled_watchdog};

/// The bound each case gives; the controlled clock turns it into a registered deadline.
const BOUND: Duration = Duration::from_millis(4_321);
const CLAUDE_CODE_TOKEN: &str = "login-check-oauth-token";
const WATCHDOG_BASE: Duration = Duration::from_secs(20);

/// Records the program's process, working directory, its entries and environment, then
/// hands over to the harness part of the stand-in.
const RECORDING_PRELUDE: &str = r#"#!/bin/sh
set -eu
printf '%s\n' "$$" > "$LOGIN_CHECK_PROCESS"
pwd -P > "$LOGIN_CHECK_CWD"
ls -A > "$LOGIN_CHECK_LISTING"
env > "$LOGIN_CHECK_ENVIRONMENT"
"#;

/// A `claude` that answers the adapter's arguments and first frame with the scripted
/// frames, its session, model and directory filled in, and then exits or hangs.
const CLAUDE_CODE_STAND_IN: &str = r#"model=
session=
previous=
for argument in "$@"; do
  case "$previous" in
    --model) model=$argument ;;
    --session-id) session=$argument ;;
  esac
  previous=$argument
done
IFS= read -r _initial
sed -e "s|@SESSION@|$session|g" -e "s|@MODEL@|$model|g" -e "s|@CWD@|$PWD|g" "$LOGIN_CHECK_FRAMES"
if [ -n "${LOGIN_CHECK_HANG-}" ]; then
  trap '' INT TERM
  printf 'ready\n' > "$LOGIN_CHECK_READY"
  while :; do sleep 60; done
fi
"#;

/// Hands over to the Codex adapter tests' App Server process fixture.
const CODEX_STAND_IN: &str = r#"exec "$LOGIN_CHECK_PROGRAM" "$@"
"#;

struct Observed {
    process: Pid,
    cwd: PathBuf,
    listing: String,
    environment: String,
}

struct StandIn {
    temporary: tempfile::TempDir,
    controls: PathBuf,
    ready: FixtureSignal,
    environment: BTreeMap<OsString, OsString>,
}

impl StandIn {
    fn new() -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(temporary.path()).unwrap();
        let controls = root.join("controls");
        let home = root.join("home");
        for directory in [&controls, &home] {
            std::fs::create_dir(directory).unwrap();
        }
        let ready = FixtureSignal::create(controls.join("ready"));
        let mut environment = BTreeMap::from([
            (
                OsString::from("PATH"),
                std::env::var_os("PATH").unwrap_or_else(|| OsString::from("/usr/bin:/bin")),
            ),
            (OsString::from("HOME"), home.into_os_string()),
            (
                OsString::from("LOGIN_CHECK_READY"),
                ready.path().as_os_str().to_owned(),
            ),
        ]);
        for (variable, file) in [
            ("LOGIN_CHECK_PROCESS", "process"),
            ("LOGIN_CHECK_CWD", "cwd"),
            ("LOGIN_CHECK_LISTING", "listing"),
            ("LOGIN_CHECK_ENVIRONMENT", "environment"),
        ] {
            environment.insert(
                OsString::from(variable),
                controls.join(file).into_os_string(),
            );
        }
        Self {
            temporary,
            controls,
            ready,
            environment,
        }
    }

    fn root(&self) -> PathBuf {
        std::fs::canonicalize(self.temporary.path()).unwrap()
    }

    fn set(&mut self, name: &str, value: impl Into<OsString>) {
        self.environment.insert(OsString::from(name), value.into());
    }

    fn install(&self, name: &str, script: &str) -> PathBuf {
        let executable = self.root().join(name);
        std::fs::write(&executable, script).unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
        executable
    }

    fn snapshot(&self) -> EnvironmentSnapshot {
        EnvironmentSnapshot::new(self.environment.clone())
    }

    fn started(&self) -> bool {
        self.controls.join("process").exists()
    }

    fn observed(&self) -> Observed {
        let read = |file: &str| std::fs::read_to_string(self.controls.join(file)).unwrap();
        Observed {
            process: Pid::from_raw(read("process").trim().parse().unwrap()).unwrap(),
            cwd: PathBuf::from(read("cwd").trim_end()),
            listing: read("listing"),
            environment: read("environment"),
        }
    }

    /// The program received the caller's login variable, ran in an empty directory that
    /// is gone now, and left no process of its group behind.
    fn assert_contained_launch(&self, variable: &str, value: &OsStr) {
        let observed = self.observed();
        let expected = format!("{variable}={}", value.to_str().unwrap());
        assert!(
            observed.environment.lines().any(|line| line == expected),
            "the program did not receive {variable}"
        );
        assert_eq!(observed.listing, "", "the working directory was not empty");
        assert!(observed.cwd.is_absolute());
        assert!(!observed.cwd.exists(), "the working directory remains");
        assert!(process_group_is_quiescent(observed.process));
    }
}

fn with_watchdog<Output>(
    case: &'static str,
    future: impl Future<Output = Output>,
) -> impl Future<Output = Output> {
    host_scaled_watchdog(case, WATCHDOG_BASE, future)
}

fn claude_init() -> Value {
    json!({
        "type": "system",
        "subtype": "init",
        "cwd": "@CWD@",
        "session_id": "@SESSION@",
        "model": "@MODEL@",
        "permissionMode": "bypassPermissions",
        "claude_code_version": CLAUDE_CODE_STREAM_JSON_V1_QUALIFICATION_VERSION,
    })
}

fn claude_completed_turn() -> Vec<Value> {
    vec![
        claude_init(),
        json!({
            "type": "result",
            "subtype": "success",
            "is_error": false,
            "terminal_reason": "completed",
            "result": "OK",
            "session_id": "@SESSION@",
        }),
    ]
}

/// Claude Code's API-error frame and its `result`, as the provider's refusal arrives.
fn claude_api_error(code: &str, status: u16) -> Vec<Value> {
    vec![
        claude_init(),
        json!({
            "type": "assistant",
            "message": {
                "id": "msg-login-check",
                "model": "<synthetic>",
                "role": "assistant",
                "type": "message",
                "content": [{"type": "text", "text": "provider text"}],
            },
            "parent_tool_use_id": null,
            "session_id": "@SESSION@",
            "error": code,
            "api_error_status": status,
            "is_api_error_message": true,
        }),
        json!({
            "type": "result",
            "subtype": "success",
            "is_error": true,
            "terminal_reason": "api_error",
            "result": "provider text",
            "session_id": "@SESSION@",
        }),
    ]
}

fn claude_stand_in(frames: &[Value], hang: bool) -> (StandIn, ValidatedClaudeCodeInstallation) {
    let mut stand_in = StandIn::new();
    let config = stand_in.root().join("claude-config");
    std::fs::create_dir(&config).unwrap();
    let frames_path = stand_in.controls.join("frames.jsonl");
    let mut transcript = String::new();
    for frame in frames {
        transcript.push_str(&serde_json::to_string(frame).unwrap());
        transcript.push('\n');
    }
    std::fs::write(&frames_path, transcript).unwrap();
    stand_in.set("CLAUDE_CONFIG_DIR", config);
    stand_in.set("LOGIN_CHECK_FRAMES", frames_path);
    stand_in.set(CLAUDE_CODE_LOGIN_VARIABLE, CLAUDE_CODE_TOKEN);
    if hang {
        stand_in.set("LOGIN_CHECK_HANG", "1");
    }
    let executable = stand_in.install(
        "claude",
        &format!("{RECORDING_PRELUDE}{CLAUDE_CODE_STAND_IN}"),
    );
    (
        stand_in,
        ValidatedClaudeCodeInstallation::fixture(executable),
    )
}

fn codex_stand_in(scenario: &str) -> (StandIn, ValidatedCodexInstallation, PathBuf) {
    let mut stand_in = StandIn::new();
    let root = stand_in.root();
    let codex_home = root.join("codex-home");
    let native_temporary = root.join("native-tmp");
    for directory in [&codex_home, &native_temporary] {
        std::fs::create_dir(directory).unwrap();
    }
    let program = stand_in.install("codex-app-server-fixture", FAKE_CODEX);
    stand_in.set("LOGIN_CHECK_PROGRAM", program);
    stand_in.set("TMPDIR", native_temporary);
    stand_in.set(CODEX_LOGIN_VARIABLE, codex_home.clone());
    stand_in.set("CODEX_FIXTURE_HELPER", std::env::current_exe().unwrap());
    stand_in.set("CODEX_FIXTURE_SCENARIO", scenario);
    stand_in.set(
        "CODEX_FIXTURE_VERSION",
        CODEX_APP_SERVER_V1_QUALIFICATION_VERSION,
    );
    stand_in.set("CODEX_FIXTURE_RESPONSE", "OK");
    stand_in.set("CODEX_FIXTURE_READY", stand_in.ready.path().to_owned());
    for (variable, file) in [
        ("CODEX_FIXTURE_ARGUMENTS", "arguments"),
        ("CODEX_FIXTURE_REQUESTS", "requests.jsonl"),
        ("CODEX_FIXTURE_PROCESS", "fixture-process"),
        ("CODEX_FIXTURE_DESCENDANT", "descendant"),
    ] {
        let path = stand_in.controls.join(file);
        stand_in.set(variable, path);
    }
    let executable = stand_in.install("codex", &format!("{RECORDING_PRELUDE}{CODEX_STAND_IN}"));
    (
        stand_in,
        ValidatedCodexInstallation::fixture(executable),
        codex_home,
    )
}

async fn check_claude_code(
    stand_in: &StandIn,
    installation: &ValidatedClaudeCodeInstallation,
) -> HarnessLoginCheck {
    let (clock, _control) = ControlledClock::new();
    check_claude_code_login(installation, stand_in.snapshot(), BOUND, clock).await
}

async fn check_codex(
    stand_in: &StandIn,
    installation: &ValidatedCodexInstallation,
) -> HarnessLoginCheck {
    let (clock, _control) = ControlledClock::new();
    check_codex_login(
        installation,
        stand_in.snapshot(),
        BOUND,
        clock,
        "0.0.0-test",
    )
    .await
}

#[tokio::test]
async fn a_claude_code_turn_that_completes_is_accepted() {
    with_watchdog("claude code accepted", async {
        let (stand_in, installation) = claude_stand_in(&claude_completed_turn(), false);
        let check = check_claude_code(&stand_in, &installation).await;
        assert_eq!(check, HarnessLoginCheck::Accepted);
        stand_in.assert_contained_launch(CLAUDE_CODE_LOGIN_VARIABLE, CLAUDE_CODE_TOKEN.as_ref());
        // The check leaves the service's Claude Code configuration directory
        // as it found it (REQ-GRANT-37, REQ-GRANT-90).
        let config = stand_in.root().join("claude-config");
        assert_eq!(std::fs::read_dir(config).unwrap().count(), 0);
    })
    .await;
}

#[tokio::test]
async fn a_claude_code_authentication_failure_is_refused() {
    with_watchdog("claude code refused", async {
        let (stand_in, installation) =
            claude_stand_in(&claude_api_error("authentication_failed", 401), false);
        let check = check_claude_code(&stand_in, &installation).await;
        assert_eq!(check, HarnessLoginCheck::Refused);
        stand_in.assert_contained_launch(CLAUDE_CODE_LOGIN_VARIABLE, CLAUDE_CODE_TOKEN.as_ref());
    })
    .await;
}

#[tokio::test]
async fn a_claude_code_overload_gives_no_answer_with_its_harness_error() {
    with_watchdog("claude code overloaded", async {
        let (stand_in, installation) = claude_stand_in(&claude_api_error("overloaded", 529), false);
        let check = check_claude_code(&stand_in, &installation).await;
        assert_eq!(
            check,
            HarnessLoginCheck::NoAnswer(HarnessLoginCheckCause::HarnessError {
                class: "overloaded"
            })
        );
        stand_in.assert_contained_launch(CLAUDE_CODE_LOGIN_VARIABLE, CLAUDE_CODE_TOKEN.as_ref());
    })
    .await;
}

#[tokio::test]
async fn a_claude_code_turn_that_never_ends_times_out_at_the_bound_and_is_stopped() {
    with_watchdog("claude code timed out", async {
        let (stand_in, installation) = claude_stand_in(&[claude_init()], true);
        let (clock, control) = ControlledClock::new();
        let check = check_claude_code_login(&installation, stand_in.snapshot(), BOUND, clock);
        let check = assert_times_out_at_the_bound(&stand_in, check, control).await;
        assert_eq!(
            check,
            HarnessLoginCheck::NoAnswer(HarnessLoginCheckCause::TimedOut)
        );
        stand_in.assert_contained_launch(CLAUDE_CODE_LOGIN_VARIABLE, CLAUDE_CODE_TOKEN.as_ref());
    })
    .await;
}

#[tokio::test]
async fn a_claude_code_program_that_cannot_start_gives_no_answer_as_a_start_failure() {
    with_watchdog("claude code start failure", async {
        let (stand_in, _) = claude_stand_in(&claude_completed_turn(), false);
        let missing = ValidatedClaudeCodeInstallation::fixture(stand_in.root().join("missing"));
        let check = check_claude_code(&stand_in, &missing).await;
        assert!(
            matches!(
                check,
                HarnessLoginCheck::NoAnswer(HarnessLoginCheckCause::StartFailed { .. })
            ),
            "{check:?}"
        );
        assert!(!stand_in.started());
    })
    .await;
}

#[tokio::test]
async fn a_check_without_the_login_variable_starts_nothing() {
    with_watchdog("claude code without its login", async {
        let (mut stand_in, installation) = claude_stand_in(&claude_completed_turn(), false);
        stand_in
            .environment
            .remove(OsStr::new(CLAUDE_CODE_LOGIN_VARIABLE));
        let check = check_claude_code(&stand_in, &installation).await;
        assert_eq!(
            check,
            HarnessLoginCheck::NoAnswer(HarnessLoginCheckCause::Preparation {
                stage: HarnessLoginCheckStage::LoginVariable,
                error: std::io::ErrorKind::NotFound,
            })
        );
        assert!(!stand_in.started());
    })
    .await;
}

#[tokio::test]
async fn a_codex_turn_that_completes_is_accepted() {
    with_watchdog("codex accepted", async {
        let (stand_in, installation, home) = codex_stand_in("login-check-completed");
        let check = check_codex(&stand_in, &installation).await;
        assert_eq!(check, HarnessLoginCheck::Accepted);
        stand_in.assert_contained_launch(CODEX_LOGIN_VARIABLE, home.as_os_str());
    })
    .await;
}

#[tokio::test]
async fn a_codex_unauthorized_turn_is_refused() {
    with_watchdog("codex refused", async {
        let (stand_in, installation, home) = codex_stand_in("failure-after-start-authentication");
        let check = check_codex(&stand_in, &installation).await;
        assert_eq!(check, HarnessLoginCheck::Refused);
        stand_in.assert_contained_launch(CODEX_LOGIN_VARIABLE, home.as_os_str());
    })
    .await;
}

#[tokio::test]
async fn a_codex_overload_gives_no_answer_with_its_harness_error() {
    with_watchdog("codex overloaded", async {
        let (stand_in, installation, home) = codex_stand_in("failure-after-start-overloaded");
        let check = check_codex(&stand_in, &installation).await;
        assert_eq!(
            check,
            HarnessLoginCheck::NoAnswer(HarnessLoginCheckCause::HarnessError {
                class: "overloaded"
            })
        );
        stand_in.assert_contained_launch(CODEX_LOGIN_VARIABLE, home.as_os_str());
    })
    .await;
}

#[tokio::test]
async fn a_codex_turn_that_never_ends_times_out_at_the_bound_and_is_stopped() {
    with_watchdog("codex timed out", async {
        let (stand_in, installation, home) = codex_stand_in("cancellation-stubborn");
        let (clock, control) = ControlledClock::new();
        let check = check_codex_login(
            &installation,
            stand_in.snapshot(),
            BOUND,
            clock,
            "0.0.0-test",
        );
        let check = assert_times_out_at_the_bound(&stand_in, check, control).await;
        assert_eq!(
            check,
            HarnessLoginCheck::NoAnswer(HarnessLoginCheckCause::TimedOut)
        );
        stand_in.assert_contained_launch(CODEX_LOGIN_VARIABLE, home.as_os_str());
        let descendant = stand_in.controls.join("descendant");
        if let Ok(descendant) = std::fs::read_to_string(descendant) {
            let descendant = Pid::from_raw(descendant.trim().parse().unwrap()).unwrap();
            assert!(process_group_is_quiescent(descendant));
        }
    })
    .await;
}

#[tokio::test]
async fn a_codex_program_that_cannot_start_gives_no_answer_as_a_start_failure() {
    with_watchdog("codex start failure", async {
        let (stand_in, _, _) = codex_stand_in("login-check-completed");
        let missing = ValidatedCodexInstallation::fixture(stand_in.root().join("missing"));
        let check = check_codex(&stand_in, &missing).await;
        assert!(
            matches!(
                check,
                HarnessLoginCheck::NoAnswer(HarnessLoginCheckCause::StartFailed { .. })
            ),
            "{check:?}"
        );
        assert!(!stand_in.started());
    })
    .await;
}

/// Drives a check whose program never ends: once the program is mid-turn, the bound the
/// check armed must be the one given, and expiring it stops the program.
async fn assert_times_out_at_the_bound(
    stand_in: &StandIn,
    check: impl Future<Output = HarnessLoginCheck>,
    mut control: crate::workflow::test_support::ClockControl,
) -> HarnessLoginCheck {
    let ready = stand_in.ready.clone();
    let release = async move {
        assert_eq!(ready.receive().await, b"ready\n");
        let mut armed = Vec::new();
        while let Ok(deadline) = control.deadlines.try_recv() {
            armed.push(deadline);
        }
        assert!(armed.contains(&BOUND), "armed deadlines: {armed:?}");
        control.expired.send(true).unwrap();
        control
    };
    let (check, _control) = tokio::join!(check, release);
    check
}

#[test]
fn the_working_directory_lives_under_the_system_temporary_directory() {
    let directory = CheckDirectory::create().unwrap();
    let path = directory.working_directory_path();
    assert!(path.starts_with(std::fs::canonicalize(std::env::temp_dir()).unwrap()));
    let metadata = std::fs::metadata(&path).unwrap();
    assert_eq!(metadata.permissions().mode() & 0o777, 0o700);
    assert_eq!(std::fs::read_dir(&path).unwrap().count(), 0);
    directory.remove();
    assert!(!Path::new(&path).exists());
}
