use std::os::unix::fs::{DirBuilderExt as _, OpenOptionsExt as _};

use super::*;
use crate::agent_login::claude_code::TOKEN_VARIABLE;
use crate::agent_login::codex::HOME_VARIABLE as CODEX_HOME;
use crate::agent_login::directory::LoginDirectory;

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
fn keep_token(manager: &AssignmentManager) -> PathBuf {
    let directory = login_directory(manager);
    create_private_directory(directory.path());
    let path = directory.claude_code_token();
    write_private(&path, DELIVERED.as_bytes());
    path
}

/// Keeps a Codex login in the runner's Codex home, as setup leaves it.
fn keep_codex_login(manager: &AssignmentManager) -> PathBuf {
    let directory = login_directory(manager);
    create_private_directory(directory.path());
    create_private_directory(&directory.codex_home());
    let path = directory.codex_home().join("auth.json");
    write_private(&path, b"{\"tokens\":{}}");
    path
}

fn accepted_variable(manager: &AssignmentManager, name: &str) -> Option<OsString> {
    match &manager.slot {
        Some(LocalSlot::Accepted(accepted)) => accepted
            .admitted
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

// REQ-AGENTLOGIN-34, REQ-WFEXEC-71
#[tokio::test]
async fn a_kept_token_reaches_only_the_steps_of_a_workflow_that_passes_it() {
    let (_temporary, mut manager) = manager_without_agent_logins(PASSING_WORKFLOW);
    keep_token(&manager);
    let offered = offer("bg");

    offer_then_prepare(&mut manager, &offered).await;

    assert_eq!(accepted_token(&manager), Some(OsString::from(DELIVERED)));
    assert_eq!(
        manager.environment.variable(OsStr::new(TOKEN_VARIABLE)),
        None
    );
    assert_succeeded(&execute_to_terminal(&mut manager, &offered).await);
}

// REQ-AGENTLOGIN-34, REQ-WFEXEC-71
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
    keep_token(&manager);
    let offered = offer("bg");

    offer_then_prepare(&mut manager, &offered).await;

    assert_eq!(accepted_token(&manager), None);
    assert_succeeded(&execute_to_terminal(&mut manager, &offered).await);
}

// REQ-AGENTLOGIN-34, REQ-AGENTLOGIN-35
#[tokio::test]
async fn a_token_in_the_process_environment_passes_through_unchanged() {
    let workflow = PASSING_WORKFLOW.replace(DELIVERED, "service-token");
    let (_temporary, mut manager) = manager_without_agent_logins(&workflow);
    keep_token(&manager);
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

// REQ-AGENTLOGIN-34, REQ-AGENTLOGIN-35
#[tokio::test]
async fn each_admission_reads_the_token_the_runner_keeps_then() {
    let (_temporary, mut manager) = manager_without_agent_logins(PASSING_WORKFLOW);
    let path = keep_token(&manager);
    let first = offer("bg");
    offer_then_prepare(&mut manager, &first).await;
    assert_eq!(accepted_token(&manager), Some(OsString::from(DELIVERED)));
    release_unrun(&mut manager, &first).await;

    fs::remove_file(&path).unwrap();
    let next = offer("bh");
    offer_then_prepare(&mut manager, &next).await;

    assert_eq!(accepted_token(&manager), None);
}

// REQ-AGENTLOGIN-27, REQ-WFEXEC-71
#[tokio::test]
async fn a_kept_codex_login_reaches_only_the_steps_of_a_workflow_that_passes_codex_home() {
    let (_temporary, mut manager) = manager_without_agent_logins(PASSING_CODEX_WORKFLOW);
    keep_codex_login(&manager);
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

// REQ-AGENTLOGIN-27, REQ-WFEXEC-71
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
    keep_codex_login(&manager);
    let offered = offer("bg");

    offer_then_prepare(&mut manager, &offered).await;

    assert_eq!(accepted_variable(&manager, CODEX_HOME), None);
    assert_succeeded(&execute_to_terminal(&mut manager, &offered).await);
}

// REQ-AGENTLOGIN-27
#[tokio::test]
async fn a_codex_home_in_the_process_environment_passes_through_unchanged() {
    let (temporary, mut manager) = manager_without_agent_logins(PASSING_CODEX_WORKFLOW);
    keep_codex_login(&manager);
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

// REQ-AGENTLOGIN-27
#[tokio::test]
async fn each_admission_looks_for_the_codex_login_the_runner_keeps_then() {
    let (_temporary, mut manager) = manager_without_agent_logins(PASSING_CODEX_WORKFLOW);
    let path = keep_codex_login(&manager);
    let first = offer("bg");
    offer_then_prepare(&mut manager, &first).await;
    assert!(accepted_variable(&manager, CODEX_HOME).is_some());
    release_unrun(&mut manager, &first).await;

    fs::remove_file(&path).unwrap();
    let next = offer("bh");
    offer_then_prepare(&mut manager, &next).await;

    assert_eq!(accepted_variable(&manager, CODEX_HOME), None);
}
