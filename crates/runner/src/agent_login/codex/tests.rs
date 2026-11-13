use std::fs;
use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _, symlink};
use std::path::Path;

use super::*;
use crate::agent_login::cloud::LoginMaterial;
use crate::agent_login::{FailureCause, FailureStage, StepFailure};
use crate::control_protocol::AgentLoginProvider;
use crate::control_protocol::AgentLoginState;

const AGENT_LOGIN_ID: &str = "agl_01k0z6r1w8f4jy2m7q9v3x5abc";
const OTHER_AGENT_LOGIN_ID: &str = "agl_01k0z6r1w8f4jy2m7q9v3x5abd";

fn now() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_794_445_200).unwrap()
}

fn delivery(generation: u64, material: &[u8]) -> Delivery {
    delivery_of(AGENT_LOGIN_ID, generation, material)
}

fn delivery_of(agent_login_id: &str, generation: u64, material: &[u8]) -> Delivery {
    Delivery {
        provider: AgentLoginProvider::Codex,
        agent_login_id: agent_login_id.to_owned(),
        generation,
        record_schema_version: 1,
        material: LoginMaterial::from_bytes(material),
    }
}

/// A state directory and the runner's agent-login directory beside its
/// state file.
fn state() -> (tempfile::TempDir, LoginDirectory) {
    let state = tempfile::tempdir().unwrap();
    fs::set_permissions(state.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let directory = LoginDirectory::beside(&state.path().join("runner-state.json"));
    (state, directory)
}

fn installed(
    directory: &LoginDirectory,
    recorded: Option<&Installation>,
    generation: u64,
    material: &[u8],
) -> Installation {
    install_codex(directory, recorded, &delivery(generation, material), now()).expect("an install")
}

fn mode(path: &Path) -> u32 {
    fs::symlink_metadata(path).unwrap().mode() & 0o7777
}

fn entries(directory: &Path) -> Vec<String> {
    let mut names = fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect::<Vec<_>>();
    names.sort();
    names
}

// REQ-AGENTLOGIN-26
#[test]
fn installs_the_exact_delivered_bytes_privately_in_the_runners_codex_home() {
    let (state, directory) = state();
    let material = b"{\"tokens\":{\"access_token\":\"generation-one\"}}";

    let installation = installed(&directory, None, 1, material);

    let root = state.path().join(".runner-agent-login");
    let home = root.join("codex");
    assert_eq!(directory.path(), root);
    assert_eq!(directory.codex_home(), home);
    assert_eq!(
        installation,
        Installation {
            agent_login_id: AGENT_LOGIN_ID.to_owned(),
            generation: 1,
            installed_at: now(),
            installed_path: home.join(AUTH_FILE),
        }
    );
    assert_eq!(auth_path(&directory), home.join(AUTH_FILE));
    assert_eq!(fs::read(home.join(AUTH_FILE)).unwrap(), material);
    assert_eq!(mode(&home.join(AUTH_FILE)), 0o600);
    assert_eq!(
        fs::read(home.join(CONFIG_FILE)).unwrap(),
        b"cli_auth_credentials_store = \"file\"\n"
    );
    assert_eq!(mode(&root), 0o700);
    assert_eq!(mode(&home), 0o700);
    assert_eq!(entries(&home), [AUTH_FILE, CONFIG_FILE]);
    assert_eq!(entries(state.path()), [".runner-agent-login"]);
}

// REQ-AGENTLOGIN-26
#[test]
fn refuses_an_older_generation_than_it_installed() {
    let (_state, directory) = state();
    let target = auth_path(&directory);
    let second = installed(&directory, None, 2, b"{\"generation\":2}");

    let third = installed(&directory, Some(&second), 3, b"{\"generation\":3}");
    assert_eq!(third.generation, 3);
    assert_eq!(fs::read(&target).unwrap(), b"{\"generation\":3}");
    assert_eq!(fs::symlink_metadata(&target).unwrap().nlink(), 1);

    // Codex refreshes its login in place; an older generation must not
    // overwrite it.
    fs::write(&target, b"{\"generation\":3,\"refreshed\":true}").unwrap();

    let refusal = install_codex(
        &directory,
        Some(&third),
        &delivery(1, b"{\"generation\":1}"),
        now(),
    )
    .unwrap_err();
    assert_eq!(refusal, InstallRefusal::StaleGeneration);
    assert_eq!(refusal.state(), AgentLoginState::StaleGeneration);
    assert_eq!(
        fs::read(&target).unwrap(),
        b"{\"generation\":3,\"refreshed\":true}"
    );

    // Another agent login replaces the one the runner installed whatever
    // its generation.
    let other = install_codex(
        &directory,
        Some(&third),
        &delivery_of(OTHER_AGENT_LOGIN_ID, 1, b"{\"other\":1}"),
        now(),
    );
    assert!(other.is_ok());
    assert_eq!(fs::read(&target).unwrap(), b"{\"other\":1}");
    assert_eq!(entries(&directory.codex_home()), [AUTH_FILE, CONFIG_FILE]);
}

// REQ-AGENTLOGIN-26
#[test]
fn what_is_in_the_runners_directory_is_its_own_to_replace() {
    let (_state, directory) = state();
    let home = directory.codex_home();
    fs::create_dir_all(&home).unwrap();
    fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o755)).unwrap();
    fs::set_permissions(&home, fs::Permissions::from_mode(0o750)).unwrap();
    fs::write(home.join(AUTH_FILE), b"{\"unrecorded\":true}").unwrap();
    fs::write(
        home.join(CONFIG_FILE),
        b"cli_auth_credentials_store = \"keyring\"\n",
    )
    .unwrap();
    // A record of an install elsewhere vouches for nothing here.
    let elsewhere = Installation {
        agent_login_id: AGENT_LOGIN_ID.to_owned(),
        generation: 4,
        installed_at: now(),
        installed_path: Path::new("/elsewhere/.codex").join(AUTH_FILE),
    };

    installed(&directory, Some(&elsewhere), 4, b"{\"delivered\":true}");

    assert_eq!(
        fs::read(home.join(AUTH_FILE)).unwrap(),
        b"{\"delivered\":true}"
    );
    assert_eq!(fs::read(home.join(CONFIG_FILE)).unwrap(), CONFIG);
    assert_eq!(mode(directory.path()), 0o700);
    assert_eq!(mode(&home), 0o700);

    // A link at the location is replaced, not followed.
    let (linked_state, directory) = state();
    let outside = linked_state.path().join("outside.json");
    fs::write(&outside, b"{\"outside\":true}").unwrap();
    installed(&directory, None, 1, b"{}");
    fs::remove_file(auth_path(&directory)).unwrap();
    symlink(&outside, auth_path(&directory)).unwrap();
    installed(&directory, None, 1, b"{\"generation\":1}");
    assert!(
        fs::symlink_metadata(auth_path(&directory))
            .unwrap()
            .is_file()
    );
    assert_eq!(fs::read(&outside).unwrap(), b"{\"outside\":true}");
}

// REQ-AGENTLOGIN-26
#[test]
fn a_directory_that_is_not_the_runners_own_is_an_install_failure() {
    let (linked_state, directory) = state();
    let elsewhere = linked_state.path().join("elsewhere");
    fs::create_dir(&elsewhere).unwrap();
    symlink(&elsewhere, directory.path()).unwrap();

    let occupied = InstallRefusal::InstallFailed(StepFailure {
        stage: FailureStage::Directory,
        cause: FailureCause::Io(std::io::ErrorKind::NotADirectory),
    });
    let refusal = install_codex(&directory, None, &delivery(1, b"{}"), now()).unwrap_err();
    assert_eq!(refusal, occupied);
    assert_eq!(refusal.state(), AgentLoginState::InstallFailed);
    assert!(entries(&elsewhere).is_empty());

    let (_state, directory) = state();
    fs::write(directory.path(), b"").unwrap();
    assert_eq!(
        install_codex(&directory, None, &delivery(1, b"{}"), now()),
        Err(occupied)
    );
}

// REQ-AGENTLOGIN-27
#[test]
fn the_runners_codex_home_is_offered_only_when_it_keeps_a_login_and_the_service_sets_none() {
    let (_state, directory) = state();
    let service = ServiceEnvironment::default();
    assert_eq!(offered_home(&service, &directory), None);

    installed(&directory, None, 1, b"{}");
    assert_eq!(
        offered_home(&service, &directory),
        Some((
            OsString::from(CODEX_LOGIN_VARIABLE),
            directory.codex_home().into_os_string()
        ))
    );

    let own_home = ServiceEnvironment::from_lookup(|name| {
        (name == CODEX_LOGIN_VARIABLE).then(|| OsString::from("/srv/codex"))
    });
    assert!(foreign_login(&own_home));
    assert_eq!(offered_home(&own_home, &directory), None);

    fs::remove_file(auth_path(&directory)).unwrap();
    fs::create_dir(auth_path(&directory)).unwrap();
    assert_eq!(offered_home(&service, &directory), None);
}
