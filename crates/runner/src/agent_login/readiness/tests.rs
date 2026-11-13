use std::fs;
use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

use super::*;
use crate::agent_login::cloud::{Delivery, LoginMaterial};
use crate::agent_login::codex::install_codex;
use crate::agent_login::directory::LoginDirectory;

const AGENT_LOGIN_ID: &str = "agl_01k0z6r1w8f4jy2m7q9v3x5abc";
const MATERIAL: &[u8] = b"{\"tokens\":{\"refresh_token\":\"rt-material-6f1d0c\"}}";

fn cli_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn validator() -> jsonschema::Validator {
    let schema: serde_json::Value = serde_json::from_slice(
        &fs::read(cli_root().join("schemas/runner-agent-logins-v1.schema.json")).unwrap(),
    )
    .unwrap();
    jsonschema::draft202012::new(&schema).unwrap()
}

fn time(seconds: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(seconds).unwrap()
}

/// A state directory as enrollment leaves it.
fn state_directory() -> tempfile::TempDir {
    let directory = tempfile::tempdir().unwrap();
    fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
    directory
}

// REQ-AGENTLOGIN-28
#[test]
fn the_document_lives_beside_the_runner_state() {
    assert_eq!(
        readiness_path(Path::new("/var/lib/um-runner/state.json")),
        Path::new("/var/lib/um-runner/.runner-agent-logins.json")
    );
}

// REQ-AGENTLOGIN-28, REQ-AGENTLOGIN-32
#[test]
fn the_document_records_an_install_without_its_material() {
    let login_root = tempfile::tempdir().unwrap();
    let directory = LoginDirectory::beside(&login_root.path().join("state.json"));
    let delivery = Delivery {
        provider: AgentLoginProvider::Codex,
        agent_login_id: AGENT_LOGIN_ID.to_owned(),
        generation: 3,
        record_schema_version: 1,
        material: LoginMaterial::from_bytes(MATERIAL),
    };
    let installation =
        install_codex(&directory, None, &delivery, time(1_794_444_900)).expect("an install");
    let document = ReadinessDocument {
        updated_at: time(1_794_445_200),
        logins: vec![LoginReadiness {
            provider: AgentLoginProvider::Codex,
            state: AgentLoginState::Ready,
            checked_at: Some(time(1_794_445_200)),
            login: Some(RecordedLogin::Installed(installation.clone())),
            selected: true,
            last_use: None,
        }],
        unknown: Vec::new(),
    };
    let state = state_directory();
    let path = readiness_path(&state.path().join("state.json"));

    document.write(&path).unwrap();

    let bytes = fs::read(&path).unwrap();
    let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert!(validator().is_valid(&value), "{value}");
    assert_eq!(
        value["providers"],
        serde_json::json!([
            {
                "provider": "codex",
                "state": "ready",
                "agentLoginId": AGENT_LOGIN_ID,
                "generation": 3,
                "checkedAt": "2026-11-12T01:00:00Z",
                "installedAt": "2026-11-12T00:55:00Z",
                "installedPath": installation.installed_path.to_str().unwrap(),
                "selected": true,
            },
            {"provider": "github", "state": "not_delivered"},
            {"provider": "gitlab", "state": "not_delivered"},
            {"provider": "linear", "state": "not_delivered"},
        ])
    );
    for window in MATERIAL.windows(8) {
        assert!(
            !bytes
                .windows(window.len())
                .any(|candidate| candidate == window),
            "the readiness document holds login material"
        );
    }
    let metadata = fs::symlink_metadata(&path).unwrap();
    assert_eq!(metadata.mode() & 0o7777, 0o600);
    assert_eq!(
        ReadinessDocument::load(&path).unwrap().as_ref(),
        Some(&document)
    );
    assert_eq!(
        document
            .login(AgentLoginProvider::Codex)
            .and_then(LoginReadiness::installation),
        Some(&installation)
    );
}

// REQ-AGENTLOGIN-28
#[test]
fn every_readiness_state_round_trips_and_a_missing_document_is_none() {
    let state = state_directory();
    let path = readiness_path(&state.path().join("state.json"));
    assert_eq!(ReadinessDocument::load(&path), Ok(None));

    let validator = validator();
    for (state, login) in [
        (AgentLoginState::Ready, None),
        (
            AgentLoginState::NotAuthorized,
            Some(RecordedLogin::Named(AGENT_LOGIN_ID.to_owned())),
        ),
        (AgentLoginState::Revoked, None),
        (
            AgentLoginState::ForeignLogin,
            Some(RecordedLogin::Named(AGENT_LOGIN_ID.to_owned())),
        ),
        (AgentLoginState::UnsupportedTarget, None),
        (AgentLoginState::StaleGeneration, None),
        (AgentLoginState::CloudUnavailable, None),
        (AgentLoginState::InstallFailed, None),
    ] {
        for selected in [true, false] {
            let document = ReadinessDocument {
                updated_at: time(1_794_445_200),
                logins: vec![LoginReadiness {
                    provider: AgentLoginProvider::Codex,
                    state,
                    checked_at: None,
                    login: login.clone(),
                    selected,
                    last_use: None,
                }],
                unknown: Vec::new(),
            };
            document.write(&path).unwrap();
            let value: serde_json::Value =
                serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
            assert!(validator.is_valid(&value), "{value}");
            assert_eq!(value["providers"][0]["selected"], selected, "{value}");
            assert_eq!(ReadinessDocument::load(&path), Ok(Some(document)));
        }
    }
    let entries = fs::read_dir(state.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<Vec<_>>();
    assert_eq!(entries, [READINESS_FILE]);
}

// REQ-AGENTLOGIN-28, REQ-AGENTLOGIN-42, REQ-AGENTLOGIN-43
#[test]
fn the_last_use_of_an_install_round_trips_beside_it_and_only_there() {
    let state = state_directory();
    let path = readiness_path(&state.path().join("state.json"));
    let validator = validator();
    let installation = Installation {
        agent_login_id: AGENT_LOGIN_ID.to_owned(),
        generation: 2,
        installed_at: time(1_794_444_900),
        installed_path: PathBuf::from("/var/lib/um-runner/.runner-agent-login/codex/auth.json"),
    };
    for outcome in [UseOutcome::Accepted, UseOutcome::Refused] {
        let document = ReadinessDocument {
            updated_at: time(1_794_445_200),
            logins: vec![LoginReadiness {
                provider: AgentLoginProvider::Codex,
                state: AgentLoginState::Ready,
                checked_at: Some(time(1_794_445_200)),
                login: Some(RecordedLogin::Installed(installation.clone())),
                selected: true,
                last_use: Some(LastUse {
                    outcome,
                    at: time(1_794_445_080),
                }),
            }],
            unknown: Vec::new(),
        };
        document.write(&path).unwrap();

        let value: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert!(validator.is_valid(&value), "{value}");
        assert_eq!(
            value["providers"][0]["lastUse"],
            serde_json::to_value(outcome).unwrap()
        );
        assert_eq!(value["providers"][0]["lastUseAt"], "2026-11-12T00:58:00Z");
        assert_eq!(ReadinessDocument::load(&path), Ok(Some(document.clone())));
        // The control socket reports the last use and the selection, but
        // neither when the use was nor where the install is
        // (REQ-AGENTLOGIN-43, REQ-RUNNERENROLL-63).
        let reported = serde_json::to_value(document.reported().unwrap()).unwrap();
        let entry = &reported["providers"][0];
        assert_eq!(
            entry["lastUse"],
            serde_json::to_value(outcome).unwrap(),
            "{reported}"
        );
        assert_eq!(entry["selected"], true, "{reported}");
        assert!(entry.get("lastUseAt").is_none(), "{reported}");
        assert!(entry.get("installedPath").is_none(), "{reported}");
    }

    let installed = serde_json::json!({
        "provider": "codex",
        "state": "ready",
        "agentLoginId": AGENT_LOGIN_ID,
        "generation": 2,
        "installedAt": "2026-11-12T00:55:00Z",
        "installedPath": "/var/lib/um-runner/.runner-agent-login/codex/auth.json",
    });
    let with = |entry: &serde_json::Value, members: serde_json::Value| {
        let mut entry = entry.clone();
        entry
            .as_object_mut()
            .unwrap()
            .extend(members.as_object().unwrap().clone());
        entry
    };
    for entry in [
        // Without an install.
        serde_json::json!({
            "provider": "codex",
            "state": "ready",
            "agentLoginId": AGENT_LOGIN_ID,
            "lastUse": "refused",
            "lastUseAt": "2026-11-12T00:58:00Z",
        }),
        // Without its time, or a time without a use.
        with(&installed, serde_json::json!({"lastUse": "accepted"})),
        with(
            &installed,
            serde_json::json!({"lastUseAt": "2026-11-12T00:58:00Z"}),
        ),
        // A use the runner does not record.
        with(
            &installed,
            serde_json::json!({"lastUse": "expired", "lastUseAt": "2026-11-12T00:58:00Z"}),
        ),
    ] {
        let written = serde_json::json!({
            "schemaVersion": 1,
            "updatedAt": "2026-11-12T01:00:00Z",
            "providers": [entry],
        });
        assert!(!validator.is_valid(&written), "{written}");
        let _ = fs::remove_file(&path);
        fs::write(&path, serde_json::to_vec(&written).unwrap()).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(
            ReadinessDocument::load(&path),
            Err(ReadinessError::Invalid),
            "{written}"
        );
    }
}

// REQ-AGENTLOGIN-28
#[test]
fn an_entry_for_a_provider_this_build_does_not_know_survives_a_rewrite_verbatim() {
    let state = state_directory();
    let path = readiness_path(&state.path().join("state.json"));
    let gemini = serde_json::json!({
        "provider": "gemini",
        "state": "awaiting_consent",
        "agentLoginId": AGENT_LOGIN_ID,
        "generation": 2,
        "checkedAt": "2026-11-12T00:50:00Z",
        "installedAt": "2026-11-12T00:45:00Z",
        "installedPath": "/var/lib/um-runner/.runner-agent-login/gemini/oauth.json",
    });
    let written = serde_json::json!({
        "schemaVersion": 1,
        "updatedAt": "2026-11-12T00:50:00Z",
        "providers": [
            {"provider": "codex", "state": "revoked"},
            gemini,
            {"provider": "github", "state": "not_delivered"},
        ],
    });
    let validator = validator();
    assert!(validator.is_valid(&written), "{written}");
    fs::write(&path, serde_json::to_vec(&written).unwrap()).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();

    let mut document = ReadinessDocument::load(&path).unwrap().unwrap();
    assert_eq!(
        document.logins,
        [LoginReadiness {
            provider: AgentLoginProvider::Codex,
            state: AgentLoginState::Revoked,
            checked_at: None,
            login: None,
            selected: false,
            last_use: None,
        }]
    );
    document.updated_at = time(1_794_445_200);
    document.logins[0].state = AgentLoginState::NotAuthorized;
    document.write(&path).unwrap();

    let value: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    assert!(validator.is_valid(&value), "{value}");
    let entries = value["providers"].as_array().unwrap();
    assert_eq!(
        entries
            .iter()
            .filter(|entry| entry["provider"] == "gemini")
            .collect::<Vec<_>>(),
        [&gemini]
    );
    assert_eq!(ReadinessDocument::load(&path), Ok(Some(document.clone())));
    // The control socket's providers are closed: the entry is not reported.
    let reported = document.reported().expect("the reported readiness");
    assert_eq!(reported.providers.len(), entries.len() - 1);
}

// REQ-AGENTLOGIN-28
#[test]
fn an_entry_written_before_selection_was_recorded_is_selected_by_its_state() {
    let state = state_directory();
    let path = readiness_path(&state.path().join("state.json"));
    for (state, selected) in [
        (AgentLoginState::Ready, true),
        (AgentLoginState::ForeignLogin, true),
        (AgentLoginState::UnsupportedTarget, true),
        (AgentLoginState::StaleGeneration, true),
        (AgentLoginState::CloudUnavailable, true),
        (AgentLoginState::InstallFailed, true),
        (AgentLoginState::NotAuthorized, false),
        (AgentLoginState::Revoked, false),
    ] {
        let _ = fs::remove_file(&path);
        let written = serde_json::json!({
            "schemaVersion": 1,
            "updatedAt": "2026-11-12T01:00:00Z",
            "providers": [{"provider": "codex", "state": state.as_str()}],
        });
        fs::write(&path, serde_json::to_vec(&written).unwrap()).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        let document = ReadinessDocument::load(&path).unwrap().unwrap();
        let codex = document.login(AgentLoginProvider::Codex).unwrap();
        assert_eq!(codex.selected, selected, "{state:?}");
    }
}

// REQ-AGENTLOGIN-28
#[test]
fn only_a_document_that_is_not_the_runners_record_is_replaced() {
    use std::io::ErrorKind;
    // Not the runner's private file, over its bound, or not this document:
    // it vouches for nothing and a setup replaces it.
    for replaced in [
        ReadinessError::Invalid,
        ReadinessError::Unreadable(ErrorKind::PermissionDenied),
        ReadinessError::Unreadable(ErrorKind::InvalidData),
    ] {
        assert!(replaced.replaceable(), "{replaced:?}");
    }
    // An I/O failure that kept the runner from reading it says nothing about
    // the document, which may still be the only record of an install.
    for kept in [
        ReadinessError::Unreadable(ErrorKind::Interrupted),
        ReadinessError::Unreadable(ErrorKind::Other),
        ReadinessError::Unreadable(ErrorKind::NotADirectory),
    ] {
        assert!(!kept.replaceable(), "{kept:?}");
    }
}

#[test]
fn reading_rejects_a_bad_record() {
    let state = state_directory();
    let path = readiness_path(&state.path().join("state.json"));
    let write = |value: serde_json::Value| {
        let _ = fs::remove_file(&path);
        fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    };

    for providers in [
        serde_json::json!([{"provider": "codex", "state": "not_delivered"}]),
        serde_json::json!([{"provider": "github", "state": "ready"}]),
        serde_json::json!([{"provider": "codex", "state": "ready", "generation": 1}]),
        serde_json::json!([
            {"provider": "codex", "state": "ready"},
            {"provider": "codex", "state": "revoked"},
        ]),
        serde_json::json!([{"provider": "codex", "state": "ready", "material": "x"}]),
        serde_json::json!([{"provider": "codex", "state": "awaiting_consent"}]),
        serde_json::json!([{"provider": "Gemini", "state": "ready"}]),
        serde_json::json!([{"provider": "gemini", "state": "ready", "generation": 1}]),
        serde_json::json!([{"provider": "gemini", "state": "ready", "material": "x"}]),
        serde_json::json!([
            {"provider": "gemini", "state": "ready"},
            {"provider": "gemini", "state": "revoked"},
        ]),
    ] {
        write(serde_json::json!({
            "schemaVersion": 1,
            "updatedAt": "2026-11-12T01:00:00Z",
            "providers": providers,
        }));
        assert_eq!(
            ReadinessDocument::load(&path),
            Err(ReadinessError::Invalid),
            "{providers}"
        );
    }

    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
    assert_eq!(
        ReadinessDocument::load(&path),
        Err(ReadinessError::Unreadable(
            std::io::ErrorKind::PermissionDenied
        ))
    );
}

// REQ-AGENTLOGIN-28
#[test]
fn canonical_schema_and_the_runner_accept_only_valid_readiness_fixtures() {
    let validator = validator();
    let state = state_directory();
    let document = readiness_path(&state.path().join("state.json"));
    let fixtures = cli_root().join("tests/fixtures/runner-agent-logins/v1");
    for (kind, expected) in [("valid", true), ("invalid", false)] {
        for entry in fs::read_dir(fixtures.join(kind)).unwrap() {
            let path = entry.unwrap().path();
            let bytes = fs::read(&path).unwrap();
            let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(validator.is_valid(&value), expected, "{}", path.display());
            let _ = fs::remove_file(&document);
            fs::write(&document, &bytes).unwrap();
            fs::set_permissions(&document, fs::Permissions::from_mode(0o600)).unwrap();
            let loaded = ReadinessDocument::load(&document);
            assert_eq!(loaded.is_ok(), expected, "{}", path.display());
            // What the runner writes back, the schema accepts.
            if let Ok(Some(loaded)) = loaded {
                loaded.write(&document).unwrap();
                let written: serde_json::Value =
                    serde_json::from_slice(&fs::read(&document).unwrap()).unwrap();
                assert!(validator.is_valid(&written), "{written}");
            }
        }
    }
}
