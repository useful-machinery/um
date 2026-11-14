use super::*;

use std::collections::BTreeSet;
use std::os::unix::fs::MetadataExt as _;
use std::path::PathBuf;

use super::runner_agent_login::{
    EARLIER_SYNC, READY_CODEX, SYNC, SYNC_REQUEST, ScriptedControlSocket, synced_response,
};
use super::runner_enrollment::{
    ACTIVATION_ID, ConnectionWitness, ServeProcess, StandardInput, activation_artifact,
    connection_bearer, enrollment_body, future_expiry, join_base, join_key_artifact, join_response,
    serve_command, verifier_of,
};
use super::runner_status::{
    CONNECTED_STATUS, STATUS_REQUEST, ServeStatusFixture, enrolled_state_document,
    write_status_config_at,
};

const HOME: &str = "HOME";

fn mode(path: &Path) -> u32 {
    fs::symlink_metadata(path)
        .unwrap_or_else(|error| panic!("{}: {error}", path.display()))
        .mode()
        & 0o7777
}

fn names(directory: &Path) -> BTreeSet<String> {
    fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect()
}

/// Asserts that `um_home` holds the runner home's directories at `0700` and
/// its configuration at `0600`, and no enrolled state.
fn assert_layout(um_home: &Path) {
    let runner = um_home.join("runner");
    for directory in [um_home.to_owned(), runner.clone(), runner.join("work")] {
        assert!(fs::symlink_metadata(&directory).unwrap().is_dir());
        assert_eq!(mode(&directory), 0o700, "{}", directory.display());
    }
    assert_eq!(mode(&runner.join("config.json")), 0o600);
    assert!(!runner.join("state.json").exists());
}

/// Asserts that `um_home` holds the runner home `serve` initializes and
/// nothing else under `runner/`: no state, journal, lock or socket.
fn assert_initialized(um_home: &Path) {
    assert_layout(um_home);
    assert_eq!(
        names(&um_home.join("runner")),
        BTreeSet::from(["config.json".to_owned(), "work".to_owned()])
    );
}

/// `um runner serve ARGUMENTS` with an empty `PATH`, so no harness of the
/// machine running the suite is discovered, and `UM_HOME` and `HOME` as given:
/// a variable given `None` is removed.
fn serve_with_homes(
    arguments: &[&str],
    um_home: Option<&Path>,
    home: Option<&Path>,
    empty_path: &tempfile::TempDir,
) -> Command {
    let mut command = serve_command(arguments);
    command.env("PATH", empty_path.path());
    set_homes(&mut command, um_home, home);
    command
}

/// Gives `command` `UM_HOME` and `HOME` as given, removing a variable given
/// `None`.
fn set_homes(command: &mut Command, um_home: Option<&Path>, home: Option<&Path>) {
    for (name, value) in [(UM_HOME_VARIABLE, um_home), (HOME, home)] {
        match value {
            Some(value) => command.env(name, value),
            None => command.env_remove(name),
        };
    }
}

/// A `um` home under `/tmp`, so the control socket of its runner home fits
/// `sun_path` whatever the suite's `TMPDIR`.
struct ServeHome {
    root: tempfile::TempDir,
    empty_path: tempfile::TempDir,
}

impl ServeHome {
    fn new() -> Self {
        let root = tempfile::tempdir_in("/tmp").unwrap();
        fs::set_permissions(root.path(), Permissions::from_mode(0o700)).unwrap();
        Self {
            root,
            empty_path: tempfile::tempdir().unwrap(),
        }
    }

    fn um_home(&self) -> PathBuf {
        self.root.path().join("um")
    }

    fn runner(&self) -> PathBuf {
        self.um_home().join("runner")
    }

    fn state(&self) -> PathBuf {
        self.runner().join("state.json")
    }

    /// A `development` configuration of the home's own layout, which the
    /// loopback fakes need, naming `state` as its state.
    fn write_development_config(&self, state: &Path) {
        for directory in [self.um_home(), self.runner(), self.runner().join("work")] {
            fs::create_dir_all(&directory).unwrap();
            fs::set_permissions(&directory, Permissions::from_mode(0o700)).unwrap();
        }
        let config = self.runner().join("config.json");
        fs::write(
            &config,
            serde_json::to_vec_pretty(&serde_json::json!({
                "schemaVersion": 1,
                "deploymentMode": "development",
                "runnerStatePath": state,
                "controlSocketPath": self.runner().join("control.sock"),
                "workRoot": self.runner().join("work")
            }))
            .unwrap(),
        )
        .unwrap();
        fs::set_permissions(&config, Permissions::from_mode(0o600)).unwrap();
    }

    /// Enrolled state at the home's `runner/state.json` for `endpoint`,
    /// answering the bearer it connects with and the state's bytes.
    fn write_enrolled_state(&self, endpoint: &str) -> (String, Vec<u8>) {
        let state = enrolled_state_document(endpoint);
        let bytes = serde_json::to_vec_pretty(&state).unwrap();
        fs::write(self.state(), &bytes).unwrap();
        fs::set_permissions(self.state(), Permissions::from_mode(0o600)).unwrap();
        (bearer_of(&state), bytes)
    }

    fn serve(&self, arguments: &[&str]) -> ServeProcess {
        ServeProcess::start(
            serve_with_homes(
                arguments,
                Some(&self.um_home()),
                Some(self.root.path()),
                &self.empty_path,
            ),
            StandardInput::Null,
        )
    }
}

fn bearer_of(state: &serde_json::Value) -> String {
    let credential = &state["currentCredential"];
    format!(
        "Bearer {}.{}",
        credential["id"].as_str().unwrap(),
        credential["secret"].as_str().unwrap()
    )
}

// REQ-RUNNERENROLL-109, REQ-RUNNERENROLL-110, REQ-RUNNERENROLL-30
#[test]
fn serve_on_an_empty_home_initializes_it_and_says_the_host_is_not_enrolled() {
    let empty_path = tempfile::tempdir().unwrap();
    let custom = tempfile::tempdir().unwrap();
    let elsewhere = tempfile::tempdir().unwrap();
    let home = tempfile::tempdir().unwrap();
    let custom_um_home = custom.path().join("custom-um");
    let default_um_home = home.path().join(".um");
    let empty = Path::new("");
    // Each case: UM_HOME, HOME and the `um` home they select.
    let cases: [(Option<&Path>, Option<&Path>, &Path); 3] = [
        (
            Some(&custom_um_home),
            Some(elsewhere.path()),
            &custom_um_home,
        ),
        (Some(empty), Some(home.path()), &default_um_home),
        (None, Some(home.path()), &default_um_home),
    ];
    for (um_home, home_variable, selected) in cases {
        if selected.exists() {
            fs::remove_dir_all(selected).unwrap();
        }

        let output = serve_with_homes(&[], um_home, home_variable, &empty_path)
            .output()
            .unwrap();

        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(output.status.code(), Some(1), "{stderr}");
        assert!(output.stdout.is_empty());
        for named in ["um runner enroll", "--join-key-file"] {
            assert!(stderr.contains(named), "{named}: {stderr}");
        }
        assert_initialized(selected);
        assert!(names(elsewhere.path()).is_empty());
    }

    let working_directory = tempfile::tempdir().unwrap();
    let output = serve_with_homes(&[], None, None, &empty_path)
        .current_dir(working_directory.path())
        .output()
        .unwrap();

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(1), "{stderr}");
    for named in [UM_HOME_VARIABLE, "--config"] {
        assert!(stderr.contains(named), "{named}: {stderr}");
    }
    assert!(names(working_directory.path()).is_empty());
}

// REQ-RUNNERENROLL-109
#[test]
fn serve_given_a_configuration_neither_reads_nor_creates_the_runner_home() {
    let witness = ConnectionWitness::new();
    let fixture = ServeStatusFixture::new(&witness.url);
    let um_home = tempfile::tempdir().unwrap();
    let empty_path = tempfile::tempdir().unwrap();

    let serve = ServeProcess::start(
        serve_with_homes(
            &["--config", &fixture.config],
            Some(um_home.path()),
            Some(um_home.path()),
            &empty_path,
        ),
        StandardInput::Null,
    );

    assert_eq!(
        connection_bearer(serve, &witness),
        bearer_of(&enrolled_state_document(&witness.url))
    );
    assert!(names(um_home.path()).is_empty());
}

// REQ-RUNNERENROLL-110
#[test]
fn serve_without_a_configuration_connects_with_the_homes_enrolled_state() {
    let witness = ConnectionWitness::new();
    let home = ServeHome::new();
    home.write_development_config(&home.state());
    let (bearer, state) = home.write_enrolled_state(&witness.url);

    assert_eq!(connection_bearer(home.serve(&[]), &witness), bearer);
    assert_eq!(fs::read(home.state()).unwrap(), state);
}

// REQ-RUNNERENROLL-97
#[test]
fn serve_without_a_configuration_joins_and_writes_the_homes_state() {
    let witness = ConnectionWitness::new();
    let server = api_test_support::ScriptedHttpServer::respond(join_response(0, &witness.url));
    let home = ServeHome::new();
    home.write_development_config(&home.state());
    let key_file = home.root.path().join("join-key.json");
    fs::write(
        &key_file,
        join_key_artifact(&join_base(&server), &future_expiry()),
    )
    .unwrap();
    fs::set_permissions(&key_file, Permissions::from_mode(0o600)).unwrap();

    let bearer = connection_bearer(
        home.serve(&["--join-key-file", key_file.to_str().unwrap()]),
        &witness,
    );

    assert_eq!(mode(&home.state()), 0o600);
    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(home.state()).unwrap()).unwrap();
    assert_eq!(bearer, bearer_of(&state));
    server.finish_one();
}

// REQ-RUNNERENROLL-65, REQ-RUNNERENROLL-34
#[test]
fn two_homes_serve_at_once_and_a_second_serve_on_one_home_is_refused() {
    let homes = [ServeHome::new(), ServeHome::new()];
    let witnesses = [ConnectionWitness::new(), ConnectionWitness::new()];
    let mut serving = Vec::new();
    for (home, witness) in homes.iter().zip(&witnesses) {
        home.write_development_config(&home.state());
        let (bearer, state) = home.write_enrolled_state(&witness.url);
        let mut serve = home.serve(&[]);
        let Some(request) = witness.next_connection(&mut serve) else {
            panic!("Runner Serve stopped: {}", serve.stop());
        };
        assert_eq!(header_value(&request, "authorization"), bearer);
        serving.push((serve, state));
    }

    let (exit, stderr) = homes[0].serve(&[]).wait_for_exit();

    assert!(!exit.success(), "{stderr}");
    let (first, state) = &mut serving[0];
    assert!(first.exited().is_none());
    assert!(witnesses[0].next_connection(first).is_some());
    assert_eq!(&fs::read(homes[0].state()).unwrap(), state);
    for (serve, _) in serving {
        serve.stop();
    }
}

// REQ-RUNNERENROLL-111
#[test]
fn serve_with_a_join_key_on_a_home_whose_configuration_names_another_state_sends_no_join() {
    let cloud = TcpListener::bind("127.0.0.1:0").unwrap();
    let home = ServeHome::new();
    let elsewhere = home.root.path().join("elsewhere").join("state.json");
    home.write_development_config(&elsewhere);
    let (_, state) = home.write_enrolled_state("ws://127.0.0.1:1/v1/runner/connect");
    let key_file = home.root.path().join("join-key.json");
    fs::write(
        &key_file,
        join_key_artifact(
            &format!("http://{}", cloud.local_addr().unwrap()),
            &future_expiry(),
        ),
    )
    .unwrap();
    fs::set_permissions(&key_file, Permissions::from_mode(0o600)).unwrap();

    let (exit, stderr) = home
        .serve(&["--join-key-file", key_file.to_str().unwrap()])
        .wait_for_exit();

    assert_eq!(exit.code(), Some(1), "{stderr}");
    assert_eq!(fs::read(home.state()).unwrap(), state);
    assert!(!elsewhere.parent().unwrap().exists());
    assert_no_connection(&cloud);
}

/// A credential secret staged in an enrollment journal: 32 bytes of `0x01`.
const STAGED_SECRET: &str = "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE";
const SECOND_ACTIVATION_ID: &str = "rna_01k0z6r1w8f4jy2m7q9v3x5abe";

impl ServeHome {
    fn journal(&self) -> PathBuf {
        self.runner().join(".runner-enrollment.json")
    }

    /// A protected activation file for `activation_id` at Cloud `base`.
    fn write_activation(&self, base: &str, activation_id: &str) -> String {
        let path = self.root.path().join(format!("{activation_id}.json"));
        fs::write(&path, activation_artifact(base, activation_id).to_string()).unwrap();
        fs::set_permissions(&path, Permissions::from_mode(0o600)).unwrap();
        path.to_string_lossy().into_owned()
    }

    /// `um runner enroll ARGUMENTS` with this home as `UM_HOME`.
    fn enroll(&self, arguments: &[&str]) -> Output {
        let mut command = vec!["runner", "enroll"];
        command.extend_from_slice(arguments);
        self.run(&command)
    }

    /// `um ARGUMENTS` with this home as `UM_HOME`.
    fn run(&self, arguments: &[&str]) -> Output {
        let um_home = self.um_home();
        run_with_env(
            arguments,
            &[
                (UM_HOME_VARIABLE, um_home.to_str().unwrap()),
                (HOME, self.root.path().to_str().unwrap()),
            ],
        )
    }
}

fn assert_no_connection(cloud: &TcpListener) {
    cloud.set_nonblocking(true).unwrap();
    assert_eq!(
        cloud.accept().unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
}

// REQ-RUNNERENROLL-13, REQ-RUNNERENROLL-110
#[test]
fn enroll_on_an_empty_home_initializes_it_and_its_production_configuration_refuses_loopback() {
    let cloud = TcpListener::bind("127.0.0.1:0").unwrap();
    let home = ServeHome::new();
    let activation = home.write_activation(
        &format!("http://{}", cloud.local_addr().unwrap()),
        ACTIVATION_ID,
    );

    let output = home.enroll(&["--activation-file", &activation, "--json"]);

    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert_layout(&home.um_home());
    assert!(!home.journal().exists());
    let config: serde_json::Value =
        serde_json::from_slice(&fs::read(home.runner().join("config.json")).unwrap()).unwrap();
    assert_eq!(config["deploymentMode"], "production");
    assert_no_connection(&cloud);
}

// REQ-RUNNERENROLL-13
#[test]
fn enroll_without_a_configuration_writes_the_homes_state_once() {
    let server = api_test_support::ScriptedHttpServer::respond(json_http_response(
        "201 Created",
        enrollment_body(),
    ));
    let home = ServeHome::new();
    home.write_development_config(&home.state());
    let activation = home.write_activation(&join_base(&server), ACTIVATION_ID);

    let output = home.enroll(&["--activation-file", &activation, "--json"]);

    assert!(output.status.success(), "{output:?}");
    server.finish_one();
    assert_eq!(mode(&home.state()), 0o600);
    let state = fs::read(home.state()).unwrap();
    let document: serde_json::Value = serde_json::from_slice(&state).unwrap();
    assert_eq!(
        document["currentCredential"]["id"],
        enrollment_body()["credentialId"]
    );

    let cloud = TcpListener::bind("127.0.0.1:0").unwrap();
    let second = home.write_activation(
        &format!("http://{}", cloud.local_addr().unwrap()),
        SECOND_ACTIVATION_ID,
    );
    let output = home.enroll(&["--activation-file", &second, "--json"]);

    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert_eq!(fs::read(home.state()).unwrap(), state);
    assert_no_connection(&cloud);
}

// REQ-RUNNERENROLL-22
#[test]
fn enroll_resume_without_a_configuration_completes_the_homes_staged_journal() {
    let server = api_test_support::ScriptedHttpServer::respond(json_http_response(
        "201 Created",
        enrollment_body(),
    ));
    let home = ServeHome::new();
    home.write_development_config(&home.state());
    let idempotency_key = "1".repeat(64);
    fs::write(
        home.journal(),
        serde_json::json!({
            "schemaVersion": 1,
            "activationArtifact": activation_artifact(&join_base(&server), ACTIVATION_ID),
            "credentialSecret": STAGED_SECRET,
            "credentialSecretVerifier": verifier_of(STAGED_SECRET),
            "idempotencyKey": idempotency_key,
            "replaceCredential": false,
            "stagedAt": "2026-08-06T12:00:00Z"
        })
        .to_string(),
    )
    .unwrap();
    fs::set_permissions(home.journal(), Permissions::from_mode(0o600)).unwrap();

    let output = home.enroll(&["--resume", "--json"]);

    assert!(output.status.success(), "{output:?}");
    let request = server.finish_one();
    assert_eq!(header_value(&request, "idempotency-key"), idempotency_key);
    assert!(!home.journal().exists());
    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(home.state()).unwrap()).unwrap();
    assert_eq!(state["currentCredential"]["secret"], STAGED_SECRET);
}

// REQ-RUNNERENROLL-109
#[test]
fn enroll_given_a_configuration_neither_reads_nor_creates_the_runner_home() {
    let server = api_test_support::ScriptedHttpServer::respond(json_http_response(
        "201 Created",
        enrollment_body(),
    ));
    let fixture = ServeHome::new();
    fixture.write_development_config(&fixture.state());
    let config = fixture.runner().join("config.json");
    let activation = fixture.write_activation(&join_base(&server), ACTIVATION_ID);
    let um_home = tempfile::tempdir().unwrap();

    let output = run_with_env(
        &[
            "runner",
            "enroll",
            "--activation-file",
            &activation,
            "--config",
            config.to_str().unwrap(),
            "--json",
        ],
        &[(UM_HOME_VARIABLE, um_home.path().to_str().unwrap())],
    );

    assert!(output.status.success(), "{output:?}");
    server.finish_one();
    assert_eq!(mode(&fixture.state()), 0o600);
    assert!(names(um_home.path()).is_empty());
}

// REQ-RUNNERENROLL-48, REQ-RUNNERENROLL-109
#[test]
fn enroll_replace_credential_without_a_configuration_asks_the_homes_control_socket() {
    let server = api_test_support::ScriptedHttpServer::respond(json_http_response(
        "201 Created",
        enrollment_body(),
    ));
    let home = ServeHome::new();
    home.write_development_config(&home.state());
    home.write_enrolled_state("ws://127.0.0.1:1/v1/runner/connect");
    let activation = home.write_activation(&join_base(&server), ACTIVATION_ID);
    let replacement = enrollment_body()["credentialId"].clone();
    let socket = ScriptedControlSocket::answering(
        &home.runner().join("control.sock"),
        format!(
            "{}\n",
            serde_json::json!({
                "schemaVersion": 1,
                "outcome": "reloaded",
                "credentialId": replacement
            })
        )
        .into_bytes(),
    );

    let output = home.enroll(&[
        "--replace-credential",
        "--activation-file",
        &activation,
        "--json",
    ]);

    assert_eq!(
        socket.request(),
        b"{\"schemaVersion\":1,\"operation\":\"reload_credential\"}\n"
    );
    assert!(output.status.success(), "{output:?}");
    server.finish_one();
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["livePromotion"]["outcome"], "promoted");
    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(home.state()).unwrap()).unwrap();
    assert_eq!(state["pendingCredential"]["id"], replacement);
}

/// `um runner status` and both spellings of `um runner grant sync`, each with
/// the request it sends and a live runner's answer to it.
fn reading_commands() -> [(&'static [&'static str], &'static [u8], Vec<u8>); 3] {
    [
        (
            &["runner", "status"],
            STATUS_REQUEST,
            CONNECTED_STATUS.to_vec(),
        ),
        (&SYNC, SYNC_REQUEST, synced_response(READY_CODEX)),
        (&EARLIER_SYNC, SYNC_REQUEST, synced_response(READY_CODEX)),
    ]
}

// REQ-RUNNERENROLL-112, REQ-RUNNERENROLL-44, REQ-GRANT-41
#[test]
fn reading_commands_on_an_absent_or_empty_home_find_no_runner_and_create_nothing() {
    for (command, _, _) in reading_commands() {
        let home = ServeHome::new();

        let output = home.run(command);

        assert_eq!(output.status.code(), Some(4), "{command:?}: {output:?}");
        assert!(output.stdout.is_empty(), "{command:?}");
        assert!(!home.um_home().exists(), "{command:?}");

        fs::create_dir(home.um_home()).unwrap();
        let output = home.run(command);

        assert_eq!(output.status.code(), Some(4), "{command:?}: {output:?}");
        assert!(output.stdout.is_empty(), "{command:?}");
        assert!(names(&home.um_home()).is_empty(), "{command:?}");
    }
}

// REQ-RUNNERENROLL-112, REQ-GRANT-41
#[test]
fn reading_commands_ask_the_homes_default_socket_when_it_holds_no_configuration() {
    for (command, request, response) in reading_commands() {
        let home = ServeHome::new();
        fs::create_dir_all(home.runner()).unwrap();
        let socket =
            ScriptedControlSocket::answering(&home.runner().join("control.sock"), response);

        let output = home.run(command);

        assert_eq!(socket.request(), request, "{command:?}");
        assert_eq!(output.status.code(), Some(0), "{command:?}: {output:?}");
        assert!(!output.stdout.is_empty(), "{command:?}");
        assert_eq!(
            names(&home.runner()),
            BTreeSet::from(["control.sock".to_owned()]),
            "{command:?}"
        );
    }
}

// REQ-RUNNERENROLL-112, REQ-RUNNERENROLL-41
#[test]
fn status_asks_the_socket_the_homes_configuration_names() {
    let home = ServeHome::new();
    fs::create_dir_all(home.runner()).unwrap();
    let elsewhere = home.root.path().join("elsewhere.sock");
    let socket = ScriptedControlSocket::answering(&elsewhere, CONNECTED_STATUS.to_vec());
    let config = home.runner().join("config.json");
    write_status_config_at(&config, &elsewhere);
    let before = fs::read(&config).unwrap();

    let output = home.run(&["runner", "status"]);

    assert_eq!(socket.request(), STATUS_REQUEST);
    assert!(output.status.success(), "{output:?}");
    assert!(
        String::from_utf8(output.stdout)
            .unwrap()
            .contains("rbt_01k0z6r1w8f4jy2m7q9v3x5abc")
    );
    assert_eq!(fs::read(&config).unwrap(), before);
}

// REQ-RUNNERENROLL-112
#[test]
fn status_fails_on_a_homes_configuration_that_does_not_parse_and_keeps_its_bytes() {
    let home = ServeHome::new();
    fs::create_dir_all(home.runner()).unwrap();
    let config = home.runner().join("config.json");
    fs::write(&config, b"{\"schemaVersion\":").unwrap();

    let output = home.run(&["runner", "status"]);

    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert!(output.stdout.is_empty());
    assert_eq!(fs::read(&config).unwrap(), b"{\"schemaVersion\":");
    assert_eq!(
        names(&home.runner()),
        BTreeSet::from(["config.json".to_owned()])
    );
}

// REQ-RUNNERENROLL-109, REQ-RUNNERENROLL-41
#[test]
fn status_given_a_configuration_neither_reads_nor_creates_the_runner_home() {
    let fixture = ServeHome::new();
    let socket_path = fixture.root.path().join("runner.sock");
    let socket = ScriptedControlSocket::answering(&socket_path, CONNECTED_STATUS.to_vec());
    let config = fixture.root.path().join("config.json");
    write_status_config_at(&config, &socket_path);
    let um_home = tempfile::tempdir().unwrap();

    let output = run_with_env(
        &["runner", "status", "--config", config.to_str().unwrap()],
        &[(UM_HOME_VARIABLE, um_home.path().to_str().unwrap())],
    );

    assert_eq!(socket.request(), STATUS_REQUEST);
    assert!(output.status.success(), "{output:?}");
    assert!(names(um_home.path()).is_empty());
}

const AGENT_LOGINS: [&str; 3] = ["--check", "credentials.agent-logins", "--json"];
const DOCTOR_AGENT_LOGINS: [&str; 5] = [
    "runner",
    "doctor",
    "--check",
    "credentials.agent-logins",
    "--json",
];

/// `um runner doctor ARGUMENTS` with `PATH` naming only a Git that reports
/// 2.42.0, and `UM_HOME` and `HOME` as given: a variable given `None` is
/// removed.
fn doctor_with_homes(arguments: &[&str], um_home: Option<&Path>, home: Option<&Path>) -> Output {
    let git = fake_git("printf '%s\\n' 'git version 2.42.0'");
    let mut command = Command::new(env!("CARGO_BIN_EXE_um"));
    command
        .args(["runner", "doctor"])
        .args(arguments)
        .env("PATH", git.path());
    set_homes(&mut command, um_home, home);
    command.output().unwrap()
}

/// The one check result of a `--json` doctor report.
fn only_check(output: &Output) -> serde_json::Value {
    let report: serde_json::Value = serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|error| panic!("{error}: {output:?}"));
    let [check] = <[_; 1]>::try_from(report["checks"].as_array().unwrap().clone()).unwrap();
    check
}

// REQ-RUNNERENROLL-86
#[test]
fn doctor_agent_logins_on_an_empty_home_is_readiness_unknown_and_creates_nothing() {
    let home = ServeHome::new();
    fs::create_dir(home.um_home()).unwrap();

    let output = home.run(&DOCTOR_AGENT_LOGINS);

    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert_eq!(only_check(&output)["code"], "readiness_unknown");
    assert!(names(&home.um_home()).is_empty());
}

// REQ-RUNNERENROLL-86
#[test]
fn doctor_agent_logins_reads_the_readiness_beside_the_state_the_homes_configuration_names() {
    let home = ServeHome::new();
    let state_directory = home.root.path().join("state");
    fs::create_dir(&state_directory).unwrap();
    fs::set_permissions(&state_directory, Permissions::from_mode(0o700)).unwrap();
    home.write_development_config(&state_directory.join("state.json"));
    record_codex_readiness(&state_directory, "ready");
    // A document at the default layout's place the check must not read.
    record_codex_readiness(&home.runner(), "foreign_login");

    let output = home.run(&DOCTOR_AGENT_LOGINS);

    assert!(output.status.success(), "{output:?}");
    let check = only_check(&output);
    assert_eq!(check["code"], "ok");
    assert_eq!(check["details"]["codex"], "ready");
    assert_eq!(check["details"]["github_availability"], "not_delivered");
    assert_eq!(
        names(&home.runner()),
        BTreeSet::from([
            "config.json".to_owned(),
            "work".to_owned(),
            ".runner-agent-logins.json".to_owned()
        ])
    );
}

// REQ-RUNNERENROLL-86
#[test]
fn doctor_agent_logins_without_a_um_home_or_configuration_is_config_required() {
    let output = doctor_with_homes(&AGENT_LOGINS, None, None);

    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert_eq!(only_check(&output)["code"], "config_required");

    let um_home = tempfile::tempdir().unwrap();
    let absent = um_home.path().join("no-such-runner.json");
    let arguments = [&AGENT_LOGINS[..], &["--config", absent.to_str().unwrap()]].concat();
    let output = doctor_with_homes(&arguments, Some(um_home.path()), None);

    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert_eq!(only_check(&output)["code"], "config_invalid");
    assert!(names(um_home.path()).is_empty());
}

// REQ-RUNNERENROLL-45
#[test]
fn doctor_default_checks_and_check_list_need_no_um_home_and_create_none() {
    let root = tempfile::tempdir().unwrap();
    let absent = root.path().join("um");
    for um_home in [None, Some(absent.as_path())] {
        let output = doctor_with_homes(&["--json"], um_home, None);

        assert!(output.status.success(), "{um_home:?}: {output:?}");
        let check = only_check(&output);
        assert_eq!(check["id"], "environment.command.git", "{um_home:?}");
        assert_eq!(check["status"], "pass", "{um_home:?}");

        let output = doctor_with_homes(&["--list-checks"], um_home, None);

        assert!(output.status.success(), "{um_home:?}: {output:?}");
        assert!(
            String::from_utf8(output.stdout)
                .unwrap()
                .lines()
                .any(|line| line == "credentials.agent-logins"),
            "{um_home:?}"
        );
        assert!(!absent.exists(), "{um_home:?}");
    }
}
