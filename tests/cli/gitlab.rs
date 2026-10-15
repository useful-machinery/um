use super::*;

const ORG: &str = "acme-research";
const CONNECTION: &str = "glc_01k0z6r1w8f4jy2m7q9v3x5abc";
const CANDIDATE: &str = "glca_01k0z6r1w8f4jy2m7q9v3x5abc";
const SECRET: &str = "private-gitlab-token-sentinel";
const SESSION: &str = "human-session-fixture";

fn connection() -> serde_json::Value {
    serde_json::json!({"id":CONNECTION,"organizationId":"org_01k0z6r1w8f4jy2m7q9v3x5abc",
        "instanceUrl":"https://gitlab.com", "state":"credential_required", "credentialReason":"not-configured",
        "version":3,"providerUser":null,"credential":null,"selectedRepositoryIds":[],
        "support":{"state":"unverified","observedVersion":null},
        "createdAt":"2026-09-05T12:00:00Z","updatedAt":"2026-09-05T12:01:00Z"})
}
fn initial_connection() -> serde_json::Value {
    let mut value = connection();
    value["version"] = 1.into();
    value
}
fn active_connection() -> serde_json::Value {
    let mut value = connection();
    value["state"] = "active".into();
    value["credentialReason"] = serde_json::Value::Null;
    value["version"] = 4.into();
    value["providerUser"] = serde_json::json!({"id":"42","username":"automation-bot"});
    value["credential"] = serde_json::json!({"providerTokenId":"77","kind":"personal_access_token",
        "accountUse":"dedicated_user","permissionProfile":"api","expiresAt":"2027-09-05T00:00:00Z",
        "validatedAt":"2026-09-05T12:02:00Z","expiryNotice":"none"});
    value
}
fn disconnected_connection() -> serde_json::Value {
    let mut value = connection();
    value["state"] = "disconnected".into();
    value["credentialReason"] = serde_json::Value::Null;
    value["version"] = 4.into();
    value
}
fn group_pending() -> serde_json::Value {
    let mut value = candidate("pending");
    value["kind"] = "group_access_token".into();
    value["accountUse"] = serde_json::Value::Null;
    value
}
fn candidate(state: &str) -> serde_json::Value {
    serde_json::json!({"id":CANDIDATE,"connectionId":CONNECTION,"organizationId":"org_01k0z6r1w8f4jy2m7q9v3x5abc",
        "instanceUrl":"https://gitlab.com","initiatorId":"prn_01k0z6r1w8f4jy2m7q9v3x5abc",
        "connectionVersion":3,"authorityGeneration":1,"generation":1,"kind":"personal_access_token",
        "accountUse":"dedicated_user","state":state,"failureCode":null,"providerUserId":if state == "verified" {Some("42")} else {None},
        "providerUsername":if state == "verified" {Some("automation-bot")} else {None},
        "previousProviderUserId":null,"previousProviderUsername":null,
        "providerTokenId":if state == "verified" {Some("77")} else {None},
        "permissionProfile":if state == "verified" {Some("api")} else {None},
        "tokenExpiresAt":if state == "verified" {Some("2027-09-05T00:00:00Z")} else {None},
        "validatedAt":if state == "verified" {Some("2026-09-05T12:02:00Z")} else {None},"createdAt":"2026-09-05T12:00:00Z",
        "expiresAt":"2026-09-05T12:10:00Z","terminalAt":null})
}
fn prepared(responses: Vec<Vec<u8>>) -> (ScriptedServer, tempfile::TempDir, String) {
    let server = ScriptedServer::respond(responses);
    let dir = private_credential_directory();
    let path = dir.path().join("credentials.json");
    write_credential_fixture(&path, &server.api_url, SESSION, "2999-01-01T00:00:00Z");
    (server, dir, path.to_str().unwrap().to_owned())
}
fn run_gitlab(server: &ScriptedServer, credential: &str, args: &[&str]) -> Output {
    let mut command = vec!["connection", "gitlab"];
    command.extend_from_slice(args);
    command.extend_from_slice(&["--json", "--allow-insecure-http"]);
    run_with_env(
        &command,
        &deployment_environment(&server.api_url, credential),
    )
}
fn single(output: &Output) -> serde_json::Value {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let documents: Vec<serde_json::Value> = output
        .stdout
        .split(|b| *b == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).unwrap())
        .collect();
    assert_eq!(documents.len(), 1);
    assert_eq!(documents[0]["schemaVersion"], 1);
    assert!(!String::from_utf8_lossy(&output.stdout).contains(SECRET));
    assert!(!String::from_utf8_lossy(&output.stderr).contains(SECRET));
    documents.into_iter().next().unwrap()
}
fn request(
    request: &str,
    method: &str,
    suffix: &str,
    body: Option<serde_json::Value>,
    key: Option<&str>,
) {
    assert!(
        request.starts_with(&format!(
            "{method} /api/v1/organizations/{ORG}/connections/gitlab{suffix} "
        )),
        "{request}"
    );
    if let Some(key) = key {
        assert_eq!(header_value(request, "idempotency-key"), key);
    }
    if let Some(body) = body {
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(request.split_once("\r\n\r\n").unwrap().1)
                .unwrap(),
            body
        );
    }
}

#[test]
fn connection_and_candidate_verbs_use_exact_resources_and_explicit_bodies() {
    let (server, _dir, credential) = prepared(vec![
        json_http_response("201 Created", initial_connection()),
        json_http_response(
            "200 OK",
            serde_json::json!({"items":[connection()],"nextCursor":null}),
        ),
        json_http_response("200 OK", connection()),
        json_http_response("201 Created", candidate("pending")),
        json_http_response("200 OK", candidate("pending")),
        json_http_response("200 OK", candidate("verified")),
        json_http_response("200 OK", active_connection()),
        http_response("204 No Content", None, b""),
        json_http_response("200 OK", disconnected_connection()),
    ]);
    let dir = private_credential_directory();
    let token_path = dir.path().join("token");
    fs::write(&token_path, SECRET).unwrap();
    fs::set_permissions(&token_path, Permissions::from_mode(0o600)).unwrap();
    let file = token_path.to_str().unwrap();
    let key = ["--idempotency-key", "test-key"];
    let cases: Vec<Vec<&str>> = vec![
        vec![
            "create",
            ORG,
            "https://gitlab.com",
            "--delegator-principal-id",
            "prn_01k0z6r1w8f4jy2m7q9v3x5abc",
            key[0],
            key[1],
        ],
        vec!["list", ORG, "--limit", "2"],
        vec!["show", ORG, CONNECTION],
        vec![
            "credential",
            "create",
            ORG,
            CONNECTION,
            "--expected-version",
            "3",
            "--kind",
            "dedicated-user",
            "--token-file",
            file,
            key[0],
            key[1],
        ],
        vec!["credential", "show", ORG, CONNECTION, CANDIDATE],
        vec![
            "credential",
            "activate",
            ORG,
            CONNECTION,
            CANDIDATE,
            "--expected-provider-user-id",
            "42",
            "--dedicated-automation",
            key[0],
            key[1],
        ],
        vec![
            "credential",
            "cancel",
            ORG,
            CONNECTION,
            CANDIDATE,
            key[0],
            key[1],
        ],
        vec![
            "remove",
            ORG,
            CONNECTION,
            "--expected-version",
            "3",
            "--yes",
            key[0],
            key[1],
        ],
    ];
    // Activation's explicit review performs one GET before POST.
    for args in cases {
        let doc = single(&run_gitlab(&server, &credential, &args));
        assert!(doc["outcome"].as_str().is_some());
        if args.contains(&"cancel") {
            assert_eq!(doc["outcome"], "cancellation acknowledged");
            assert!(doc["result"].get("state").is_none());
            assert_eq!(doc["result"]["id"], CANDIDATE);
        }
    }
    let requests = server.finish();
    let suffix = format!("/{CONNECTION}");
    let candidate_suffix = format!("{suffix}/credential-candidates/{CANDIDATE}");
    request(
        &requests[0],
        "POST",
        "",
        Some(
            serde_json::json!({"instanceUrl":"https://gitlab.com","delegatorPrincipalId":"prn_01k0z6r1w8f4jy2m7q9v3x5abc"}),
        ),
        Some("test-key"),
    );
    request(&requests[1], "GET", "?limit=2", None, None);
    request(&requests[2], "GET", &suffix, None, None);
    request(
        &requests[3],
        "POST",
        &format!("{suffix}/credential-candidates"),
        Some(
            serde_json::json!({"expectedVersion":3,"kind":"personal_access_token","accountUse":"dedicated_user","token":SECRET}),
        ),
        Some("test-key"),
    );
    request(&requests[4], "GET", &candidate_suffix, None, None);
    request(&requests[5], "GET", &candidate_suffix, None, None);
    request(
        &requests[6],
        "POST",
        &format!("{candidate_suffix}/activate"),
        Some(serde_json::json!({"expectedProviderUserId":"42","dedicatedAutomation":true})),
        Some("test-key"),
    );
    request(
        &requests[7],
        "DELETE",
        &candidate_suffix,
        None,
        Some("test-key"),
    );
    request(
        &requests[8],
        "POST",
        &format!("{suffix}/disconnect"),
        Some(serde_json::json!({"expectedVersion":3})),
        Some("test-key"),
    );
}

#[test]
fn active_connection_rendering_uses_only_nonsecret_metadata() {
    let mut active = active_connection();
    // A malformed upstream extension cannot turn an unknown field into CLI output.
    active["token"] = SECRET.into();
    let (server, _dir, credential) = prepared(vec![json_http_response("200 OK", active)]);
    let output = run_with_env(
        &[
            "connection",
            "gitlab",
            "show",
            ORG,
            CONNECTION,
            "--allow-insecure-http",
        ],
        &deployment_environment(&server.api_url, &credential),
    );
    assert!(output.status.success());
    let text = String::from_utf8_lossy(&output.stdout);
    for value in [
        "automation-bot",
        "42",
        "dedicated_user",
        "2027-09-05T00:00:00Z",
    ] {
        assert!(text.contains(value));
    }
    assert!(!text.contains(SECRET));
    assert_eq!(server.finish().len(), 1);
}

#[test]
fn human_candidate_review_shows_verified_identity_and_expiry_without_secret() {
    let (server, _dir, credential) =
        prepared(vec![json_http_response("200 OK", candidate("verified"))]);
    let output = run_with_env(
        &[
            "connection",
            "gitlab",
            "credential",
            "show",
            ORG,
            CONNECTION,
            CANDIDATE,
            "--allow-insecure-http",
        ],
        &deployment_environment(&server.api_url, &credential),
    );
    assert!(output.status.success());
    let text = String::from_utf8_lossy(&output.stdout);
    for value in [
        "automation-bot",
        "42",
        "dedicated_user",
        "api",
        "2027-09-05T00:00:00Z",
    ] {
        assert!(text.contains(value));
    }
    assert!(!text.contains(SECRET));
    assert_eq!(server.finish().len(), 1);
}

#[expect(
    clippy::disallowed_methods,
    reason = "PTY prompt channel synchronizes by data; timeout only bounds a failed child"
)]
fn await_terminal_prompt(
    rx: &mpsc::Receiver<Vec<u8>>,
    child: &mut std::process::Child,
    transcript: &mut String,
    prompt: &str,
) {
    while !transcript.contains(prompt) {
        // The timeout only bounds a failed child/PTY, not prompt readiness.
        match rx.recv_timeout(Duration::from_secs(30)) {
            Ok(bytes) => transcript.push_str(&String::from_utf8_lossy(&bytes)),
            Err(error) => {
                let _ = child.kill();
                let status = child.wait();
                let mut stderr = String::new();
                let _ = child.stderr.take().unwrap().read_to_string(&mut stderr);
                panic!(
                    "terminal prompt unavailable: {error}; status {status:?}; stderr {stderr}; {transcript}"
                );
            }
        }
    }
}

#[test]
fn terminal_review_requires_exact_identity_and_dedication_before_activation() {
    for (id, attestation, exit, posts) in [
        ("41", None, 8, 0),
        ("42", Some("not dedicated\n"), 2, 0),
        ("42", Some("dedicated automation\n"), 0, 1),
    ] {
        let mut responses = vec![json_http_response("200 OK", candidate("verified"))];
        if posts == 1 {
            responses.push(json_http_response("200 OK", active_connection()));
        }
        let (server, _dir, credential) = prepared(responses);
        let pty = open_test_pty(None).unwrap();
        let peer = rustix::pty::ptsname(&pty.master, Vec::new()).unwrap();
        let mut writer = fs::File::from(rustix::io::dup(&pty.master).unwrap());
        // The CLI test environment includes Python. Its child makes a new session
        // and opens the slave as controlling tty before execing the actual CLI.
        let mut command = Command::new("python3");
        command.args(["-c", "import os,sys; os.setsid(); fd=os.open(sys.argv[1], os.O_RDWR); os.dup2(fd, 0); os.write(1, b'R'); os.execv(sys.argv[2], sys.argv[2:])"])
            .arg(peer.to_str().unwrap())
            .arg(env!("CARGO_BIN_EXE_um"))
            .args(["connection", "gitlab", "credential", "activate", ORG,
                CONNECTION, CANDIDATE, "--allow-insecure-http"])
            .stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
        for variable in DEPLOYMENT_VARIABLES {
            command.env_remove(variable);
        }
        command.env_remove(CREDENTIALS_FILE_VARIABLE);
        for (name, value) in deployment_environment(&server.api_url, &credential) {
            command.env(name, value);
        }
        let mut child = command.spawn().unwrap();
        let mut ready = [0];
        child
            .stdout
            .as_mut()
            .unwrap()
            .read_exact(&mut ready)
            .unwrap();
        assert_eq!(ready, *b"R");
        let (tx, rx) = mpsc::channel();
        let reader = thread::spawn(move || {
            let mut master = fs::File::from(pty.master);
            let mut buf = [0u8; 512];
            while let Ok(n) = master.read(&mut buf) {
                if n == 0 || tx.send(buf[..n].to_vec()).is_err() {
                    break;
                }
            }
        });
        drop(pty.slave);
        let mut transcript = String::new();
        await_terminal_prompt(
            &rx,
            &mut child,
            &mut transcript,
            "Enter the verified provider user ID: ",
        );
        assert!(transcript.contains("automation-bot (42)"));
        assert!(transcript.contains("2027-09-05T00:00:00Z"));
        writer.write_all(format!("{id}\n").as_bytes()).unwrap();
        if let Some(attestation) = attestation {
            await_terminal_prompt(
                &rx,
                &mut child,
                &mut transcript,
                "Type 'dedicated automation'",
            );
            writer.write_all(attestation.as_bytes()).unwrap();
        }
        let output = child.wait_with_output().unwrap();
        drop(writer);
        reader.join().unwrap();
        assert_eq!(output.status.code(), Some(exit), "{output:?}");
        assert!(!transcript.contains(SECRET));
        assert!(!String::from_utf8_lossy(&output.stderr).contains(SECRET));
        assert_eq!(server.finish().len(), 1 + posts);
    }
}

#[test]
fn mismatched_resource_response_cannot_authorize_identity_review() {
    let mut wrong = candidate("verified");
    wrong["connectionId"] = "glc_01k0z6r1w8f4jy2m7q9v3x5abd".into();
    let (server, _dir, credential) = prepared(vec![json_http_response("200 OK", wrong)]);
    let output = run_gitlab(
        &server,
        &credential,
        &[
            "credential",
            "activate",
            ORG,
            CONNECTION,
            CANDIDATE,
            "--expected-provider-user-id",
            "42",
            "--dedicated-automation",
        ],
    );
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert_eq!(server.finish().len(), 1);
}

#[test]
fn help_links_public_setup_and_requires_explicit_activation_flags() {
    for args in [
        vec!["connection", "gitlab", "--help"],
        vec!["connection", "gitlab", "credential", "activate", "--help"],
    ] {
        let output = run(&args);
        assert!(output.status.success());
        assert!(
            String::from_utf8_lossy(&output.stdout)
                .contains("https://docs.usefulmachinery.com/agent/gitlab-setup.md")
        );
    }
    let output = run(&[
        "connection",
        "gitlab",
        "credential",
        "activate",
        ORG,
        CONNECTION,
        CANDIDATE,
        "--expected-provider-user-id",
        "42",
    ]);
    assert_eq!(output.status.code(), Some(2));
}

#[test]
fn authentication_and_candidate_conflicts_have_distinct_exits() {
    let problem = |status, code, kind| {
        problem_http_response(
            status,
            serde_json::json!({
        "type":format!("https://api.usefulmachinery.com/problems/{kind}"),"title":"Rejected", "status":code,"detail":SECRET}),
        )
    };
    let (server, dir, credential) = prepared(vec![
        problem("401 Unauthorized", 401, "unauthorized"),
        problem("409 Conflict", 409, "candidate-conflict"),
        problem("503 Service Unavailable", 503, "service-unavailable"),
    ]);
    let service_path = dir.path().join("service.key");
    fs::write(
        &service_path,
        "crd_01k0z6r1w8f4jy2m7q9v3x5abc.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n",
    )
    .unwrap();
    fs::set_permissions(&service_path, Permissions::from_mode(0o600)).unwrap();
    for (args, exit) in [
        (vec!["show", ORG, CONNECTION], 3),
        (vec!["credential", "cancel", ORG, CONNECTION, CANDIDATE], 6),
        (vec!["list", ORG], 4),
    ] {
        let args = [
            args,
            vec!["--service-api-key-file", service_path.to_str().unwrap()],
        ]
        .concat();
        let output = run_gitlab(&server, &credential, &args);
        assert_eq!(output.status.code(), Some(exit));
        assert!(output.stdout.is_empty());
        assert!(!String::from_utf8_lossy(&output.stderr).contains(SECRET));
    }
    assert_eq!(server.finish().len(), 3);
}

#[test]
fn credential_kinds_select_only_the_approved_token_declaration() {
    for (flag, kind, account) in [
        ("group-access-token", "group_access_token", None),
        (
            "service-account",
            "personal_access_token",
            Some("service_account"),
        ),
        (
            "dedicated-user",
            "personal_access_token",
            Some("dedicated_user"),
        ),
    ] {
        let mut response = candidate("pending");
        response["kind"] = kind.into();
        response["accountUse"] = account.into();
        let (server, dir, credential) = prepared(vec![json_http_response("201 Created", response)]);
        let token = dir.path().join("token");
        fs::write(&token, SECRET).unwrap();
        fs::set_permissions(&token, Permissions::from_mode(0o600)).unwrap();
        let output = run_gitlab(
            &server,
            &credential,
            &[
                "credential",
                "create",
                ORG,
                CONNECTION,
                "--expected-version",
                "3",
                "--kind",
                flag,
                "--token-file",
                token.to_str().unwrap(),
            ],
        );
        single(&output);
        let requests = server.finish();
        let body: serde_json::Value =
            serde_json::from_str(requests[0].split_once("\r\n\r\n").unwrap().1).unwrap();
        assert_eq!(body["kind"], kind);
        assert_eq!(body["accountUse"].as_str(), account);
        assert_eq!(body["expectedVersion"], 3);
        assert_eq!(body["token"], SECRET);
        if account.is_none() {
            assert!(body.get("accountUse").is_none());
        }
    }
}

#[test]
fn unsafe_token_sources_and_versions_fail_before_network_use() {
    use std::os::unix::fs::symlink;
    let (server, dir, credential) = prepared(vec![]);
    let token = dir.path().join("token");
    fs::write(&token, SECRET).unwrap();
    let alias = dir.path().join("alias");
    symlink(&token, &alias).unwrap();
    for (path, mode) in [(&token, 0o400), (&token, 0o644), (&alias, 0o600)] {
        fs::set_permissions(&token, Permissions::from_mode(mode)).unwrap();
        let args = [
            "credential",
            "create",
            ORG,
            CONNECTION,
            "--expected-version",
            "3",
            "--kind",
            "group-access-token",
            "--token-file",
            path.to_str().unwrap(),
        ];
        let output = run_gitlab(&server, &credential, &args);
        assert_eq!(
            output.status.code(),
            Some(2),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(!String::from_utf8_lossy(&output.stderr).contains(SECRET));
    }
    fs::write(&token, vec![b'x'; 16386]).unwrap();
    fs::set_permissions(&token, Permissions::from_mode(0o600)).unwrap();
    for version in ["0", "-1", "9007199254740992"] {
        let output = run_gitlab(
            &server,
            &credential,
            &[
                "credential",
                "create",
                ORG,
                CONNECTION,
                "--expected-version",
                version,
                "--kind",
                "group-access-token",
                "--token-file",
                token.to_str().unwrap(),
            ],
        );
        assert_eq!(output.status.code(), Some(2));
        let output = run_gitlab(
            &server,
            &credential,
            &[
                "remove",
                ORG,
                CONNECTION,
                "--expected-version",
                version,
                "--yes",
            ],
        );
        assert_eq!(output.status.code(), Some(2));
    }
    let output = run_gitlab(
        &server,
        &credential,
        &[
            "credential",
            "create",
            ORG,
            CONNECTION,
            "--expected-version",
            "3",
            "--kind",
            "group-access-token",
            "--token-file",
            token.to_str().unwrap(),
        ],
    );
    assert_eq!(output.status.code(), Some(2));
    let output = run_gitlab(
        &server,
        &credential,
        &["credential", "activate", ORG, CONNECTION, CANDIDATE],
    );
    assert_eq!(output.status.code(), Some(2));
    assert!(server.finish().is_empty());
}

#[test]
fn a_lost_mutation_response_replays_the_same_key_and_body() {
    let dir = private_credential_directory();
    let token = dir.path().join("token");
    fs::write(&token, SECRET).unwrap();
    fs::set_permissions(&token, Permissions::from_mode(0o600)).unwrap();
    let cases: Vec<(Vec<&str>, Vec<Vec<u8>>)> = vec![
        (
            vec!["create", ORG, "https://gitlab.com"],
            vec![json_http_response("201 Created", initial_connection())],
        ),
        (
            vec![
                "remove",
                ORG,
                CONNECTION,
                "--expected-version",
                "3",
                "--yes",
            ],
            vec![json_http_response("200 OK", disconnected_connection())],
        ),
        (
            vec![
                "credential",
                "create",
                ORG,
                CONNECTION,
                "--expected-version",
                "3",
                "--kind",
                "group-access-token",
                "--token-file",
                token.to_str().unwrap(),
            ],
            vec![json_http_response("201 Created", group_pending())],
        ),
        (
            vec!["credential", "cancel", ORG, CONNECTION, CANDIDATE],
            vec![http_response("204 No Content", None, b"")],
        ),
        (
            vec![
                "credential",
                "activate",
                ORG,
                CONNECTION,
                CANDIDATE,
                "--expected-provider-user-id",
                "42",
                "--dedicated-automation",
            ],
            vec![
                json_http_response("200 OK", candidate("verified")),
                json_http_response("200 OK", active_connection()),
            ],
        ),
    ];
    for (args, mut responses) in cases {
        // A closed socket with no HTTP response is an ambiguous transport result.
        let mut script = if args.contains(&"activate") {
            vec![responses.remove(0)]
        } else {
            Vec::new()
        };
        script.push(Vec::new());
        script.extend(responses);
        let (server, _dir, credential) = prepared(script);
        let args = [args, vec!["--idempotency-key", "replay-key"]].concat();
        single(&run_gitlab(&server, &credential, &args));
        let requests = server.finish();
        let writes = if args.contains(&"activate") {
            &requests[1..]
        } else {
            &requests[..]
        };
        assert_eq!(writes.len(), 2);
        assert_eq!(header_value(&writes[0], "idempotency-key"), "replay-key");
        assert_eq!(
            writes[0].split_once("\r\n\r\n").unwrap().1,
            writes[1].split_once("\r\n\r\n").unwrap().1
        );
        assert_eq!(
            writes[0].split_once(" HTTP/").unwrap().0,
            writes[1].split_once(" HTTP/").unwrap().0
        );
    }
}

#[test]
fn generated_key_survives_two_lost_responses_and_recovers_on_later_invocation() {
    let (server, _dir, credential) = prepared(vec![
        Vec::new(),
        Vec::new(),
        json_http_response("201 Created", initial_connection()),
    ]);
    let output = run_gitlab(&server, &credential, &["create", ORG, "https://gitlab.com"]);
    assert_eq!(output.status.code(), Some(4));
    assert!(output.stdout.is_empty());
    let diagnostic = String::from_utf8_lossy(&output.stderr);
    let key = diagnostic
        .lines()
        .find_map(|line| line.strip_prefix("request key: "))
        .unwrap();
    assert!(!key.is_empty());
    assert!(!diagnostic.contains(SECRET));
    let doc = single(&run_gitlab(
        &server,
        &credential,
        &[
            "create",
            ORG,
            "https://gitlab.com",
            "--idempotency-key",
            key,
        ],
    ));
    assert_eq!(doc["idempotencyKey"], key);
    let requests = server.finish();
    assert_eq!(requests.len(), 3);
    for request_text in &requests {
        request(
            request_text,
            "POST",
            "",
            Some(serde_json::json!({"instanceUrl":"https://gitlab.com"})),
            Some(key),
        );
    }
}

#[test]
fn explicit_activation_replay_recovers_after_terminal_or_missing_candidate() {
    let missing = problem_http_response(
        "404 Not Found",
        serde_json::json!({"type":"https://api.usefulmachinery.com/problems/not-found","title":"Not found","status":404}),
    );
    for terminal in [
        json_http_response("200 OK", candidate("activated")),
        missing,
    ] {
        let (server, _dir, credential) = prepared(vec![
            json_http_response("200 OK", candidate("verified")),
            Vec::new(),
            Vec::new(),
            terminal,
            json_http_response("200 OK", active_connection()),
        ]);
        let args = [
            "credential",
            "activate",
            ORG,
            CONNECTION,
            CANDIDATE,
            "--expected-provider-user-id",
            "42",
            "--dedicated-automation",
            "--idempotency-key",
            "activation-replay-key",
        ];
        let first = run_gitlab(&server, &credential, &args);
        assert_eq!(first.status.code(), Some(4));
        assert!(first.stdout.is_empty());
        assert!(String::from_utf8_lossy(&first.stderr).contains("activation-replay-key"));
        let replay = single(&run_gitlab(&server, &credential, &args));
        assert_eq!(replay["result"]["state"], "active");
        let calls = server.finish();
        assert_eq!(calls.len(), 5);
        for i in [1, 2, 4] {
            request(
                &calls[i],
                "POST",
                &format!("/{CONNECTION}/credential-candidates/{CANDIDATE}/activate"),
                Some(serde_json::json!({"expectedProviderUserId":"42","dedicatedAutomation":true})),
                Some("activation-replay-key"),
            );
        }
    }
}

#[test]
fn cancellation_acknowledges_an_already_activated_candidate_without_claiming_a_state_change() {
    let (server, _dir, credential) = prepared(vec![
        http_response("204 No Content", None, b""),
        json_http_response("200 OK", active_connection()),
    ]);
    let cancelled = single(&run_gitlab(
        &server,
        &credential,
        &["credential", "cancel", ORG, CONNECTION, CANDIDATE],
    ));
    assert_eq!(cancelled["outcome"], "cancellation acknowledged");
    assert_eq!(
        cancelled["result"],
        serde_json::json!({"id":CANDIDATE,"connectionId":CONNECTION})
    );
    let active = single(&run_gitlab(
        &server,
        &credential,
        &["show", ORG, CONNECTION],
    ));
    assert_eq!(active["result"]["state"], "active");
    assert_eq!(server.finish().len(), 2);
}

#[test]
fn vanished_candidate_is_unavailable_for_activation() {
    let missing = problem_http_response(
        "404 Not Found",
        serde_json::json!({
        "type":"https://api.usefulmachinery.com/problems/not-found",
        "title":"Not found","status":404}),
    );
    for script in [
        vec![missing.clone()],
        vec![json_http_response("200 OK", candidate("activated"))],
        vec![json_http_response("200 OK", candidate("verified")), missing],
    ] {
        let expected_calls = script.len();
        let (server, _dir, credential) = prepared(script);
        let output = run_gitlab(
            &server,
            &credential,
            &[
                "credential",
                "activate",
                ORG,
                CONNECTION,
                CANDIDATE,
                "--expected-provider-user-id",
                "42",
                "--dedicated-automation",
            ],
        );
        assert_eq!(output.status.code(), Some(7));
        assert!(output.stdout.is_empty());
        assert_eq!(server.finish().len(), expected_calls);
    }
    let (server, _dir, credential) =
        prepared(vec![json_http_response("200 OK", candidate("pending"))]);
    let output = run_gitlab(
        &server,
        &credential,
        &[
            "credential",
            "activate",
            ORG,
            CONNECTION,
            CANDIDATE,
            "--expected-provider-user-id",
            "42",
            "--dedicated-automation",
            "--idempotency-key",
            "pending-is-not-a-replay",
        ],
    );
    assert_eq!(output.status.code(), Some(7));
    assert_eq!(server.finish().len(), 1);
}

#[test]
fn candidate_submission_and_activation_conflicts_do_not_retry() {
    let conflict = problem_http_response(
        "409 Conflict",
        serde_json::json!({
        "type":"https://api.usefulmachinery.com/problems/candidate-conflict",
        "title":"Conflict","status":409,"detail":"candidate changed"}),
    );
    let (server, dir, credential) = prepared(vec![conflict.clone()]);
    let token = dir.path().join("token");
    fs::write(&token, SECRET).unwrap();
    fs::set_permissions(&token, Permissions::from_mode(0o600)).unwrap();
    let output = run_gitlab(
        &server,
        &credential,
        &[
            "credential",
            "create",
            ORG,
            CONNECTION,
            "--expected-version",
            "3",
            "--kind",
            "service-account",
            "--token-file",
            token.to_str().unwrap(),
        ],
    );
    assert_eq!(output.status.code(), Some(6));
    assert!(output.stdout.is_empty());
    assert!(!String::from_utf8_lossy(&output.stderr).contains(SECRET));
    assert_eq!(server.finish().len(), 1);

    let (server, _dir, credential) = prepared(vec![
        json_http_response("200 OK", candidate("verified")),
        conflict,
    ]);
    let output = run_gitlab(
        &server,
        &credential,
        &[
            "credential",
            "activate",
            ORG,
            CONNECTION,
            CANDIDATE,
            "--expected-provider-user-id",
            "42",
            "--dedicated-automation",
        ],
    );
    assert_eq!(output.status.code(), Some(6));
    assert!(output.stdout.is_empty());
    assert_eq!(server.finish().len(), 2);
}

#[test]
fn token_stdin_is_exclusive_and_never_printed() {
    let (server, _dir, credential) =
        prepared(vec![json_http_response("201 Created", group_pending())]);
    let args = [
        "connection",
        "gitlab",
        "credential",
        "create",
        ORG,
        CONNECTION,
        "--expected-version",
        "3",
        "--kind",
        "group-access-token",
        "--token-file",
        "-",
        "--json",
        "--allow-insecure-http",
    ];
    let output = run_with_stdin(
        &args,
        &deployment_environment(&server.api_url, &credential),
        SECRET.as_bytes(),
    );
    assert_eq!(single(&output)["result"]["state"], "pending");
    let request = server.finish();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(request[0].split_once("\r\n\r\n").unwrap().1)
            .unwrap()["token"],
        SECRET
    );
    let (server, _dir, credential) = prepared(vec![]);
    let args = [args.as_slice(), &["--service-api-key-file", "-"]].concat();
    let output = run_with_stdin(
        &args,
        &deployment_environment(&server.api_url, &credential),
        SECRET.as_bytes(),
    );
    assert_eq!(output.status.code(), Some(2));
    assert!(server.finish().is_empty());
}

#[test]
fn signal_after_dispatch_reports_replay_key_without_secret() {
    for (signal, code) in [
        (rustix::process::Signal::INT, 130),
        (rustix::process::Signal::TERM, 143),
    ] {
        let mut server =
            ScriptedServer::respond_with_paused_first_response(vec![json_http_response(
                "201 Created",
                group_pending(),
            )]);
        let directory = private_credential_directory();
        let path = directory.path().join("credentials.json");
        write_credential_fixture(&path, &server.api_url, SESSION, "2999-01-01T00:00:00Z");
        let token = directory.path().join("token");
        fs::write(&token, SECRET).unwrap();
        fs::set_permissions(&token, Permissions::from_mode(0o600)).unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_um"));
        command
            .args([
                "connection",
                "gitlab",
                "credential",
                "create",
                ORG,
                CONNECTION,
                "--expected-version",
                "3",
                "--kind",
                "group-access-token",
                "--token-file",
                token.to_str().unwrap(),
                "--idempotency-key",
                "signal-key",
                "--json",
                "--allow-insecure-http",
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for variable in DEPLOYMENT_VARIABLES {
            command.env_remove(variable);
        }
        command.env_remove(CREDENTIALS_FILE_VARIABLE);
        for (name, value) in deployment_environment(&server.api_url, path.to_str().unwrap()) {
            command.env(name, value);
        }
        let child = command.spawn().unwrap();
        let request = server.wait_for_request();
        assert_eq!(header_value(&request, "idempotency-key"), "signal-key");
        rustix::process::kill_process(
            rustix::process::Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap(),
            signal,
        )
        .unwrap();
        let output = child.wait_with_output().unwrap();
        server.release_paused_response();
        assert_eq!(output.status.code(), Some(code));
        assert!(output.stdout.is_empty());
        assert!(String::from_utf8_lossy(&output.stderr).contains("signal-key"));
        assert!(!String::from_utf8_lossy(&output.stderr).contains(SECRET));
        assert!(server.finish().is_empty());
    }
}

#[test]
fn pending_identity_mismatch_and_conflict_have_separate_exits() {
    let conflict = problem_http_response(
        "409 Conflict",
        serde_json::json!({"type":"https://api.usefulmachinery.com/problems/connection-version-conflict","title":"Conflict","status":409,"detail":"stale"}),
    );
    let (server, _dir, credential) = prepared(vec![
        json_http_response("200 OK", candidate("pending")),
        json_http_response("200 OK", candidate("verified")),
        conflict,
    ]);
    for (expected, code) in [("42", 7), ("41", 8)] {
        let output = run_gitlab(
            &server,
            &credential,
            &[
                "credential",
                "activate",
                ORG,
                CONNECTION,
                CANDIDATE,
                "--expected-provider-user-id",
                expected,
                "--dedicated-automation",
            ],
        );
        assert_eq!(output.status.code(), Some(code));
        assert!(output.stdout.is_empty());
    }
    let output = run_gitlab(
        &server,
        &credential,
        &[
            "remove",
            ORG,
            CONNECTION,
            "--expected-version",
            "3",
            "--yes",
        ],
    );
    assert_eq!(output.status.code(), Some(6));
    assert!(output.stdout.is_empty());
    assert_eq!(server.finish().len(), 3);
}
