use super::*;

const TOKEN: &str = "unique-agent-login-session-token";
const AGENT_LOGIN_ID: &str = "agl_01k0z6r1w8f4jy2m7q9v3x5abc";
const SECOND_AGENT_LOGIN_ID: &str = "agl_01k0z6r1w8f4jy2m7q9v3x5abd";
const ORGANIZATION_ID: &str = "org_01k0z6r1w8f4jy2m7q9v3x5abc";
const RUNNER_ID: &str = "rnr_01k0z6r1w8f4jy2m7q9v3x5abc";
const SECOND_RUNNER_ID: &str = "rnr_01k0z6r1w8f4jy2m7q9v3x5abd";
const SENTINEL: &str = "private-agent-login-material-sentinel";

fn prepared(responses: Vec<Vec<u8>>) -> (ScriptedServer, tempfile::TempDir, std::path::PathBuf) {
    let server = ScriptedServer::respond(responses);
    let directory = private_credential_directory();
    let path = directory.path().join("credentials.json");
    write_credential_fixture_for_deployment(
        &path,
        &server.api_url,
        &server.issuer,
        TOKEN,
        "2999-01-01T00:00:00Z",
    );
    (server, directory, path)
}

fn run_agent_login(
    server: &ScriptedServer,
    credential: &std::path::Path,
    args: &[&str],
    extra: &[(&str, &str)],
) -> Output {
    let mut arguments = vec!["auth", "agent-login"];
    arguments.extend_from_slice(args);
    arguments.push("--allow-insecure-http");
    let mut environment = deployment_environment_with_issuer(
        &server.api_url,
        &server.issuer,
        credential.to_str().unwrap(),
    )
    .to_vec();
    environment.extend_from_slice(extra);
    run_with_env(&arguments, &environment)
}

fn run_agent_login_with_stdin(
    server: &ScriptedServer,
    credential: &std::path::Path,
    args: &[&str],
    standard_input: &[u8],
) -> Output {
    let mut arguments = vec!["auth", "agent-login"];
    arguments.extend_from_slice(args);
    arguments.push("--allow-insecure-http");
    let environment = deployment_environment_with_issuer(
        &server.api_url,
        &server.issuer,
        credential.to_str().unwrap(),
    );
    run_with_stdin(&arguments, &environment, standard_input)
}

// The agent logins as the API presents them: a Codex login with one authorization that was
// delivered and reported and one that never asked, and a revoked Claude Code login.
fn agent_logins() -> serde_json::Value {
    serde_json::json!([
        {
            "id": AGENT_LOGIN_ID,
            "organizationId": ORGANIZATION_ID,
            "provider": "codex",
            "label": "build laptop",
            "state": "active",
            "generation": 2,
            "recordSchemaVersion": 1,
            "keyVersion": 1,
            "createdAt": "2026-11-01T09:00:00Z",
            "importedAt": "2026-11-02T10:00:00Z",
            "authorizations": [
                {
                    "id": "aga_01k0z6r1w8f4jy2m7q9v3x5abc",
                    "runnerId": RUNNER_ID,
                    "createdAt": "2026-11-01T09:05:00Z",
                    "delivery": {"generation": 2, "deliveredAt": "2026-11-02T10:01:00Z"},
                    "report": {
                        "state": "ready",
                        "generation": 2,
                        "reportedAt": "2026-11-02T10:02:00Z"
                    }
                },
                {
                    "id": "aga_01k0z6r1w8f4jy2m7q9v3x5abd",
                    "runnerId": SECOND_RUNNER_ID,
                    "createdAt": "2026-11-03T09:05:00Z"
                }
            ]
        },
        {
            "id": SECOND_AGENT_LOGIN_ID,
            "organizationId": ORGANIZATION_ID,
            "provider": "claude_code",
            "state": "revoked",
            "generation": 1,
            "recordSchemaVersion": 1,
            "keyVersion": 1,
            "createdAt": "2026-11-04T09:00:00Z",
            "importedAt": "2026-11-04T09:00:00Z",
            "revokedAt": "2026-11-05T09:00:00Z",
            "authorizations": []
        }
    ])
}

// A material member stands for any field a response might wrongly carry.
fn with_material(value: &serde_json::Value) -> serde_json::Value {
    let mut value = value.clone();
    match value.as_array_mut() {
        Some(items) => items
            .iter_mut()
            .for_each(|item| item["material"] = SENTINEL.into()),
        None => value["material"] = SENTINEL.into(),
    }
    value
}

fn page(items: serde_json::Value, next_cursor: Option<&str>) -> Vec<u8> {
    let mut body = serde_json::json!({"items": items});
    if let Some(cursor) = next_cursor {
        body["nextCursor"] = cursor.into();
    }
    json_http_response("200 OK", body)
}

fn agent_login_problem(status: &str, code: u16, slug: &str) -> Vec<u8> {
    problem_http_response(
        status,
        serde_json::json!({
            "type": format!("https://api.usefulmachinery.com/problems/{slug}"),
            "title": "Agent login result",
            "status": code
        }),
    )
}

fn revoked() -> Vec<u8> {
    http_response_with_headers(
        "204 No Content",
        None,
        &[("Idempotency-Key", ECHO_IDEMPOTENCY_KEY)],
        b"",
    )
}

// `login` as `list` and `show` print it: each authorization in force with its availability and
// next action (REQ-AGENTLOGIN-43), `statuses` in order.
fn with_statuses(mut login: serde_json::Value, statuses: &[(&str, &str)]) -> serde_json::Value {
    let authorizations = login["authorizations"].as_array_mut().unwrap();
    assert_eq!(authorizations.len(), statuses.len());
    for (authorization, (availability, next_action)) in authorizations.iter_mut().zip(statuses) {
        authorization["availability"] = (*availability).into();
        authorization["nextAction"] = (*next_action).into();
    }
    login
}

// The Codex login's authorizations in force: `ready` at its current generation with no last use,
// and one whose runner has not yet asked.
const CODEX_STATUSES: [(&str, &str); 2] = [("unverified", "none"), ("pending", "wait_for_runner")];

fn json_output(output: &Output) -> serde_json::Value {
    serde_json::from_slice(&output.stdout).unwrap()
}

// A login in the member's ambient agent homes, which no human command may read.
const AMBIENT_LOGIN_MARKER: &str = "ambient-agent-home-login-marker";

// REQ-AGENTLOGIN-19
// INV-CLI-13
#[test]
fn list_prints_each_login_with_its_authorizations_and_never_reads_an_ambient_agent_home() {
    // Each ambient home holds a login carrying the marker and admits no one but root, so a
    // read fails with a permission error, and a read that succeeds would have to keep the
    // marker out of everything the command prints and sends.
    let home = tempfile::tempdir().unwrap();
    let ambient = [(".codex", "auth.json"), (".claude", ".credentials.json")];
    for (directory, file) in ambient {
        let directory = home.path().join(directory);
        fs::create_dir(&directory).unwrap();
        fs::write(directory.join(file), AMBIENT_LOGIN_MARKER).unwrap();
        fs::set_permissions(&directory, Permissions::from_mode(0o000)).unwrap();
    }
    let codex_home = home.path().join(".codex");
    let claude_config = home.path().join(".claude");
    let (server, _directory, credential) =
        prepared(vec![page(with_material(&agent_logins()), None)]);
    let output = run_agent_login(
        &server,
        &credential,
        &["list"],
        &[
            ("HOME", home.path().to_str().unwrap()),
            ("CODEX_HOME", codex_home.to_str().unwrap()),
            ("CLAUDE_CONFIG_DIR", claude_config.to_str().unwrap()),
        ],
    );
    for (directory, _) in ambient {
        fs::set_permissions(home.path().join(directory), Permissions::from_mode(0o700)).unwrap();
    }

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_no_secret_output(&output, &[SENTINEL, TOKEN, AMBIENT_LOGIN_MARKER]);
    let text = String::from_utf8(output.stdout).unwrap();
    for line in [
        format!("agent login: {AGENT_LOGIN_ID}"),
        format!("organization: {ORGANIZATION_ID}"),
        "provider: codex".to_owned(),
        "label: build laptop".to_owned(),
        "state: active".to_owned(),
        "generation: 2".to_owned(),
        "imported: 2026-11-02T10:00:00Z".to_owned(),
        format!("runner: {RUNNER_ID}"),
        "delivered: generation 2 at 2026-11-02T10:01:00Z".to_owned(),
        "reported: ready, generation 2 at 2026-11-02T10:02:00Z".to_owned(),
        format!("runner: {SECOND_RUNNER_ID}"),
        "delivered: never".to_owned(),
        "reported: never".to_owned(),
        format!("agent login: {SECOND_AGENT_LOGIN_ID}"),
        "provider: claude_code".to_owned(),
        "state: revoked".to_owned(),
        "revoked: 2026-11-05T09:00:00Z".to_owned(),
    ] {
        assert!(
            text.lines().any(|candidate| candidate.trim() == line),
            "missing {line:?} in\n{text}"
        );
    }
    let request = server.finish().pop().unwrap();
    assert!(request.starts_with("GET /api/v1/me/agent-logins HTTP/1.1\r\n"));
    assert_eq!(
        header_value(&request, "authorization"),
        format!("Bearer {TOKEN}")
    );
    assert!(!request.contains(SENTINEL));
    assert!(!request.contains(AMBIENT_LOGIN_MARKER));
    for (directory, file) in ambient {
        let directory = home.path().join(directory);
        assert_eq!(
            fs::read_dir(&directory).unwrap().count(),
            1,
            "{directory:?}"
        );
        assert_eq!(
            fs::read_to_string(directory.join(file)).unwrap(),
            AMBIENT_LOGIN_MARKER
        );
    }
}

// REQ-AGENTLOGIN-19
#[test]
fn list_json_prints_one_page_with_its_cursor_and_passes_the_page_options() {
    let (server, _directory, credential) = prepared(vec![page(
        with_material(&agent_logins()),
        Some("opaque-next-page"),
    )]);
    let output = run_agent_login(
        &server,
        &credential,
        &["list", "--limit", "2", "--cursor", "page-before", "--json"],
        &[],
    );

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        json_output(&output),
        serde_json::json!({
            "schemaVersion": 1,
            "deployment": server.api_url,
            "outcome": "listed",
            "items": [
                with_statuses(agent_logins()[0].clone(), &CODEX_STATUSES),
                agent_logins()[1]
            ],
            "nextCursor": "opaque-next-page"
        })
    );
    assert!(output.stderr.is_empty());
    assert_no_secret_output(&output, &[SENTINEL]);
    let request = server.finish().pop().unwrap();
    assert!(
        request.starts_with("GET /api/v1/me/agent-logins?limit=2&cursor=page-before HTTP/1.1\r\n")
    );
}

// REQ-AGENTLOGIN-19
#[test]
fn show_reads_one_agent_login() {
    let login = agent_logins()[0].clone();
    let (server, _directory, credential) =
        prepared(vec![json_http_response("200 OK", with_material(&login))]);
    let output = run_agent_login(
        &server,
        &credential,
        &["show", AGENT_LOGIN_ID, "--json"],
        &[],
    );

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        json_output(&output),
        serde_json::json!({
            "schemaVersion": 1,
            "deployment": server.api_url,
            "outcome": "shown",
            "agentLogin": with_statuses(login, &CODEX_STATUSES)
        })
    );
    assert_no_secret_output(&output, &[SENTINEL]);
    let request = server.finish().pop().unwrap();
    assert!(request.starts_with(&format!(
        "GET /api/v1/me/agent-logins/{AGENT_LOGIN_ID} HTTP/1.1\r\n"
    )));
    assert_eq!(
        header_value(&request, "authorization"),
        format!("Bearer {TOKEN}")
    );
}

const THIRD_RUNNER_ID: &str = "rnr_01k0z6r1w8f4jy2m7q9v3x5abf";
const ENDED_AUTHORIZATION_ID: &str = "aga_01k0z6r1w8f4jy2m7q9v3x5abf";

// An ended authorization of `runner_id` (REQ-AGENTLOGIN-3) whose removal is `removal`
// (REQ-AGENTLOGIN-37).
fn ended_authorization(
    id: &str,
    runner_id: &str,
    delivered: bool,
    removal: serde_json::Value,
) -> serde_json::Value {
    let mut ended = serde_json::json!({
        "id": id,
        "runnerId": runner_id,
        "createdAt": "2026-11-01T09:10:00Z",
        "endedAt": "2026-11-04T09:00:00Z",
        "removal": removal
    });
    if delivered {
        ended["delivery"] =
            serde_json::json!({"generation": 1, "deliveredAt": "2026-11-01T09:11:00Z"});
        ended["report"] = serde_json::json!({
            "state": "ready",
            "generation": 1,
            "lastUse": "accepted",
            "reportedAt": "2026-11-01T09:12:00Z"
        });
    }
    ended
}

const RUN_ID: &str = "run_01k0z6r1w8f4jy2m7q9v3x5abc";
const CANCELLATION_REQUEST_ID: &str = "cmd_01k0z6r1w8f4jy2m7q9v3x5abc";

// `ended` with the stop Cloud requested of the run its runner was serving (REQ-AGENTLOGIN-38).
fn with_stop(mut ended: serde_json::Value) -> serde_json::Value {
    ended["stop"] = serde_json::json!({
        "runId": RUN_ID,
        "cancellationRequestId": CANCELLATION_REQUEST_ID,
        "requestedAt": "2026-11-04T09:00:01Z"
    });
    ended
}

// The Codex login as a Cloud that lists ended authorizations shows it: the runner in force last
// stated a refused use, and a third runner's delivered authorization has ended, the run it was
// serving was asked to stop, and that runner has not confirmed removing its copy.
fn with_ended_authorization() -> serde_json::Value {
    let mut login = agent_logins()[0].clone();
    login["authorizations"][0]["report"]["lastUse"] = "refused".into();
    login["endedAuthorizations"] = serde_json::json!([with_stop(ended_authorization(
        ENDED_AUTHORIZATION_ID,
        THIRD_RUNNER_ID,
        true,
        serde_json::json!({"state": "pending"}),
    ))]);
    login
}

fn has_line(text: &str, line: &str) -> bool {
    text.lines().any(|candidate| candidate.trim() == line)
}

// REQ-AGENTLOGIN-19
// REQ-AGENTLOGIN-38
#[test]
fn show_prints_ended_authorizations_with_their_removal_stop_and_a_reports_last_use() {
    let login = with_ended_authorization();
    let (server, _directory, credential) = prepared(vec![
        json_http_response("200 OK", with_material(&login)),
        json_http_response("200 OK", with_material(&login)),
    ]);
    let output = run_agent_login(
        &server,
        &credential,
        &["show", AGENT_LOGIN_ID, "--json"],
        &[],
    );
    let human = run_agent_login(&server, &credential, &["show", AGENT_LOGIN_ID], &[]);

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let document = json_output(&output);
    assert_eq!(
        document["agentLogin"],
        with_statuses(
            login,
            &[
                ("reconnect_required", "reconnect"),
                ("pending", "wait_for_runner")
            ]
        )
    );
    assert_eq!(
        document["agentLogin"]["authorizations"][0]["report"]["lastUse"],
        "refused"
    );
    let ended = &document["agentLogin"]["endedAuthorizations"][0];
    assert_eq!(ended["runnerId"], THIRD_RUNNER_ID);
    assert_eq!(ended["endedAt"], "2026-11-04T09:00:00Z");
    assert_eq!(ended["removal"], serde_json::json!({"state": "pending"}));
    assert!(ended.get("availability").is_none(), "{document}");
    assert_eq!(ended["stop"]["runId"], RUN_ID);
    assert_eq!(
        ended["stop"]["cancellationRequestId"],
        CANCELLATION_REQUEST_ID
    );
    assert_no_secret_output(&output, &[SENTINEL]);
    assert!(human.status.success());
    assert_no_secret_output(&human, &[SENTINEL]);
    let text = String::from_utf8(human.stdout).unwrap();
    let (in_force, ended) = text.split_once("ended authorizations: 1").unwrap();
    assert!(
        has_line(in_force, &format!("runner: {RUNNER_ID}")),
        "{text}"
    );
    assert!(has_line(in_force, "last use: refused"), "{text}");
    assert!(
        has_line(in_force, "availability: reconnect_required"),
        "{text}"
    );
    assert!(
        in_force.contains(&format!("um auth agent-login reconnect {AGENT_LOGIN_ID}")),
        "{text}"
    );
    assert!(!ended.contains("availability"), "{text}");
    assert!(!in_force.contains(THIRD_RUNNER_ID), "{text}");
    assert!(
        has_line(ended, &format!("runner: {THIRD_RUNNER_ID}")),
        "{text}"
    );
    assert!(has_line(ended, "ended: 2026-11-04T09:00:00Z"), "{text}");
    assert!(has_line(ended, "removal: pending"), "{text}");
    assert!(!in_force.contains(RUN_ID), "{text}");
    for id in [RUN_ID, CANCELLATION_REQUEST_ID] {
        assert!(ended.contains(id), "{text}");
    }
    assert_eq!(server.finish().len(), 2);
}

// The agent login a revoke leaves: every runner it was authorized for is listed with its
// removal state (REQ-AGENTLOGIN-37), and the first runner's run was asked to stop
// (REQ-AGENTLOGIN-38).
fn revoked_with_ended_authorizations() -> serde_json::Value {
    let mut login = agent_logins()[0].clone();
    login["state"] = "revoked".into();
    login["revokedAt"] = "2026-11-05T09:00:00Z".into();
    login["authorizations"] = serde_json::json!([]);
    login["endedAuthorizations"] = serde_json::json!([
        with_stop(ended_authorization(
            "aga_01k0z6r1w8f4jy2m7q9v3x5abc",
            RUNNER_ID,
            true,
            serde_json::json!({"state": "pending"}),
        )),
        ended_authorization(
            "aga_01k0z6r1w8f4jy2m7q9v3x5abd",
            SECOND_RUNNER_ID,
            false,
            serde_json::json!({"state": "not_delivered"}),
        ),
        ended_authorization(
            ENDED_AUTHORIZATION_ID,
            THIRD_RUNNER_ID,
            true,
            serde_json::json!({"state": "confirmed", "confirmedAt": "2026-11-05T09:03:00Z"}),
        ),
    ]);
    login
}

// REQ-AGENTLOGIN-20
// REQ-AGENTLOGIN-38
#[test]
fn revoke_reads_the_agent_login_after_and_reports_each_runners_removal_and_stop() {
    let after = revoked_with_ended_authorizations();
    let (server, _directory, credential) = prepared(vec![
        revoked(),
        json_http_response("200 OK", with_material(&after)),
        revoked(),
        json_http_response("200 OK", with_material(&after)),
    ]);
    let output = run_agent_login(
        &server,
        &credential,
        &["revoke", AGENT_LOGIN_ID, "--yes", "--json"],
        &[],
    );
    let human = run_agent_login(
        &server,
        &credential,
        &["revoke", AGENT_LOGIN_ID, "--yes"],
        &[],
    );

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        json_output(&output),
        serde_json::json!({
            "schemaVersion": 1,
            "deployment": server.api_url,
            "outcome": "revoked",
            "agentLoginId": AGENT_LOGIN_ID,
            "endedAuthorizations": after["endedAuthorizations"]
        })
    );
    let ended = &json_output(&output)["endedAuthorizations"];
    assert_eq!(ended[0]["stop"]["runId"], RUN_ID);
    assert_eq!(
        ended[0]["stop"]["cancellationRequestId"],
        CANCELLATION_REQUEST_ID
    );
    for unstopped in [&ended[1], &ended[2]] {
        assert!(unstopped.get("stop").is_none(), "{unstopped}");
    }
    assert_no_secret_output(&output, &[SENTINEL]);
    assert!(human.status.success());
    assert_no_secret_output(&human, &[SENTINEL]);
    let text = String::from_utf8(human.stdout).unwrap();
    for runner in [RUNNER_ID, SECOND_RUNNER_ID, THIRD_RUNNER_ID] {
        assert!(has_line(&text, &format!("runner: {runner}")), "{text}");
    }
    // The stop is printed under the runner it was requested for, and only there.
    let (_, first) = text.split_once(&format!("runner: {RUNNER_ID}")).unwrap();
    let (first, others) = first
        .split_once(&format!("runner: {SECOND_RUNNER_ID}"))
        .unwrap();
    for id in [RUN_ID, CANCELLATION_REQUEST_ID] {
        assert!(first.contains(id), "{text}");
    }
    assert!(!others.contains(CANCELLATION_REQUEST_ID), "{text}");
    for removal in [
        "removal: pending",
        "removal: not_delivered",
        "removal: confirmed at 2026-11-05T09:03:00Z",
    ] {
        assert!(has_line(&text, removal), "{text}");
    }
    assert!(text.contains("session at the provider"), "{text}");
    let requests = server.finish();
    assert_eq!(requests.len(), 4);
    for (index, request) in requests.iter().enumerate() {
        let method = if index % 2 == 0 { "DELETE" } else { "GET" };
        assert!(
            request.starts_with(&format!(
                "{method} /api/v1/me/agent-logins/{AGENT_LOGIN_ID} HTTP/1.1\r\n"
            )),
            "{request}"
        );
    }
}

// REQ-AGENTLOGIN-20
#[test]
fn revoke_reports_revoked_when_the_read_after_it_fails() {
    let (server, _directory, credential) = prepared(vec![
        revoked(),
        agent_login_problem("404 Not Found", 404, "agent-login-not-found"),
        revoked(),
        Vec::new(),
    ]);
    let output = run_agent_login(
        &server,
        &credential,
        &["revoke", AGENT_LOGIN_ID, "--yes", "--json"],
        &[],
    );
    let human = run_agent_login(
        &server,
        &credential,
        &["revoke", AGENT_LOGIN_ID, "--yes"],
        &[],
    );

    assert_eq!(
        output.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        json_output(&output),
        serde_json::json!({
            "schemaVersion": 1,
            "deployment": server.api_url,
            "outcome": "revoked",
            "agentLoginId": AGENT_LOGIN_ID
        })
    );
    assert_eq!(human.status.code(), Some(0));
    assert!(
        String::from_utf8(human.stdout)
            .unwrap()
            .contains(AGENT_LOGIN_ID)
    );
    let requests = server.finish();
    assert_eq!(requests.len(), 4);
    assert!(requests[1].starts_with("GET "));
    assert!(requests[3].starts_with("GET "));
}

// REQ-AGENTLOGIN-20
#[test]
fn revoke_deletes_the_agent_login_with_one_key_and_no_body() {
    // A Cloud that lists no ended authorizations answers the read after each revoke; the
    // result is the one it has always been.
    let after = json_http_response("200 OK", agent_logins()[1].clone());
    let (server, _directory, credential) =
        prepared(vec![revoked(), after.clone(), revoked(), after]);
    let output = run_agent_login(
        &server,
        &credential,
        &["revoke", AGENT_LOGIN_ID, "--yes", "--json"],
        &[],
    );
    let human = run_agent_login(
        &server,
        &credential,
        &["revoke", AGENT_LOGIN_ID, "--yes"],
        &[],
    );

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        json_output(&output),
        serde_json::json!({
            "schemaVersion": 1,
            "deployment": server.api_url,
            "outcome": "revoked",
            "agentLoginId": AGENT_LOGIN_ID
        })
    );
    assert!(human.status.success());
    let text = String::from_utf8(human.stdout).unwrap();
    assert!(text.contains(AGENT_LOGIN_ID));
    assert!(text.contains("session at the provider"), "{text}");
    let requests = server.finish();
    assert_eq!(requests.len(), 4);
    let deletes = [&requests[0], &requests[2]];
    for request in deletes {
        assert!(request.starts_with(&format!(
            "DELETE /api/v1/me/agent-logins/{AGENT_LOGIN_ID} HTTP/1.1\r\n"
        )));
        assert!(!header_value(request, "idempotency-key").is_empty());
        assert_eq!(request.split_once("\r\n\r\n").unwrap().1, "");
    }
    assert_ne!(
        header_value(deletes[0], "idempotency-key"),
        header_value(deletes[1], "idempotency-key")
    );
}

// REQ-AGENTLOGIN-20
#[test]
fn revoke_replays_its_key_once_after_a_lost_response() {
    let (server, _directory, credential) = prepared(vec![
        Vec::new(),
        revoked(),
        json_http_response("200 OK", agent_logins()[1].clone()),
    ]);
    let output = run_agent_login(
        &server,
        &credential,
        &["revoke", AGENT_LOGIN_ID, "--yes", "--json"],
        &[],
    );

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(json_output(&output)["outcome"], "revoked");
    let requests = server.finish();
    assert_eq!(requests.len(), 3);
    assert_eq!(
        header_value(&requests[0], "idempotency-key"),
        header_value(&requests[1], "idempotency-key")
    );
}

// REQ-AGENTLOGIN-38
#[test]
fn revoke_answered_500_fails_and_running_it_again_reports_revoked() {
    // The revocation is durable before Cloud requests the stops, so a 500 leaves it done and
    // the stops owed; running the command again is the retry.
    let failed = agent_login_problem("500 Internal Server Error", 500, "internal-server-error");
    let after = revoked_with_ended_authorizations();
    let (server, _directory, credential) = prepared(vec![
        failed.clone(),
        failed,
        revoked(),
        json_http_response("200 OK", after.clone()),
    ]);
    let failure = run_agent_login(
        &server,
        &credential,
        &["revoke", AGENT_LOGIN_ID, "--yes", "--json"],
        &[],
    );
    let retry = run_agent_login(
        &server,
        &credential,
        &["revoke", AGENT_LOGIN_ID, "--yes", "--json"],
        &[],
    );

    assert!(!failure.status.success());
    assert!(
        retry.status.success(),
        "{}",
        String::from_utf8_lossy(&retry.stderr)
    );
    let document = json_output(&retry);
    assert_eq!(document["outcome"], "revoked");
    assert_eq!(
        document["endedAuthorizations"],
        after["endedAuthorizations"]
    );
    let requests = server.finish();
    assert_eq!(requests.len(), 4);
    for request in &requests[..3] {
        assert!(request.starts_with(&format!(
            "DELETE /api/v1/me/agent-logins/{AGENT_LOGIN_ID} HTTP/1.1\r\n"
        )));
    }
}

// Runs an agent-login command against a deployment that records any connection, and asserts
// that the command exited 2 with nothing written to standard output and no connection made.
fn run_refused_before_any_request(args: &[&str]) -> Output {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let api_url = format!("http://{}/api", listener.local_addr().unwrap());
    let issuer = format!("http://{}/auth/", listener.local_addr().unwrap());
    let directory = private_credential_directory();
    let credential = directory.path().join("credentials.json");
    write_credential_fixture_for_deployment(
        &credential,
        &api_url,
        &issuer,
        TOKEN,
        "2999-01-01T00:00:00Z",
    );
    let environment =
        deployment_environment_with_issuer(&api_url, &issuer, credential.to_str().unwrap());
    let mut arguments = vec!["auth", "agent-login"];
    arguments.extend_from_slice(args);
    arguments.push("--allow-insecure-http");
    let output = run_with_env(&arguments, &environment);

    assert_eq!(output.status.code(), Some(2), "{args:?}");
    assert!(output.stdout.is_empty(), "{args:?}");
    assert_eq!(
        listener.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock,
        "{args:?}"
    );
    output
}

// REQ-AGENTLOGIN-20
#[test]
fn revoke_without_confirmation_exits_2_before_any_request() {
    run_refused_before_any_request(&["revoke", AGENT_LOGIN_ID]);
}

// REQ-AGENTLOGIN-21
#[test]
fn refusals_report_their_outcome_with_exit_status_1() {
    let material = tempfile::tempdir().unwrap();
    let file = private_file(material.path(), "auth.json", CODEX_MATERIAL.as_bytes());
    let import: &[&str] = &[
        "import",
        "acme",
        "codex",
        "--from-file",
        file.to_str().unwrap(),
        "--json",
    ];
    let cases: [(&[&str], Vec<u8>, &str); 6] = [
        (
            &["list", "--json"],
            agent_login_problem("503 Service Unavailable", 503, "provider-unavailable"),
            "provider_unavailable",
        ),
        (
            &["show", AGENT_LOGIN_ID, "--json"],
            agent_login_problem("404 Not Found", 404, "agent-login-not-found"),
            "agent_login_not_found",
        ),
        (
            &["revoke", AGENT_LOGIN_ID, "--yes", "--json"],
            agent_login_problem("404 Not Found", 404, "agent-login-not-found"),
            "agent_login_not_found",
        ),
        (
            import,
            agent_login_problem("404 Not Found", 404, "not-found"),
            "organization_not_found",
        ),
        (
            import,
            agent_login_problem("400 Bad Request", 400, "agent-login-material-invalid"),
            "material_invalid",
        ),
        (
            import,
            agent_login_problem("409 Conflict", 409, "quantity-limit-reached"),
            "limit_reached",
        ),
    ];
    for (args, response, outcome) in cases {
        let (server, _directory, credential) = prepared(vec![response]);
        let output = run_agent_login(&server, &credential, args, &[]);
        assert_eq!(output.status.code(), Some(1), "{args:?}");
        assert_eq!(json_output(&output)["outcome"], outcome, "{args:?}");
        server.finish();
    }
    let (server, _directory, credential) = prepared(vec![agent_login_problem(
        "404 Not Found",
        404,
        "agent-login-not-found",
    )]);
    let output = run_agent_login(&server, &credential, &["show", AGENT_LOGIN_ID], &[]);
    assert_eq!(output.status.code(), Some(1));
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("um auth agent-login list")
    );
    server.finish();
}

// The material a member imports: a Codex `auth.json` whose bytes, trailing newline and
// spacing included, are what Cloud must receive.
const CODEX_MATERIAL: &str =
    "{\"tokens\": {\"access_token\": \"private-agent-login-material-sentinel\"}}\n";

fn encoded(material: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(material)
}

fn private_file(directory: &Path, name: &str, contents: &[u8]) -> std::path::PathBuf {
    let path = directory.join(name);
    fs::write(&path, contents).unwrap();
    fs::set_permissions(&path, Permissions::from_mode(0o600)).unwrap();
    path
}

// The agent login an import answers with, echoing the request's key.
fn stored(status: &str, provider: &str, generation: i64) -> Vec<u8> {
    let mut login = agent_logins()[0].clone();
    login["provider"] = provider.into();
    login["generation"] = generation.into();
    login["authorizations"] = serde_json::json!([]);
    http_response_with_headers(
        status,
        Some("application/json"),
        &[("Idempotency-Key", ECHO_IDEMPOTENCY_KEY)],
        &serde_json::to_vec(&login).unwrap(),
    )
}

fn request_body(request: &str) -> serde_json::Value {
    serde_json::from_str(request.split_once("\r\n\r\n").unwrap().1).unwrap()
}

// REQ-AGENTLOGIN-22
#[test]
fn import_codex_sends_the_file_byte_for_byte_and_prints_the_login() {
    let (server, directory, credential) = prepared(vec![
        stored("201 Created", "codex", 1),
        stored("201 Created", "codex", 1),
    ]);
    let file = private_file(directory.path(), "auth.json", CODEX_MATERIAL.as_bytes());
    let path = file.to_str().unwrap();
    let human = run_agent_login(
        &server,
        &credential,
        &["import", "acme", "codex", "--from-file", path],
        &[],
    );
    let labelled = run_agent_login(
        &server,
        &credential,
        &[
            "import",
            "acme",
            "codex",
            "--from-file",
            path,
            "--label",
            "build laptop",
            "--json",
        ],
        &[],
    );

    assert!(
        human.status.success(),
        "{}",
        String::from_utf8_lossy(&human.stderr)
    );
    let text = String::from_utf8(human.stdout.clone()).unwrap();
    for fact in [
        AGENT_LOGIN_ID,
        ORGANIZATION_ID,
        "provider: codex",
        "generation: 1",
    ] {
        assert!(text.contains(fact), "{fact}: {text}");
    }
    assert_eq!(labelled.status.code(), Some(0));
    let document = json_output(&labelled);
    assert_eq!(document["outcome"], "imported");
    assert_eq!(document["agentLogin"]["id"], AGENT_LOGIN_ID);
    assert_eq!(document["agentLogin"]["generation"], 1);
    for output in [&human, &labelled] {
        assert_no_secret_output(output, &[SENTINEL, &encoded(CODEX_MATERIAL.as_bytes())]);
    }
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    for request in &requests {
        assert!(request.starts_with("POST /api/v1/me/agent-logins HTTP/1.1\r\n"));
        assert!(!header_value(request, "idempotency-key").is_empty());
    }
    let body = request_body(&requests[0]);
    assert_eq!(
        body,
        serde_json::json!({
            "organization": "acme",
            "provider": "codex",
            "recordSchemaVersion": 1,
            "material": encoded(CODEX_MATERIAL.as_bytes())
        })
    );
    assert_eq!(request_body(&requests[1])["label"], "build laptop");
}

const OTHER_ORGANIZATION_ID: &str = "org_01k0z6r1w8f4jy2m7q9v3x5abd";

// The agent login `--agent-login` names, as Cloud shows it before anything is sent.
fn target(provider: &str, organization_id: &str) -> Vec<u8> {
    let mut login = agent_logins()[0].clone();
    login["provider"] = provider.into();
    login["organizationId"] = organization_id.into();
    json_http_response("200 OK", login)
}

// A revoked agent login, which Cloud still shows its owner (REQ-AGENTLOGIN-3) and stores no
// generation for (REQ-AGENTLOGIN-6).
fn revoked_target() -> Vec<u8> {
    let mut login = agent_logins()[0].clone();
    login["state"] = "revoked".into();
    login["authorizations"] = serde_json::json!([]);
    json_http_response("200 OK", login)
}

// The organization a slug names.
fn organization(id: &str, slug: &str) -> Vec<u8> {
    json_http_response(
        "200 OK",
        serde_json::json!({
            "id": id,
            "state": "active",
            "displayName": "Acme",
            "slug": slug,
            "createdAt": "2026-11-01T09:00:00Z",
            "updatedAt": "2026-11-01T09:00:00Z"
        }),
    )
}

// REQ-AGENTLOGIN-22
#[test]
fn import_into_an_agent_login_stores_its_next_generation_and_replays_a_lost_response() {
    let (server, directory, credential) = prepared(vec![
        target("codex", ORGANIZATION_ID),
        organization(ORGANIZATION_ID, "acme"),
        Vec::new(),
        stored("200 OK", "codex", 2),
    ]);
    let file = private_file(directory.path(), "auth.json", CODEX_MATERIAL.as_bytes());
    let output = run_agent_login(
        &server,
        &credential,
        &[
            "import",
            "acme",
            "codex",
            "--from-file",
            file.to_str().unwrap(),
            "--agent-login",
            AGENT_LOGIN_ID,
            "--json",
        ],
        &[],
    );

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let document = json_output(&output);
    assert_eq!(document["outcome"], "imported");
    assert_eq!(document["agentLogin"]["generation"], 2);
    assert_no_secret_output(&output, &[SENTINEL]);
    let requests = server.finish();
    assert_eq!(requests.len(), 4);
    assert!(requests[0].starts_with(&format!(
        "GET /api/v1/me/agent-logins/{AGENT_LOGIN_ID} HTTP/1.1\r\n"
    )));
    assert!(requests[1].starts_with("GET /api/v1/organizations/acme HTTP/1.1\r\n"));
    let requests = &requests[2..];
    for request in requests {
        assert!(request.starts_with(&format!(
            "POST /api/v1/me/agent-logins/{AGENT_LOGIN_ID}/generations HTTP/1.1\r\n"
        )));
        assert_eq!(
            request_body(request),
            serde_json::json!({
                "recordSchemaVersion": 1,
                "material": encoded(CODEX_MATERIAL.as_bytes())
            })
        );
    }
    assert_eq!(
        header_value(&requests[0], "idempotency-key"),
        header_value(&requests[1], "idempotency-key")
    );
}

// REQ-AGENTLOGIN-22
#[test]
fn import_into_an_agent_login_of_another_provider_or_organization_exits_2_before_sending_material()
{
    let cases: [(&str, Vec<Vec<u8>>, &str); 3] = [
        (
            "acme",
            vec![target("claude_code", ORGANIZATION_ID)],
            "claude_code",
        ),
        (
            "acme",
            vec![
                target("codex", ORGANIZATION_ID),
                organization(OTHER_ORGANIZATION_ID, "acme"),
            ],
            ORGANIZATION_ID,
        ),
        (
            OTHER_ORGANIZATION_ID,
            vec![target("codex", ORGANIZATION_ID)],
            ORGANIZATION_ID,
        ),
    ];
    for (organization_ref, responses, mismatch) in cases {
        let reads = responses.len();
        let (server, directory, credential) = prepared(responses);
        let file = private_file(directory.path(), "compact.json", CODEX_MATERIAL.as_bytes());
        let output = run_agent_login(
            &server,
            &credential,
            &[
                "import",
                organization_ref,
                "codex",
                "--from-file",
                file.to_str().unwrap(),
                "--agent-login",
                AGENT_LOGIN_ID,
                "--json",
            ],
            &[],
        );

        assert_eq!(
            output.status.code(),
            Some(2),
            "{organization_ref} {mismatch}"
        );
        assert!(output.stdout.is_empty(), "{organization_ref} {mismatch}");
        let stderr = String::from_utf8(output.stderr.clone()).unwrap();
        assert!(stderr.contains(AGENT_LOGIN_ID), "{stderr}");
        assert!(stderr.contains(mismatch), "{stderr}");
        assert_no_secret_output(&output, &[SENTINEL, &encoded(CODEX_MATERIAL.as_bytes())]);
        let requests = server.finish();
        assert_eq!(requests.len(), reads);
        assert!(requests[0].starts_with(&format!(
            "GET /api/v1/me/agent-logins/{AGENT_LOGIN_ID} HTTP/1.1\r\n"
        )));
        assert!(
            requests
                .iter()
                .all(|request| request.starts_with("GET ") && !request.contains("generations")),
            "{requests:?}"
        );
    }

    // An unknown agent login is Cloud's refusal, not a contradiction.
    let (server, directory, credential) = prepared(vec![agent_login_problem(
        "404 Not Found",
        404,
        "agent-login-not-found",
    )]);
    let file = private_file(directory.path(), "compact.json", CODEX_MATERIAL.as_bytes());
    let output = run_agent_login(
        &server,
        &credential,
        &[
            "import",
            "acme",
            "codex",
            "--from-file",
            file.to_str().unwrap(),
            "--agent-login",
            AGENT_LOGIN_ID,
            "--json",
        ],
        &[],
    );
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(json_output(&output)["outcome"], "agent_login_not_found");
    assert_eq!(server.finish().len(), 1);

    // Nor is a revoked one, which takes no generation: no material is sent to it.
    let (server, directory, credential) = prepared(vec![revoked_target()]);
    let file = private_file(directory.path(), "compact.json", CODEX_MATERIAL.as_bytes());
    let output = run_agent_login(
        &server,
        &credential,
        &[
            "import",
            "acme",
            "codex",
            "--from-file",
            file.to_str().unwrap(),
            "--agent-login",
            AGENT_LOGIN_ID,
            "--json",
        ],
        &[],
    );
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(json_output(&output)["outcome"], "agent_login_not_found");
    assert_no_secret_output(&output, &[SENTINEL, &encoded(CODEX_MATERIAL.as_bytes())]);
    let requests = server.finish();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].starts_with("GET "), "{requests:?}");
}

// REQ-AGENTLOGIN-22
// REQ-AGENTLOGIN-23
#[test]
fn import_reads_standard_input() {
    let (server, _directory, credential) = prepared(vec![
        stored("201 Created", "codex", 1),
        stored("201 Created", "claude_code", 1),
    ]);
    let codex = run_agent_login_with_stdin(
        &server,
        &credential,
        &["import", "acme", "codex", "--from-file", "-"],
        CODEX_MATERIAL.as_bytes(),
    );
    let claude = run_agent_login_with_stdin(
        &server,
        &credential,
        &["import", "acme", "claude_code", "--token-file", "-"],
        format!("{SENTINEL}\r\n").as_bytes(),
    );

    for output in [&codex, &claude] {
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_no_secret_output(output, &[SENTINEL]);
    }
    let requests = server.finish();
    assert_eq!(
        request_body(&requests[0])["material"],
        encoded(CODEX_MATERIAL.as_bytes())
    );
    assert_eq!(
        request_body(&requests[1])["material"],
        encoded(SENTINEL.as_bytes())
    );
}

// REQ-AGENTLOGIN-23
#[test]
fn import_claude_code_sends_the_token_line_without_its_newline() {
    let (server, directory, credential) = prepared(vec![stored("201 Created", "claude_code", 1)]);
    let file = private_file(
        directory.path(),
        "token",
        format!("{SENTINEL}\n").as_bytes(),
    );
    let output = run_agent_login(
        &server,
        &credential,
        &[
            "import",
            "acme",
            "claude_code",
            "--token-file",
            file.to_str().unwrap(),
        ],
        &[],
    );

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8(output.stdout.clone())
            .unwrap()
            .contains("provider: claude_code")
    );
    assert_no_secret_output(&output, &[SENTINEL]);
    let body = request_body(&server.finish()[0]);
    assert_eq!(body["provider"], "claude_code");
    assert_eq!(body["recordSchemaVersion"], 1);
    assert_eq!(body["material"], encoded(SENTINEL.as_bytes()));
}

// REQ-AGENTLOGIN-22
// REQ-AGENTLOGIN-23
#[test]
fn unsafe_or_malformed_material_and_a_mismatched_source_exit_2_before_any_request() {
    use std::os::unix::fs::symlink;
    let (server, directory, credential) = prepared(vec![]);
    let codex = private_file(directory.path(), "auth.json", CODEX_MATERIAL.as_bytes());
    let token = private_file(directory.path(), "token", SENTINEL.as_bytes());
    let alias = directory.path().join("alias.json");
    symlink(&codex, &alias).unwrap();
    let mut oversized = format!("{{\"padding\": \"{SENTINEL}");
    oversized.push_str(&"x".repeat(32 * 1024));
    oversized.push_str("\"}");
    let oversized = private_file(directory.path(), "oversized.json", oversized.as_bytes());
    let array = private_file(
        directory.path(),
        "array.json",
        format!("[\"{SENTINEL}\"]").as_bytes(),
    );
    let string = private_file(
        directory.path(),
        "string.json",
        format!("\"{SENTINEL}\"").as_bytes(),
    );
    let spaced = private_file(
        directory.path(),
        "spaced",
        format!("{SENTINEL} more\n").as_bytes(),
    );
    let empty = private_file(directory.path(), "empty", b"");
    let wide = private_file(directory.path(), "wide", "x".repeat(4097).as_bytes());
    let world_readable = private_file(directory.path(), "readable.json", CODEX_MATERIAL.as_bytes());
    fs::set_permissions(&world_readable, Permissions::from_mode(0o644)).unwrap();
    let read_only = private_file(
        directory.path(),
        "read-only.json",
        CODEX_MATERIAL.as_bytes(),
    );
    fs::set_permissions(&read_only, Permissions::from_mode(0o400)).unwrap();
    let path = |path: &Path| path.to_str().unwrap().to_owned();

    let cases = [
        ("codex", "--from-file", path(&world_readable)),
        ("codex", "--from-file", path(&read_only)),
        ("codex", "--from-file", path(&alias)),
        ("codex", "--from-file", path(&oversized)),
        ("codex", "--from-file", path(&array)),
        ("codex", "--from-file", path(&string)),
        ("codex", "--token-file", path(&token)),
        ("claude_code", "--from-file", path(&codex)),
        ("claude_code", "--token-file", path(&spaced)),
        ("claude_code", "--token-file", path(&empty)),
        ("claude_code", "--token-file", path(&wide)),
    ];
    for (provider, flag, file) in &cases {
        let output = run_agent_login(
            &server,
            &credential,
            &["import", "acme", provider, flag, file],
            &[],
        );
        assert_eq!(output.status.code(), Some(2), "{provider} {flag} {file}");
        assert!(output.stdout.is_empty(), "{provider} {flag} {file}");
        assert_no_secret_output(&output, &[SENTINEL]);
    }
    assert!(server.finish().is_empty());
}

// How the fake `codex` behaves after it records its invocation.
enum SignIn {
    // Writes this auth.json into $CODEX_HOME with a group- and world-readable mode, as a
    // file codex writes need not be 0600 for um to read it.
    Writes(&'static str),
    Exits(u8),
    WritesNothing,
    // Asks um to terminate, as a member's interrupt would, and waits to be stopped.
    Interrupts,
    // Is executable and cannot be started: its interpreter does not exist.
    CannotStart,
}

// A fake `codex`, alone on PATH, that records its arguments, CODEX_HOME, that home's mode
// and its config.toml under `record`, and prints a line on standard output.
struct FakeCodex {
    _directory: tempfile::TempDir,
    bin: std::path::PathBuf,
    record: std::path::PathBuf,
}

impl FakeCodex {
    fn new(sign_in: SignIn) -> Self {
        use super::pi_installation::quote;
        let directory = tempfile::tempdir().unwrap();
        let bin = directory.path().join("bin");
        let record = directory.path().join("record");
        fs::create_dir(&bin).unwrap();
        fs::create_dir(&record).unwrap();
        let record_path = |name: &str| quote(record.join(name).to_str().unwrap());
        let then = match sign_in {
            SignIn::Writes(material) => format!(
                "umask 022\nprintf '%s' {} > \"$CODEX_HOME/auth.json\"",
                quote(material)
            ),
            SignIn::Exits(status) => format!("exit {status}"),
            SignIn::WritesNothing => "exit 0".to_owned(),
            SignIn::Interrupts => "kill -TERM \"$PPID\"\nexec sleep 30".to_owned(),
            SignIn::CannotStart => String::new(),
        };
        let interpreter = if matches!(sign_in, SignIn::CannotStart) {
            "/nonexistent/um-test-interpreter"
        } else {
            "/bin/sh"
        };
        // um runs this script with only `bin` on PATH; the script's own `ls` and `cp` come
        // from the test's PATH, since a build sandbox may have nothing under /usr/bin or /bin.
        let tools = std::env::var("PATH").unwrap_or_default();
        let script = format!(
            "#!{interpreter}\nPATH={tools}\nexport PATH\n\
             printf '%s\\n' \"$*\" > {arguments}\n\
             printf '%s' \"$CODEX_HOME\" > {home}\n\
             ls -ld \"$CODEX_HOME\" > {mode}\n\
             cp \"$CODEX_HOME/config.toml\" {config}\n\
             echo codex-sign-in-prompt\n\
             {then}\n",
            arguments = record_path("arguments"),
            home = record_path("home"),
            tools = quote(&tools),
            mode = record_path("mode"),
            config = record_path("config.toml"),
        );
        let executable = bin.join("codex");
        fs::write(&executable, script).unwrap();
        fs::set_permissions(&executable, Permissions::from_mode(0o755)).unwrap();
        Self {
            _directory: directory,
            bin,
            record,
        }
    }

    fn recorded(&self, name: &str) -> Option<String> {
        fs::read_to_string(self.record.join(name)).ok()
    }
}

// A member's home with an ambient Codex login, which --login must neither read nor change.
struct AmbientHome {
    home: tempfile::TempDir,
    // Where the command may create its temporary Codex home.
    temporary: tempfile::TempDir,
}

const AMBIENT_MARKER: &str = "ambient-codex-login-marker";

impl AmbientHome {
    fn new() -> Self {
        let home = tempfile::tempdir().unwrap();
        fs::create_dir(home.path().join(".codex")).unwrap();
        private_file(
            &home.path().join(".codex"),
            "auth.json",
            format!("{{\"marker\": \"{AMBIENT_MARKER}\"}}").as_bytes(),
        );
        Self {
            home,
            temporary: tempfile::tempdir().unwrap(),
        }
    }

    fn ambient_codex(&self) -> std::path::PathBuf {
        self.home.path().join(".codex")
    }

    fn run(
        &self,
        server: &ScriptedServer,
        credential: &Path,
        path: &Path,
        args: &[&str],
    ) -> Output {
        let ambient = self.ambient_codex();
        run_agent_login(
            server,
            credential,
            args,
            &[
                ("HOME", self.home.path().to_str().unwrap()),
                ("CODEX_HOME", ambient.to_str().unwrap()),
                ("TMPDIR", self.temporary.path().to_str().unwrap()),
                ("PATH", path.to_str().unwrap()),
            ],
        )
    }

    // The ambient login is untouched and the command left nothing in its temporary directory.
    fn assert_untouched_and_clean(&self) {
        let ambient = self.ambient_codex();
        assert_eq!(
            fs::read_dir(&ambient).unwrap().count(),
            1,
            "ambient Codex home gained an entry"
        );
        assert_eq!(
            fs::read_to_string(ambient.join("auth.json")).unwrap(),
            format!("{{\"marker\": \"{AMBIENT_MARKER}\"}}")
        );
        assert_eq!(
            fs::read_dir(self.temporary.path()).unwrap().count(),
            0,
            "temporary Codex home was left behind"
        );
    }
}

// REQ-AGENTLOGIN-24
// INV-CLI-13
#[test]
fn import_codex_login_signs_in_in_a_private_temporary_home_and_uploads_what_it_wrote() {
    let (server, _directory, credential) = prepared(vec![stored("201 Created", "codex", 1)]);
    let codex = FakeCodex::new(SignIn::Writes(CODEX_MATERIAL));
    let home = AmbientHome::new();
    let output = home.run(
        &server,
        &credential,
        &codex.bin,
        &["import", "acme", "codex", "--login", "--json"],
    );

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    // Standard output stays the one JSON document; the sign-in's output reaches the member on
    // standard error.
    assert_eq!(json_output(&output)["outcome"], "imported");
    assert!(String::from_utf8_lossy(&output.stderr).contains("codex-sign-in-prompt"));
    assert_no_secret_output(&output, &[SENTINEL, AMBIENT_MARKER]);
    assert_eq!(codex.recorded("arguments").unwrap(), "login\n");
    let codex_home = std::path::PathBuf::from(codex.recorded("home").unwrap());
    assert_ne!(codex_home, home.ambient_codex());
    assert!(codex_home.starts_with(home.temporary.path()));
    assert!(
        codex.recorded("mode").unwrap().starts_with("drwx------"),
        "{:?}",
        codex.recorded("mode")
    );
    let config = codex.recorded("config.toml").unwrap();
    assert!(
        config
            .lines()
            .any(|line| line.trim() == "cli_auth_credentials_store = \"file\""),
        "{config}"
    );
    assert!(!codex_home.exists());
    home.assert_untouched_and_clean();
    let requests = server.finish();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].starts_with("POST /api/v1/me/agent-logins HTTP/1.1\r\n"));
    assert_eq!(
        request_body(&requests[0])["material"],
        encoded(CODEX_MATERIAL.as_bytes())
    );
    assert!(!requests[0].contains(&encoded(AMBIENT_MARKER.as_bytes())));
}

// REQ-AGENTLOGIN-24
#[test]
fn a_codex_sign_in_that_fails_leaves_no_login_or_is_interrupted_stores_nothing() {
    let (server, _directory, credential) = prepared(vec![]);
    let absent = tempfile::tempdir().unwrap();
    let cases = [
        ("exits nonzero", Some(FakeCodex::new(SignIn::Exits(3)))),
        (
            "writes no login",
            Some(FakeCodex::new(SignIn::WritesNothing)),
        ),
        (
            "writes no JSON object",
            Some(FakeCodex::new(SignIn::Writes("[\"not-a-login\"]"))),
        ),
        ("is interrupted", Some(FakeCodex::new(SignIn::Interrupts))),
        (
            "cannot be started",
            Some(FakeCodex::new(SignIn::CannotStart)),
        ),
        ("is not on PATH", None),
    ];
    for (case, codex) in &cases {
        let home = AmbientHome::new();
        let path = codex.as_ref().map_or(absent.path(), |codex| &codex.bin);
        for json in [false, true] {
            let mut args = vec!["import", "acme", "codex", "--login"];
            if json {
                args.push("--json");
            }
            let output = home.run(&server, &credential, path, &args);

            assert_eq!(output.status.code(), Some(1), "{case}");
            if json {
                assert_eq!(
                    json_output(&output)["outcome"],
                    "provider_login_failed",
                    "{case}"
                );
            } else {
                assert!(output.stdout.is_empty(), "{case}");
                assert!(!output.stderr.is_empty(), "{case}");
            }
            assert_no_secret_output(&output, &[AMBIENT_MARKER]);
            home.assert_untouched_and_clean();
        }
        if let Some(codex) = codex {
            let started = (*case != "cannot be started").then(|| "login\n".to_owned());
            assert_eq!(codex.recorded("arguments"), started, "{case}");
        }
    }
    assert!(server.finish().is_empty());
}

// REQ-AGENTLOGIN-24
#[test]
fn import_codex_login_checks_the_session_and_the_target_before_it_signs_in() {
    let invalid_grant = || {
        json_http_response(
            "400 Bad Request",
            serde_json::json!({"error": "invalid_grant"}),
        )
    };
    let not_found = || agent_login_problem("404 Not Found", 404, "agent-login-not-found");
    // A refusal the command must report before it starts `codex`.
    struct Refusal {
        case: &'static str,
        // The stored session's expiry, or no stored session.
        expiry: Option<&'static str>,
        responses: Vec<Vec<u8>>,
        names_agent_login: bool,
        status: i32,
        // The `--json` outcome, or none for a usage refusal, which prints no document.
        outcome: Option<&'static str>,
    }
    const LIVE: Option<&str> = Some("2999-01-01T00:00:00Z");
    let cases = [
        Refusal {
            case: "no session",
            expiry: None,
            responses: vec![],
            names_agent_login: false,
            status: 3,
            outcome: Some("unauthenticated"),
        },
        Refusal {
            case: "an expired session",
            expiry: Some("2000-01-01T00:00:00Z"),
            responses: vec![invalid_grant()],
            names_agent_login: false,
            status: 3,
            outcome: Some("unauthenticated"),
        },
        Refusal {
            case: "an unknown agent login",
            expiry: LIVE,
            responses: vec![not_found()],
            names_agent_login: true,
            status: 1,
            outcome: Some("agent_login_not_found"),
        },
        Refusal {
            case: "a revoked agent login",
            expiry: LIVE,
            responses: vec![revoked_target()],
            names_agent_login: true,
            status: 1,
            outcome: Some("agent_login_not_found"),
        },
        Refusal {
            case: "an agent login of another provider",
            expiry: LIVE,
            responses: vec![target("claude_code", ORGANIZATION_ID)],
            names_agent_login: true,
            status: 2,
            outcome: None,
        },
        Refusal {
            case: "an agent login of another organization",
            expiry: LIVE,
            responses: vec![
                target("codex", ORGANIZATION_ID),
                organization(OTHER_ORGANIZATION_ID, "acme"),
            ],
            names_agent_login: true,
            status: 2,
            outcome: None,
        },
    ];
    for Refusal {
        case,
        expiry,
        responses,
        names_agent_login,
        status,
        outcome,
    } in cases
    {
        let reads = responses.len();
        let server = ScriptedServer::respond(responses);
        let directory = private_credential_directory();
        let credential = directory.path().join("credentials.json");
        if let Some(expiry) = expiry {
            write_credential_fixture_for_deployment(
                &credential,
                &server.api_url,
                &server.issuer,
                TOKEN,
                expiry,
            );
        }
        let codex = FakeCodex::new(SignIn::Writes(CODEX_MATERIAL));
        let home = AmbientHome::new();
        let mut args = vec!["import", "acme", "codex", "--login", "--json"];
        if names_agent_login {
            args.extend_from_slice(&["--agent-login", AGENT_LOGIN_ID]);
        }
        let output = home.run(&server, &credential, &codex.bin, &args);

        assert_eq!(output.status.code(), Some(status), "{case}");
        match outcome {
            Some(outcome) => assert_eq!(json_output(&output)["outcome"], outcome, "{case}"),
            None => assert!(output.stdout.is_empty(), "{case}"),
        }
        assert_eq!(codex.recorded("arguments"), None, "{case}");
        assert!(
            !String::from_utf8_lossy(&output.stderr).contains("codex-sign-in-prompt"),
            "{case}"
        );
        home.assert_untouched_and_clean();
        let requests = server.finish();
        assert_eq!(requests.len(), reads, "{case}");
        assert!(
            requests
                .iter()
                .all(|request| !request.contains("POST /api/")),
            "{case}: {requests:?}"
        );
    }
}

// REQ-AGENTLOGIN-23
// REQ-AGENTLOGIN-24
#[test]
fn import_requires_exactly_one_source_and_refuses_login_for_claude_code() {
    let (server, directory, credential) = prepared(vec![]);
    let codex = FakeCodex::new(SignIn::Writes(CODEX_MATERIAL));
    let home = AmbientHome::new();
    let file = private_file(directory.path(), "auth.json", CODEX_MATERIAL.as_bytes());
    let file = file.to_str().unwrap();
    let long_label = "l".repeat(65);
    let cases: [&[&str]; 7] = [
        &["import", "acme", "codex", "--login", "--from-file", file],
        &["import", "acme", "codex"],
        &["import", "acme", "claude_code", "--login"],
        &["import", "acme", "codex", "--login", "--token-file", file],
        // A label Cloud would refuse (REQ-AGENTLOGIN-1) is refused before the sign-in.
        &["import", "acme", "codex", "--login", "--label", ""],
        &["import", "acme", "codex", "--login", "--label", &long_label],
        &[
            "import",
            "acme",
            "codex",
            "--from-file",
            file,
            "--label",
            "two\nlines",
        ],
    ];
    for args in cases {
        let output = home.run(&server, &credential, &codex.bin, args);
        assert_eq!(output.status.code(), Some(2), "{args:?}");
        assert!(output.stdout.is_empty(), "{args:?}");
        assert_no_secret_output(&output, &[SENTINEL]);
        home.assert_untouched_and_clean();
    }
    assert_eq!(codex.recorded("arguments"), None);
    assert!(server.finish().is_empty());
}

// The requests of a reconnect: the read of the agent login, then each send of its next
// generation, which carries `material` and nothing else the member chose.
fn assert_reconnect_requests(requests: &[String], material: &[u8]) {
    assert!(
        requests[0].starts_with(&format!(
            "GET /api/v1/me/agent-logins/{AGENT_LOGIN_ID} HTTP/1.1\r\n"
        )),
        "{requests:?}"
    );
    assert!(requests.len() > 1, "{requests:?}");
    for request in &requests[1..] {
        assert!(
            request.starts_with(&format!(
                "POST /api/v1/me/agent-logins/{AGENT_LOGIN_ID}/generations HTTP/1.1\r\n"
            )),
            "{request}"
        );
        assert_eq!(
            request_body(request),
            serde_json::json!({"recordSchemaVersion": 1, "material": encoded(material)})
        );
    }
}

// Every request is a read: no material reached the deployment.
fn assert_only_reads(requests: &[String], reads: usize) {
    assert_eq!(requests.len(), reads, "{requests:?}");
    assert!(
        requests
            .iter()
            .all(|request| request.starts_with("GET ") && !request.contains("generations")),
        "{requests:?}"
    );
}

// REQ-AGENTLOGIN-44
#[test]
fn reconnect_reads_the_agent_login_and_stores_the_sign_in_its_provider_takes() {
    let (server, directory, credential) = prepared(vec![
        target("codex", ORGANIZATION_ID),
        stored("200 OK", "codex", 3),
        target("claude_code", ORGANIZATION_ID),
        Vec::new(),
        stored("200 OK", "claude_code", 2),
        target("codex", ORGANIZATION_ID),
        stored("200 OK", "codex", 3),
    ]);
    let codex = private_file(directory.path(), "auth.json", CODEX_MATERIAL.as_bytes());
    let token = private_file(
        directory.path(),
        "token",
        format!("{SENTINEL}\n").as_bytes(),
    );
    let codex = codex.to_str().unwrap();
    let from_file = run_agent_login(
        &server,
        &credential,
        &["reconnect", AGENT_LOGIN_ID, "--from-file", codex, "--json"],
        &[],
    );
    let token_file = run_agent_login(
        &server,
        &credential,
        &[
            "reconnect",
            AGENT_LOGIN_ID,
            "--token-file",
            token.to_str().unwrap(),
            "--json",
        ],
        &[],
    );
    let human = run_agent_login(
        &server,
        &credential,
        &["reconnect", AGENT_LOGIN_ID, "--from-file", codex],
        &[],
    );

    for (output, generation) in [(&from_file, 3), (&token_file, 2)] {
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let document = json_output(output);
        assert_eq!(document["outcome"], "reconnected");
        assert_eq!(document["agentLogin"]["id"], AGENT_LOGIN_ID);
        assert_eq!(document["agentLogin"]["generation"], generation);
    }
    assert_eq!(human.status.code(), Some(0));
    let text = String::from_utf8(human.stdout.clone()).unwrap();
    for line in [
        format!("agent login: {AGENT_LOGIN_ID}"),
        "generation: 3".into(),
    ] {
        assert!(has_line(&text, &line), "missing {line:?} in\n{text}");
    }
    for output in [&from_file, &token_file, &human] {
        assert_no_secret_output(output, &[SENTINEL, &encoded(CODEX_MATERIAL.as_bytes())]);
    }
    let requests = server.finish();
    assert_eq!(requests.len(), 7);
    // The organization comes from the read: no slug is resolved.
    assert!(
        requests
            .iter()
            .all(|request| !request.contains("/api/v1/organizations")),
        "{requests:?}"
    );
    assert_reconnect_requests(&requests[..2], CODEX_MATERIAL.as_bytes());
    // A lost response is replayed once with the same key.
    assert_reconnect_requests(&requests[2..5], SENTINEL.as_bytes());
    assert_eq!(
        header_value(&requests[3], "idempotency-key"),
        header_value(&requests[4], "idempotency-key")
    );
    assert_reconnect_requests(&requests[5..], CODEX_MATERIAL.as_bytes());
}

// REQ-AGENTLOGIN-44
#[test]
fn reconnect_refuses_a_source_its_provider_does_not_take_and_an_unknown_or_revoked_login() {
    let not_found = || agent_login_problem("404 Not Found", 404, "agent-login-not-found");
    let directory = tempfile::tempdir().unwrap();
    let codex_file = private_file(directory.path(), "auth.json", CODEX_MATERIAL.as_bytes());
    let token_file = private_file(directory.path(), "token", SENTINEL.as_bytes());
    let codex_file = codex_file.to_str().unwrap();
    let token_file = token_file.to_str().unwrap();
    // The response to the read, the source, and the `--json` outcome of a refusal (exit 1), or
    // none for a usage refusal (exit 2), which prints no document.
    let refused = Some("agent_login_not_found");
    let cases: [(Vec<u8>, &[&str], Option<&str>); 6] = [
        (
            target("codex", ORGANIZATION_ID),
            &["--token-file", token_file],
            None,
        ),
        (
            target("claude_code", ORGANIZATION_ID),
            &["--from-file", codex_file],
            None,
        ),
        (target("claude_code", ORGANIZATION_ID), &["--login"], None),
        (not_found(), &["--from-file", codex_file], refused),
        (revoked_target(), &["--from-file", codex_file], refused),
        (revoked_target(), &["--login"], refused),
    ];
    for (response, source, outcome) in cases {
        let (server, _directory, credential) = prepared(vec![response]);
        let codex = FakeCodex::new(SignIn::Writes(CODEX_MATERIAL));
        let home = AmbientHome::new();
        let mut args = vec!["reconnect", AGENT_LOGIN_ID];
        args.extend_from_slice(source);
        args.push("--json");
        let output = home.run(&server, &credential, &codex.bin, &args);

        let status = if outcome.is_some() { 1 } else { 2 };
        assert_eq!(output.status.code(), Some(status), "{source:?}");
        match outcome {
            Some(outcome) => assert_eq!(json_output(&output)["outcome"], outcome, "{source:?}"),
            None => assert!(output.stdout.is_empty(), "{source:?}"),
        }
        assert_eq!(codex.recorded("arguments"), None, "{source:?}");
        assert_no_secret_output(&output, &[SENTINEL, &encoded(CODEX_MATERIAL.as_bytes())]);
        home.assert_untouched_and_clean();
        assert_only_reads(&server.finish(), 1);
    }
}

// REQ-AGENTLOGIN-44
// REQ-AGENTLOGIN-22
// REQ-AGENTLOGIN-23
#[test]
fn reconnect_refuses_an_unsafe_or_malformed_file_as_import_does() {
    use std::os::unix::fs::symlink;
    let directory = tempfile::tempdir().unwrap();
    let codex = private_file(directory.path(), "auth.json", CODEX_MATERIAL.as_bytes());
    let alias = directory.path().join("alias.json");
    symlink(&codex, &alias).unwrap();
    let world_readable = private_file(directory.path(), "readable.json", CODEX_MATERIAL.as_bytes());
    fs::set_permissions(&world_readable, Permissions::from_mode(0o644)).unwrap();
    let array = private_file(
        directory.path(),
        "array.json",
        format!("[\"{SENTINEL}\"]").as_bytes(),
    );
    let spaced = private_file(
        directory.path(),
        "spaced",
        format!("{SENTINEL} more\n").as_bytes(),
    );
    let path = |path: &Path| path.to_str().unwrap().to_owned();
    let cases = [
        ("codex", "--from-file", path(&world_readable)),
        ("codex", "--from-file", path(&alias)),
        ("codex", "--from-file", path(&array)),
        ("claude_code", "--token-file", path(&spaced)),
    ];
    for (provider, flag, file) in &cases {
        let (server, _directory, credential) = prepared(vec![target(provider, ORGANIZATION_ID)]);
        let output = run_agent_login(
            &server,
            &credential,
            &["reconnect", AGENT_LOGIN_ID, flag, file, "--json"],
            &[],
        );
        assert_eq!(output.status.code(), Some(2), "{provider} {flag} {file}");
        assert!(output.stdout.is_empty(), "{provider} {flag} {file}");
        assert_no_secret_output(&output, &[SENTINEL]);
        assert_only_reads(&server.finish(), 1);
    }
}

// REQ-AGENTLOGIN-44
// REQ-AGENTLOGIN-24
// INV-CLI-13
#[test]
fn reconnect_codex_login_signs_in_in_a_private_temporary_home_after_the_read() {
    let (server, _directory, credential) = prepared(vec![
        target("codex", ORGANIZATION_ID),
        stored("200 OK", "codex", 3),
    ]);
    let codex = FakeCodex::new(SignIn::Writes(CODEX_MATERIAL));
    let home = AmbientHome::new();
    let output = home.run(
        &server,
        &credential,
        &codex.bin,
        &["reconnect", AGENT_LOGIN_ID, "--login", "--json"],
    );

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let document = json_output(&output);
    assert_eq!(document["outcome"], "reconnected");
    assert_eq!(document["agentLogin"]["generation"], 3);
    assert_no_secret_output(&output, &[SENTINEL, AMBIENT_MARKER]);
    assert_eq!(codex.recorded("arguments").unwrap(), "login\n");
    let codex_home = std::path::PathBuf::from(codex.recorded("home").unwrap());
    assert!(codex_home.starts_with(home.temporary.path()));
    home.assert_untouched_and_clean();
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert_reconnect_requests(&requests, CODEX_MATERIAL.as_bytes());
    assert!(!requests[1].contains(&encoded(AMBIENT_MARKER.as_bytes())));
}

const AUTHORIZATION_ID: &str = "aga_01k0z6r1w8f4jy2m7q9v3x5abe";

fn authorized() -> Vec<u8> {
    http_response_with_headers(
        "201 Created",
        Some("application/json"),
        &[("Idempotency-Key", ECHO_IDEMPOTENCY_KEY)],
        &serde_json::to_vec(&serde_json::json!({
            "id": AUTHORIZATION_ID,
            "runnerId": RUNNER_ID,
            "createdAt": "2026-11-06T09:00:00Z"
        }))
        .unwrap(),
    )
}

// The statement `authorize` makes conveys both exposures; its wording is not a contract, so
// each is identified by one stable phrase.
fn assert_states_both_exposures(stderr: &[u8]) {
    let text = String::from_utf8_lossy(stderr).to_lowercase();
    assert!(text.contains("whichever member"), "{text}");
    assert!(text.contains("every step"), "{text}");
}

// REQ-AGENTLOGIN-25
#[test]
fn authorize_states_who_can_use_the_login_before_it_authorizes_the_runner() {
    let (server, _directory, credential) = prepared(vec![authorized(), Vec::new(), authorized()]);
    let human = run_agent_login(
        &server,
        &credential,
        &["authorize", AGENT_LOGIN_ID, "--runner", RUNNER_ID, "--yes"],
        &[],
    );
    let output = run_agent_login(
        &server,
        &credential,
        &[
            "authorize",
            AGENT_LOGIN_ID,
            "--runner",
            RUNNER_ID,
            "--yes",
            "--json",
        ],
        &[],
    );

    assert!(
        human.status.success(),
        "{}",
        String::from_utf8_lossy(&human.stderr)
    );
    assert_states_both_exposures(&human.stderr);
    let text = String::from_utf8(human.stdout).unwrap();
    assert!(text.contains(AUTHORIZATION_ID), "{text}");
    assert!(text.contains(RUNNER_ID), "{text}");
    assert!(output.status.success());
    assert_states_both_exposures(&output.stderr);
    let document = json_output(&output);
    assert_eq!(document["outcome"], "authorized");
    assert_eq!(document["agentLoginId"], AGENT_LOGIN_ID);
    assert_eq!(document["authorization"]["id"], AUTHORIZATION_ID);
    assert_eq!(document["authorization"]["runnerId"], RUNNER_ID);
    let requests = server.finish();
    assert_eq!(requests.len(), 3);
    for request in &requests {
        assert!(request.starts_with(&format!(
            "POST /api/v1/me/agent-logins/{AGENT_LOGIN_ID}/authorizations HTTP/1.1\r\n"
        )));
        assert_eq!(
            request_body(request),
            serde_json::json!({"runnerId": RUNNER_ID})
        );
    }
    // The JSON run lost its first response and replayed that request's key once.
    assert_ne!(
        header_value(&requests[0], "idempotency-key"),
        header_value(&requests[1], "idempotency-key")
    );
    assert_eq!(
        header_value(&requests[1], "idempotency-key"),
        header_value(&requests[2], "idempotency-key")
    );
}

// REQ-AGENTLOGIN-25
#[test]
fn authorize_and_deauthorize_without_confirmation_exit_2_before_any_request() {
    let output =
        run_refused_before_any_request(&["authorize", AGENT_LOGIN_ID, "--runner", RUNNER_ID]);
    assert_states_both_exposures(&output.stderr);
    run_refused_before_any_request(&["deauthorize", AGENT_LOGIN_ID, "--runner", RUNNER_ID]);
    // A runner is named by its ID; a name cannot be resolved without an organization.
    run_refused_before_any_request(&["authorize", AGENT_LOGIN_ID, "--runner", "builder", "--yes"]);
}

// REQ-AGENTLOGIN-25
#[test]
fn deauthorize_ends_the_named_runners_authorization_in_force() {
    // The read after the change lists the authorization just ended with its removal state.
    let mut after = agent_logins()[0].clone();
    after["authorizations"] = serde_json::json!([after["authorizations"][0].clone()]);
    after["endedAuthorizations"] = serde_json::json!([ended_authorization(
        "aga_01k0z6r1w8f4jy2m7q9v3x5abd",
        SECOND_RUNNER_ID,
        true,
        serde_json::json!({"state": "pending"}),
    )]);
    let (server, _directory, credential) = prepared(vec![
        json_http_response("200 OK", agent_logins()[0].clone()),
        revoked(),
        json_http_response("200 OK", after.clone()),
        json_http_response("200 OK", agent_logins()[0].clone()),
        revoked(),
        json_http_response("200 OK", after),
    ]);
    let output = run_agent_login(
        &server,
        &credential,
        &[
            "deauthorize",
            AGENT_LOGIN_ID,
            "--runner",
            SECOND_RUNNER_ID,
            "--yes",
            "--json",
        ],
        &[],
    );

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        json_output(&output),
        serde_json::json!({
            "schemaVersion": 1,
            "deployment": server.api_url,
            "outcome": "deauthorized",
            "agentLoginId": AGENT_LOGIN_ID,
            "authorizationId": "aga_01k0z6r1w8f4jy2m7q9v3x5abd",
            "runnerId": SECOND_RUNNER_ID,
            "removal": {"state": "pending"}
        })
    );
    let human = run_agent_login(
        &server,
        &credential,
        &[
            "deauthorize",
            AGENT_LOGIN_ID,
            "--runner",
            SECOND_RUNNER_ID,
            "--yes",
        ],
        &[],
    );
    assert!(human.status.success());
    let text = String::from_utf8(human.stdout).unwrap();
    assert!(
        has_line(&text, &format!("runner: {SECOND_RUNNER_ID}")),
        "{text}"
    );
    assert!(has_line(&text, "removal: pending"), "{text}");
    assert!(!text.contains("cmd_"), "{text}");
    let requests = server.finish();
    assert_eq!(requests.len(), 6);
    for run in requests.chunks(3) {
        assert!(run[0].starts_with(&format!(
            "GET /api/v1/me/agent-logins/{AGENT_LOGIN_ID} HTTP/1.1\r\n"
        )));
        assert!(run[1].starts_with(&format!(
            "DELETE /api/v1/me/agent-logins/{AGENT_LOGIN_ID}/authorizations/aga_01k0z6r1w8f4jy2m7q9v3x5abd HTTP/1.1\r\n"
        )));
        assert!(!header_value(&run[1], "idempotency-key").is_empty());
        assert!(run[2].starts_with(&format!(
            "GET /api/v1/me/agent-logins/{AGENT_LOGIN_ID} HTTP/1.1\r\n"
        )));
    }
}

// REQ-AGENTLOGIN-25
// REQ-AGENTLOGIN-38
#[test]
fn deauthorize_reports_the_stop_requested_of_the_runners_run() {
    let mut after = agent_logins()[0].clone();
    after["authorizations"] = serde_json::json!([after["authorizations"][0].clone()]);
    after["endedAuthorizations"] = serde_json::json!([with_stop(ended_authorization(
        "aga_01k0z6r1w8f4jy2m7q9v3x5abd",
        SECOND_RUNNER_ID,
        true,
        serde_json::json!({"state": "pending"}),
    ))]);
    let (server, _directory, credential) = prepared(vec![
        json_http_response("200 OK", agent_logins()[0].clone()),
        revoked(),
        json_http_response("200 OK", after.clone()),
        json_http_response("200 OK", agent_logins()[0].clone()),
        revoked(),
        json_http_response("200 OK", after.clone()),
    ]);
    let arguments = [
        "deauthorize",
        AGENT_LOGIN_ID,
        "--runner",
        SECOND_RUNNER_ID,
        "--yes",
    ];
    let mut json = arguments.to_vec();
    json.push("--json");
    let output = run_agent_login(&server, &credential, &json, &[]);
    let human = run_agent_login(&server, &credential, &arguments, &[]);

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let document = json_output(&output);
    assert_eq!(document["removal"], serde_json::json!({"state": "pending"}));
    assert_eq!(document["stop"], after["endedAuthorizations"][0]["stop"]);
    assert!(human.status.success());
    let text = String::from_utf8(human.stdout).unwrap();
    for id in [RUN_ID, CANCELLATION_REQUEST_ID] {
        assert!(text.contains(id), "{text}");
    }
    assert_eq!(server.finish().len(), 6);
}

// REQ-AGENTLOGIN-21
// REQ-AGENTLOGIN-25
#[test]
fn deauthorize_reports_authorization_not_found_when_the_runner_holds_none() {
    let (server, _directory, credential) = prepared(vec![json_http_response(
        "200 OK",
        agent_logins()[0].clone(),
    )]);
    let output = run_agent_login(
        &server,
        &credential,
        &[
            "deauthorize",
            AGENT_LOGIN_ID,
            "--runner",
            "rnr_01k0z6r1w8f4jy2m7q9v3x5abe",
            "--yes",
            "--json",
        ],
        &[],
    );

    assert_eq!(output.status.code(), Some(1));
    assert_eq!(json_output(&output)["outcome"], "authorization_not_found");
    let requests = server.finish();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].starts_with("GET "));
}

// REQ-AGENTLOGIN-21
// REQ-AGENTLOGIN-25
#[test]
fn authorize_refusals_exit_1_with_their_remedy() {
    let cases = [
        (
            agent_login_problem("404 Not Found", 404, "agent-login-runner-not-found"),
            "runner_not_found",
            "um runner list",
        ),
        (
            agent_login_problem("409 Conflict", 409, "agent-login-runner-conflict"),
            "runner_holds_login",
            "um auth agent-login deauthorize",
        ),
        (
            agent_login_problem("409 Conflict", 409, "agent-login-already-authorized"),
            "login_already_authorized",
            "um auth agent-login import",
        ),
    ];
    for (response, outcome, remedy) in cases {
        let (server, _directory, credential) = prepared(vec![response.clone(), response]);
        let args = ["authorize", AGENT_LOGIN_ID, "--runner", RUNNER_ID, "--yes"];
        let output = run_agent_login(
            &server,
            &credential,
            &[&args[..], &["--json"]].concat(),
            &[],
        );
        let human = run_agent_login(&server, &credential, &args, &[]);

        assert_eq!(output.status.code(), Some(1), "{outcome}");
        assert_eq!(json_output(&output)["outcome"], outcome);
        assert_eq!(human.status.code(), Some(1), "{outcome}");
        let text = String::from_utf8(human.stderr).unwrap();
        assert!(text.contains(remedy), "{outcome}: {text}");
        assert_eq!(server.finish().len(), 2, "{outcome}");
    }
}
