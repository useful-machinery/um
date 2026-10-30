use super::*;

const ORG: &str = "acme-research";
const PROJECT: &str = "prj_01k0z6r1w8f4jy2m7q9v3x5abc";
const SCHEDULE: &str = "sch_01k0z6r1w8f4jy2m7q9v3x5abc";
const OCCURRENCE: &str = "occ_01k0z6r1w8f4jy2m7q9v3x5abc";
const MISSED_OCCURRENCE: &str = "occ_01k0z6r1w8f4jy2m7q9v3x5abd";
const GRANT: &str = "scg_01k0z6r1w8f4jy2m7q9v3x5abc";
const RUN: &str = "run_01k0z6r1w8f4jy2m7q9v3x5abc";
const BLOCKING_RUN: &str = "run_01k0z6r1w8f4jy2m7q9v3x5abd";
const SECRET: &str = "private-schedule-token-sentinel";
const SERVICE_API_KEY: &str =
    "crd_01k0z6r1w8f4jy2m7q9v3x5abc.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
const UPCOMING: [&str; 3] = [
    "2026-10-24T09:30:00Z",
    "2026-10-24T09:45:00Z",
    "2026-10-24T10:00:00Z",
];

fn prepared(responses: Vec<Vec<u8>>) -> (ScriptedServer, tempfile::TempDir, String) {
    let server = ScriptedServer::respond(responses);
    let directory = private_credential_directory();
    let path = directory.path().join("credentials.json");
    write_credential_fixture(&path, &server.api_url, SECRET, "2999-01-01T00:00:00Z");
    (server, directory, path.to_str().unwrap().to_owned())
}
fn call(server: &ScriptedServer, credential: &str, args: &[&str]) -> Output {
    run_with_env(args, &deployment_environment(&server.api_url, credential))
}
fn value(output: &Output) -> serde_json::Value {
    serde_json::from_slice(&output.stdout).unwrap()
}
fn schedule_args<'a>(leaf: &[&'a str], extra: &[&'a str]) -> Vec<&'a str> {
    let mut args = vec!["project", "schedule"];
    args.extend_from_slice(leaf);
    args.extend_from_slice(&[ORG, PROJECT]);
    args.extend_from_slice(extra);
    args.push("--allow-insecure-http");
    args
}
fn configuration() -> serde_json::Value {
    serde_json::json!({"name":"nightly build","enabled":true,
        "timing":{"cron":"*/15 * * * *","timeZone":"Europe/Berlin"},
        "target":{"workflowPath":".um/cloud/build.yaml","executionPrincipalId":"prn_01k0z6r1w8f4jy2m7q9v3x5abc"},
        "inputs":{"message":"ready","settings":{"depth":2},"nullable":null},
        "integrationContext":{"team":"platform"},
        "publication":{"exportName":"published"}})
}
fn schedule() -> serde_json::Value {
    let mut schedule = configuration();
    schedule.as_object_mut().unwrap().extend(
        serde_json::json!({"id":SCHEDULE,"projectId":PROJECT,"version":3,"grantId":GRANT,
            "nextDueAt":UPCOMING[0],"upcoming":UPCOMING,
            "lastOccurrence":{"id":OCCURRENCE,"dueAt":"2026-10-24T09:15:00Z","state":"skipped_active","reasonCode":"active_run"},
            "createdAt":"2026-10-01T00:00:00Z","updatedAt":"2026-10-02T00:00:00Z"})
        .as_object()
        .unwrap()
        .clone(),
    );
    schedule
}
fn confirmed_schedule(status: &str) -> Vec<u8> {
    confirmed(status, &serde_json::to_vec(&schedule()).unwrap())
}
fn confirmed(status: &str, body: &[u8]) -> Vec<u8> {
    http_response_with_headers(
        status,
        (!body.is_empty()).then_some("application/json"),
        &[(
            "Idempotency-Key",
            api_test_support::REQUEST_IDEMPOTENCY_KEY_ECHO,
        )],
        body,
    )
}
fn problem(status: &str, code: u16, kind: &str) -> Vec<u8> {
    problem_http_response(
        status,
        serde_json::json!({"type":format!("https://api.usefulmachinery.com/problems/{kind}"),"title":"Refused","status":code}),
    )
}
fn occurrences() -> serde_json::Value {
    serde_json::json!([
        {"id":OCCURRENCE,"scheduleId":SCHEDULE,"scheduleVersion":3,"grantId":GRANT,
         "dueAt":"2026-10-24T09:15:00Z","missedCount":0,"createdAt":"2026-10-24T09:15:01Z",
         "settledAt":"2026-10-24T09:15:02Z","expiresAt":"2026-11-23T09:15:00Z",
         "state":"skipped_active","reasonCode":"active_run","attemptNumber":1,"blockingRunId":BLOCKING_RUN},
        {"id":MISSED_OCCURRENCE,"scheduleId":SCHEDULE,"scheduleVersion":3,"grantId":GRANT,
         "dueAt":"2026-10-24T08:15:00Z","missedCount":2,"createdAt":"2026-10-24T08:52:00Z",
         "settledAt":"2026-10-24T08:52:03Z","expiresAt":"2026-11-23T08:15:00Z",
         "state":"run_created","attemptNumber":1,"runId":RUN}
    ])
}
fn body(request: &str) -> serde_json::Value {
    serde_json::from_str(request.split_once("\r\n\r\n").unwrap().1).unwrap()
}

// REQ-SCHEDULE-40, REQ-SCHEDULE-41, REQ-SCHEDULE-42, REQ-SCHEDULE-43
#[test]
fn schedule_lifecycle_sends_the_file_and_reports_each_outcome() {
    let (server, dir, credential) = prepared(vec![
        confirmed_schedule("201 Created"),
        confirmed_schedule("200 OK"),
        problem("409 Conflict", 409, "source-connection-conflict"),
        confirmed_schedule("200 OK"),
        confirmed_schedule("200 OK"),
        confirmed("204 No Content", b""),
    ]);
    let config_file = dir.path().join("config.json");
    fs::write(&config_file, serde_json::to_vec(&configuration()).unwrap()).unwrap();
    let file = config_file.to_str().unwrap();
    let create = call(
        &server,
        &credential,
        &schedule_args(&["create"], &["--config-file", file, "--json"]),
    );
    assert!(
        create.status.success(),
        "{}",
        String::from_utf8_lossy(&create.stderr)
    );
    assert_eq!(value(&create)["outcome"], "created");
    assert_eq!(value(&create)["organizationRef"], ORG);
    assert_eq!(value(&create)["schedule"], schedule());
    let update_file = serde_json::json!({"name":"nightly","timing":{"cron":"0 3 * * *","timeZone":"UTC"},"publication":null});
    fs::write(&config_file, serde_json::to_vec(&update_file).unwrap()).unwrap();
    let update = || {
        call(
            &server,
            &credential,
            &schedule_args(
                &["update"],
                &[
                    SCHEDULE,
                    "--config-file",
                    file,
                    "--expected-version",
                    "3",
                    "--json",
                ],
            ),
        )
    };
    let updated = update();
    assert!(updated.status.success());
    assert_eq!(value(&updated)["outcome"], "updated");
    let conflict = update();
    assert_eq!(conflict.status.code(), Some(1));
    assert_eq!(value(&conflict)["outcome"], "conflict");
    let mut outputs = vec![create, updated, conflict];
    for (action, outcome) in [
        ("disable", "disabled"),
        ("enable", "enabled"),
        ("delete", "deleted"),
    ] {
        let mut extra = vec![SCHEDULE, "--json"];
        if action == "delete" {
            extra.push("--yes");
        }
        let output = call(&server, &credential, &schedule_args(&[action], &extra));
        assert!(
            output.status.success(),
            "{action}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(value(&output)["outcome"], outcome);
        if action == "delete" {
            assert!(value(&output)["schedule"].is_null());
        } else {
            assert_eq!(value(&output)["schedule"]["id"], SCHEDULE);
        }
        outputs.push(output);
    }
    let requests = server.finish();
    let base = format!("/api/v1/organizations/{ORG}/projects/{PROJECT}/schedules");
    let expected = [
        format!("POST {base} "),
        format!("PATCH {base}/{SCHEDULE} "),
        format!("PATCH {base}/{SCHEDULE} "),
        format!("POST {base}/{SCHEDULE}/disable "),
        format!("POST {base}/{SCHEDULE}/enable "),
        format!("DELETE {base}/{SCHEDULE} "),
    ];
    assert_eq!(requests.len(), expected.len());
    for (request, line) in requests.iter().zip(&expected) {
        assert!(request.starts_with(line.as_str()), "{request}");
    }
    assert_eq!(body(&requests[0]), configuration());
    let mut patch = update_file;
    patch["expectedVersion"] = 3.into();
    assert_eq!(body(&requests[1]), patch);
    for output in &outputs {
        assert!(!String::from_utf8_lossy(&output.stdout).contains(SECRET));
        assert!(!String::from_utf8_lossy(&output.stderr).contains(SECRET));
    }
}

// REQ-SCHEDULE-41
#[test]
fn invalid_schedule_configuration_never_dispatches() {
    let (server, dir, credential) = prepared(vec![]);
    let config_file = dir.path().join("config.json");
    let file = config_file.to_str().unwrap();
    let mut unknown = configuration();
    unknown["token"] = "secret".into();
    let mut null_name = configuration();
    null_name["name"] = serde_json::Value::Null;
    let mut oversized = configuration();
    oversized["name"] = "a".repeat(1024 * 1024).into();
    let create = schedule_args(&["create"], &["--config-file", file, "--json"]);
    let update = schedule_args(
        &["update"],
        &[
            SCHEDULE,
            "--config-file",
            file,
            "--expected-version",
            "1",
            "--json",
        ],
    );
    for (contents, args) in [
        (unknown, &create),
        (null_name, &create),
        (oversized, &create),
        (serde_json::json!({"enabled":false}), &update),
        (serde_json::json!({}), &update),
    ] {
        fs::write(&config_file, serde_json::to_vec(&contents).unwrap()).unwrap();
        let output = call(&server, &credential, args);
        assert_eq!(output.status.code(), Some(2), "{contents}");
        assert_eq!(value(&output)["outcome"], "invalid_configuration");
    }
    let competing = call(
        &server,
        &credential,
        &schedule_args(
            &["create"],
            &[
                "--config-file",
                "-",
                "--service-api-key-file",
                "-",
                "--json",
            ],
        ),
    );
    assert_eq!(competing.status.code(), Some(2));
    assert_eq!(value(&competing)["outcome"], "invalid_configuration");
    assert!(server.finish().is_empty());
}

// REQ-SCHEDULE-42, REQ-SCHEDULE-21
#[test]
fn show_reports_timing_and_tombstones_but_never_a_partial_schedule() {
    let tombstone = serde_json::json!({"id":SCHEDULE,"projectId":PROJECT,"deleted":true,"deletedAt":"2026-10-24T10:00:00Z"});
    let mut extended = tombstone.clone();
    extended["name"] = "nightly".into();
    let mut elsewhere = schedule();
    elsewhere["projectId"] = "prj_01k0z6r1w8f4jy2m7q9v3x5abd".into();
    let (server, _dir, credential) = prepared(vec![
        json_http_response("200 OK", schedule()),
        json_http_response("200 OK", schedule()),
        json_http_response("200 OK", tombstone),
        json_http_response("200 OK", extended),
        json_http_response("200 OK", elsewhere),
    ]);
    let show = |json: bool| {
        let mut extra = vec![SCHEDULE];
        if json {
            extra.push("--json");
        }
        call(&server, &credential, &schedule_args(&["show"], &extra))
    };
    let plain = show(false);
    assert!(plain.status.success());
    let text = String::from_utf8(plain.stdout).unwrap();
    for expected in ["*/15 * * * *", "Europe/Berlin"].iter().chain(&UPCOMING) {
        assert!(text.contains(expected), "{text}");
    }
    let active = show(true);
    assert_eq!(value(&active)["outcome"], "shown");
    assert_eq!(value(&active)["schedule"], schedule());
    let deleted = show(true);
    assert!(deleted.status.success());
    assert_eq!(value(&deleted)["outcome"], "shown");
    assert_eq!(value(&deleted)["schedule"]["deleted"], true);
    for _ in 0..2 {
        let refused = show(true);
        assert_eq!(refused.status.code(), Some(1));
        assert_eq!(value(&refused)["outcome"], "invalid_response");
        assert!(value(&refused)["schedule"].is_null());
    }
    assert_eq!(server.finish().len(), 5);
}

// REQ-SCHEDULE-40, REQ-SCHEDULE-42, SC-SCHEDULE-1
#[test]
fn occurrence_list_reports_skips_and_folded_misses() {
    let page = serde_json::json!({"items":occurrences(),"nextCursor":"c2"});
    let (server, _dir, credential) = prepared(vec![
        json_http_response("200 OK", page.clone()),
        json_http_response("200 OK", page),
        json_http_response("200 OK", occurrences()[0].clone()),
    ]);
    let filters = [
        SCHEDULE,
        "--state",
        "skipped_active",
        "--limit",
        "10",
        "--cursor",
        "c1",
    ];
    let mut json_filters = filters.to_vec();
    json_filters.push("--json");
    let listed = call(
        &server,
        &credential,
        &schedule_args(&["occurrence", "list"], &json_filters),
    );
    assert!(
        listed.status.success(),
        "{}",
        String::from_utf8_lossy(&listed.stderr)
    );
    let listed = value(&listed);
    assert_eq!(listed["outcome"], "listed");
    assert_eq!(listed["nextCursor"], "c2");
    assert_eq!(listed["items"], occurrences());
    assert_eq!(listed["items"][0]["blockingRunId"], BLOCKING_RUN);
    assert_eq!(listed["items"][1]["missedCount"], 2);
    let plain = call(
        &server,
        &credential,
        &schedule_args(&["occurrence", "list"], &filters),
    );
    assert!(plain.status.success());
    let text = String::from_utf8(plain.stdout).unwrap();
    let line = |id: &str| text.lines().find(|line| line.contains(id)).unwrap();
    for expected in [
        "2026-10-24T09:15:00Z",
        "skipped_active",
        "active_run",
        BLOCKING_RUN,
    ] {
        assert!(line(OCCURRENCE).contains(expected), "{text}");
    }
    for expected in ["2026-10-24T08:15:00Z", "run_created", RUN] {
        assert!(line(MISSED_OCCURRENCE).contains(expected), "{text}");
    }
    assert!(text.lines().last().unwrap().contains("c2"), "{text}");
    let shown = call(
        &server,
        &credential,
        &schedule_args(&["occurrence", "show"], &[SCHEDULE, OCCURRENCE, "--json"]),
    );
    assert_eq!(value(&shown)["outcome"], "shown");
    assert_eq!(value(&shown)["occurrence"], occurrences()[0]);
    for invalid in [["--limit", "0"], ["--limit", "101"], ["--state", "later"]] {
        let mut extra = vec![SCHEDULE];
        extra.extend_from_slice(&invalid);
        let output = call(
            &server,
            &credential,
            &schedule_args(&["occurrence", "list"], &extra),
        );
        assert_eq!(output.status.code(), Some(2), "{invalid:?}");
    }
    let requests = server.finish();
    assert_eq!(requests.len(), 3);
    let path =
        format!("/api/v1/organizations/{ORG}/projects/{PROJECT}/schedules/{SCHEDULE}/occurrences");
    let target = requests[0].split(' ').nth(1).unwrap();
    let (request_path, query) = target.split_once('?').unwrap();
    assert!(requests[0].starts_with("GET "));
    assert_eq!(request_path, path);
    let mut parameters: Vec<_> = query.split('&').collect();
    parameters.sort_unstable();
    assert_eq!(
        parameters,
        ["cursor=c1", "limit=10", "state=skipped_active"]
    );
    assert!(requests[2].starts_with(&format!("GET {path}/{OCCURRENCE} ")));
}

// REQ-SCHEDULE-43
#[test]
fn schedule_refusals_report_closed_outcomes_and_typed_reasons() {
    let (server, dir, credential) = prepared(vec![
        problem("400 Bad Request", 400, "timing-too-frequent"),
        problem("400 Bad Request", 400, "bad-request"),
        problem("401 Unauthorized", 401, "unauthorized"),
        problem("403 Forbidden", 403, "forbidden"),
        problem("404 Not Found", 404, "not-found"),
        problem("503 Service Unavailable", 503, "unavailable"),
    ]);
    let config_file = dir.path().join("config.json");
    fs::write(&config_file, serde_json::to_vec(&configuration()).unwrap()).unwrap();
    let file = config_file.to_str().unwrap();
    let key_file = dir.path().join("service.key");
    fs::write(&key_file, format!("{SERVICE_API_KEY}\n")).unwrap();
    fs::set_permissions(&key_file, Permissions::from_mode(0o600)).unwrap();
    let key_file = key_file.to_str().unwrap();
    let create = schedule_args(&["create"], &["--config-file", file, "--json"]);
    let too_frequent = call(&server, &credential, &create);
    assert_eq!(too_frequent.status.code(), Some(2));
    assert_eq!(value(&too_frequent)["outcome"], "invalid_input");
    assert_eq!(value(&too_frequent)["reason"], "timing-too-frequent");
    let generic = call(&server, &credential, &create);
    assert_eq!(generic.status.code(), Some(2));
    assert_eq!(value(&generic)["outcome"], "invalid_input");
    assert!(value(&generic).get("reason").is_none());
    let unauthenticated = call(
        &server,
        &credential,
        &schedule_args(
            &["show"],
            &[SCHEDULE, "--service-api-key-file", key_file, "--json"],
        ),
    );
    assert_eq!(unauthenticated.status.code(), Some(3));
    assert_eq!(
        value(&unauthenticated)["outcome"],
        "authentication_required"
    );
    for (outcome, code) in [("forbidden", 1), ("not_found", 1), ("unavailable", 4)] {
        let output = call(
            &server,
            &credential,
            &schedule_args(&["show"], &[SCHEDULE, "--json"]),
        );
        assert_eq!(output.status.code(), Some(code), "{outcome}");
        assert_eq!(value(&output)["outcome"], outcome);
    }
    assert_eq!(server.finish().len(), 6);
}

// REQ-SCHEDULE-12, REQ-SCHEDULE-43, REQ-SCHEDULE-49
#[test]
fn schedule_quota_source_and_workflow_refusals_are_classified() {
    for (action, status, kind, outcome, exit) in [
        (
            "create",
            "409 Conflict",
            "quantity-limit-reached",
            "conflict",
            1,
        ),
        (
            "enable",
            "409 Conflict",
            "workflow-source-unavailable",
            "conflict",
            1,
        ),
        (
            "create",
            "422 Unprocessable Entity",
            "workflow-source-invalid",
            "invalid_input",
            2,
        ),
    ] {
        let code: u16 = status[..3].parse().unwrap();
        let (server, dir, credential) = prepared(vec![problem(status, code, kind)]);
        let config_file = dir.path().join("config.json");
        fs::write(&config_file, serde_json::to_vec(&configuration()).unwrap()).unwrap();
        let args = if action == "create" {
            schedule_args(
                &["create"],
                &["--config-file", config_file.to_str().unwrap(), "--json"],
            )
        } else {
            schedule_args(&[action], &[SCHEDULE, "--json"])
        };
        let output = call(&server, &credential, &args);
        assert_eq!(value(&output)["outcome"], outcome, "{action} {kind}");
        assert_eq!(output.status.code(), Some(exit), "{action} {kind}");
        if outcome == "invalid_input" {
            assert_eq!(value(&output)["reason"], kind);
        }
        assert_eq!(server.finish().len(), 1, "{action} {kind}");
    }
}

// REQ-SCHEDULE-43
#[test]
fn lost_schedule_responses_reuse_one_key() {
    let (server, _dir, credential) = prepared(vec![Vec::new(), confirmed_schedule("200 OK")]);
    let output = call(
        &server,
        &credential,
        &schedule_args(&["disable"], &[SCHEDULE, "--json"]),
    );
    assert!(output.status.success());
    assert_eq!(value(&output)["outcome"], "disabled");
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert!(requests.iter().all(|request| request.starts_with("POST ")));
    assert_eq!(
        header_value(&requests[0], "idempotency-key"),
        header_value(&requests[1], "idempotency-key")
    );

    let (server, _dir, credential) = prepared(vec![Vec::new(), Vec::new()]);
    let output = call(
        &server,
        &credential,
        &schedule_args(&["enable"], &[SCHEDULE, "--json"]),
    );
    assert_eq!(output.status.code(), Some(4));
    assert_eq!(value(&output)["outcome"], "commitment_unknown");
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        value(&output)["idempotencyKey"],
        header_value(&requests[0], "idempotency-key")
    );
    assert_eq!(
        header_value(&requests[0], "idempotency-key"),
        header_value(&requests[1], "idempotency-key")
    );
}

// REQ-SCHEDULE-40, REQ-SCHEDULE-42, REQ-SCHEDULE-43
#[test]
fn schedule_list_pages_with_limit_and_cursor_and_never_emits_partial_items() {
    let mut elsewhere = schedule();
    elsewhere["projectId"] = "prj_01k0z6r1w8f4jy2m7q9v3x5abd".into();
    let (server, _dir, credential) = prepared(vec![
        json_http_response(
            "200 OK",
            serde_json::json!({"items":[schedule()],"nextCursor":"c2"}),
        ),
        json_http_response(
            "200 OK",
            serde_json::json!({"items":[schedule(), elsewhere]}),
        ),
    ]);
    let list = || {
        call(
            &server,
            &credential,
            &schedule_args(&["list"], &["--limit", "10", "--cursor", "c1", "--json"]),
        )
    };
    let listed = list();
    assert!(
        listed.status.success(),
        "{}",
        String::from_utf8_lossy(&listed.stderr)
    );
    assert_eq!(value(&listed)["outcome"], "listed");
    assert_eq!(value(&listed)["nextCursor"], "c2");
    assert_eq!(value(&listed)["items"], serde_json::json!([schedule()]));
    let refused = list();
    assert_eq!(refused.status.code(), Some(1));
    assert_eq!(value(&refused)["outcome"], "invalid_response");
    assert!(value(&refused)["items"].is_null());
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    let path = format!("/api/v1/organizations/{ORG}/projects/{PROJECT}/schedules");
    for request in &requests {
        let target = request.split(' ').nth(1).unwrap();
        let (request_path, query) = target.split_once('?').unwrap();
        assert!(request.starts_with("GET "));
        assert_eq!(request_path, path);
        let mut parameters: Vec<_> = query.split('&').collect();
        parameters.sort_unstable();
        assert_eq!(parameters, ["cursor=c1", "limit=10"]);
    }
}
