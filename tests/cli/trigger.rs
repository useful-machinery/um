use super::*;

const ORG: &str = "acme-research";
const PROJECT: &str = "prj_01k0z6r1w8f4jy2m7q9v3x5abc";
const TRIGGER: &str = "ltr_01k0z6r1w8f4jy2m7q9v3x5abc";
const EVALUATION: &str = "lev_01k0z6r1w8f4jy2m7q9v3x5abc";
const SECRET: &str = "private-trigger-token-sentinel";

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
fn configuration() -> serde_json::Value {
    serde_json::json!({"enabled":true, "source":{"type":"linear","connectionId":"lcn_01k0z6r1w8f4jy2m7q9v3x5abc"},
        "target":{"workflowPath":".um/cloud/run.yaml","executionPrincipalId":"prn_01k0z6r1w8f4jy2m7q9v3x5abc"},
        "conditions":[{"field":"event.action","operator":"eq","value":"update"}],
        "inputs":{"ticket":{"kind":"file","source":{"type":"snapshot"}},"metadata":{"kind":"json","source":{"type":"field","field":"current.relations"}},
                  "nullable":{"kind":"json","source":{"type":"literal","value":null}},"title":{"kind":"text","source":{"type":"field","field":"current.title"}}},
        "integrationContext":{"issue":{"type":"field","field":"current.identifier"},"fixed":{"type":"literal","value":"ready"}}})
}
fn trigger() -> serde_json::Value {
    let mut trigger = configuration();
    trigger.as_object_mut().unwrap().extend(
        serde_json::json!({"id":TRIGGER,"projectId":PROJECT,"version":3,"grantId":"grant-1",
        "createdAt":"2026-01-01T00:00:00Z","updatedAt":"2026-01-02T00:00:00Z"})
        .as_object()
        .unwrap()
        .clone(),
    );
    trigger
}
fn confirmed_trigger(status: &str) -> Vec<u8> {
    http_response_with_headers(
        status,
        Some("application/json"),
        &[(
            "Idempotency-Key",
            api_test_support::REQUEST_IDEMPOTENCY_KEY_ECHO,
        )],
        &serde_json::to_vec(&trigger()).unwrap(),
    )
}
fn evaluation() -> serde_json::Value {
    serde_json::json!({"id":EVALUATION,"triggerId":TRIGGER,"configurationVersion":2,"connectionId":"lcn_1",
        "grantId":"grant-1","acceptedAt":"2026-01-01T00:00:00Z","expiresAt":"2026-02-01T00:00:00Z",
        "state":"skipped_active","reasonCode":"active_run","cycleNumber":1,"attemptNumber":2,
        "cycleStartedAt":"2026-01-01T00:00:00Z","cycleDeadline":"2026-01-02T00:00:00Z",
        "blockingRunId":"run-other","event":{"valid":false,"invalidFields":["issueId"]},
        "conditionResults":[{"index":0,"matched":false,"reasonCode":"evidence_unavailable"}]})
}
#[test]
fn trigger_lifecycle_replaces_collections_and_reports_conflicts() {
    let (server, dir, credential) = prepared(vec![
        confirmed_trigger("201 Created"),
        confirmed_trigger("200 OK"),
        problem_http_response(
            "409 Conflict",
            serde_json::json!({"type":"https://api.usefulmachinery.com/problems/source-connection-conflict","title":"Conflict","status":409}),
        ),
        confirmed_trigger("200 OK"),
        confirmed_trigger("200 OK"),
        http_response_with_headers(
            "204 No Content",
            None,
            &[(
                "Idempotency-Key",
                api_test_support::REQUEST_IDEMPOTENCY_KEY_ECHO,
            )],
            b"",
        ),
    ]);
    let config_file = dir.path().join("config.json");
    fs::write(&config_file, serde_json::to_vec(&configuration()).unwrap()).unwrap();
    let file = config_file.to_str().unwrap();
    let base = ["project", "trigger"];
    let create = call(
        &server,
        &credential,
        &[
            base[0],
            base[1],
            "create",
            ORG,
            PROJECT,
            "--config-file",
            file,
            "--json",
            "--allow-insecure-http",
        ],
    );
    assert!(
        create.status.success(),
        "{}",
        String::from_utf8_lossy(&create.stderr)
    );
    assert_eq!(value(&create)["trigger"]["version"], 3);
    fs::write(&config_file, br#"{"inputs":{},"integrationContext":{}}"#).unwrap();
    let update = || {
        call(
            &server,
            &credential,
            &[
                "project",
                "trigger",
                "update",
                ORG,
                PROJECT,
                TRIGGER,
                "--config-file",
                file,
                "--expected-version",
                "3",
                "--json",
                "--allow-insecure-http",
            ],
        )
    };
    assert!(update().status.success());
    let conflict = update();
    assert!(!conflict.status.success());
    assert_eq!(value(&conflict)["outcome"], "conflict");
    for action in ["disable", "enable", "delete"] {
        let mut args = vec![
            "project",
            "trigger",
            action,
            ORG,
            PROJECT,
            TRIGGER,
            "--json",
            "--allow-insecure-http",
        ];
        if action == "delete" {
            args.push("--yes");
        }
        let output = call(&server, &credential, &args);
        assert!(
            output.status.success(),
            "{action}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let requests = server.finish();
    assert_eq!(requests.len(), 6);
    let created: serde_json::Value =
        serde_json::from_str(requests[0].split_once("\r\n\r\n").unwrap().1).unwrap();
    assert!(
        created["inputs"]["nullable"]["source"]
            .get("value")
            .is_some()
    );
    assert!(created["inputs"]["nullable"]["source"]["value"].is_null());
    assert_eq!(
        created["inputs"]["title"]["source"]["field"],
        "current.title"
    );
    assert_eq!(created["integrationContext"]["fixed"]["value"], "ready");
    let body: serde_json::Value =
        serde_json::from_str(requests[1].split_once("\r\n\r\n").unwrap().1).unwrap();
    assert_eq!(
        body,
        serde_json::json!({"expectedVersion":3,"inputs":{},"integrationContext":{}})
    );
    assert!(requests[1].starts_with(&format!(
        "PATCH /api/v1/organizations/{ORG}/projects/{PROJECT}/triggers/{TRIGGER} "
    )));
    assert!(!String::from_utf8_lossy(&create.stdout).contains(SECRET));
}

#[test]
fn lost_lifecycle_response_reuses_one_key_and_never_reports_early_success() {
    let (server, _dir, credential) = prepared(vec![Vec::new(), confirmed_trigger("200 OK")]);
    let output = call(
        &server,
        &credential,
        &[
            "project",
            "trigger",
            "disable",
            ORG,
            PROJECT,
            TRIGGER,
            "--json",
            "--allow-insecure-http",
        ],
    );
    assert!(output.status.success());
    assert_eq!(value(&output)["outcome"], "disabled");
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        header_value(&requests[0], "idempotency-key"),
        header_value(&requests[1], "idempotency-key")
    );
    assert!(requests.iter().all(|request| request.starts_with("POST ")));
}

#[test]
fn two_lost_lifecycle_responses_expose_only_one_recoverable_request() {
    let (server, _dir, credential) = prepared(vec![Vec::new(), Vec::new()]);
    let output = call(
        &server,
        &credential,
        &[
            "project",
            "trigger",
            "enable",
            ORG,
            PROJECT,
            TRIGGER,
            "--json",
            "--allow-insecure-http",
        ],
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

#[test]
fn rejected_mutation_replay_retains_request_identity_and_closed_failure() {
    for (reply, outcome) in [
        (
            problem_http_response(
                "403 Forbidden",
                serde_json::json!({"type":"https://api.usefulmachinery.com/problems/forbidden","title":"Forbidden","status":403}),
            ),
            "forbidden",
        ),
        (
            http_response_with_headers(
                "429 Too Many Requests",
                Some("application/problem+json"),
                &[("Retry-After", "5")],
                b"{}",
            ),
            "rate_limited",
        ),
    ] {
        let (server, _dir, credential) = prepared(vec![Vec::new(), reply]);
        let output = call(
            &server,
            &credential,
            &[
                "project",
                "trigger",
                "disable",
                ORG,
                PROJECT,
                TRIGGER,
                "--json",
                "--allow-insecure-http",
            ],
        );
        assert_eq!(output.status.code(), Some(4));
        assert_eq!(value(&output)["outcome"], "commitment_unknown");
        assert_eq!(value(&output)["replayOutcome"], outcome);
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
}

#[test]
fn lost_create_response_followed_by_forbidden_replay_keeps_creation_key() {
    let (server, dir, credential) = prepared(vec![
        Vec::new(),
        problem_http_response(
            "403 Forbidden",
            serde_json::json!({"type":"https://api.usefulmachinery.com/problems/forbidden","title":"Forbidden","status":403}),
        ),
    ]);
    let path = dir.path().join("trigger.json");
    fs::write(&path, serde_json::to_vec(&configuration()).unwrap()).unwrap();
    let output = call(
        &server,
        &credential,
        &[
            "project",
            "trigger",
            "create",
            ORG,
            PROJECT,
            "--config-file",
            path.to_str().unwrap(),
            "--json",
            "--allow-insecure-http",
        ],
    );
    assert_eq!(output.status.code(), Some(4));
    assert_eq!(value(&output)["outcome"], "commitment_unknown");
    assert_eq!(value(&output)["replayOutcome"], "forbidden");
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert!(requests.iter().all(|request| request.starts_with("POST ")));
    assert_eq!(
        value(&output)["idempotencyKey"],
        header_value(&requests[0], "idempotency-key")
    );
    assert_eq!(
        header_value(&requests[0], "idempotency-key"),
        header_value(&requests[1], "idempotency-key")
    );
}

#[test]
fn mutation_responses_require_exact_identity_and_complete_configuration() {
    let mut later = trigger();
    later["source"]["providerToken"] = "sensitive".into();
    for reply in [
        json_http_response("200 OK", trigger()), // Missing request identity.
        confirmed_trigger("201 Created"),        // Wrong status for a lifecycle action.
        http_response_with_headers(
            "200 OK",
            Some("application/json"),
            &[(
                "Idempotency-Key",
                api_test_support::REQUEST_IDEMPOTENCY_KEY_ECHO,
            )],
            &serde_json::to_vec(&later).unwrap(),
        ),
    ] {
        let (server, _dir, credential) = prepared(vec![reply]);
        let output = call(
            &server,
            &credential,
            &[
                "project",
                "trigger",
                "enable",
                ORG,
                PROJECT,
                TRIGGER,
                "--json",
                "--allow-insecure-http",
            ],
        );
        assert_eq!(value(&output)["outcome"], "commitment_unknown");
        assert!(!output.status.success());
        assert!(!String::from_utf8_lossy(&output.stdout).contains("sensitive"));
        assert_eq!(server.finish().len(), 1);
    }
}

#[test]
fn delete_requires_exact_confirmed_response() {
    for reply in [
        http_response_with_headers(
            "307 Temporary Redirect",
            None,
            &[("Location", "/api/elsewhere")],
            b"",
        ),
        http_response("200 OK", Some("application/json"), b"{}"),
        http_response("204 No Content", None, b""),
        http_response_with_headers(
            "204 No Content",
            None,
            &[("Idempotency-Key", "another-key")],
            b"",
        ),
    ] {
        let (server, _dir, credential) = prepared(vec![reply]);
        let output = call(
            &server,
            &credential,
            &[
                "project",
                "trigger",
                "delete",
                ORG,
                PROJECT,
                TRIGGER,
                "--yes",
                "--json",
                "--allow-insecure-http",
            ],
        );
        assert!(!output.status.success());
        assert_eq!(value(&output)["outcome"], "commitment_unknown");
        let requests = server.finish();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].starts_with("DELETE "));
        assert_eq!(
            value(&output)["idempotencyKey"],
            header_value(&requests[0], "idempotency-key")
        );
    }
}

#[test]
fn list_rejects_late_schema_without_emitting_partial_items() {
    let mut later_trigger = trigger();
    later_trigger["inputs"]["metadata"]["source"]["newSelector"] = "sensitive".into();
    let mut later_evaluation = evaluation();
    later_evaluation["event"]["newFact"] = "sensitive".into();
    let (server, _dir, credential) = prepared(vec![
        json_http_response("200 OK", serde_json::json!({"items":[later_trigger]})),
        json_http_response("200 OK", serde_json::json!({"items":[later_evaluation]})),
    ]);
    for args in [
        vec!["project", "trigger", "list", ORG, PROJECT],
        vec![
            "project",
            "trigger",
            "evaluation",
            "list",
            ORG,
            PROJECT,
            TRIGGER,
        ],
    ] {
        let mut args = args;
        args.extend(["--json", "--allow-insecure-http"]);
        let output = call(&server, &credential, &args);
        assert!(!output.status.success());
        assert_eq!(value(&output)["outcome"], "invalid_response");
        assert!(!String::from_utf8_lossy(&output.stdout).contains("sensitive"));
    }
    assert_eq!(server.finish().len(), 2);
}

#[test]
fn filters_and_cursors_are_forwarded_once_and_history_preserves_immutable_facts() {
    let mut other_trigger = trigger();
    other_trigger["id"] = "ltr_second".into();
    other_trigger["createdAt"] = "2025-12-23T00:00:00Z".into();
    let mut other_evaluation = evaluation();
    other_evaluation["id"] = "lev_second".into();
    other_evaluation["acceptedAt"] = "2025-12-23T00:00:00Z".into();
    let (server, _dir, credential) = prepared(vec![
        json_http_response(
            "200 OK",
            serde_json::json!({"items":[trigger()],"nextCursor":"opaque-next"}),
        ),
        json_http_response("200 OK", serde_json::json!({"items":[other_trigger]})),
        json_http_response(
            "200 OK",
            serde_json::json!({"items":[evaluation()],"nextCursor":"different-next"}),
        ),
        json_http_response("200 OK", serde_json::json!({"items":[other_evaluation]})),
        json_http_response("200 OK", evaluation()),
    ]);
    let triggers = call(
        &server,
        &credential,
        &[
            "project",
            "trigger",
            "list",
            ORG,
            PROJECT,
            "--limit",
            "2",
            "--cursor",
            "page-one",
            "--json",
            "--allow-insecure-http",
        ],
    );
    assert_eq!(value(&triggers)["nextCursor"], "opaque-next");
    assert_eq!(value(&triggers)["items"][0]["id"], TRIGGER);
    let next_triggers = call(
        &server,
        &credential,
        &[
            "project",
            "trigger",
            "list",
            ORG,
            PROJECT,
            "--limit",
            "2",
            "--cursor",
            "opaque-next",
            "--json",
            "--allow-insecure-http",
        ],
    );
    assert_eq!(value(&next_triggers)["items"][0]["id"], "ltr_second");
    assert!(value(&next_triggers).get("nextCursor").is_none());
    let history = call(
        &server,
        &credential,
        &[
            "project",
            "trigger",
            "evaluation",
            "list",
            ORG,
            PROJECT,
            TRIGGER,
            "--limit",
            "7",
            "--cursor",
            "next-page",
            "--state",
            "skipped_active",
            "--run-id",
            "run-other",
            "--json",
            "--allow-insecure-http",
        ],
    );
    assert!(
        history.status.success(),
        "{}",
        String::from_utf8_lossy(&history.stderr)
    );
    assert_eq!(value(&history)["items"][0]["blockingRunId"], "run-other");
    assert_eq!(value(&history)["nextCursor"], "different-next");
    let next_history = call(
        &server,
        &credential,
        &[
            "project",
            "trigger",
            "evaluation",
            "list",
            ORG,
            PROJECT,
            TRIGGER,
            "--limit",
            "7",
            "--cursor",
            "different-next",
            "--state",
            "skipped_active",
            "--run-id",
            "run-other",
            "--json",
            "--allow-insecure-http",
        ],
    );
    assert_eq!(value(&next_history)["items"][0]["id"], "lev_second");
    assert!(value(&next_history).get("nextCursor").is_none());
    let show = call(
        &server,
        &credential,
        &[
            "project",
            "trigger",
            "evaluation",
            "show",
            ORG,
            PROJECT,
            TRIGGER,
            EVALUATION,
            "--json",
            "--allow-insecure-http",
        ],
    );
    assert_eq!(value(&show)["evaluation"], value(&history)["items"][0]);
    let requests = server.finish();
    assert_eq!(requests.len(), 5);
    assert!(requests[0].contains("limit=2&cursor=page-one"));
    assert!(requests[1].contains("limit=2&cursor=opaque-next"));
    assert!(requests[2].contains("limit=7&cursor=next-page&state=skipped_active&runId=run-other"));
    assert!(
        requests[3].contains("limit=7&cursor=different-next&state=skipped_active&runId=run-other")
    );
}

#[test]
fn human_and_json_inspection_agree_on_selection_attempt_and_snapshot_mapping() {
    let (server, _dir, credential) = prepared(vec![
        json_http_response("200 OK", trigger()),
        json_http_response("200 OK", trigger()),
        json_http_response("200 OK", evaluation()),
        json_http_response("200 OK", evaluation()),
    ]);
    for (leaf, id, field) in [
        (
            vec!["project", "trigger", "show", ORG, PROJECT, TRIGGER],
            TRIGGER,
            "trigger",
        ),
        (
            vec![
                "project",
                "trigger",
                "evaluation",
                "show",
                ORG,
                PROJECT,
                TRIGGER,
                EVALUATION,
            ],
            EVALUATION,
            "evaluation",
        ),
    ] {
        let mut human_args = leaf.clone();
        human_args.push("--allow-insecure-http");
        let human = call(&server, &credential, &human_args);
        let mut json_args = leaf;
        json_args.extend(["--json", "--allow-insecure-http"]);
        let machine = call(&server, &credential, &json_args);
        assert!(human.status.success() && machine.status.success(), "{id}");
        let human_value: serde_json::Value = serde_json::from_slice(
            human
                .stdout
                .splitn(2, |byte| *byte == b'\n')
                .nth(1)
                .unwrap(),
        )
        .unwrap();
        assert_eq!(human_value, value(&machine)[field]);
        assert_eq!(human_value["id"], id);
        if field == "trigger" {
            assert_eq!(
                human_value["inputs"]["ticket"]["source"]["type"],
                "snapshot"
            );
            assert_eq!(human_value["version"], 3);
        } else {
            assert_eq!(human_value["configurationVersion"], 2);
            assert_eq!(human_value["attemptNumber"], 2);
            assert_eq!(human_value["reasonCode"], "active_run");
            assert_eq!(human_value["blockingRunId"], "run-other");
            assert!(human_value.get("runId").is_none());
        }
    }
    assert_eq!(server.finish().len(), 4);
}

#[test]
fn deleted_trigger_has_only_tombstone_fields_and_late_schema_is_rejected() {
    let (server, _dir, credential) = prepared(vec![
        json_http_response(
            "200 OK",
            serde_json::json!({"id":TRIGGER,"projectId":PROJECT,"deleted":true,"deletedAt":"2026-01-03T00:00:00Z"}),
        ),
        json_http_response(
            "200 OK",
            serde_json::json!({"id":TRIGGER,"projectId":PROJECT,"deleted":true,"deletedAt":"2026-01-03T00:00:00Z","credential":"sensitive"}),
        ),
        json_http_response("200 OK", {
            let mut later = trigger();
            later["source"]["providerToken"] = "sensitive".into();
            later
        }),
    ]);
    for (index, expected) in ["shown", "invalid_response", "invalid_response"]
        .iter()
        .enumerate()
    {
        let output = call(
            &server,
            &credential,
            &[
                "project",
                "trigger",
                "show",
                ORG,
                PROJECT,
                TRIGGER,
                "--json",
                "--allow-insecure-http",
            ],
        );
        assert_eq!(value(&output)["outcome"], *expected);
        if index == 0 {
            assert_eq!(
                value(&output)["trigger"],
                serde_json::json!({"id":TRIGGER,"projectId":PROJECT,"deleted":true,"deletedAt":"2026-01-03T00:00:00Z"})
            );
        }
        assert!(!String::from_utf8_lossy(&output.stdout).contains("sensitive"));
    }
    assert_eq!(server.finish().len(), 3);
}

#[test]
fn evaluation_show_rejects_late_schema_instead_of_dropping_history() {
    let mut later = evaluation();
    later["event"]["issueBody"] = "sensitive".into();
    let (server, _dir, credential) = prepared(vec![json_http_response("200 OK", later)]);
    let output = call(
        &server,
        &credential,
        &[
            "project",
            "trigger",
            "evaluation",
            "show",
            ORG,
            PROJECT,
            TRIGGER,
            EVALUATION,
            "--json",
            "--allow-insecure-http",
        ],
    );
    assert_eq!(value(&output)["outcome"], "invalid_response");
    assert!(!String::from_utf8_lossy(&output.stdout).contains("sensitive"));
    assert_eq!(server.finish().len(), 1);
}

#[cfg(target_os = "linux")]
#[test]
fn interrupted_mutation_reports_request_identity_without_new_cycle() {
    for (signal, code) in [
        (rustix::process::Signal::INT, 130),
        (rustix::process::Signal::TERM, 143),
    ] {
        let mut server =
            ScriptedServer::respond_with_paused_first_response(vec![json_http_response(
                "200 OK",
                trigger(),
            )]);
        let directory = private_credential_directory();
        let path = directory.path().join("credentials.json");
        write_credential_fixture(&path, &server.api_url, SECRET, "2999-01-01T00:00:00Z");
        let mut command = Command::new(env!("CARGO_BIN_EXE_um"));
        command
            .args([
                "project",
                "trigger",
                "disable",
                ORG,
                PROJECT,
                TRIGGER,
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
        rustix::process::kill_process(
            rustix::process::Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap(),
            signal,
        )
        .unwrap();
        let output = child.wait_with_output().unwrap();
        server.release_paused_response();
        assert_eq!(output.status.code(), Some(code));
        assert_eq!(value(&output)["outcome"], "commitment_unknown");
        assert_eq!(
            value(&output)["idempotencyKey"],
            header_value(&request, "idempotency-key")
        );
        assert!(server.finish().is_empty());
    }
}

#[test]
fn invalid_file_and_competing_stdin_never_dispatch() {
    let (server, dir, credential) = prepared(vec![]);
    let config_file = dir.path().join("config.json");
    fs::write(
        &config_file,
        br#"{"enabled":true,"source":{"type":"linear","connectionId":"lcn_1","token":"secret"}}"#,
    )
    .unwrap();
    let invalid = call(
        &server,
        &credential,
        &[
            "project",
            "trigger",
            "create",
            ORG,
            PROJECT,
            "--config-file",
            config_file.to_str().unwrap(),
            "--json",
            "--allow-insecure-http",
        ],
    );
    assert_eq!(invalid.status.code(), Some(2));
    assert_eq!(value(&invalid)["outcome"], "invalid_configuration");
    assert!(!String::from_utf8_lossy(&invalid.stderr).contains("secret"));
    let competing = call(
        &server,
        &credential,
        &[
            "project",
            "trigger",
            "create",
            ORG,
            PROJECT,
            "--config-file",
            "-",
            "--service-api-key-file",
            "-",
            "--json",
            "--allow-insecure-http",
        ],
    );
    assert_eq!(competing.status.code(), Some(2));
    assert_eq!(value(&competing)["outcome"], "invalid_configuration");
    assert!(server.finish().is_empty());
}
