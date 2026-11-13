use super::*;

use base64::Engine as _;
use ring::digest::{SHA256, digest};

#[cfg(target_os = "linux")]
use std::os::unix::ffi::OsStringExt as _;
use std::process::Stdio;

pub(super) const TOKEN: &str = "unique-cloud-run-command-token-sentinel";
pub(super) const REFRESHED_TOKEN: &str = "unique-cloud-run-refreshed-token-sentinel";
pub(super) const ORGANIZATION: &str = "acme-research";
pub(super) const ORGANIZATION_ID: &str = "org_01k0z6r1w8f4jy2m7q9v3x5abc";
pub(super) const PROJECT_ID: &str = "prj_01k0z6r1w8f4jy2m7q9v3x5abc";
pub(super) const RUN_ID: &str = "run_01k0z6r1w8f4jy2m7q9v3x5abc";
const ATTEMPT_ID: &str = "atm_01k0z6r1w8f4jy2m7q9v3x5abc";
const EXECUTION_SPEC_ID: &str = "xsp_01k0z6r1w8f4jy2m7q9v3x5abc";
const REPOSITORY_CONNECTION_ID: &str = "rpc_01k0z6r1w8f4jy2m7q9v3x5abc";
pub(super) const INPUT_SET_ID: &str = "ris_01k0z6r1w8f4jy2m7q9v3x5abc";
const WORKFLOW_PATH: &str = "workflows/build.yaml";
pub(super) const DEFINITION_ID: &str = "def_01k0z6r1w8f4jy2m7q9v3x5abc";

pub(super) mod continuation_command_tests {
    use super::*;
    use std::net::TcpListener;
    #[cfg(target_os = "linux")]
    use std::process::Stdio;
    #[cfg(target_os = "linux")]
    use std::sync::mpsc;

    const REQUEST_ID: &str = "cmd_01k0z6r1w8f4jy2m7q9v3x5abc";
    const PRIOR: &str = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
    const CURRENT: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    fn projection(state: &str) -> serde_json::Value {
        let mut run = run_body_with_state(match state {
            "changed-receipt" => "running",
            "third" => "succeeded",
            other => other,
        });
        if state == "interrupted" {
            run["interruption"]["phase"] = serde_json::json!("accepted");
            run["interruption"]["cause"] = serde_json::json!("retained_workspace_unavailable");
            run["interruption"]["stopConfirmed"] = serde_json::json!(false);
        }
        let preparation = if state == "interrupted" {
            "unavailable"
        } else if state == "running" || state == "changed-receipt" || state == "third" {
            "ready"
        } else {
            "pending"
        };
        run["workflowDefinitionSource"]["workflowSourceClosureDigest"]["value"] =
            serde_json::json!(PRIOR);
        run["continuation"] = serde_json::json!({
            "request": {"fromSteps": ["build", "test"], "definition": "inherited", "expectedRunVersion": 7},
            "fromSteps": ["build", "test"], "reexecutedSteps": ["build", "test"],
            "inheritedSteps": [{"id":"source", "priorState":"inherited", "definitionChanged":false}],
            "definitionSource": {"kind":"inherited", "manifestDigest": {"algorithm":"sha256", "value":PRIOR},
                "priorManifestDigest":{"algorithm":"sha256", "value":PRIOR}},
            "workspace": {"executionRoot":"/work/current", "priorExecutionRoot":"/work/prior",
                "preparation":preparation, "startSnapshot":null, "priorSettlementSnapshot":null,
                "modified":"unknown", "quiescence":null}
        });
        if state == "third" {
            run["currentAttemptId"] = serde_json::json!("atm_01k0z6r1w8f4jy2m7q9v3x5abd");
            run["currentAttemptNumber"] = serde_json::json!(3);
            run["version"] = serde_json::json!(9);
            run["continuation"]["request"]["fromSteps"] = serde_json::json!(["test"]);
            run["continuation"]["fromSteps"] = serde_json::json!(["test"]);
            run["continuation"]["reexecutedSteps"] = serde_json::json!(["test"]);
            run["continuation"]["inheritedSteps"] = serde_json::json!([
                {"id":"source", "priorState":"inherited", "definitionChanged":false},
                {"id":"build", "priorState":"succeeded", "definitionChanged":false}
            ]);
            run["continuation"]["definitionSource"]["kind"] = serde_json::json!("replaced");
            run["continuation"]["definitionSource"]["manifestDigest"]["value"] =
                serde_json::json!(CURRENT);
            run["continuation"]["request"]["definition"] = serde_json::json!({"replaced":{
                "commitOid":"abcdef0123456789abcdef0123456789abcdef01",
                "workflowPath":"workflows/replacement.yaml"}});
            run["continuation"]["workspace"]["priorExecutionRoot"] =
                serde_json::json!("/work/current");
            run["continuation"]["workspace"]["executionRoot"] = serde_json::json!("/work/third");
            run["portableResult"] = serde_json::json!("available");
            run["artifactDelivery"] = serde_json::json!({"state":"succeeded", "artifactSetId":"ats_01k0z6r1w8f4jy2m7q9v3x5abc"});
        }
        if preparation == "ready" {
            run["continuation"]["workspace"]["startSnapshot"] = serde_json::json!({"algorithm":"git_worktree_sha256_v1","value":CURRENT,"takenAt":"2026-08-10T12:00:01Z"});
            run["continuation"]["workspace"]["quiescence"] = serde_json::json!({"groupsRecorded":0,"groupsTerminated":0,"groupsAbsent":0,"provenAt":"2026-08-10T12:00:01Z"});
        }
        run
    }

    fn envelope(state: &str) -> serde_json::Value {
        let run = projection(state);
        let definition = projection("preparing")["workflowDefinitionSource"].clone();
        serde_json::json!({"request":{
            "requestId":REQUEST_ID,"organizationId":ORGANIZATION_ID,"runId":RUN_ID,
            "attemptId":ATTEMPT_ID,"attemptNumber":2,"acceptedAt":"2026-08-10T12:00:00Z",
            "definitionSource":definition,"reexecutedSteps":["build","test"],
            "inheritedSteps":[{"id":"source","priorState":"inherited","definitionChanged":false}]
        },"run":run})
    }

    fn execute(steps: &[&str], args: &[&str]) -> (Output, Vec<String>) {
        execute_with_env(steps, args, &[])
    }

    /// Runs `um` with `args` and `environment` against a server that answers one accepted
    /// socket per step. Each accepted socket is a scripted response barrier, not a timed race.
    /// Besides the named fixtures, `submitted` admits the POSTed request as it was sent, as
    /// Cloud admits a definition-backed run (REQ-CONTINUE-84), and `violation:CODE` refuses it
    /// with that one admission violation, `conflict:SLUG` answers 409 with that problem type and
    /// no violation list, and `gone:SLUG` answers 410 with that problem type.
    pub(crate) fn execute_with_env(
        steps: &[&str],
        args: &[&str],
        environment: &[(&'static str, &str)],
    ) -> (Output, Vec<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let api_url = format!("http://{}/api", listener.local_addr().unwrap());
        let directory = private_credential_directory();
        let credentials = directory.path().join("credentials.json");
        write_credential_fixture(&credentials, &api_url, TOKEN, "2999-01-01T00:00:00Z");
        let steps = steps
            .iter()
            .map(|step| step.to_string())
            .collect::<Vec<_>>();
        let server = std::thread::spawn(move || {
            let mut requests = Vec::new();
            for step in steps {
                let (mut stream, _) = listener.accept().unwrap();
                let captured = request_text(&mut stream);
                if step == "truncated-401" {
                    stream.write_all(b"HTTP/1.1 401 Unauthorized\r\nContent-Type: application/problem+json\r\nContent-Length: 100\r\nConnection: close\r\n\r\n{}").unwrap();
                } else if step != "lost" {
                    let post = captured.starts_with("POST ");
                    let status = if post { "201 Created" } else { "200 OK" };
                    let location = format!(
                        "/v1/organizations/{ORGANIZATION_ID}/runs/{RUN_ID}/continuation-requests/{REQUEST_ID}"
                    );
                    let key = if post {
                        Some(header_value(&captured, "idempotency-key"))
                    } else {
                        None
                    };
                    let mut headers = vec![("Cache-Control", "private, no-store")];
                    if let Some(key) = key {
                        headers.push(("Location", &location));
                        headers.push(("Idempotency-Key", key));
                    }
                    let mut body = if step == "show-running" {
                        projection("running")
                    } else if step == "show-interrupted" {
                        projection("interrupted")
                    } else if step == "show-third" {
                        projection("third")
                    } else {
                        envelope(&step)
                    };
                    if step == "changed-receipt" {
                        body["request"]["acceptedAt"] = serde_json::json!("2026-08-11T12:00:00Z");
                    }
                    if step == "submitted" {
                        let submitted: serde_json::Value =
                            serde_json::from_str(captured.split("\r\n\r\n").nth(1).unwrap())
                                .unwrap();
                        let steps = submitted["fromSteps"].clone();
                        body = envelope("preparing");
                        body["request"]["reexecutedSteps"] = steps.clone();
                        body["request"]["inheritedSteps"] = serde_json::json!([]);
                        body["run"]["continuation"]["request"] = submitted;
                        body["run"]["continuation"]["fromSteps"] = steps.clone();
                        body["run"]["continuation"]["reexecutedSteps"] = steps;
                        body["run"]["continuation"]["inheritedSteps"] = serde_json::json!([]);
                    }
                    if step == "changed-request" {
                        body["run"]["continuation"]["request"]["fromSteps"] =
                            serde_json::json!(["test", "build"]);
                    }
                    let violation_code = step.strip_prefix("violation:");
                    let response = if step == "admission-version"
                        || step == "admission-node"
                        || violation_code.is_some()
                    {
                        let violation = if let Some(code) = violation_code {
                            serde_json::json!({ "code": code })
                        } else if step == "admission-version" {
                            serde_json::json!({"code":"expected_run_version_conflict"})
                        } else {
                            serde_json::json!({"code":"continuation_references_unsatisfied", "node":"test", "producer":"source", "reference":"steps.source.outputs.file", "priorState":"failed"})
                        };
                        http_response_with_headers("409 Conflict", Some("application/problem+json"), &[],
                            &serde_json::to_vec(&serde_json::json!({"type":"https://api.usefulmachinery.com/problems/continuation-admission-invalid", "title":"Continuation admission invalid", "status":409, "continuationViolations":[violation]})).unwrap())
                    } else if step == "not-found" {
                        http_response_with_headers("404 Not Found", Some("application/problem+json"), &[],
                            br#"{"type":"https://api.usefulmachinery.com/problems/not-found","title":"Not Found","status":404}"#)
                    } else if let Some(slug) = step.strip_prefix("conflict:") {
                        http_response_with_headers("409 Conflict", Some("application/problem+json"), &[],
                            &serde_json::to_vec(&serde_json::json!({"type":format!("https://api.usefulmachinery.com/problems/{slug}"), "title":"Conflict", "status":409})).unwrap())
                    } else if let Some(slug) = step.strip_prefix("gone:") {
                        http_response_with_headers("410 Gone", Some("application/problem+json"), &[],
                            &serde_json::to_vec(&serde_json::json!({"type":format!("https://api.usefulmachinery.com/problems/{slug}"), "title":"Gone", "status":410})).unwrap())
                    } else {
                        http_response_with_headers(
                            status,
                            Some("application/json"),
                            &headers,
                            &serde_json::to_vec(&body).unwrap(),
                        )
                    };
                    stream.write_all(&response).unwrap();
                }
                requests.push(captured);
            }
            requests
        });
        let environment = deployment_environment(&api_url, credentials.to_str().unwrap())
            .into_iter()
            .chain(environment.iter().copied())
            .collect::<Vec<_>>();
        let output = run_with_env(args, &environment);
        (output, server.join().unwrap())
    }

    fn args() -> Vec<&'static str> {
        vec![
            "run",
            "continue",
            ORGANIZATION,
            RUN_ID,
            "--from",
            "build",
            "--from",
            "test",
            "--expected-version",
            "7",
            "--json",
            "--allow-insecure-http",
        ]
    }

    // REQ-CONTINUE-1, REQ-CONTINUE-56, REQ-CONTINUE-57
    #[test]
    fn lost_201_replays_same_request_then_reports_admission_not_execution() {
        let (output, requests) = execute(&["lost", "preparing"], &args());
        assert_eq!(output.status.code(), Some(0), "{output:?}");
        let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(result["outcome"], "accepted");
        assert_eq!(result["request"]["attemptId"], ATTEMPT_ID);
        assert_eq!(
            result["run"]["continuation"]["workspace"]["preparation"],
            "pending"
        );
        assert_eq!(requests.len(), 2);
        assert_eq!(
            header_value(&requests[0], "idempotency-key"),
            header_value(&requests[1], "idempotency-key")
        );
        assert_eq!(
            header_value(&requests[0], "authorization"),
            header_value(&requests[1], "authorization")
        );
        assert_eq!(
            requests[0].split("\r\n\r\n").nth(1),
            requests[1].split("\r\n\r\n").nth(1)
        );
        let body: serde_json::Value =
            serde_json::from_str(requests[0].split("\r\n\r\n").nth(1).unwrap()).unwrap();
        assert_eq!(
            body,
            serde_json::json!({"fromSteps":["build","test"],"definition":"inherited","expectedRunVersion":7})
        );
    }

    // REQ-CONTINUE-55
    #[test]
    fn retired_replacement_pair_is_unknown_and_the_definition_is_always_inherited() {
        let (output, requests) = execute(
            &[],
            &[
                "run",
                "continue",
                ORGANIZATION,
                RUN_ID,
                "--from",
                "build",
                "--workflow-commit",
                "abcdef0123456789abcdef0123456789abcdef01",
                "--workflow-path",
                "workflows/replacement.yaml",
                "--allow-insecure-http",
            ],
        );
        assert_eq!(output.status.code(), Some(2), "{output:?}");
        assert!(output.stdout.is_empty(), "{output:?}");
        assert!(requests.is_empty());

        let (output, requests) = execute(
            &["submitted"],
            &[
                "run",
                "continue",
                ORGANIZATION,
                RUN_ID,
                "--from",
                "build",
                "--expected-version",
                "3",
                "--json",
                "--allow-insecure-http",
            ],
        );
        assert_eq!(output.status.code(), Some(0), "{output:?}");
        assert_eq!(requests.len(), 1);
        assert_eq!(
            request_body(&requests[0]),
            serde_json::json!({"fromSteps":["build"],"definition":"inherited","expectedRunVersion":3})
        );
    }

    // REQ-CONTINUE-56, REQ-CONTINUE-61
    #[test]
    fn human_output_distinguishes_inheritance_and_keeps_an_earlier_replaced_definition() {
        let mut args = args();
        args.retain(|arg| *arg != "--json");
        let (output, _) = execute(&["preparing"], &args);
        assert_eq!(output.status.code(), Some(0), "{output:?}");
        let text = String::from_utf8(output.stdout).unwrap();
        assert!(text.contains("accepted attempt:"));
        assert!(text.contains("inherited: source"));
        assert!(text.contains("preparation: pending"));
        assert!(text.contains("portable result: absent"));
        assert!(text.contains("execution root: /work/current"));
        assert!(!text.contains("effective commit:"));

        // A receipt admitted before the replacement pair was retired keeps its replaced
        // definition, and both the waiting command and `um run show` still render it.
        args.push("--wait");
        let (waited, _) = execute(&["preparing", "third"], &args);
        let (shown, _) = execute(
            &["show-third"],
            &["run", "show", ORGANIZATION, RUN_ID, "--allow-insecure-http"],
        );
        for output in [waited, shown] {
            assert_eq!(output.status.code(), Some(0), "{output:?}");
            let text = String::from_utf8(output.stdout).unwrap();
            assert!(text.contains("effective commit: abcdef0123456789abcdef0123456789abcdef01"));
            assert!(text.contains("effective workflow: workflows/replacement.yaml"));
        }
    }

    // REQ-CONTINUE-61
    #[test]
    fn run_show_reads_current_ready_and_boot_loss_projection_without_mutation() {
        for (state, marker) in [
            ("show-running", "preparation: ready"),
            ("show-interrupted", "retained_workspace_unavailable"),
        ] {
            let (output, requests) = execute(
                &[state],
                &["run", "show", ORGANIZATION, RUN_ID, "--allow-insecure-http"],
            );
            assert_eq!(output.status.code(), Some(0), "{output:?}");
            assert_eq!(requests.len(), 1);
            assert!(requests[0].starts_with("GET "));
            let text = String::from_utf8(output.stdout).unwrap();
            assert!(text.contains(marker));
            assert!(text.contains("reexecution partition: build, test"));
            assert!(text.contains("modified: unknown"));
            assert!(text.contains("portable result: absent"));
        }
    }

    // REQ-CONTINUE-62
    #[test]
    fn show_wait_observes_boot_loss_without_submitting_another_attempt() {
        let (output, requests) = execute(
            &["show-running", "show-interrupted"],
            &[
                "run",
                "show",
                ORGANIZATION,
                RUN_ID,
                "--wait",
                "--json",
                "--allow-insecure-http",
            ],
        );
        assert_eq!(output.status.code(), Some(0), "{output:?}");
        let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(
            result["run"]["interruption"]["cause"],
            "retained_workspace_unavailable"
        );
        assert_eq!(result["run"]["portableResult"], "absent");
        assert!(requests.iter().all(|request| request.starts_with("GET ")));
    }

    // REQ-CONTINUE-60
    #[cfg(target_os = "linux")]
    #[test]
    fn signal_stops_receipt_observation_without_a_second_submission() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let api_url = format!("http://{}/api", listener.local_addr().unwrap());
        let directory = private_credential_directory();
        let credentials = directory.path().join("credentials.json");
        write_credential_fixture(&credentials, &api_url, TOKEN, "2999-01-01T00:00:00Z");
        let (ready, reached) = mpsc::sync_channel(0);
        let (release, released) = mpsc::sync_channel(0);
        let server = std::thread::spawn(move || {
            let (mut post, _) = listener.accept().unwrap();
            let captured = request_text(&mut post);
            let key = header_value(&captured, "idempotency-key").to_owned();
            let location = format!(
                "/v1/organizations/{ORGANIZATION_ID}/runs/{RUN_ID}/continuation-requests/{REQUEST_ID}"
            );
            post.write_all(&http_response_with_headers(
                "201 Created",
                Some("application/json"),
                &[
                    ("Idempotency-Key", &key),
                    ("Location", &location),
                    ("Cache-Control", "private, no-store"),
                ],
                &serde_json::to_vec(&envelope("preparing")).unwrap(),
            ))
            .unwrap();
            let (mut get, _) = listener.accept().unwrap();
            let observed = request_text(&mut get);
            ready.send(()).unwrap();
            released.recv().unwrap();
            drop(get);
            listener.set_nonblocking(true).unwrap();
            assert!(
                matches!(listener.accept(), Err(error) if error.kind() == io::ErrorKind::WouldBlock)
            );
            (captured, observed)
        });
        let environment = deployment_environment(&api_url, credentials.to_str().unwrap());
        let mut command = Command::new(env!("CARGO_BIN_EXE_um"));
        let mut args = args();
        args.push("--wait");
        command
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .env_remove(CREDENTIALS_FILE_VARIABLE);
        for variable in DEPLOYMENT_VARIABLES {
            command.env_remove(variable);
        }
        for (name, value) in environment {
            command.env(name, value);
        }
        let child = command.spawn().unwrap();
        reached.recv().unwrap();
        rustix::process::kill_process(
            rustix::process::Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap(),
            rustix::process::Signal::TERM,
        )
        .unwrap();
        let output = child.wait_with_output().unwrap();
        release.send(()).unwrap();
        let (post, get) = server.join().unwrap();
        assert_eq!(output.status.code(), Some(143), "{output:?}");
        let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(result["outcome"], "observation_stopped");
        assert_eq!(result["request"]["requestId"], REQUEST_ID);
        assert!(post.starts_with("POST ") && get.starts_with("GET "));
    }

    // REQ-CONTINUE-60
    #[test]
    fn older_receipt_keeps_its_partition_when_current_attempt_and_portable_result_advance() {
        let mut args = args();
        args.push("--wait");
        let (output, requests) = execute(&["preparing", "third"], &args);
        assert_eq!(output.status.code(), Some(0), "{output:?}");
        let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(result["request"]["attemptNumber"], 2);
        assert_eq!(
            result["run"]["workflowDefinitionSource"]["workflowSourceClosureDigest"]["value"],
            PRIOR
        );
        assert_eq!(
            result["run"]["continuation"]["definitionSource"]["manifestDigest"]["value"],
            CURRENT
        );
        assert_eq!(
            result["request"]["reexecutedSteps"],
            serde_json::json!(["build", "test"])
        );
        assert_eq!(result["run"]["currentAttemptNumber"], 3);
        assert_eq!(
            result["run"]["continuation"]["reexecutedSteps"],
            serde_json::json!(["test"])
        );
        assert_eq!(result["run"]["portableResult"], "available");
        assert_eq!(
            result["run"]["continuation"]["inheritedSteps"][0]["priorState"],
            "inherited"
        );
        assert_eq!(requests.len(), 2);
        let (human, reads) = execute(
            &["show-third"],
            &["run", "show", ORGANIZATION, RUN_ID, "--allow-insecure-http"],
        );
        assert_eq!(human.status.code(), Some(0), "{human:?}");
        assert_eq!(reads.len(), 1);
        let text = String::from_utf8(human.stdout).unwrap();
        assert!(text.contains("reexecution partition: test"));
        assert!(text.contains("inherited: source (prior: inherited"));
        assert!(text.contains("portable result: available"));
    }

    // REQ-CONTINUE-57
    #[test]
    fn two_lost_responses_leave_one_key_and_unknown_acceptance() {
        let (output, requests) = execute(&["lost", "lost"], &args());
        assert_eq!(output.status.code(), Some(4), "{output:?}");
        let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(result["outcome"], "acceptance_unknown");
        assert_eq!(
            result["idempotencyKey"],
            header_value(&requests[0], "idempotency-key")
        );
        assert_eq!(
            header_value(&requests[0], "idempotency-key"),
            header_value(&requests[1], "idempotency-key")
        );
        assert!(result["request"].is_null());
    }

    // REQ-CONTINUE-57
    #[test]
    fn lost_response_then_unreadable_401_cannot_refresh_or_submit_again() {
        let (output, requests) = execute(&["lost", "truncated-401"], &args());
        assert_eq!(output.status.code(), Some(4), "{output:?}");
        let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(result["outcome"], "acceptance_unknown");
        assert_eq!(result["code"], "acceptance_unknown");
        assert_eq!(requests.len(), 2);
        assert_eq!(
            header_value(&requests[0], "idempotency-key"),
            header_value(&requests[1], "idempotency-key")
        );
        assert_eq!(
            requests[0].split("\r\n\r\n").nth(1),
            requests[1].split("\r\n\r\n").nth(1)
        );
    }

    // REQ-CONTINUE-59
    #[test]
    fn admission_violations_retain_stale_version_and_reference_details() {
        for (step, code) in [
            ("admission-version", "expected_run_version_conflict"),
            ("admission-node", "continuation_references_unsatisfied"),
        ] {
            let (output, requests) = execute(&[step], &args());
            assert_eq!(output.status.code(), Some(1), "{output:?}");
            assert_eq!(requests.len(), 1);
            let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(result["outcome"], "error");
            assert_eq!(result["code"], "continuation_admission_invalid");
            assert_eq!(result["diagnostic"][0]["code"], code);
            if step == "admission-node" {
                assert_eq!(result["diagnostic"][0]["node"], "test");
                assert_eq!(result["diagnostic"][0]["producer"], "source");
                assert_eq!(
                    result["diagnostic"][0]["reference"],
                    "steps.source.outputs.file"
                );
                assert_eq!(result["diagnostic"][0]["priorState"], "failed");
            }
        }
        let mut human_args = args();
        human_args.retain(|arg| *arg != "--json");
        let (output, requests) = execute(&["admission-node"], &human_args);
        assert_eq!(requests.len(), 1);
        assert_eq!(output.status.code(), Some(1), "{output:?}");
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert!(stderr.contains("continuation_references_unsatisfied"));
        assert!(stderr.contains(
            "node: test producer: source reference: steps.source.outputs.file prior state: failed"
        ));
    }

    // REQ-CONTINUE-57
    #[test]
    fn not_found_after_lost_response_is_not_proof_of_non_admission() {
        let (output, requests) = execute(&["lost", "not-found"], &args());
        assert_eq!(output.status.code(), Some(4), "{output:?}");
        let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(result["outcome"], "acceptance_unknown");
        assert!(result["request"].is_null());
        assert_eq!(requests.len(), 2);
    }

    // REQ-CONTINUE-87
    #[test]
    fn a_path_run_retired_answer_is_reported_as_path_run_retired() {
        for (step, code) in [
            ("gone:path-run-retired", "path_run_retired"),
            ("gone:run-input-content-gone", "submission_failed"),
        ] {
            let (output, requests) = execute(&[step], &args());
            assert_eq!(output.status.code(), Some(1), "{output:?}");
            let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(result["outcome"], "error");
            assert_eq!(result["code"], code);
            assert!(result["request"].is_null());
            assert_eq!(requests.len(), 1);

            let human = args()
                .into_iter()
                .filter(|arg| *arg != "--json")
                .collect::<Vec<_>>();
            let (output, _) = execute(&[step], &human);
            assert_eq!(output.status.code(), Some(1), "{output:?}");
            assert!(String::from_utf8_lossy(&output.stderr).contains(&format!("code: {code}")));
        }
    }

    // REQ-CONTINUE-58
    #[test]
    fn post_cannot_report_a_different_semantic_request_as_accepted() {
        let (output, requests) = execute(&["changed-request"], &args());
        assert_eq!(output.status.code(), Some(4), "{output:?}");
        let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(result["outcome"], "acceptance_unknown");
        assert!(result["request"].is_null());
        assert_eq!(requests.len(), 1);
    }

    // REQ-CONTINUE-58
    #[test]
    fn receipt_get_must_not_rewrite_original_admission() {
        let mut args = args();
        args.push("--wait");
        let (output, requests) = execute(&["preparing", "changed-receipt"], &args);
        assert_eq!(output.status.code(), Some(1), "{output:?}");
        let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(result["code"], "protocol_error");
        assert_eq!(result["request"]["acceptedAt"], "2026-08-10T12:00:00Z");
        assert_eq!(requests.len(), 2);
    }

    // REQ-CONTINUE-60
    #[test]
    fn receipt_get_observes_ready_then_boot_loss_without_claiming_portable_result() {
        let mut args = args();
        args.push("--wait");
        let (output, requests) = execute(&["preparing", "running", "interrupted"], &args);
        assert_eq!(output.status.code(), Some(0), "{output:?}");
        let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(result["outcome"], "observed");
        assert_eq!(
            result["run"]["continuation"]["workspace"]["preparation"],
            "unavailable"
        );
        assert_eq!(
            result["run"]["interruption"]["cause"],
            "retained_workspace_unavailable"
        );
        assert_eq!(result["run"]["portableResult"], "absent");
        assert!(
            requests[1..]
                .iter()
                .all(|line| line.starts_with("GET ") && line.contains(REQUEST_ID))
        );
    }
}

mod retry_command_tests {
    use super::*;
    use std::net::TcpListener;
    use std::sync::mpsc;

    const REQUEST_ID: &str = "cmd_01k0z6r1w8f4jy2m7q9v3x5abc";

    fn receipt(state: &str, rejection: Option<&str>) -> serde_json::Value {
        serde_json::json!({
            "id": REQUEST_ID, "organizationId": ORGANIZATION_ID, "runId": RUN_ID,
            "observedVersion": 7, "acceptedAt": "2026-08-10T12:00:00Z",
            "state": state, "fault": null,
            "resolvedAt": if state == "pending" { None } else { Some("2026-08-10T12:01:00Z") },
            "rejection": if state == "rejected" { rejection } else { None },
            "failure": if state == "failed" { serde_json::json!({
                "code": "coordinator_rejected",
                "diagnostic": {"errorType": rejection.unwrap_or("invariant_retry_source_missing")}
            }) } else { serde_json::Value::Null },
            "attemptId": if state == "applied" { Some(ATTEMPT_ID) } else { None },
            "attemptNumber": if state == "applied" { Some(2) } else { None },
            "runVersion": if state == "applied" { Some(8) } else { None }
        })
    }

    // A scripted TCP peer echoes the generated key, rather than predicting random input.
    // Each accepted connection is a synchronization point; an IO timeout only bounds a
    // broken fixture, never determines when an assertion is safe.
    fn execute(steps: &[(&str, Option<&str>)], args: &[&str]) -> (Output, Vec<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let api_url = format!("http://{}/api", listener.local_addr().unwrap());
        let credential_directory = private_credential_directory();
        let credential_path = credential_directory.path().join("credentials.json");
        write_credential_fixture(&credential_path, &api_url, TOKEN, "2999-01-01T00:00:00Z");
        let script: Vec<_> = steps
            .iter()
            .map(|(state, code)| (state.to_string(), code.map(str::to_owned)))
            .collect();
        let server = std::thread::spawn(move || {
            let mut requests = Vec::new();
            for (state, code) in script {
                let (mut stream, _) = listener.accept().unwrap();
                let lines = request_text(&mut stream);
                let key = lines.lines().find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("idempotency-key: ")
                        .map(str::to_owned)
                });
                let reply = match state.as_str() {
                    "lost" => None,
                    "pending" | "applied" | "rejected" | "failed" | "applied-post" => {
                        let post = lines.starts_with("POST ");
                        let state = if state == "applied-post" {
                            "applied"
                        } else if post {
                            "pending"
                        } else {
                            state.as_str()
                        };
                        let body = serde_json::to_vec(&receipt(state, code.as_deref())).unwrap();
                        let location = format!(
                            "/v1/organizations/{ORGANIZATION_ID}/runs/{RUN_ID}/retry-requests/{REQUEST_ID}"
                        );
                        let mut headers = vec![("Cache-Control", "private, no-store")];
                        if post {
                            headers.push(("Idempotency-Key", key.as_deref().unwrap()));
                            headers.push(("Location", location.as_str()));
                        }
                        Some(http_response_with_headers(
                            if post { "202 Accepted" } else { "200 OK" },
                            Some("application/json"),
                            &headers,
                            &body,
                        ))
                    }
                    other => {
                        let (status, title) = match other {
                            "unauthorized" => ("401 Unauthorized", "Unauthorized"),
                            "forbidden" => ("403 Forbidden", "Forbidden"),
                            "not-found" => ("404 Not Found", "Not Found"),
                            "rate-limited" => ("429 Too Many Requests", "Too Many Requests"),
                            "unavailable" => ("503 Service Unavailable", "Service Unavailable"),
                            "run-input-content-gone" => ("410 Gone", "Gone"),
                            _ => ("409 Conflict", "Conflict"),
                        };
                        let status_number: u16 = status.split(' ').next().unwrap().parse().unwrap();
                        let mut body = serde_json::json!({"type":format!("https://api.usefulmachinery.com/problems/{other}"),"title":title,"status":status_number});
                        if let Some(holder) = code.as_deref() {
                            body["workflow"] = serde_json::json!({"holderRunId": holder});
                        }
                        let bytes = serde_json::to_vec(&body).unwrap();
                        Some(http_response_with_headers(
                            status,
                            Some("application/problem+json"),
                            if matches!(other, "rate-limited" | "unavailable") {
                                &[("Retry-After", "1")]
                            } else {
                                &[]
                            },
                            &bytes,
                        ))
                    }
                };
                if let Some(reply) = reply {
                    stream.write_all(&reply).unwrap();
                }
                requests.push(lines);
            }
            requests
        });
        let environment = deployment_environment(&api_url, credential_path.to_str().unwrap());
        let output = run_with_env(args, &environment);
        (output, server.join().unwrap())
    }

    fn args() -> [&'static str; 8] {
        [
            "run",
            "retry",
            ORGANIZATION,
            RUN_ID,
            "--expected-version",
            "7",
            "--json",
            "--allow-insecure-http",
        ]
    }

    // REQ-RETRY-31, REQ-RETRY-33, REQ-RETRY-39
    #[test]
    fn lost_acceptance_replays_one_key_and_only_get_can_apply() {
        let (output, requests) = execute(
            &[("lost", None), ("pending", None), ("applied", None)],
            &args(),
        );
        assert_eq!(output.status.code(), Some(0));
        assert!(output.stderr.is_empty());
        let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(json["outcome"], "applied");
        assert_eq!(json["requestId"], REQUEST_ID);
        assert_eq!(requests.len(), 3);
        assert_eq!(
            header_value(&requests[0], "idempotency-key"),
            header_value(&requests[1], "idempotency-key")
        );
        assert_eq!(
            requests[0].split("\r\n\r\n").nth(1),
            requests[1].split("\r\n\r\n").nth(1)
        );
        assert!(requests[2].starts_with("GET "));
        assert!(
            requests
                .iter()
                .all(|request| !request.starts_with("DELETE "))
        );
    }

    // REQ-RETRY-31, REQ-RETRY-39
    #[test]
    fn two_lost_post_responses_report_unresolved_transport_and_the_one_key() {
        let (output, requests) = execute(&[("lost", None), ("lost", None)], &args());
        assert_eq!(output.status.code(), Some(4));
        assert!(output.stderr.is_empty());
        let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(json["outcome"], "acceptance_unknown");
        assert_eq!(json["requestId"], serde_json::Value::Null);
        assert_eq!(
            json["idempotencyKey"],
            header_value(&requests[0], "idempotency-key")
        );
        assert_eq!(
            header_value(&requests[0], "idempotency-key"),
            header_value(&requests[1], "idempotency-key")
        );
        assert!(requests.iter().all(|request| request.starts_with("POST ")));
    }

    // REQ-RETRY-32
    #[test]
    fn definitive_post_rate_limit_does_not_claim_unknown_acceptance() {
        let (output, requests) = execute(&[("rate-limited", None)], &args());
        assert_eq!(output.status.code(), Some(4));
        let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(json["outcome"], "error");
        assert_eq!(json["code"], "rate_limited");
        assert_eq!(requests.len(), 1);
    }

    // REQ-RETRY-31
    #[test]
    fn rate_limited_replay_does_not_erase_ambiguous_first_post() {
        let (output, requests) = execute(&[("lost", None), ("rate-limited", None)], &args());
        assert_eq!(output.status.code(), Some(4));
        let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(json["outcome"], "acceptance_unknown");
        assert_eq!(
            json["idempotencyKey"],
            header_value(&requests[0], "idempotency-key")
        );
        assert_eq!(json["requestId"], serde_json::Value::Null);
        assert_eq!(requests.len(), 2);
        assert_eq!(
            header_value(&requests[0], "idempotency-key"),
            header_value(&requests[1], "idempotency-key")
        );
    }

    // REQ-RETRY-31
    #[test]
    fn unauthorized_replay_cannot_erase_unresolved_acceptance() {
        let (output, requests) = execute(&[("lost", None), ("unauthorized", None)], &args());
        assert_eq!(output.status.code(), Some(4));
        let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(json["outcome"], "acceptance_unknown");
        assert_eq!(
            json["idempotencyKey"],
            header_value(&requests[0], "idempotency-key")
        );
        assert_eq!(requests.len(), 2);
        assert_eq!(
            header_value(&requests[0], "idempotency-key"),
            header_value(&requests[1], "idempotency-key")
        );
    }

    // REQ-RETRY-33
    #[test]
    fn resolved_replay_still_requires_a_get_before_reporting_applied() {
        let (output, requests) = execute(
            &[("lost", None), ("applied-post", None), ("applied", None)],
            &args(),
        );
        assert_eq!(output.status.code(), Some(0));
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["outcome"],
            "applied"
        );
        assert_eq!(requests.len(), 3);
        assert!(requests[2].starts_with("GET "));
        assert_eq!(
            header_value(&requests[0], "idempotency-key"),
            header_value(&requests[1], "idempotency-key")
        );
    }

    // REQ-RETRY-33
    #[test]
    fn pending_get_is_not_application_until_a_later_get_applies() {
        let (output, requests) = execute(
            &[("pending", None), ("pending", None), ("applied", None)],
            &args(),
        );
        assert_eq!(output.status.code(), Some(0));
        let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(json["outcome"], "applied");
        assert_eq!(json["receipt"]["attemptNumber"], 2);
        assert_eq!(requests.len(), 3);
        assert!(
            requests[1..]
                .iter()
                .all(|request| request.starts_with("GET "))
        );
    }

    // REQ-RETRY-34
    #[test]
    fn rejected_receipts_preserve_every_code() {
        for code in [
            "run_not_found",
            "idempotency_conflict",
            "expected_run_version_conflict",
            "attempt_active",
            "latest_attempt_succeeded",
            "source_unavailable",
            "latest_attempt_rejected",
            "run_input_retry_unavailable",
        ] {
            let (output, requests) =
                execute(&[("pending", None), ("rejected", Some(code))], &args());
            assert_eq!(output.status.code(), Some(1));
            assert!(output.stderr.is_empty());
            let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(json["outcome"], "rejected");
            assert_eq!(json["code"], code);
            assert_eq!(requests.len(), 2);
        }
    }

    // REQ-RETRY-36
    #[test]
    // REQ-RETRY-48, REQ-DIAG-35
    fn coordinator_failure_stops_observation_and_preserves_diagnostic() {
        let (output, requests) = execute(&[("pending", None), ("failed", None)], &args());
        assert_eq!(output.status.code(), Some(1));
        assert!(output.stderr.is_empty());
        let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(json["outcome"], "failed");
        assert_eq!(json["code"], "coordinator_rejected");
        assert_eq!(
            json["receipt"]["failure"]["diagnostic"]["errorType"],
            "invariant_retry_source_missing"
        );
        assert_eq!(requests.len(), 2);
        assert!(requests[0].starts_with("POST "));
        assert!(requests[1].starts_with("GET "));
    }

    #[test]
    // REQ-DIAG-35
    fn invalid_coordinator_failure_diagnostic_is_a_protocol_error() {
        let (output, requests) = execute(
            &[("pending", None), ("failed", Some("unsafe\nreason"))],
            &args(),
        );
        assert_eq!(output.status.code(), Some(1));
        let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(json["outcome"], "error");
        assert_eq!(json["code"], "protocol_error");
        assert_eq!(requests.len(), 2);
    }

    #[test]
    fn receipt_loss_and_authorization_loss_never_resubmit() {
        for (problem, code, exit) in [("not-found", "not_found", 1), ("forbidden", "forbidden", 1)]
        {
            let (output, requests) = execute(&[("pending", None), (problem, None)], &args());
            assert_eq!(output.status.code(), Some(exit));
            assert!(output.stderr.is_empty());
            let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(json["outcome"], "error");
            assert_eq!(json["code"], code);
            assert_eq!(json["requestId"], REQUEST_ID);
            assert!(requests[0].starts_with("POST "));
            assert!(requests[1].starts_with("GET "));
        }
    }

    #[test]
    fn replacement_login_after_unauthorized_post_never_receives_the_replay() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let api_url = format!("http://{}/api", listener.local_addr().unwrap());
        let directory = private_credential_directory();
        let path = directory.path().join("credentials.json");
        write_credential_fixture(&path, &api_url, TOKEN, "2999-01-01T00:00:00Z");
        let environment = deployment_environment(&api_url, path.to_str().unwrap());
        let server_api_url = api_url.clone();
        let server_path = path.clone();
        let (release, released) = mpsc::sync_channel(0);
        let server = std::thread::spawn(move || {
            let (mut post, _) = listener.accept().unwrap();
            let request = request_text(&mut post);
            let replacement = server_path.with_extension("replacement");
            write_credential_fixture_with_refresh_token(
                &replacement,
                &server_api_url,
                "http://auth.fixture.example/",
                "replacement-access-token",
                "2999-01-01T00:00:00Z",
                "replacement-refresh-token",
            );
            fs::rename(replacement, &server_path).unwrap();
            post.write_all(&http_response_with_headers(
                "401 Unauthorized",
                Some("application/problem+json"),
                &[],
                br#"{"type":"https://api.usefulmachinery.com/problems/unauthorized","title":"Unauthorized","status":401}"#,
            ))
            .unwrap();
            drop(post);
            released.recv().unwrap();
            listener.set_nonblocking(true).unwrap();
            assert!(
                matches!(listener.accept(), Err(error) if error.kind() == io::ErrorKind::WouldBlock)
            );
            request
        });
        let output = run_with_env(&args(), &environment);
        release.send(()).unwrap();
        let request = server.join().unwrap();
        assert!(request.starts_with("POST "));
        assert!(
            request
                .to_ascii_lowercase()
                .contains(&format!("authorization: bearer {TOKEN}\r\n").to_ascii_lowercase())
        );
        assert_eq!(output.status.code(), Some(3));
        let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(json["outcome"], "error");
        assert_eq!(json["code"], "authentication_required");
        assert_eq!(
            json["idempotencyKey"],
            header_value(&request, "idempotency-key")
        );
    }

    // REQ-RETRY-36
    #[test]
    fn rejected_service_identity_does_not_resubmit_after_acceptance() {
        let directory = private_credential_directory();
        let key_file = directory.path().join("key");
        fs::write(
            &key_file,
            "crd_01k0z6r1w8f4jy2m7q9v3x5abc.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n",
        )
        .unwrap();
        fs::set_permissions(&key_file, Permissions::from_mode(0o600)).unwrap();
        let mut command = args().to_vec();
        command.extend(["--service-api-key-file", key_file.to_str().unwrap()]);
        let (output, requests) = execute(&[("pending", None), ("unauthorized", None)], &command);
        assert_eq!(output.status.code(), Some(3));
        let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(json["outcome"], "error");
        assert_eq!(json["code"], "authentication_required");
        assert_eq!(json["requestId"], REQUEST_ID);
        assert!(requests[1].starts_with("GET "));
        assert_eq!(requests.len(), 2);
    }

    // REQ-RETRY-33
    #[test]
    fn retry_after_does_not_resubmit_accepted_mutation() {
        for throttled in ["rate-limited", "unavailable"] {
            let (output, requests) = execute(
                &[("pending", None), (throttled, None), ("applied", None)],
                &args(),
            );
            assert_eq!(output.status.code(), Some(0));
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["outcome"],
                "applied"
            );
            assert_eq!(requests.len(), 3);
            assert!(
                requests[1..]
                    .iter()
                    .all(|request| request.starts_with("GET "))
            );
        }
    }

    // REQ-RETRY-38
    #[cfg(target_os = "linux")]
    #[test]
    fn interruption_preserves_the_known_request_or_unknown_key_without_resubmission() {
        for (after_acceptance, signal, exit, outcome) in [
            (
                false,
                rustix::process::Signal::INT,
                130,
                "acceptance_unknown",
            ),
            (
                true,
                rustix::process::Signal::TERM,
                143,
                "observation_stopped",
            ),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let api_url = format!("http://{}/api", listener.local_addr().unwrap());
            let credentials = private_credential_directory();
            let credentials_path = credentials.path().join("credentials.json");
            write_credential_fixture(&credentials_path, &api_url, TOKEN, "2999-01-01T00:00:00Z");
            let (ready, reached) = mpsc::sync_channel(0);
            let (release, released) = mpsc::sync_channel(0);
            let server = std::thread::spawn(move || {
                let (mut post, _) = listener.accept().unwrap();
                let request = request_text(&mut post);
                let key = header_value(&request, "idempotency-key").to_owned();
                let mut requests = vec![request];
                if after_acceptance {
                    let location = format!(
                        "/v1/organizations/{ORGANIZATION_ID}/runs/{RUN_ID}/retry-requests/{REQUEST_ID}"
                    );
                    let body = serde_json::to_vec(&receipt("pending", None)).unwrap();
                    let response = http_response_with_headers(
                        "202 Accepted",
                        Some("application/json"),
                        &[
                            ("Idempotency-Key", &key),
                            ("Cache-Control", "private, no-store"),
                            ("Location", &location),
                        ],
                        &body,
                    );
                    post.write_all(&response).unwrap();
                    drop(post);
                    let (mut get, _) = listener.accept().unwrap();
                    requests.push(request_text(&mut get));
                    ready.send(key.clone()).unwrap();
                    released.recv().unwrap();
                    drop(get);
                } else {
                    ready.send(key.clone()).unwrap();
                    released.recv().unwrap();
                    drop(post);
                }
                listener.set_nonblocking(true).unwrap();
                assert!(
                    matches!(listener.accept(), Err(error) if error.kind() == io::ErrorKind::WouldBlock)
                );
                requests
            });
            let env = deployment_environment(&api_url, credentials_path.to_str().unwrap());
            let mut command = Command::new(env!("CARGO_BIN_EXE_um"));
            command
                .args(args())
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .env_remove(CREDENTIALS_FILE_VARIABLE);
            for variable in DEPLOYMENT_VARIABLES {
                command.env_remove(variable);
            }
            for (name, value) in env {
                command.env(name, value);
            }
            let child = command.spawn().unwrap();
            let key = reached.recv().unwrap();
            rustix::process::kill_process(
                rustix::process::Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap(),
                signal,
            )
            .unwrap();
            let output = child.wait_with_output().unwrap();
            release.send(()).unwrap();
            let requests = server.join().unwrap();
            assert_eq!(output.status.code(), Some(exit));
            assert!(output.stderr.is_empty());
            let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(json["outcome"], outcome);
            if after_acceptance {
                assert_eq!(json["requestId"], REQUEST_ID);
                assert_eq!(requests.len(), 2);
                assert!(requests[1].starts_with("GET "));
            } else {
                assert_eq!(json["idempotencyKey"], key);
                assert_eq!(json["requestId"], serde_json::Value::Null);
                assert_eq!(requests.len(), 1);
            }
        }
    }

    // REQ-RETRY-37
    #[test]
    fn pending_acceptance_times_out_without_claiming_application() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let api_url = format!("http://{}/api", listener.local_addr().unwrap());
        let directory = private_credential_directory();
        let path = directory.path().join("credentials.json");
        write_credential_fixture(&path, &api_url, TOKEN, "2999-01-01T00:00:00Z");
        let (release, released) = mpsc::sync_channel(0);
        let server = std::thread::spawn(move || {
            let (mut post, _) = listener.accept().unwrap();
            let request = request_text(&mut post);
            let key = header_value(&request, "idempotency-key");
            let location = format!(
                "/v1/organizations/{ORGANIZATION_ID}/runs/{RUN_ID}/retry-requests/{REQUEST_ID}"
            );
            let body = serde_json::to_vec(&receipt("pending", None)).unwrap();
            post.write_all(&http_response_with_headers(
                "202 Accepted",
                Some("application/json"),
                &[
                    ("Idempotency-Key", key),
                    ("Cache-Control", "private, no-store"),
                    ("Location", &location),
                ],
                &body,
            ))
            .unwrap();
            released.recv().unwrap();
            request
        });
        let environment = deployment_environment(&api_url, path.to_str().unwrap());
        let mut command = args().to_vec();
        command.extend(["--timeout", "25ms"]);
        let output = run_with_env(&command, &environment);
        release.send(()).unwrap();
        let post = server.join().unwrap();
        assert!(post.starts_with("POST "));
        assert_eq!(output.status.code(), Some(1));
        assert!(output.stderr.is_empty());
        let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(json["outcome"], "timed_out");
        assert_eq!(json["requestId"], REQUEST_ID);
        assert_eq!(json["receipt"]["state"], "pending");
    }

    // REQ-RETRY-30, REQ-RETRY-40
    #[test]
    fn retry_human_result_and_invalid_version() {
        let human = [
            "run",
            "retry",
            ORGANIZATION,
            RUN_ID,
            "--allow-insecure-http",
        ];
        let (output, requests) = execute(&[("pending", None), ("applied", None)], &human);
        assert!(output.status.success());
        assert!(output.stderr.is_empty());
        assert!(String::from_utf8_lossy(&output.stdout).contains(REQUEST_ID));
        assert_eq!(requests.len(), 2);
        let (rejected, requests) = execute(
            &[("pending", None), ("rejected", Some("attempt_active"))],
            &human,
        );
        assert_eq!(rejected.status.code(), Some(1));
        assert!(rejected.stdout.is_empty());
        assert!(String::from_utf8_lossy(&rejected.stderr).contains("code: attempt_active"));
        assert_eq!(requests.len(), 2);
        let invalid = run(&[
            "run",
            "retry",
            ORGANIZATION,
            RUN_ID,
            "--expected-version",
            "0",
        ]);
        assert_eq!(invalid.status.code(), Some(2));
        assert!(invalid.stdout.is_empty());
        assert!(!invalid.stderr.is_empty());
    }

    // REQ-RETRY-35
    #[test]
    fn conflicts_are_not_reported_as_applied() {
        let (output, requests) = execute(&[("run-retry-pending", None)], &args());
        assert_eq!(output.status.code(), Some(1));
        assert!(output.stderr.is_empty());
        let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(json["outcome"], "retry_pending");
        assert_eq!(json["code"], "retry_pending");
        assert!(json.get("holderRunId").is_none());
        assert_eq!(requests.len(), 1);
    }

    // REQ-RETRY-35
    #[test]
    fn subject_refusals_name_the_holding_run() {
        const HOLDER: &str = "run_01k0z6r1w8f4jy2m7q9v3x5abd";
        for (problem, code) in [
            ("subject-busy", "subject_busy"),
            ("subject-settled", "subject_settled"),
        ] {
            let (output, requests) = execute(&[(problem, Some(HOLDER))], &args());
            assert_eq!(output.status.code(), Some(1));
            assert!(output.stderr.is_empty());
            let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(json["outcome"], code);
            assert_eq!(json["code"], code);
            assert_eq!(json["holderRunId"], HOLDER);
            assert!(json["receipt"].is_null());
            assert_eq!(requests.len(), 1);
            let human = [
                "run",
                "retry",
                ORGANIZATION,
                RUN_ID,
                "--allow-insecure-http",
            ];
            let (output, _) = execute(&[(problem, Some(HOLDER))], &human);
            assert_eq!(output.status.code(), Some(1));
            assert!(output.stdout.is_empty());
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(stderr.contains(&format!("code: {code}")), "{stderr}");
            assert!(stderr.contains(HOLDER), "{stderr}");
        }
    }

    // REQ-RETRY-35, REQ-RETRY-52: Cloud refuses the retry of a run by workflow path 409
    // `path-run-retired`, which the scripted peer answers as it does any conflict.
    #[test]
    fn a_path_run_retired_answer_is_reported_as_path_run_retired() {
        let (output, requests) = execute(&[("path-run-retired", None)], &args());
        assert_eq!(output.status.code(), Some(1), "{output:?}");
        assert!(output.stderr.is_empty());
        let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(json["outcome"], "error");
        assert_eq!(json["code"], "path_run_retired");
        assert_eq!(requests.len(), 1);

        let human = args()
            .into_iter()
            .filter(|arg| *arg != "--json")
            .collect::<Vec<_>>();
        let (output, _) = execute(&[("path-run-retired", None)], &human);
        assert_eq!(output.status.code(), Some(1), "{output:?}");
        assert!(output.stdout.is_empty());
        assert!(String::from_utf8_lossy(&output.stderr).contains("code: path_run_retired"));

        let (other, requests) = execute(&[("run-input-content-gone", None)], &args());
        let json: serde_json::Value = serde_json::from_slice(&other.stdout).unwrap();
        assert_eq!(other.status.code(), Some(4), "{other:?}");
        assert_eq!(json["code"], "acceptance_unknown");
        assert_eq!(requests.len(), 1);
    }
}

fn request_text(stream: &mut std::net::TcpStream) -> String {
    String::from_utf8(read_request(stream)).expect("scripted peer request should be UTF-8")
}

pub(super) fn prepared_run(responses: Vec<Vec<u8>>) -> (ScriptedServer, tempfile::TempDir, String) {
    let server = ScriptedServer::respond(responses);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture(
        &credential_path,
        &server.api_url,
        TOKEN,
        "2999-01-01T00:00:00Z",
    );
    let credential_path = credential_path.to_str().unwrap().to_owned();
    (server, credential_directory, credential_path)
}

// REQ-TRIGGER-53
// REQ-RUN-66
#[test]
fn list_runs_filters_and_renders_text_and_json() {
    let body = serde_json::json!({
        "items": [{
            "id": RUN_ID, "displayName": "Release checks", "projectId": PROJECT_ID,
            "workflowPath": WORKFLOW_PATH, "state": "running",
            "createdAt": "2025-01-03T00:00:00Z", "updatedAt": "2025-01-03T01:00:00Z",
            "integrationContext": {"source": "linear", "linearIssue": "LIV-123"},
            "placement": {"runnerId": "rnr_01k0z6r1w8f4jy2m7q9v3x5abc",
                "runnerName": "build-runner", "poolId": "rpl_01k0z6r1w8f4jy2m7q9v3x5abc",
                "poolName": "build-pool"}
        }],
        "nextCursor": "opaque-page"
    });
    let reply = http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[("Cache-Control", "private, no-store")],
        &serde_json::to_vec(&body).unwrap(),
    );
    let (server, _directory, credential_path) = prepared_run(vec![reply.clone(), reply]);
    let environment = deployment_environment(&server.api_url, &credential_path);
    let arguments = [
        "run",
        "list",
        ORGANIZATION,
        "--project-id",
        PROJECT_ID,
        "--state-group",
        "active",
        "--created-after",
        "2025-01-01T00:00:00Z",
        "--integration-context",
        "source=linear",
        "--integration-context",
        "linearIssue=LIV-123",
        "--limit",
        "1",
        "--cursor",
        "prior-page",
        "--allow-insecure-http",
    ];
    let text = run_with_env(&arguments, &environment);
    assert_eq!(
        text.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&text.stderr)
    );
    let report = String::from_utf8(text.stdout).unwrap();
    println!("um run list {ORGANIZATION} [filters]:\n{report}");
    assert!(
        report.contains(RUN_ID)
            && report.contains("build-runner")
            && report.contains("LIV-123")
            && report.contains("opaque-page")
    );
    let json_args = [arguments.as_slice(), &["--json"]].concat();
    let structured = run_with_env(&json_args, &environment);
    assert_eq!(
        structured.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&structured.stderr)
    );
    let parsed: serde_json::Value = serde_json::from_slice(&structured.stdout).unwrap();
    println!(
        "um run list {ORGANIZATION} [filters] --json:\n{}",
        String::from_utf8_lossy(&structured.stdout)
    );
    assert_eq!(parsed, body);
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    for request in requests {
        assert!(
            request.contains("projectId=")
                && request.contains("stateGroup=active")
                && request.contains("createdAfter=")
                && request.contains("integrationContext=source%3Dlinear")
                && request.contains("integrationContext=linearIssue%3DLIV-123")
                && request.contains("cursor=prior-page")
                && request.contains("limit=1"),
            "{request}"
        );
    }
}

// REQ-RUN-66
#[test]
fn list_runs_accepts_full_pages_with_large_valid_contexts() {
    let context = (0..15)
        .map(|index| {
            (
                format!("key{index}"),
                serde_json::Value::String("<".repeat(1024)),
            )
        })
        .collect::<serde_json::Map<String, serde_json::Value>>();
    let item = serde_json::json!({
        "id": RUN_ID, "displayName": "Release checks", "projectId": PROJECT_ID,
        "workflowPath": WORKFLOW_PATH, "state": "running",
        "createdAt": "2025-01-03T00:00:00Z", "updatedAt": "2025-01-03T01:00:00Z",
        "integrationContext": context, "placement": null
    });
    let items = (0..100)
        .rev()
        .map(|index| {
            let mut row = item.clone();
            row["id"] = serde_json::json!(format!("run_01k0z6r1w8f4jy2m7q9v3x5{index:03}"));
            row
        })
        .collect::<Vec<_>>();
    let body = serde_json::json!({"items": items, "nextCursor": "next-page"});
    // Gateway's Go JSON encoder escapes HTML-sensitive context bytes as \u003c.
    let encoded = serde_json::to_string(&body)
        .unwrap()
        .replace('<', "\\u003c")
        .into_bytes();
    assert!(encoded.len() > 8 * 1024 * 1024);
    let reply = http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[("Cache-Control", "private, no-store")],
        &encoded,
    );
    let (server, _directory, credential_path) = prepared_run(vec![reply.clone(), reply]);
    let environment = deployment_environment(&server.api_url, &credential_path);
    let arguments = [
        "run",
        "list",
        ORGANIZATION,
        "--limit",
        "100",
        "--allow-insecure-http",
    ];
    let text = run_with_env(&arguments, &environment);
    assert_eq!(
        text.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&text.stderr)
    );
    let report = String::from_utf8(text.stdout).unwrap();
    assert_eq!(
        report
            .lines()
            .filter(|line| line.starts_with("run_"))
            .count(),
        100
    );
    assert!(report.contains("next cursor: next-page"));
    let json_args = [arguments.as_slice(), &["--json"]].concat();
    let structured = run_with_env(&json_args, &environment);
    assert_eq!(
        structured.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&structured.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&structured.stdout).unwrap(),
        body
    );
    assert_eq!(server.finish().len(), 2);
}

fn acceptance_body(run_id: &str, replayed: bool) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "runId": run_id,
        "replayed": replayed
    }))
    .unwrap()
}

pub(super) fn acceptance_response(replayed: bool) -> Vec<u8> {
    acceptance_response_for(
        "202 Accepted",
        RUN_ID,
        replayed,
        &[
            ("Idempotency-Key", ECHO_IDEMPOTENCY_KEY),
            (
                "Location",
                "/v1/organizations/acme-research/runs/run_01k0z6r1w8f4jy2m7q9v3x5abc",
            ),
        ],
    )
}

pub(super) fn acceptance_response_for(
    status: &str,
    run_id: &str,
    replayed: bool,
    headers: &[(&str, &str)],
) -> Vec<u8> {
    http_response_with_headers(
        status,
        Some("application/json"),
        headers,
        &acceptance_body(run_id, replayed),
    )
}

fn chunked_json_response(status: &str, body: &[u8]) -> Vec<u8> {
    let mut response = format!(
        "HTTP/1.1 {status}\r\nConnection: close\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n{:x}\r\n",
        body.len()
    )
    .into_bytes();
    response.extend_from_slice(body);
    response.extend_from_slice(b"\r\n0\r\n\r\n");
    response
}

fn run_body() -> serde_json::Value {
    run_body_with_state("running")
}

pub(super) fn run_body_with_state(state: &str) -> serde_json::Value {
    let interruption = if state == "interrupted" {
        serde_json::json!({
            "phase": "running",
            "cause": "executor_shutdown",
            "executorFault": null,
            "stopConfirmed": true
        })
    } else {
        serde_json::Value::Null
    };
    serde_json::json!({
        "id": RUN_ID,
        "organizationId": ORGANIZATION_ID,
        "projectId": PROJECT_ID,
        "displayName": "Release checks",
        "executionSpecId": EXECUTION_SPEC_ID,
        "state": state,
        "version": 7,
        "currentAttemptId": ATTEMPT_ID,
        "currentAttemptNumber": 2,
        "sourceBranch": "release/next",
        "workspaceSource": null,
        "workspaceBranch": null,
        "workspaceMembership": null,
        "workflowDefinitionSource": {
            "repositoryConnectionId": REPOSITORY_CONNECTION_ID,
            "objectFormat": "sha1",
            "commitOid": "0123456789abcdef0123456789abcdef01234567",
            "workflowPath": WORKFLOW_PATH,
            "workflowSourceClosureDigest": {
                "algorithm": "sha256",
                "value": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
            }
        },
        "primaryWorkspaceSource": {
            "kind": "connected_repository",
            "providerKind": "github",
            "repositoryConnectionId": REPOSITORY_CONNECTION_ID,
            "objectFormat": "sha1",
            "commitOid": "0123456789abcdef0123456789abcdef01234567",
            "materializationContract": "git_full_clone_v1"
        },
        "sourceDisplaySnapshot": {
            "organizationDisplayName": "Example Organization",
            "projectName": "example-project",
            "repository": {
                "providerKind": "github",
                "fullName": "example/repository"
            }
        },
        "inputs": {
            "inputSetId": INPUT_SET_ID,
            "inputCount": 2,
            "attachmentCount": 2,
            "aggregateBytes": 4096,
            "availability": "available"
        },
        "integrationContext": {
            "issueId": "issue-private-context-sentinel",
            "source": "linear"
        },
        "actor": {"kind": "app"},
        "publication": null,
        "cancellation": null,
        "interruption": interruption,
        "failure": null,
        "rejection": null,
        "artifactDelivery": null,
        "portableResult": "absent",
        "continuation": null,
        "createdAt": "2026-08-10T12:00:00Z",
        "updatedAt": "2026-08-10T12:05:00Z"
    })
}

fn automatic_publication(outcome: &str) -> serde_json::Value {
    let publication_id = "pub_01k0z6r1w8f4jy2m7q9v3x5abc";
    let pull_request = if outcome == "pull_request_already_merged" {
        serde_json::json!({
            "providerId": "123456", "number": 42,
            "url": "https://example.test/review/42",
            "disposition": "reused", "state": "merged"
        })
    } else if outcome == "pull_request_published" {
        serde_json::json!({
            "providerId": "123456", "number": 42,
            "url": "https://example.test/review/42",
            "disposition": "created", "state": "open"
        })
    } else {
        serde_json::Value::Null
    };
    let branch = if outcome == "no_changes" {
        serde_json::Value::Null
    } else {
        serde_json::json!({
            "headOid": "89abcdef0123456789abcdef0123456789abcdef",
            "disposition": if outcome == "pull_request_already_merged" { "reused" } else { "created" },
            "url": "https://example.test/review/branch"
        })
    };
    serde_json::json!({
        "id": publication_id, "organizationId": ORGANIZATION_ID,
        "projectId": PROJECT_ID, "runId": RUN_ID,
        "artifactSetId": "ats_01k0z6r1w8f4jy2m7q9v3x5abc",
        "exportName": "changes", "state": "succeeded", "version": 2,
        "providerKind": "github",
        "artifact": {
            "artifactVersion": 1, "objectFormat": "sha1",
            "baseOid": "0123456789abcdef0123456789abcdef01234567",
            "headOid": "89abcdef0123456789abcdef0123456789abcdef",
            "treeOid": "fedcba9876543210fedcba9876543210fedcba98",
            "expiresAt": "2026-10-03T18:00:00Z"
        },
        "target": {
            "repositoryConnectionId": REPOSITORY_CONNECTION_ID,
            "providerRepositoryId": "123456", "fullName": "example/repository",
            "kind": "new_change_request", "baseBranch": "main", "branchVersion": 2, "destinationBranch": format!("um/{RUN_ID}/changes")
        },
        "pullRequestMetadata": {"title": "Review", "body": "Run publication", "titleSource": "default", "descriptionSource": "default"},
        "branch": branch, "pullRequest": pull_request, "outcome": outcome,
        "failure": null,
        "actorPrincipalId": "prn_01k0z6r1w8f4jy2m7q9v3x5abc",
        "createdAt": "2026-09-03T18:00:00Z", "updatedAt": "2026-09-03T18:00:02Z",
        "startedAt": "2026-09-03T18:00:01Z", "terminalAt": "2026-09-03T18:00:02Z"
    })
}

fn automatic_handoff(state: &str) -> serde_json::Value {
    serde_json::json!({
        "exportName": "changes", "state": state,
        "publicationId": if state == "started" {
            serde_json::Value::String("pub_01k0z6r1w8f4jy2m7q9v3x5abc".into())
        } else { serde_json::Value::Null },
        "failure": null
    })
}

// REQ-PUBLISH-89, REQ-PUBLISH-92, REQ-PUBLISH-94
#[test]
fn run_show_preserves_the_bound_existing_request_in_handoff_history() {
    let mut body = run_body_with_state("succeeded");
    body["publication"] = automatic_handoff("pending");
    body["publication"]["target"] = serde_json::json!({
        "kind":"existing_change_request", "number":42, "providerChangeRequestId":"420",
        "sourceRef":"human/feature", "expectedHeadOid":"0123456789abcdef0123456789abcdef01234567"
    });
    let (output, server) = run_show_with_response(run_response(body.clone()));
    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let shown: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        shown["run"]["publication"]["target"],
        body["publication"]["target"]
    );
    server.finish();
    let mut invalid = body;
    invalid["publication"]["target"]["repository"] = serde_json::json!("untrusted/repo");
    let (output, server) = run_show_with_response(run_response(invalid));
    assert_eq!(output.status.code(), Some(1));
    server.finish();
}

// SC-RUN-6, REQ-RUN-82: `um run show` shows the run's actor, passes it through in JSON,
// and reports a run without a well-formed actor as an invalid response.
#[test]
fn run_show_reports_the_run_actor() {
    let principal = "prn_01k0z6r1w8f4jy2m7q9v3x5abc";
    for (actor, line) in [
        (
            serde_json::json!({"kind": "member", "requesterPrincipalId": principal}),
            format!("actor: member {principal}"),
        ),
        (serde_json::json!({"kind": "app"}), "actor: app".to_owned()),
    ] {
        let mut body = run_body();
        body["actor"] = actor.clone();
        let (output, server) = run_show_with_response(run_response(body.clone()));
        assert!(output.status.success());
        let shown: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(shown["run"]["actor"], actor);
        server.finish();

        let (server, _directory, credential_path) = prepared_run(vec![run_response(body)]);
        let environment = deployment_environment(&server.api_url, &credential_path);
        let output = run_with_env(
            &["run", "show", ORGANIZATION, RUN_ID, "--allow-insecure-http"],
            &environment,
        );
        assert!(output.status.success());
        let stdout = String::from_utf8(output.stdout).unwrap();
        assert!(
            stdout.lines().any(|candidate| candidate == line),
            "missing {line:?} in {stdout:?}"
        );
        server.finish();
    }
    for actor in [
        None,
        Some(serde_json::json!({"kind": "member"})),
        Some(serde_json::json!({"kind": "member", "requesterPrincipalId": "prn_short"})),
        Some(serde_json::json!({"kind": "app", "requesterPrincipalId": principal})),
    ] {
        let mut body = run_body();
        match actor {
            Some(actor) => body["actor"] = actor,
            None => {
                body.as_object_mut().unwrap().remove("actor");
            }
        }
        let (output, server) = run_show_with_response(run_response(body));
        assert_invalid_response(&output);
        server.finish();
    }
}

// REQ-COMPAT-16
#[test]
fn generated_client_requests_name_the_cli_version_in_the_user_agent() {
    let (output, server) = run_show_with_response(run_response(run_body_with_state("succeeded")));

    assert!(output.status.success());
    let request = server.finish().pop().unwrap();
    assert_eq!(
        header_value(&request, "user-agent"),
        format!("um/{BUILD_VERSION}")
    );
}

fn publication_response(body: serde_json::Value) -> Vec<u8> {
    http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[("Cache-Control", "private, no-store")],
        &serde_json::to_vec(&body).unwrap(),
    )
}

pub(super) fn run_response(body: serde_json::Value) -> Vec<u8> {
    http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[("Cache-Control", "private, no-store")],
        &serde_json::to_vec(&body).unwrap(),
    )
}

const CANCELLATION_ID: &str = "cmd_01k0z6r1w8f4jy2m7q9v3x5abc";

fn cancellation_envelope(state: &str, mode: &str, run: serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "request": {
            "id": CANCELLATION_ID,
            "organizationId": ORGANIZATION_ID,
            "runId": RUN_ID,
            "attemptId": ATTEMPT_ID,
            "mode": mode,
            "acceptedAt": "2026-08-10T12:00:00Z",
            "state": state,
            "resolution": if state == "resolved" { serde_json::json!({
                "kind": "already_terminal", "resolvedAt": "2026-08-10T12:05:00Z",
                "effectiveRequestId": null, "runVersion": 7
            }) } else { serde_json::Value::Null }
        },
        "run": run
    })
}

fn cancellation_response(status: &str, key: &str, body: serde_json::Value) -> Vec<u8> {
    http_response_with_headers(
        status,
        Some("application/json"),
        &[
            ("Idempotency-Key", key),
            ("Cache-Control", "private, no-store"),
            (
                "Location",
                "/v1/organizations/org_01k0z6r1w8f4jy2m7q9v3x5abc/runs/run_01k0z6r1w8f4jy2m7q9v3x5abc/cancellation-requests/cmd_01k0z6r1w8f4jy2m7q9v3x5abc",
            ),
        ],
        &serde_json::to_vec(&body).unwrap(),
    )
}

// REQ-CANCEL-49
#[cfg(target_os = "linux")]
#[test]
fn cancellation_signal_before_acceptance_preserves_key_and_never_observes() {
    let key = "signal-recovery-key";
    let mut server =
        ScriptedServer::respond_with_paused_first_response(vec![cancellation_response(
            "202 Accepted",
            key,
            cancellation_envelope("pending", "graceful", serde_json::Value::Null),
        )]);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture(
        &credential_path,
        &server.api_url,
        TOKEN,
        "2999-01-01T00:00:00Z",
    );
    let environment = deployment_environment(&server.api_url, credential_path.to_str().unwrap());
    let mut command = Command::new(env!("CARGO_BIN_EXE_um"));
    command
        .args([
            "run",
            "cancel",
            ORGANIZATION,
            RUN_ID,
            "--idempotency-key",
            key,
            "--wait",
            "--json",
            "--allow-insecure-http",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env_remove(CREDENTIALS_FILE_VARIABLE);
    for variable in DEPLOYMENT_VARIABLES {
        command.env_remove(variable);
    }
    for (name, value) in environment {
        command.env(name, value);
    }
    let child = command.spawn().unwrap();
    let request = server.next_request();
    assert!(request.contains("/cancellation-requests HTTP/1.1"));
    assert_eq!(header_value(&request, "idempotency-key"), key);
    rustix::process::kill_process(
        rustix::process::Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap(),
        rustix::process::Signal::INT,
    )
    .unwrap();
    let output = child.wait_with_output().unwrap();
    server.release_paused_response();
    assert_eq!(output.status.code(), Some(130));
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "acceptance_unknown");
    assert_eq!(result["error"]["idempotencyKey"], key);
    assert_eq!(result["error"]["requestedMode"], "graceful");
    assert_eq!(result["cancellationRequest"], serde_json::Value::Null);
    assert!(server.finish().is_empty());
}

// REQ-CANCEL-49
#[cfg(target_os = "linux")]
#[test]
fn cancellation_signal_after_acceptance_stops_observation_without_new_post() {
    let key = "accepted-signal-key";
    let pending = cancellation_envelope("pending", "force", serde_json::Value::Null);
    let mut server = ScriptedServer::respond_with_paused_last_response(vec![
        cancellation_response("202 Accepted", key, pending.clone()),
        http_response_with_headers(
            "200 OK",
            Some("application/json"),
            &[("Cache-Control", "private, no-store")],
            &serde_json::to_vec(&pending).unwrap(),
        ),
    ]);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture(
        &credential_path,
        &server.api_url,
        TOKEN,
        "2999-01-01T00:00:00Z",
    );
    let environment = deployment_environment(&server.api_url, credential_path.to_str().unwrap());
    let mut command = Command::new(env!("CARGO_BIN_EXE_um"));
    command
        .args([
            "run",
            "cancel",
            ORGANIZATION,
            RUN_ID,
            "--force",
            "--idempotency-key",
            key,
            "--wait",
            "--json",
            "--allow-insecure-http",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env_remove(CREDENTIALS_FILE_VARIABLE);
    for variable in DEPLOYMENT_VARIABLES {
        command.env_remove(variable);
    }
    for (name, value) in environment {
        command.env(name, value);
    }
    let child = command.spawn().unwrap();
    let first = server.next_request();
    assert!(first.contains("/cancellation-requests HTTP/1.1"));
    let second = server.next_request();
    assert!(second.contains(&format!(
        "/cancellation-requests/{CANCELLATION_ID} HTTP/1.1"
    )));
    rustix::process::kill_process(
        rustix::process::Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap(),
        rustix::process::Signal::TERM,
    )
    .unwrap();
    let output = child.wait_with_output().unwrap();
    server.release_paused_response();
    assert_eq!(output.status.code(), Some(143));
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "observation_stopped");
    assert_eq!(result["cancellationRequest"]["id"], CANCELLATION_ID);
    assert_eq!(result["error"]["idempotencyKey"], key);
    assert_eq!(result["error"]["requestedMode"], "force");
    assert!(server.finish().is_empty());
}

// REQ-CANCEL-48, REQ-CANCEL-50, REQ-CANCEL-51
#[test]
fn cancellation_observation_failure_retains_reconciliation_identity_without_post_retry() {
    let key = "accepted-then-forbidden-key";
    for json in [true, false] {
        let mut run = run_body_with_state("interrupted");
        run["artifactDelivery"] = serde_json::json!({
            "state": "failed", "phase": "upload", "code": "carrier_upload_failed"
        });
        let pending = cancellation_envelope("pending", "graceful", run);
        let (server, _directory, credential_path) = prepared_run(vec![
            cancellation_response("202 Accepted", key, pending),
            problem_http_response(
                "403 Forbidden",
                serde_json::json!({
                    "type":"https://api.usefulmachinery.com/problems/forbidden", "title":"Forbidden", "status":403
                }),
            ),
        ]);
        let environment = deployment_environment(&server.api_url, &credential_path);
        let mut args = vec![
            "run",
            "cancel",
            ORGANIZATION,
            RUN_ID,
            "--idempotency-key",
            key,
            "--wait",
            "--allow-insecure-http",
        ];
        if json {
            args.push("--json");
        }
        let output = run_with_env(&args, &environment);
        assert_eq!(output.status.code(), Some(1));
        if json {
            let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(result["outcome"], "error");
            assert_eq!(result["error"]["code"], "forbidden");
            assert_eq!(result["error"]["idempotencyKey"], key);
            assert_eq!(result["error"]["requestedMode"], "graceful");
            assert_eq!(result["error"]["requestId"], CANCELLATION_ID);
            assert_eq!(result["cancellationRequest"]["state"], "pending");
        } else {
            assert!(output.stdout.is_empty());
            let report = String::from_utf8(output.stderr).unwrap();
            for field in [
                &format!("request: {CANCELLATION_ID}"),
                "requested mode: graceful",
                "request state: pending",
                "effective mode: not applied",
                "run state: interrupted",
                "interruption cause: executor_shutdown",
                "stop confirmed: yes",
                "artifact delivery: failed (upload: carrier_upload_failed)",
            ] {
                assert!(
                    report.lines().any(|line| line == field),
                    "missing {field} in {report}"
                );
            }
        }
        let requests = server.finish();
        assert_eq!(requests.len(), 2);
        assert!(requests[0].contains("/cancellation-requests HTTP/1.1"));
        assert!(requests[1].contains(&format!(
            "/cancellation-requests/{CANCELLATION_ID} HTTP/1.1"
        )));
    }
}

// REQ-CANCEL-40, REQ-CANCEL-41
#[test]
fn plain_force_cancellation_unknown_retains_requested_mode() {
    let key = "unknown-force-key";
    let (server, _directory, credential_path) = prepared_run(vec![Vec::new(), Vec::new()]);
    let output = run_with_env(
        &[
            "run",
            "cancel",
            ORGANIZATION,
            RUN_ID,
            "--force",
            "--idempotency-key",
            key,
            "--allow-insecure-http",
        ],
        &deployment_environment(&server.api_url, &credential_path),
    );
    assert_eq!(output.status.code(), Some(4));
    assert!(output.stdout.is_empty());
    let report = String::from_utf8(output.stderr).unwrap();
    assert!(report.contains("requested mode: force"));
    assert!(report.contains(&format!("idempotency key: {key}")));
    assert!(report.contains("request: not observed"));
    assert!(report.contains("run state: not observed"));
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    for request in requests {
        assert_eq!(header_value(&request, "idempotency-key"), key);
        assert_eq!(request_body(&request)["mode"], "force");
    }
}

// REQ-CANCEL-46, REQ-CANCEL-51
#[test]
fn plain_cancellation_timeout_retains_accepted_snapshot() {
    let key = "plain-timeout-key";
    let mut run = run_body_with_state("running");
    run["cancellation"] = serde_json::json!({
        "mode": "graceful", "gracefulRequestId": CANCELLATION_ID, "forceRequestId": null
    });
    let pending = cancellation_envelope("pending", "graceful", run);
    let mut server = ScriptedServer::respond_with_paused_last_response(vec![
        cancellation_response("202 Accepted", key, pending.clone()),
        http_response_with_headers(
            "200 OK",
            Some("application/json"),
            &[("Cache-Control", "private, no-store")],
            &serde_json::to_vec(&pending).unwrap(),
        ),
    ]);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture(
        &credential_path,
        &server.api_url,
        TOKEN,
        "2999-01-01T00:00:00Z",
    );
    let output = run_with_env(
        &[
            "run",
            "cancel",
            ORGANIZATION,
            RUN_ID,
            "--idempotency-key",
            key,
            "--wait",
            "--timeout",
            "2s",
            "--allow-insecure-http",
        ],
        &deployment_environment(&server.api_url, credential_path.to_str().unwrap()),
    );
    server.release_paused_response();
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    let report = String::from_utf8(output.stderr).unwrap();
    for field in [
        &format!("request: {CANCELLATION_ID}"),
        "requested mode: graceful",
        "request state: pending",
        "effective mode: graceful",
        "run state: running",
    ] {
        assert!(
            report.lines().any(|line| line == field),
            "missing {field} in {report}"
        );
    }
    assert!(report.contains("wait_timed_out"));
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert!(requests[0].contains("/cancellation-requests HTTP/1.1"));
    assert!(requests[1].contains(&format!(
        "/cancellation-requests/{CANCELLATION_ID} HTTP/1.1"
    )));
}

// REQ-CANCEL-45
#[test]
fn cancellation_wait_ends_as_creation_rejection_without_inventing_a_run() {
    let key = "rejected-create-key";
    let pending = cancellation_envelope("pending", "graceful", serde_json::Value::Null);
    let mut rejected = cancellation_envelope("resolved", "graceful", serde_json::Value::Null);
    rejected["request"]["resolution"]["kind"] = serde_json::json!("creation_rejected");
    rejected["request"]["resolution"]["runVersion"] = serde_json::Value::Null;
    let (server, _directory, credential_path) = prepared_run(vec![
        cancellation_response("202 Accepted", key, pending),
        http_response_with_headers(
            "200 OK",
            Some("application/json"),
            &[("Cache-Control", "private, no-store")],
            &serde_json::to_vec(&rejected).unwrap(),
        ),
    ]);
    let environment = deployment_environment(&server.api_url, &credential_path);
    let output = run_with_env(
        &[
            "run",
            "cancel",
            ORGANIZATION,
            RUN_ID,
            "--idempotency-key",
            key,
            "--wait",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert_eq!(output.status.code(), Some(1));
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "error");
    assert_eq!(result["error"]["code"], "creation_rejected");
    assert!(result["run"].is_null());
    assert_eq!(
        result["cancellationRequest"]["resolution"]["kind"],
        "creation_rejected"
    );
    assert_eq!(server.finish().len(), 2);
}

// REQ-CANCEL-43
#[test]
fn cancellation_wait_requires_resolved_receipt_and_terminal_run() {
    let key = "resolved-wait-key";
    let pending = cancellation_envelope("pending", "force", run_body());
    let mut settled =
        cancellation_envelope("resolved", "force", run_body_with_state("interrupted"));
    settled["request"]["resolution"]["kind"] = serde_json::json!("applied");
    settled["request"]["resolution"]["effectiveRequestId"] = serde_json::json!(CANCELLATION_ID);
    settled["run"]["cancellation"] = serde_json::json!({
        "mode": "force", "gracefulRequestId": null, "forceRequestId": CANCELLATION_ID
    });
    let (server, _directory, credential_path) = prepared_run(vec![
        cancellation_response("202 Accepted", key, pending),
        http_response_with_headers(
            "200 OK",
            Some("application/json"),
            &[("Cache-Control", "private, no-store")],
            &serde_json::to_vec(&settled).unwrap(),
        ),
    ]);
    let environment = deployment_environment(&server.api_url, &credential_path);
    let output = run_with_env(
        &[
            "run",
            "cancel",
            ORGANIZATION,
            RUN_ID,
            "--force",
            "--idempotency-key",
            key,
            "--wait",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert_eq!(output.status.code(), Some(0));
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "settled");
    assert_eq!(result["run"]["state"], "interrupted");
    assert_eq!(
        result["cancellationRequest"]["resolution"]["kind"],
        "applied"
    );
    assert_eq!(result["run"]["interruption"]["cause"], "executor_shutdown");
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert!(requests[0].contains("/cancellation-requests HTTP/1.1"));
    assert!(requests[1].contains(&format!(
        "/cancellation-requests/{CANCELLATION_ID} HTTP/1.1"
    )));
}

// REQ-CANCEL-44
// REQ-PUBLISH-64
#[test]
fn cancellation_wait_does_not_observe_a_linked_publication() {
    let key = "cancel-while-publication-started";
    let mut succeeded = run_body_with_state("succeeded");
    succeeded["publication"] = automatic_handoff("started");
    let (server, _directory, credential_path) = prepared_run(vec![
        cancellation_response(
            "202 Accepted",
            key,
            cancellation_envelope("pending", "graceful", run_body()),
        ),
        http_response_with_headers(
            "200 OK",
            Some("application/json"),
            &[("Cache-Control", "private, no-store")],
            &serde_json::to_vec(&cancellation_envelope("resolved", "graceful", succeeded)).unwrap(),
        ),
    ]);
    let output = run_with_env(
        &[
            "run",
            "cancel",
            ORGANIZATION,
            RUN_ID,
            "--idempotency-key",
            key,
            "--wait",
            "--json",
            "--allow-insecure-http",
        ],
        &deployment_environment(&server.api_url, &credential_path),
    );
    assert_eq!(output.status.code(), Some(0));
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "settled");
    assert_eq!(result["run"]["state"], "succeeded");
    assert_eq!(result["run"]["publication"]["state"], "started");
    assert!(result["publication"].is_null());
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert!(requests[0].starts_with("POST "));
    assert!(requests[1].contains(&format!("/cancellation-requests/{CANCELLATION_ID} ")));
}

// REQ-CANCEL-38
#[test]
fn explicit_force_escalation_uses_a_separate_key_and_never_reuses_graceful_strength() {
    let normal_key = "normal-request-key";
    let force_key = "force-escalation-key";
    let (server, _directory, credential_path) = prepared_run(vec![
        cancellation_response(
            "202 Accepted",
            normal_key,
            cancellation_envelope("pending", "graceful", run_body()),
        ),
        cancellation_response(
            "202 Accepted",
            force_key,
            cancellation_envelope("pending", "force", run_body()),
        ),
    ]);
    let environment = deployment_environment(&server.api_url, &credential_path);
    for (key, force) in [(normal_key, false), (force_key, true)] {
        let mut args = vec![
            "run",
            "cancel",
            ORGANIZATION,
            RUN_ID,
            "--idempotency-key",
            key,
            "--json",
            "--allow-insecure-http",
        ];
        if force {
            args.push("--force");
        }
        let output = run_with_env(&args, &environment);
        assert_eq!(output.status.code(), Some(0));
        let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(result["outcome"], "accepted");
        assert_eq!(
            result["cancellationRequest"]["mode"],
            if force { "force" } else { "graceful" }
        );
    }
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert_eq!(header_value(&requests[0], "idempotency-key"), normal_key);
    assert_eq!(header_value(&requests[1], "idempotency-key"), force_key);
    assert_eq!(request_body(&requests[0])["mode"], "graceful");
    assert_eq!(request_body(&requests[1])["mode"], "force");
}

// REQ-CANCEL-42
#[test]
fn cancellation_local_authentication_failure_still_writes_one_json_result() {
    let environment = deployment_environment("http://127.0.0.1:1", "/dev/null/credentials.json");
    let output = run_with_env(
        &[
            "run",
            "cancel",
            ORGANIZATION,
            RUN_ID,
            "--idempotency-key",
            "never-dispatched-key",
            "--service-api-key-file",
            "/dev/null/api-key",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert_eq!(output.status.code(), Some(1));
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "error");
    assert_eq!(result["error"]["code"], "submission_failed");
    assert_eq!(result["error"]["idempotencyKey"], "never-dispatched-key");
}

// REQ-CANCEL-42
#[test]
fn cancellation_definite_rate_limit_is_not_reported_as_unknown_acceptance() {
    let key = "rate-limit-key";
    let (server, _directory, credential_path) = prepared_run(vec![problem_http_response(
        "429 Too Many Requests",
        serde_json::json!({
            "type": "https://api.usefulmachinery.com/problems/too-many-requests", "title": "Slow down", "status": 429
        }),
    )]);
    let environment = deployment_environment(&server.api_url, &credential_path);
    let output = run_with_env(
        &[
            "run",
            "cancel",
            ORGANIZATION,
            RUN_ID,
            "--idempotency-key",
            key,
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert_eq!(output.status.code(), Some(4));
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "error");
    assert_eq!(result["error"]["code"], "unavailable");
    assert_eq!(result["error"]["idempotencyKey"], key);
    assert_eq!(server.finish().len(), 1);
}

// REQ-CANCEL-42
#[test]
fn cancellation_key_conflict_does_not_send_escalation_or_claim_acceptance() {
    let key = "same-key-mode-conflict";
    let (server, _directory, credential_path) = prepared_run(vec![problem_http_response(
        "409 Conflict",
        serde_json::json!({
            "type": "https://api.usefulmachinery.com/problems/idempotency-conflict",
            "title": "Conflict", "status": 409
        }),
    )]);
    let environment = deployment_environment(&server.api_url, &credential_path);
    let output = run_with_env(
        &[
            "run",
            "cancel",
            ORGANIZATION,
            RUN_ID,
            "--force",
            "--idempotency-key",
            key,
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert_eq!(output.status.code(), Some(1));
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "error");
    assert_eq!(result["error"]["code"], "idempotency_conflict");
    assert_eq!(result["error"]["idempotencyKey"], key);
    assert_eq!(result["error"]["requestedMode"], "force");
    assert!(result["cancellationRequest"].is_null());
    assert_eq!(server.finish().len(), 1);
}

// REQ-CANCEL-39, REQ-CANCEL-40
#[test]
fn cancellation_retry_reuses_key_and_mode_without_a_second_control() {
    let key = "cancellation-retry-key";
    let pending = cancellation_envelope("pending", "force", serde_json::Value::Null);
    let truncated = format!(
        "HTTP/1.1 202 Accepted\r\nConnection: close\r\nContent-Type: application/json\r\nIdempotency-Key: {key}\r\nContent-Length: 4096\r\n\r\n{{"
    ).into_bytes();
    let (server, _directory, credential_path) = prepared_run(vec![
        truncated,
        cancellation_response("202 Accepted", key, pending),
    ]);
    let environment = deployment_environment(&server.api_url, &credential_path);
    let output = run_with_env(
        &[
            "run",
            "cancel",
            ORGANIZATION,
            RUN_ID,
            "--force",
            "--idempotency-key",
            key,
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert_eq!(output.status.code(), Some(0));
    let document: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(document["outcome"], "accepted");
    assert_eq!(document["cancellationRequest"]["state"], "pending");
    assert_eq!(document["run"], serde_json::Value::Null);
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    for request in requests {
        assert_eq!(header_value(&request, "idempotency-key"), key);
        assert!(request.contains("\"mode\":\"force\""));
    }
}

// REQ-CANCEL-39, REQ-CANCEL-50
#[test]
fn cancellation_terminal_noop_and_explicit_force_preserve_receipt_and_run() {
    for (mode, flag) in [("graceful", None), ("force", Some("--force"))] {
        let key = "same-key-for-reconciliation";
        let body = run_body_with_state("succeeded");
        let (server, _directory, credential_path) = prepared_run(vec![cancellation_response(
            "200 OK",
            key,
            cancellation_envelope("resolved", mode, body.clone()),
        )]);
        let environment = deployment_environment(&server.api_url, &credential_path);
        let mut args = vec![
            "run",
            "cancel",
            ORGANIZATION,
            RUN_ID,
            "--idempotency-key",
            key,
            "--json",
            "--allow-insecure-http",
        ];
        if let Some(flag) = flag {
            args.push(flag);
        }
        let output = run_with_env(&args, &environment);
        assert_eq!(output.status.code(), Some(0));
        assert!(output.stderr.is_empty());
        let document: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(document["operation"], "cancel");
        assert_eq!(document["runId"], RUN_ID);
        assert_eq!(document["run"], body);
        assert_eq!(document["cancellationRequest"]["state"], "resolved");
        assert_eq!(document["cancellationRequest"]["mode"], mode);
        assert_eq!(document["replayed"], serde_json::Value::Null);
        assert_eq!(document["error"], serde_json::Value::Null);
        let requests = server.finish();
        assert_eq!(requests.len(), 1);
        assert_eq!(header_value(&requests[0], "idempotency-key"), key);
        assert!(requests[0].contains(&format!("\"mode\":\"{mode}\"")));
    }
}

// REQ-CANCEL-41
#[test]
fn cancellation_rejects_slug_location_even_when_receipt_is_valid() {
    let key = "slug-location-key";
    let response = cancellation_response(
        "202 Accepted",
        key,
        cancellation_envelope("pending", "graceful", serde_json::Value::Null),
    );
    let response = String::from_utf8(response)
        .unwrap()
        .replace(
            &format!("/organizations/{ORGANIZATION_ID}/"),
            &format!("/organizations/{ORGANIZATION}/"),
        )
        .into_bytes();
    let (server, _directory, credential_path) = prepared_run(vec![response]);
    let output = run_with_env(
        &[
            "run",
            "cancel",
            ORGANIZATION,
            RUN_ID,
            "--idempotency-key",
            key,
            "--json",
            "--allow-insecure-http",
        ],
        &deployment_environment(&server.api_url, &credential_path),
    );
    assert_eq!(output.status.code(), Some(4));
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "acceptance_unknown");
    assert_eq!(result["error"]["idempotencyKey"], key);
    assert_eq!(server.finish().len(), 1);
}

#[test]
fn cancellation_retry_can_observe_pending_receipt_with_prior_failed_attempt() {
    let key = "retry-pending-key";
    let mut pending = cancellation_envelope("pending", "graceful", run_body_with_state("failed"));
    pending["request"]["attemptId"] = serde_json::Value::Null;
    let mut resolved =
        cancellation_envelope("resolved", "graceful", run_body_with_state("cancelled"));
    resolved["request"]["resolution"]["kind"] = serde_json::json!("applied");
    resolved["request"]["resolution"]["effectiveRequestId"] = serde_json::json!(CANCELLATION_ID);
    let (server, _directory, credential_path) = prepared_run(vec![
        cancellation_response("202 Accepted", key, pending),
        http_response_with_headers(
            "200 OK",
            Some("application/json"),
            &[("Cache-Control", "private, no-store")],
            &serde_json::to_vec(&resolved).unwrap(),
        ),
    ]);
    let output = run_with_env(
        &[
            "run",
            "cancel",
            ORGANIZATION,
            RUN_ID,
            "--idempotency-key",
            key,
            "--wait",
            "--json",
            "--allow-insecure-http",
        ],
        &deployment_environment(&server.api_url, &credential_path),
    );
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "settled");
    assert_eq!(result["run"]["state"], "cancelled");
    assert_eq!(result["cancellationRequest"]["state"], "resolved");
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert!(requests[1].contains(&format!(
        "/cancellation-requests/{CANCELLATION_ID} HTTP/1.1"
    )));
}

/// `um workflow run create DEFINITION --cloud ORGANIZATION --project PROJECT`, the command
/// that creates a Cloud run (REQ-RUN-94), with a source branch and a display name.
pub(super) fn create_args(json: bool) -> Vec<&'static str> {
    let mut args = vec![
        "workflow",
        "run",
        "create",
        DEFINITION_ID,
        "--cloud",
        ORGANIZATION,
        "--project",
        PROJECT_ID,
        "--source-branch",
        "release/next",
        "--display-name",
        "Release checks",
    ];
    if json {
        args.push("--json");
    }
    args.push("--allow-insecure-http");
    args
}

pub(super) fn sha256(bytes: &[u8]) -> [u8; 32] {
    let observed = digest(&SHA256, bytes);
    let mut sha256 = [0_u8; 32];
    sha256.copy_from_slice(observed.as_ref());
    sha256
}

pub(super) fn hex_digest(bytes: &[u8]) -> String {
    sha256(bytes)
        .into_iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn scalar_manifest_digest(bytes: &[u8], kind: &str) -> String {
    let canonical = format!(
        "{{\"inputs\":{{\"request\":{{\"kind\":\"{kind}\",\"sha256\":\"{}\",\"sizeBytes\":{}}}}},\"schemaVersion\":1}}",
        hex_digest(bytes),
        bytes.len()
    );
    hex_digest(canonical.as_bytes())
}

pub(super) fn scalar_input_set_body(
    bytes: &[u8],
    kind: &str,
    state: &str,
    uploaded: bool,
    replayed: bool,
) -> serde_json::Value {
    let mut body = serde_json::json!({
        "id": INPUT_SET_ID,
        "organizationId": ORGANIZATION_ID,
        "projectId": PROJECT_ID,
        "boundsProfile": 1,
        "manifest": {
            "schemaVersion": 1,
            "inputs": {
                "request": {
                    "kind": kind,
                    "sizeBytes": bytes.len(),
                    "sha256": hex_digest(bytes)
                }
            }
        },
        "manifestDigest": {
            "algorithm": "sha256",
            "value": scalar_manifest_digest(bytes, kind)
        },
        "inputCount": 1,
        "attachmentCount": 0,
        "aggregateSizeBytes": bytes.len(),
        "state": state,
        "createdAt": "2026-08-24T01:00:00Z",
        "openDeadlineAt": "2999-08-25T01:00:00Z",
        "members": [{"memberId": "inputs/request", "uploadConfirmed": uploaded}],
        "replayed": replayed
    });
    if state == "sealed" {
        body["sealedAt"] = serde_json::json!("2026-08-24T01:02:00Z");
        body["sealedDeadlineAt"] = serde_json::json!("2026-08-25T01:02:00Z");
    }
    body
}

pub(super) fn create_scalar_input_set_response(
    bytes: &[u8],
    kind: &str,
    replayed: bool,
) -> Vec<u8> {
    http_response_with_headers(
        "201 Created",
        Some("application/json"),
        &[
            ("Idempotency-Key", ECHO_IDEMPOTENCY_KEY),
            (
                "Location",
                "/v1/organizations/acme-research/run-input-sets/ris_01k0z6r1w8f4jy2m7q9v3x5abc",
            ),
        ],
        &serde_json::to_vec(&scalar_input_set_body(bytes, kind, "open", false, replayed)).unwrap(),
    )
}

pub(super) fn create_input_set_response(bytes: &[u8], replayed: bool) -> Vec<u8> {
    create_scalar_input_set_response(bytes, "text", replayed)
}

pub(super) fn scalar_upload_capability_response(
    bytes: &[u8],
    media_type: &str,
    url: &str,
) -> Vec<u8> {
    http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[("Cache-Control", "private, no-store")],
        &serde_json::to_vec(&serde_json::json!({
            "inputSetId": INPUT_SET_ID,
            "capabilityExpiresAt": "2998-08-24T01:05:00Z",
            "members": [{
                "memberId": "inputs/request",
                "url": url,
                "requiredHeaders": {
                    "contentLength": bytes.len().to_string(),
                    "contentType": media_type,
                    "ifNoneMatch": "*",
                    "xAmzChecksumSha256": base64::engine::general_purpose::STANDARD.encode(sha256(bytes))
                }
            }]
        }))
        .unwrap(),
    )
}

pub(super) fn upload_capability_response(bytes: &[u8], url: &str) -> Vec<u8> {
    scalar_upload_capability_response(bytes, "text/plain; charset=utf-8", url)
}

pub(super) fn seal_scalar_input_set_response(bytes: &[u8], kind: &str, replayed: bool) -> Vec<u8> {
    http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[("Idempotency-Key", ECHO_IDEMPOTENCY_KEY)],
        &serde_json::to_vec(&scalar_input_set_body(
            bytes, kind, "sealed", true, replayed,
        ))
        .unwrap(),
    )
}

pub(super) fn seal_input_set_response(bytes: &[u8], replayed: bool) -> Vec<u8> {
    seal_scalar_input_set_response(bytes, "text", replayed)
}

pub(super) fn request_body(request: &str) -> serde_json::Value {
    serde_json::from_str(request.split_once("\r\n\r\n").unwrap().1).unwrap()
}

pub(super) fn assert_invalid_response(output: &Output) {
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    if result.get("operation").is_none() {
        assert_eq!(output.status.code(), Some(1));
        assert_eq!(result["outcome"], "invalid_response");
    } else if result["operation"] == "create" {
        assert_eq!(output.status.code(), Some(4));
        assert_eq!(result["outcome"], "acceptance_unknown");
        assert_eq!(result["error"]["code"], "acceptance_unknown");
        assert!(
            result["error"]["idempotencyKey"]
                .as_str()
                .is_some_and(|key| !key.is_empty())
        );
    } else {
        assert_eq!(output.status.code(), Some(1));
        assert_eq!(result["outcome"], "error");
        assert_eq!(result["error"]["code"], "protocol_error");
    }
}

fn run_show_with_response(response: Vec<u8>) -> (Output, ScriptedServer) {
    let (server, _directory, credential_path) = prepared_run(vec![response]);
    let environment = deployment_environment(&server.api_url, &credential_path);
    let output = run_with_env(
        &[
            "run",
            "show",
            ORGANIZATION,
            RUN_ID,
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    (output, server)
}

// REQ-RUN-94
#[test]
fn run_create_is_no_longer_a_command() {
    let (server, _directory, credential_path) = prepared_run(Vec::new());
    let output = run_with_env(
        &[
            "run",
            "create",
            ORGANIZATION,
            "--project-id",
            PROJECT_ID,
            "--workflow-path",
            WORKFLOW_PATH,
            "--json",
            "--allow-insecure-http",
        ],
        &deployment_environment(&server.api_url, &credential_path),
    );
    assert_eq!(output.status.code(), Some(2), "{output:?}");
    assert!(output.stdout.is_empty(), "{output:?}");
    assert!(server.finish().is_empty());
}

// REQ-CANCEL-47, REQ-CANCEL-54
#[test]
fn cloud_run_wait_flags_require_explicit_observation_and_bare_wait_is_absent() {
    for args in [
        vec![
            "workflow",
            "run",
            "create",
            DEFINITION_ID,
            "--cloud",
            ORGANIZATION,
            "--project",
            PROJECT_ID,
            "--timeout",
            "1s",
        ],
        vec!["run", "show", ORGANIZATION, RUN_ID, "--timeout", "1s"],
        vec!["run", "cancel", ORGANIZATION, RUN_ID, "--timeout", "1s"],
        vec!["run", "wait", ORGANIZATION, RUN_ID],
    ] {
        let output = run(&args);
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
    }
}

// REQ-DIAG-28, REQ-DIAG-30
// REQ-PUBLISH-62, REQ-PUBLISH-63
#[test]
fn show_wait_separates_failed_publication_handoff_from_execution() {
    for (json, wait) in [(true, true), (false, true), (false, false)] {
        let mut body = run_body_with_state("succeeded");
        body["publication"] = serde_json::json!({
            "exportName":"review", "state":"failed", "publicationId":null,
            "failure": {"phase":"preflight", "code":"actor_authority_lost", "retryable":false,
                "diagnostic": {"stage":"acceptance", "deliveryPhase":"artifact_upload",
                    "deliveryCode":"future_delivery_code", "errorType":"future_handoff_error"}}
        });
        let (server, _directory, credential_path) = prepared_run(vec![run_response(body.clone())]);
        let environment = deployment_environment(&server.api_url, &credential_path);
        let mut arguments = vec!["run", "show", ORGANIZATION, RUN_ID];
        if json {
            arguments.push("--json");
        }
        if wait {
            arguments.push("--wait");
        }
        arguments.push("--allow-insecure-http");
        let output = run_with_env(&arguments, &environment);
        assert_eq!(output.status.code(), Some(0));
        if json {
            let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(result["outcome"], "settled");
            assert_eq!(result["run"]["state"], "succeeded");
            assert_eq!(result["run"]["publication"]["state"], "failed");
            assert_eq!(result["run"]["publication"], body["publication"]);
            assert!(result["publication"].is_null());
            assert!(result["error"].is_null());
        } else {
            let report = String::from_utf8(output.stdout).unwrap();
            assert!(report.starts_with('✗'));
            for line in [
                "state: succeeded",
                "automatic publication handoff:",
                "  state: failed",
                "  failure: actor_authority_lost",
                "  phase: preflight",
            ] {
                assert!(
                    report.lines().any(|actual| actual == line),
                    "missing {line} in {report}"
                );
            }
            let facts: Vec<_> = report
                .lines()
                .filter(|line| line.trim_start().starts_with("diagnostic "))
                .collect();
            let diagnostic = body["publication"]["failure"]["diagnostic"]
                .as_object()
                .unwrap();
            assert_eq!(facts.len(), diagnostic.len());
            for value in diagnostic.values() {
                let fact = facts
                    .iter()
                    .find(|line| line.contains(value.as_str().unwrap()))
                    .unwrap_or_else(|| panic!("missing {value} in {report}"));
                assert!(report.find("actor_authority_lost").unwrap() < report.find(*fact).unwrap());
            }
            if wait {
                assert!(report.contains("um publication create"));
            }
        }
        assert_eq!(server.finish().len(), 1);
    }
}

// REQ-DIAG-28
#[test]
fn human_run_observation_reports_failed_automatic_publication() {
    let publication_id = "pub_01k0z6r1w8f4jy2m7q9v3x5abc";
    let mut run = run_body_with_state("succeeded");
    run["publication"] = serde_json::json!({
        "exportName": "review", "state": "started", "publicationId": publication_id,
        "failure": null
    });
    let publication = serde_json::json!({
        "id": publication_id, "organizationId": ORGANIZATION_ID,
        "projectId": PROJECT_ID, "runId": RUN_ID,
        "artifactSetId": "ats_01k0z6r1w8f4jy2m7q9v3x5abc",
        "exportName": "review", "state": "failed", "version": 2,
        "providerKind": "github",
        "artifact": {
            "artifactVersion": 1, "objectFormat": "sha1",
            "baseOid": "0123456789abcdef0123456789abcdef01234567",
            "headOid": "89abcdef0123456789abcdef0123456789abcdef",
            "treeOid": "fedcba9876543210fedcba9876543210fedcba98",
            "expiresAt": "2026-10-03T18:00:00Z"
        },
        "target": {
            "repositoryConnectionId": REPOSITORY_CONNECTION_ID,
            "providerRepositoryId": "123456", "fullName": "example/repository",
            "kind": "new_change_request", "baseBranch": "main", "branchVersion": 2, "destinationBranch": format!("um/{RUN_ID}/review")
        },
        "pullRequestMetadata": { "title": "Review", "body": "Run publication", "titleSource": "default", "descriptionSource": "default" },
        "branch": null, "pullRequest": null, "outcome": null,
        "failure": { "phase": "branch", "code": "provider_unavailable", "retryable": true,
            "diagnostic": {"stage":"push", "gitResult":"future_git_result", "gitCommand":"push",
                "gitExitCode":17, "httpStatus":429, "transportError":"future_transport_error",
                "providerRequestId":"request-abc123", "retryAfter":"2026-09-03T19:00:00Z",
                "budgetResource":"future_budget_resource", "errorType":"future_error_type"} },
        "actorPrincipalId": "prn_01k0z6r1w8f4jy2m7q9v3x5abc",
        "createdAt": "2026-09-03T18:00:00Z", "updatedAt": "2026-09-03T18:00:02Z",
        "startedAt": "2026-09-03T18:00:01Z", "terminalAt": "2026-09-03T18:00:02Z"
    });
    for wait in [true, false] {
        let (server, _directory, credential_path) = prepared_run(vec![
            run_response(run.clone()),
            http_response_with_headers(
                "200 OK",
                Some("application/json"),
                &[("Cache-Control", "private, no-store")],
                &serde_json::to_vec(&publication).unwrap(),
            ),
        ]);
        let environment = deployment_environment(&server.api_url, &credential_path);
        let mut args = vec!["run", "show", ORGANIZATION, RUN_ID, "--allow-insecure-http"];
        if wait {
            args.insert(args.len() - 1, "--wait");
        }
        let output = run_with_env(&args, &environment);
        assert_eq!(output.status.code(), Some(0));
        let report = String::from_utf8(output.stdout).unwrap();
        assert!(report.starts_with('✗'));
        for line in [
            "state: succeeded",
            "automatic publication handoff:",
            "  state: started",
            "automatic publication:",
            "  state: failed",
            "  failure: provider_unavailable",
            "  phase: branch",
            "  retryable: true",
        ] {
            assert!(
                report.lines().any(|actual| actual == line),
                "missing {line} in {report}"
            );
        }
        assert!(report.contains(publication_id));
        let facts: Vec<_> = report
            .lines()
            .filter(|line| line.trim_start().starts_with("diagnostic "))
            .collect();
        let diagnostic = publication["failure"]["diagnostic"].as_object().unwrap();
        assert_eq!(facts.len(), diagnostic.len());
        for value in diagnostic.values() {
            let text = value
                .as_str()
                .map_or_else(|| value.to_string(), str::to_owned);
            let fact = facts
                .iter()
                .find(|line| line.contains(&text))
                .unwrap_or_else(|| panic!("missing {text} in {report}"));
            assert!(report.find("provider_unavailable").unwrap() < report.find(*fact).unwrap());
        }
        assert!(report.contains("um publication show"));
        assert_eq!(server.finish().len(), 2);
    }
}

// REQ-PUBLISH-63
#[test]
fn human_run_observation_reports_successful_automatic_publication_url() {
    let publication_id = "pub_01k0z6r1w8f4jy2m7q9v3x5abc";
    let mut run = run_body_with_state("succeeded");
    run["publication"] = serde_json::json!({
        "exportName": "review", "state": "started", "publicationId": publication_id,
        "failure": null
    });
    let mut publication = serde_json::json!({
        "id": publication_id, "organizationId": ORGANIZATION_ID,
        "projectId": PROJECT_ID, "runId": RUN_ID,
        "artifactSetId": "ats_01k0z6r1w8f4jy2m7q9v3x5abc",
        "exportName": "review", "state": "succeeded", "version": 2,
        "providerKind": "github",
        "artifact": {
            "artifactVersion": 1, "objectFormat": "sha1",
            "baseOid": "0123456789abcdef0123456789abcdef01234567",
            "headOid": "89abcdef0123456789abcdef0123456789abcdef",
            "treeOid": "fedcba9876543210fedcba9876543210fedcba98",
            "expiresAt": "2026-10-03T18:00:00Z"
        },
        "target": {
            "repositoryConnectionId": REPOSITORY_CONNECTION_ID,
            "providerRepositoryId": "123456", "fullName": "example/repository",
            "kind": "new_change_request", "baseBranch": "main", "branchVersion": 2, "destinationBranch": format!("um/{RUN_ID}/review")
        },
        "pullRequestMetadata": { "title": "Review", "body": "Run publication", "titleSource": "default", "descriptionSource": "default" },
        "branch": {
            "headOid": "89abcdef0123456789abcdef0123456789abcdef",
            "disposition": "created", "url": "https://example.test/review/branch"
        },
        "pullRequest": null, "outcome": "pull_request_published",
        "failure": null,
        "actorPrincipalId": "prn_01k0z6r1w8f4jy2m7q9v3x5abc",
        "createdAt": "2026-09-03T18:00:00Z", "updatedAt": "2026-09-03T18:00:02Z",
        "startedAt": "2026-09-03T18:00:01Z", "terminalAt": "2026-09-03T18:00:02Z"
    });
    publication["pullRequest"] = serde_json::json!({
        "providerId": "123456", "number": 42,
        "url": "https://actor:secret@example.test/review/42?token=private#fragment",
        "disposition": "created", "state": "open"
    });
    for wait in [true, false] {
        let (server, _directory, credential_path) = prepared_run(vec![
            run_response(run.clone()),
            http_response_with_headers(
                "200 OK",
                Some("application/json"),
                &[("Cache-Control", "private, no-store")],
                &serde_json::to_vec(&publication).unwrap(),
            ),
        ]);
        let mut args = vec!["run", "show", ORGANIZATION, RUN_ID, "--allow-insecure-http"];
        if wait {
            args.insert(args.len() - 1, "--wait");
        }
        let output = run_with_env(
            &args,
            &deployment_environment(&server.api_url, &credential_path),
        );
        assert!(output.status.success());
        let report = String::from_utf8(output.stdout).unwrap();
        assert!(report.starts_with('✓'));
        assert!(report.contains("automatic publication:"));
        assert!(report.contains("  state: succeeded"));
        assert!(report.contains("pull request url: https://example.test/review/42"));
        assert!(!report.contains("secret"));
        assert!(!report.contains("token=private"));
        assert_eq!(server.finish().len(), 2);
    }
}

// REQ-PUBLISH-62
#[test]
fn show_wait_tracks_automatic_attempt_through_handoff_and_publication() {
    let mut pending = run_body_with_state("succeeded");
    pending["publication"] = automatic_handoff("pending");
    let mut started = pending.clone();
    started["publication"] = automatic_handoff("started");
    let mut queued = automatic_publication("no_changes");
    queued["state"] = serde_json::json!("queued");
    queued["outcome"] = serde_json::Value::Null;
    queued["terminalAt"] = serde_json::Value::Null;
    queued["startedAt"] = serde_json::Value::Null;
    for outcome in ["no_changes", "pull_request_already_merged"] {
        let (server, _directory, credential_path) = prepared_run(vec![
            run_response(pending.clone()),
            run_response(started.clone()),
            publication_response(queued.clone()),
            run_response(started.clone()),
            publication_response(automatic_publication(outcome)),
        ]);
        let output = run_with_env(
            &[
                "run",
                "show",
                ORGANIZATION,
                RUN_ID,
                "--json",
                "--wait",
                "--timeout",
                "2m",
                "--allow-insecure-http",
            ],
            &deployment_environment(&server.api_url, &credential_path),
        );
        assert_eq!(
            output.status.code(),
            Some(0),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(result["outcome"], "settled");
        assert_eq!(result["run"]["state"], "succeeded");
        assert_eq!(
            result["run"]["publication"]["publicationId"],
            result["publication"]["id"]
        );
        assert_eq!(result["publication"]["outcome"], outcome);
        assert!(result["replayed"].is_null());
        assert!(result["error"].is_null());
        assert!(!output.stderr.is_empty());
        let requests = server.finish();
        assert_eq!(requests.len(), 5);
        for request in &requests {
            assert!(
                request.starts_with("GET "),
                "unexpected mutation while waiting: {request}"
            );
            if request.contains("/publications/") {
                assert!(request.contains("/publications/pub_01k0z6r1w8f4jy2m7q9v3x5abc "));
            }
        }
    }
}

// REQ-PUBLISH-22, REQ-PUBLISH-62
#[test]
fn show_wait_keeps_successful_execution_separate_from_failed_publication() {
    let mut run = run_body_with_state("succeeded");
    run["publication"] = automatic_handoff("started");
    let mut failed = automatic_publication("no_changes");
    failed["state"] = serde_json::json!("failed");
    failed["outcome"] = serde_json::Value::Null;
    failed["failure"] = serde_json::json!({
        "phase": "branch", "code": "provider_unavailable", "retryable": true
    });
    let (server, _directory, credential_path) =
        prepared_run(vec![run_response(run), publication_response(failed)]);
    let output = run_with_env(
        &[
            "run",
            "show",
            ORGANIZATION,
            RUN_ID,
            "--json",
            "--wait",
            "--allow-insecure-http",
        ],
        &deployment_environment(&server.api_url, &credential_path),
    );
    assert_eq!(output.status.code(), Some(0));
    let document: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(document["outcome"], "settled");
    assert_eq!(document["run"]["state"], "succeeded");
    assert_eq!(document["run"]["publication"]["state"], "started");
    assert_eq!(document["publication"]["state"], "failed");
    assert_eq!(
        document["publication"]["failure"]["code"],
        "provider_unavailable"
    );
    assert_eq!(document["error"], serde_json::Value::Null);
    assert_eq!(server.finish().len(), 2);
}

// REQ-PUBLISH-91, REQ-PUBLISH-62: run waits preserve a terminal existing-target
// path-policy refusal independently of the successful run.
#[test]
fn run_wait_decodes_existing_target_workflow_policy_failure() {
    let mut run = run_body_with_state("succeeded");
    run["publication"] = automatic_handoff("started");
    let mut failed = automatic_publication("no_changes");
    failed["state"] = serde_json::json!("failed");
    failed["outcome"] = serde_json::Value::Null;
    failed["target"]["kind"] = serde_json::json!("existing_change_request");
    failed["target"]["number"] = serde_json::json!(42);
    failed["target"]["providerChangeRequestId"] = serde_json::json!("420");
    failed["target"]["destinationBranch"] = serde_json::json!("human/feature");
    failed["target"]["sourceRef"] = serde_json::json!("human/feature");
    failed["target"]["expectedHeadOid"] =
        serde_json::json!("0123456789abcdef0123456789abcdef01234567");
    failed["failure"] = serde_json::json!({
        "phase": "branch", "code": "workflow_file_change_unsupported",
        "retryable": false, "diagnostic": {"stage": "path_policy"}
    });
    let (server, _directory, credential_path) = prepared_run(vec![
        run_response(run),
        publication_response(failed.clone()),
    ]);
    let output = run_with_env(
        &[
            "run",
            "show",
            ORGANIZATION,
            RUN_ID,
            "--wait",
            "--json",
            "--allow-insecure-http",
        ],
        &deployment_environment(&server.api_url, &credential_path),
    );
    assert_eq!(
        output.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let document: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(document["run"]["state"], "succeeded");
    assert_eq!(document["publication"]["failure"], failed["failure"]);
    server.finish();
}

// REQ-PUBLISH-76: the run wait uses the same tagged failure decoder as publication show.
#[test]
fn run_wait_decodes_gitlab_merge_request_failure_phase() {
    let mut run = run_body_with_state("succeeded");
    run["publication"] = automatic_handoff("started");
    let mut failed = automatic_publication("no_changes");
    failed["providerKind"] = serde_json::json!("gitlab");
    failed["target"]["fullName"] = serde_json::json!("group/subgroup/project");
    failed["target"]["gitlabConnectionId"] = serde_json::json!("glc_01k0z6r1w8f4jy2m7q9v3x5abc");
    failed["target"]["instanceUrl"] = serde_json::json!("https://gitlab.com");
    failed["providerActor"] = serde_json::Value::Null;
    failed["providerAuthorityGeneration"] = serde_json::Value::Null;
    let metadata = failed
        .as_object_mut()
        .unwrap()
        .remove("pullRequestMetadata")
        .unwrap();
    failed["mergeRequestMetadata"] = metadata;
    failed["mergeRequestMetadata"]["title"] = serde_json::json!("Draft: Review");
    failed.as_object_mut().unwrap().remove("pullRequest");
    failed["mergeRequest"] = serde_json::Value::Null;
    failed["state"] = serde_json::json!("failed");
    failed["outcome"] = serde_json::Value::Null;
    failed["failure"] = serde_json::json!({"phase": "merge_request", "code": "provider_unavailable", "retryable": true});
    for (phase, valid) in [("merge_request", true), ("pull_request", false)] {
        failed["failure"]["phase"] = serde_json::json!(phase);
        let (server, _directory, credential_path) = prepared_run(vec![
            run_response(run.clone()),
            publication_response(failed.clone()),
        ]);
        let output = run_with_env(
            &[
                "run",
                "show",
                ORGANIZATION,
                RUN_ID,
                "--wait",
                "--json",
                "--allow-insecure-http",
            ],
            &deployment_environment(&server.api_url, &credential_path),
        );
        if valid {
            let document: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(output.status.code(), Some(0));
            assert_eq!(document["publication"]["failure"]["phase"], "merge_request");
        } else {
            assert_invalid_response(&output);
        }
        server.finish();
    }
}

#[test]
fn run_plain_publication_outcomes_do_not_invent_a_new_pull_request() {
    for (outcome, pull_request) in [("no_changes", false), ("pull_request_already_merged", true)] {
        let mut run = run_body_with_state("succeeded");
        run["publication"] = automatic_handoff("started");
        let (server, _directory, credential_path) = prepared_run(vec![
            run_response(run),
            publication_response(automatic_publication(outcome)),
        ]);
        let output = run_with_env(
            &[
                "run",
                "show",
                ORGANIZATION,
                RUN_ID,
                "--wait",
                "--allow-insecure-http",
            ],
            &deployment_environment(&server.api_url, &credential_path),
        );
        assert_eq!(output.status.code(), Some(0));
        let report = String::from_utf8(output.stdout).unwrap();
        assert!(report.contains(&format!("  outcome: {outcome}")));
        assert_eq!(
            report.contains("  pull request: 42 (reused, merged)"),
            pull_request
        );
        assert_eq!(report.contains("  pull request url: "), pull_request);
        assert!(!report.contains("(created, open)"));
        assert_eq!(server.finish().len(), 2);
    }
}

// REQ-PUBLISH-62
#[cfg(target_os = "linux")]
#[test]
fn publication_observation_signal_stops_show_wait_and_retains_handoff() {
    let mut run = run_body_with_state("succeeded");
    run["publication"] = automatic_handoff("started");
    let mut server = ScriptedServer::respond_with_paused_last_response(vec![
        run_response(run),
        publication_response(automatic_publication("no_changes")),
    ]);
    let directory = private_credential_directory();
    let path = directory.path().join("credentials.json");
    write_credential_fixture(&path, &server.api_url, TOKEN, "2999-01-01T00:00:00Z");
    let environment = deployment_environment(&server.api_url, path.to_str().unwrap());
    let mut command = Command::new(env!("CARGO_BIN_EXE_um"));
    command
        .args([
            "run",
            "show",
            ORGANIZATION,
            RUN_ID,
            "--json",
            "--wait",
            "--allow-insecure-http",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env_remove(CREDENTIALS_FILE_VARIABLE);
    for variable in DEPLOYMENT_VARIABLES {
        command.env_remove(variable);
    }
    for (name, value) in environment {
        command.env(name, value);
    }
    let child = command.spawn().unwrap();
    let run_read = server.wait_for_request();
    assert!(run_read.starts_with("GET "));
    let publication_read = server.wait_for_request();
    assert!(publication_read.starts_with("GET "));
    assert!(publication_read.contains("/publications/pub_01k0z6r1w8f4jy2m7q9v3x5abc "));
    rustix::process::kill_process(
        rustix::process::Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap(),
        rustix::process::Signal::TERM,
    )
    .unwrap();
    let output = child.wait_with_output().unwrap();
    server.release_paused_response();
    assert_eq!(output.status.code(), Some(143));
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "observation_stopped");
    assert_eq!(result["runId"], RUN_ID);
    assert_eq!(result["run"]["publication"]["state"], "started");
    assert!(result["publication"].is_null());
    assert_eq!(result["error"]["code"], "observation_stopped");
    assert_eq!(server.finish().len(), 0);
}

fn retained_inputs_response(bytes: &[u8]) -> Vec<u8> {
    http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[("Cache-Control", "private, no-store")],
        &serde_json::to_vec(&serde_json::json!({
            "inputSetId": INPUT_SET_ID,
            "manifest": {
                "schemaVersion": 1,
                "inputs": {
                    "request": {
                        "kind": "text",
                        "sizeBytes": bytes.len(),
                        "sha256": hex_digest(bytes)
                    }
                }
            },
            "manifestDigest": {
                "algorithm": "sha256",
                "value": scalar_manifest_digest(bytes, "text")
            },
            "sealedAt": "2026-08-24T01:02:00Z",
            "contentExpiresAt": null,
            "inputCount": 1,
            "attachmentCount": 0,
            "aggregateSizeBytes": bytes.len(),
            "availability": "available"
        }))
        .unwrap(),
    )
}

fn download_capability_response(bytes: &[u8], url: &str) -> Vec<u8> {
    http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[("Cache-Control", "private, no-store")],
        &serde_json::to_vec(&serde_json::json!({
            "inputSetId": INPUT_SET_ID,
            "capabilityExpiresAt": "2998-08-24T01:05:00Z",
            "members": [{
                "memberId": "inputs/request",
                "attachmentIndex": null,
                "displayName": null,
                "mediaType": "text/plain; charset=utf-8",
                "sizeBytes": bytes.len(),
                "sha256": hex_digest(bytes),
                "url": url
            }]
        }))
        .unwrap(),
    )
}

fn retained_inputs_response_for_manifest(
    manifest: &serde_json::Value,
    input_count: usize,
    attachment_count: usize,
    aggregate_size_bytes: usize,
) -> Vec<u8> {
    http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[("Cache-Control", "private, no-store")],
        &serde_json::to_vec(&serde_json::json!({
            "inputSetId": INPUT_SET_ID,
            "manifest": manifest,
            "manifestDigest": {
                "algorithm": "sha256",
                "value": hex_digest(&serde_json::to_vec(manifest).unwrap())
            },
            "sealedAt": "2026-08-24T01:02:00Z",
            "contentExpiresAt": null,
            "inputCount": input_count,
            "attachmentCount": attachment_count,
            "aggregateSizeBytes": aggregate_size_bytes,
            "availability": "available"
        }))
        .unwrap(),
    )
}

fn download_capability_response_for_members(members: serde_json::Value) -> Vec<u8> {
    http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[("Cache-Control", "private, no-store")],
        &serde_json::to_vec(&serde_json::json!({
            "inputSetId": INPUT_SET_ID,
            "capabilityExpiresAt": "2998-08-24T01:05:00Z",
            "members": members
        }))
        .unwrap(),
    )
}

// REQ-RUNINPUT-61
#[test]
fn explicit_input_set_flow_creates_open_then_uploads_seals_and_consumes() {
    let input_bytes = b"explicit single-use input\n";
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("request.txt");
    fs::write(&path, input_bytes).unwrap();

    let (create_server, _credentials, credential_path) =
        prepared_run(vec![create_input_set_response(input_bytes, false)]);
    let environment = deployment_environment(&create_server.api_url, &credential_path);
    let create = run_with_env(
        &[
            "run",
            "input-set",
            "create",
            ORGANIZATION,
            "--project-id",
            PROJECT_ID,
            "--input-text-file",
            "request",
            path.to_str().unwrap(),
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(
        create.status.success(),
        "{}",
        String::from_utf8_lossy(&create.stderr)
    );
    let result: serde_json::Value = serde_json::from_slice(&create.stdout).unwrap();
    assert_eq!(result["outcome"], "created");
    assert_eq!(result["inputSet"]["id"], INPUT_SET_ID);
    assert_eq!(result["inputSet"]["state"], "open");
    let requests = create_server.finish();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].contains("/run-input-sets HTTP/1.1"));
    assert!(!requests[0].contains("upload-capabilities"));
    assert!(!requests[0].contains("/seal"));

    let storage = OneShotServer::respond("204 No Content", None, b"");
    let signed_url = format!("{}/object?signature=input-set-upload", storage.api_url);
    let open_response = http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[],
        &serde_json::to_vec(&scalar_input_set_body(
            input_bytes,
            "text",
            "open",
            false,
            false,
        ))
        .unwrap(),
    );
    let (upload_server, _credentials, credential_path) = prepared_run(vec![
        open_response,
        upload_capability_response(input_bytes, &signed_url),
    ]);
    let environment = deployment_environment(&upload_server.api_url, &credential_path);
    let upload = run_with_env(
        &[
            "run",
            "input-set",
            "upload",
            ORGANIZATION,
            INPUT_SET_ID,
            "--member-file",
            "inputs/request",
            path.to_str().unwrap(),
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(upload.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&upload.stdout).unwrap()["outcome"],
        "uploaded"
    );
    let requests = upload_server.finish();
    assert_eq!(requests.len(), 2);
    assert!(requests[1].contains("/upload-capabilities HTTP/1.1"));
    storage.finish();

    let uploaded_response = http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[],
        &serde_json::to_vec(&scalar_input_set_body(
            input_bytes,
            "text",
            "open",
            true,
            false,
        ))
        .unwrap(),
    );
    let (seal_server, _credentials, credential_path) = prepared_run(vec![
        uploaded_response,
        seal_input_set_response(input_bytes, false),
    ]);
    let environment = deployment_environment(&seal_server.api_url, &credential_path);
    let seal = run_with_env(
        &[
            "run",
            "input-set",
            "seal",
            ORGANIZATION,
            INPUT_SET_ID,
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(seal.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&seal.stdout).unwrap()["outcome"],
        "sealed"
    );
    let requests = seal_server.finish();
    assert_eq!(requests.len(), 2);
    assert!(requests[1].contains("/seal HTTP/1.1"));

    let (consume_server, _credentials, credential_path) =
        prepared_run(vec![acceptance_response(false)]);
    let environment = deployment_environment(&consume_server.api_url, &credential_path);
    let mut arguments = create_args(true);
    let insertion = arguments.len() - 1;
    arguments.splice(insertion..insertion, ["--input-set-id", INPUT_SET_ID]);
    let consume = run_with_env(&arguments, &environment);

    assert!(consume.status.success());
    let result: serde_json::Value = serde_json::from_slice(&consume.stdout).unwrap();
    assert_eq!(result["outcome"], "accepted");
    assert_eq!(result["runId"], RUN_ID);
    assert!(result["run"].is_null());
    let requests = consume_server.finish();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].contains("/runs HTTP/1.1"));
    assert_eq!(request_body(&requests[0])["inputSetId"], INPUT_SET_ID);
}

// REQ-RUNINPUT-61, REQ-RUNINPUT-64
#[test]
fn input_set_mutation_commands_upload_seal_and_delete_with_fresh_requests() {
    let input_bytes = b"selected single-use input\n";
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("request.txt");
    fs::write(&path, input_bytes).unwrap();

    let storage = OneShotServer::respond("204 No Content", None, b"");
    let signed_url = format!("{}/object?signature=input-set-upload", storage.api_url);
    let open_response = http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[],
        &serde_json::to_vec(&scalar_input_set_body(
            input_bytes,
            "text",
            "open",
            false,
            false,
        ))
        .unwrap(),
    );
    let (upload_server, _credentials, credential_path) = prepared_run(vec![
        open_response,
        upload_capability_response(input_bytes, &signed_url),
    ]);
    let environment = deployment_environment(&upload_server.api_url, &credential_path);
    let upload = run_with_env(
        &[
            "run",
            "input-set",
            "upload",
            ORGANIZATION,
            INPUT_SET_ID,
            "--member-file",
            "inputs/request",
            path.to_str().unwrap(),
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert!(upload.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&upload.stdout).unwrap()["outcome"],
        "uploaded"
    );
    let requests = upload_server.finish();
    assert_eq!(requests.len(), 2);
    assert!(requests[0].contains(&format!("/run-input-sets/{INPUT_SET_ID} HTTP/1.1")));
    assert!(requests[1].contains(&format!(
        "/run-input-sets/{INPUT_SET_ID}/upload-capabilities HTTP/1.1"
    )));
    storage.finish();
    assert_no_secret_output(&upload, &[TOKEN, "input-set-upload"]);

    let uploaded_response = http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[],
        &serde_json::to_vec(&scalar_input_set_body(
            input_bytes,
            "text",
            "open",
            true,
            false,
        ))
        .unwrap(),
    );
    let (seal_server, _credentials, credential_path) = prepared_run(vec![
        uploaded_response,
        seal_input_set_response(input_bytes, false),
    ]);
    let environment = deployment_environment(&seal_server.api_url, &credential_path);
    let seal = run_with_env(
        &[
            "run",
            "input-set",
            "seal",
            ORGANIZATION,
            INPUT_SET_ID,
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert!(seal.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&seal.stdout).unwrap()["outcome"],
        "sealed"
    );
    let requests = seal_server.finish();
    assert_eq!(requests.len(), 2);
    assert!(requests[1].contains(&format!("/run-input-sets/{INPUT_SET_ID}/seal HTTP/1.1")));
    let seal_key = header_value(&requests[1], "idempotency-key");
    assert_eq!(seal_key.len(), 64);

    let unconfirmed = Command::new(env!("CARGO_BIN_EXE_um"))
        .args(["run", "input-set", "delete", ORGANIZATION, INPUT_SET_ID])
        .output()
        .unwrap();
    assert_eq!(unconfirmed.status.code(), Some(2));

    let response = http_response_with_headers(
        "204 No Content",
        None,
        &[("Idempotency-Key", ECHO_IDEMPOTENCY_KEY)],
        b"",
    );
    let (delete_server, _credentials, credential_path) = prepared_run(vec![response]);
    let environment = deployment_environment(&delete_server.api_url, &credential_path);
    let delete = run_with_env(
        &[
            "run",
            "input-set",
            "delete",
            ORGANIZATION,
            INPUT_SET_ID,
            "--yes",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert!(delete.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&delete.stdout).unwrap()["outcome"],
        "deleted"
    );
    let request = delete_server.finish().remove(0);
    assert!(request.starts_with(&format!(
        "DELETE /api/v1/organizations/{ORGANIZATION}/run-input-sets/{INPUT_SET_ID} HTTP/1.1"
    )));
    let delete_key = header_value(&request, "idempotency-key");
    assert_eq!(delete_key.len(), 64);
    assert_ne!(seal_key, delete_key);
}

// REQ-RUNINPUT-63
#[test]
fn sealing_an_already_sealed_input_set_reports_a_lifecycle_conflict() {
    let input_bytes = b"already sealed";
    let sealed_response = http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[],
        &serde_json::to_vec(&scalar_input_set_body(
            input_bytes,
            "text",
            "sealed",
            true,
            false,
        ))
        .unwrap(),
    );
    let (server, _credentials, credential_path) = prepared_run(vec![sealed_response]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(
        &[
            "run",
            "input-set",
            "seal",
            ORGANIZATION,
            INPUT_SET_ID,
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stderr.is_empty());
    let receipt: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(receipt["outcome"], "conflict");
    assert_eq!(receipt["inputSetId"], INPUT_SET_ID);
    let requests = server.finish();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].contains(&format!("/run-input-sets/{INPUT_SET_ID} HTTP/1.1")));
    assert!(!requests[0].contains("/seal HTTP/1.1"));
}

// REQ-RUNINPUT-62
#[test]
fn input_set_upload_rejects_changed_member_bytes_before_capability_issue() {
    let expected = b"original immutable member";
    let changed = b"changed immutable member";
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("request.txt");
    fs::write(&path, changed).unwrap();
    let open_response = http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[],
        &serde_json::to_vec(&scalar_input_set_body(
            expected, "text", "open", false, false,
        ))
        .unwrap(),
    );
    let (server, _credentials, credential_path) = prepared_run(vec![open_response]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(
        &[
            "run",
            "input-set",
            "upload",
            ORGANIZATION,
            INPUT_SET_ID,
            "--member-file",
            "inputs/request",
            path.to_str().unwrap(),
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["outcome"],
        "invalid_input"
    );
    assert_eq!(server.finish().len(), 1);
    assert_no_secret_output(
        &output,
        &[
            TOKEN,
            std::str::from_utf8(expected).unwrap(),
            std::str::from_utf8(changed).unwrap(),
        ],
    );
}

// REQ-RUNINPUT-61
#[test]
fn input_set_upload_resumes_one_exact_attachment_member() {
    let uploaded_bytes = b"already accepted sibling";
    let selected_bytes = b"missing second attachment";
    let directory = tempfile::tempdir().unwrap();
    let selected_path = directory.path().join("second.bin");
    fs::write(&selected_path, selected_bytes).unwrap();
    let manifest = serde_json::json!({
        "schemaVersion": 1,
        "inputs": {
            "evidence": {
                "kind": "attachments",
                "items": [
                    {
                        "index": 0,
                        "displayName": "first.txt",
                        "mediaType": "text/plain",
                        "sizeBytes": uploaded_bytes.len(),
                        "sha256": hex_digest(uploaded_bytes)
                    },
                    {
                        "index": 1,
                        "displayName": "second.bin",
                        "mediaType": "application/octet-stream",
                        "sizeBytes": selected_bytes.len(),
                        "sha256": hex_digest(selected_bytes)
                    }
                ]
            }
        }
    });
    let set = serde_json::json!({
        "id": INPUT_SET_ID,
        "organizationId": ORGANIZATION_ID,
        "projectId": PROJECT_ID,
        "boundsProfile": 1,
        "manifest": manifest.clone(),
        "manifestDigest": {
            "algorithm": "sha256",
            "value": hex_digest(&serde_json::to_vec(&manifest).unwrap())
        },
        "inputCount": 1,
        "attachmentCount": 2,
        "aggregateSizeBytes": uploaded_bytes.len() + selected_bytes.len(),
        "state": "open",
        "createdAt": "2026-08-24T01:00:00Z",
        "openDeadlineAt": "2999-08-25T01:00:00Z",
        "members": [
            {"memberId": "inputs/evidence/000000", "uploadConfirmed": true},
            {"memberId": "inputs/evidence/000001", "uploadConfirmed": false}
        ],
        "replayed": false
    });
    let storage = OneShotServer::respond("204 No Content", None, b"");
    let signed_url = format!("{}/second?signature=resume", storage.api_url);
    let capabilities = http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[("Cache-Control", "private, no-store")],
        &serde_json::to_vec(&serde_json::json!({
            "inputSetId": INPUT_SET_ID,
            "capabilityExpiresAt": "2998-08-24T01:05:00Z",
            "members": [{
                "memberId": "inputs/evidence/000001",
                "url": signed_url,
                "requiredHeaders": {
                    "contentLength": selected_bytes.len().to_string(),
                    "contentType": "application/octet-stream",
                    "ifNoneMatch": "*",
                    "xAmzChecksumSha256": base64::engine::general_purpose::STANDARD.encode(sha256(selected_bytes))
                }
            }]
        }))
        .unwrap(),
    );
    let open_response = http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[],
        &serde_json::to_vec(&set).unwrap(),
    );
    let (server, _credentials, credential_path) = prepared_run(vec![open_response, capabilities]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(
        &[
            "run",
            "input-set",
            "upload",
            ORGANIZATION,
            INPUT_SET_ID,
            "--member-file",
            "inputs/evidence/000001",
            selected_path.to_str().unwrap(),
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(output.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["outcome"],
        "uploaded"
    );
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        request_body(&requests[1]),
        serde_json::json!({"members": ["inputs/evidence/000001"]})
    );
    assert_eq!(
        storage
            .finish()
            .split_once("\r\n\r\n")
            .unwrap()
            .1
            .as_bytes(),
        selected_bytes
    );
    assert_no_secret_output(
        &output,
        &[
            TOKEN,
            "resume",
            std::str::from_utf8(selected_bytes).unwrap(),
        ],
    );
}

// REQ-RUNINPUT-61
#[test]
fn input_set_and_retained_input_show_commands_emit_structural_json() {
    let input_bytes = b"show input";
    let input_set_response = http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[],
        &serde_json::to_vec(&scalar_input_set_body(
            input_bytes,
            "text",
            "open",
            false,
            false,
        ))
        .unwrap(),
    );
    let (input_set_server, _credentials, credential_path) = prepared_run(vec![input_set_response]);
    let environment = deployment_environment(&input_set_server.api_url, &credential_path);
    let input_set_output = run_with_env(
        &[
            "run",
            "input-set",
            "show",
            ORGANIZATION,
            INPUT_SET_ID,
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert!(input_set_output.status.success());
    let result: serde_json::Value = serde_json::from_slice(&input_set_output.stdout).unwrap();
    assert_eq!(result["outcome"], "found");
    assert_eq!(
        result["inputSet"]["manifest"]["inputs"]["request"]["kind"],
        "text"
    );
    input_set_server.finish();

    let (retained_server, _credentials, credential_path) =
        prepared_run(vec![retained_inputs_response(input_bytes)]);
    let environment = deployment_environment(&retained_server.api_url, &credential_path);
    let retained_output = run_with_env(
        &[
            "run",
            "input",
            "show",
            ORGANIZATION,
            RUN_ID,
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert!(retained_output.status.success());
    let result: serde_json::Value = serde_json::from_slice(&retained_output.stdout).unwrap();
    assert_eq!(result["outcome"], "found");
    assert_eq!(result["inventory"]["inputSetId"], INPUT_SET_ID);
    assert_eq!(
        result["inventory"]["manifest"]["inputs"]["request"]["kind"],
        "text"
    );
    assert_no_secret_output(&retained_output, &[TOKEN, "show input"]);
    retained_server.finish();
}

// REQ-RUNINPUT-65
#[test]
fn retained_input_show_reports_counts_bytes_and_content_expiry() {
    let input_bytes = b"show expiry";
    let unscheduled = retained_inputs_response(input_bytes);
    let unscheduled_body = String::from_utf8(unscheduled.clone())
        .unwrap()
        .split_once("\r\n\r\n")
        .unwrap()
        .1
        .to_owned();
    let scheduled = http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[("Cache-Control", "private, no-store")],
        unscheduled_body
            .replacen(
                r#""contentExpiresAt":null"#,
                r#""contentExpiresAt":"2026-09-23T01:02:00Z""#,
                1,
            )
            .as_bytes(),
    );
    assert_ne!(
        scheduled, unscheduled,
        "fixture should carry contentExpiresAt"
    );
    for (response, expiry) in [
        (scheduled, "2026-09-23T01:02:00Z"),
        (unscheduled, "not scheduled"),
    ] {
        let (server, _credentials, credential_path) = prepared_run(vec![response]);
        let environment = deployment_environment(&server.api_url, &credential_path);
        let output = run_with_env(
            &[
                "run",
                "input",
                "show",
                ORGANIZATION,
                RUN_ID,
                "--allow-insecure-http",
            ],
            &environment,
        );
        assert!(output.status.success());
        let stdout = String::from_utf8(output.stdout).unwrap();
        let facts: Vec<&str> = stdout.lines().collect();
        for fact in [
            format!("input set: {INPUT_SET_ID}"),
            "inputs: 1".to_owned(),
            "attachment members: 0".to_owned(),
            format!("bytes: {}", input_bytes.len()),
            format!("content expires: {expiry}"),
        ] {
            assert!(
                facts.contains(&fact.as_str()),
                "missing {fact:?} in {stdout}"
            );
        }
        server.finish();
    }
}

#[test]
fn run_input_http_decoders_reject_duplicate_members_before_using_success_responses() {
    let input_bytes = b"duplicate response member";
    let response_body = |response: Vec<u8>| {
        String::from_utf8(response)
            .unwrap()
            .split_once("\r\n\r\n")
            .unwrap()
            .1
            .to_owned()
    };
    let duplicate = |body: String, original: &str, replacement: &str| {
        let replaced = body.replacen(original, replacement, 1);
        assert_ne!(
            replaced, body,
            "fixture member should exist exactly as encoded"
        );
        replaced
    };

    let input_set_body = serde_json::to_string(&scalar_input_set_body(
        input_bytes,
        "text",
        "open",
        false,
        false,
    ))
    .unwrap();
    let input_set_body = duplicate(
        input_set_body,
        r#""state":"open""#,
        r#""state":"open","state":"sealed""#,
    );
    let (server, _credentials, credential_path) = prepared_run(vec![http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[],
        input_set_body.as_bytes(),
    )]);
    let environment = deployment_environment(&server.api_url, &credential_path);
    let output = run_with_env(
        &[
            "run",
            "input-set",
            "show",
            ORGANIZATION,
            INPUT_SET_ID,
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert_invalid_response(&output);
    assert_eq!(server.finish().len(), 1);

    let retained_body = duplicate(
        response_body(retained_inputs_response(input_bytes)),
        r#""kind":"text""#,
        r#""kind":"text","\u006bind":"text""#,
    );
    let (server, _credentials, credential_path) = prepared_run(vec![http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[("Cache-Control", "private, no-store")],
        retained_body.as_bytes(),
    )]);
    let environment = deployment_environment(&server.api_url, &credential_path);
    let output = run_with_env(
        &[
            "run",
            "input",
            "show",
            ORGANIZATION,
            RUN_ID,
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert_invalid_response(&output);
    assert_eq!(server.finish().len(), 1);

    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("request.txt");
    fs::write(&source, input_bytes).unwrap();
    let upload_body = duplicate(
        response_body(upload_capability_response(
            input_bytes,
            "https://storage.invalid/object?signature=duplicate-upload",
        )),
        r#""contentType":"text/plain; charset=utf-8""#,
        r#""contentType":"text/plain; charset=utf-8","content\u0054ype":"text/plain; charset=utf-8""#,
    );
    let open_response = http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[],
        &serde_json::to_vec(&scalar_input_set_body(
            input_bytes,
            "text",
            "open",
            false,
            false,
        ))
        .unwrap(),
    );
    let (server, _credentials, credential_path) = prepared_run(vec![
        open_response,
        http_response_with_headers(
            "200 OK",
            Some("application/json"),
            &[("Cache-Control", "private, no-store")],
            upload_body.as_bytes(),
        ),
    ]);
    let environment = deployment_environment(&server.api_url, &credential_path);
    let output = run_with_env(
        &[
            "run",
            "input-set",
            "upload",
            ORGANIZATION,
            INPUT_SET_ID,
            "--member-file",
            "inputs/request",
            source.to_str().unwrap(),
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert_invalid_response(&output);
    assert_eq!(server.finish().len(), 2);

    let download_body = duplicate(
        response_body(download_capability_response(
            input_bytes,
            "https://storage.invalid/object?signature=duplicate-download",
        )),
        r#""displayName":null"#,
        r#""displayName":null,"display\u004eame":null"#,
    );
    let destination = directory.path().join("duplicate-download");
    let (server, _credentials, credential_path) = prepared_run(vec![
        retained_inputs_response(input_bytes),
        http_response_with_headers(
            "200 OK",
            Some("application/json"),
            &[("Cache-Control", "private, no-store")],
            download_body.as_bytes(),
        ),
    ]);
    let environment = deployment_environment(&server.api_url, &credential_path);
    let output = run_with_env(
        &[
            "run",
            "input",
            "download",
            ORGANIZATION,
            RUN_ID,
            "--output",
            destination.to_str().unwrap(),
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert_invalid_response(&output);
    assert!(!destination.exists());
    assert_eq!(server.finish().len(), 2);
    assert_no_secret_output(&output, &[TOKEN, "duplicate-upload", "duplicate-download"]);
}

// REQ-RUNINPUT-66, REQ-RUNINPUT-72, REQ-RUNINPUT-75
#[test]
fn retained_inputs_download_uses_inventory_bound_logical_paths() {
    let input_bytes = b"private retained input bytes\n";
    let storage = OneShotServer::respond("200 OK", Some("text/plain"), input_bytes);
    let signed_url = format!("{}/object?signature=retained-input", storage.api_url);
    let (download_server, _credentials, credential_path) = prepared_run(vec![
        retained_inputs_response(input_bytes),
        download_capability_response(input_bytes, &signed_url),
    ]);
    let environment = deployment_environment(&download_server.api_url, &credential_path);
    let destination_parent = tempfile::tempdir().unwrap();
    let destination = destination_parent.path().join("inputs-download");

    let output = run_with_env(
        &[
            "run",
            "input",
            "download",
            ORGANIZATION,
            RUN_ID,
            "--output",
            destination.to_str().unwrap(),
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        fs::read(destination.join("inputs/request")).unwrap(),
        input_bytes
    );
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "downloaded");
    assert_eq!(result["inputSetId"], INPUT_SET_ID);
    assert_eq!(result["memberCount"], 1);
    let requests = download_server.finish();
    assert_eq!(requests.len(), 2);
    assert!(requests[0].contains(&format!("/runs/{RUN_ID}/inputs HTTP/1.1")));
    assert_eq!(
        request_body(&requests[1]),
        serde_json::json!({"members": ["inputs/request"]})
    );
    let storage_request = storage.finish();
    assert!(storage_request.starts_with("GET /api/object?signature="));
    assert!(!storage_request.contains("authorization:"));
    assert_no_secret_output(
        &output,
        &[
            TOKEN,
            "retained-input",
            std::str::from_utf8(input_bytes).unwrap(),
        ],
    );
}

// REQ-RUNINPUT-66
#[test]
fn retained_inputs_download_selects_exact_members_only() {
    let first = b"unselected private bytes";
    let second = b"selected private bytes";
    let manifest = serde_json::json!({
        "schemaVersion": 1,
        "inputs": {
            "first": {
                "kind": "text",
                "sizeBytes": first.len(),
                "sha256": hex_digest(first)
            },
            "second": {
                "kind": "text",
                "sizeBytes": second.len(),
                "sha256": hex_digest(second)
            }
        }
    });
    let storage = OneShotServer::respond("200 OK", Some("text/plain"), second);
    let signed_url = format!(
        "{}/second?signature=unique-selected-capability-sentinel",
        storage.api_url
    );
    let capabilities = download_capability_response_for_members(serde_json::json!([{
        "memberId": "inputs/second",
        "attachmentIndex": null,
        "displayName": null,
        "mediaType": "text/plain; charset=utf-8",
        "sizeBytes": second.len(),
        "sha256": hex_digest(second),
        "url": signed_url
    }]));
    let (server, _credentials, credential_path) = prepared_run(vec![
        retained_inputs_response_for_manifest(&manifest, 2, 0, first.len() + second.len()),
        capabilities,
    ]);
    let environment = deployment_environment(&server.api_url, &credential_path);
    let destination_parent = tempfile::tempdir().unwrap();
    let destination = destination_parent.path().join("selected-inputs");

    let output = run_with_env(
        &[
            "run",
            "input",
            "download",
            ORGANIZATION,
            RUN_ID,
            "--output",
            destination.to_str().unwrap(),
            "--member",
            "inputs/second",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(fs::read(destination.join("inputs/second")).unwrap(), second);
    assert!(!destination.join("inputs/first").exists());
    let receipt: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(receipt["memberCount"], 1);
    assert_eq!(receipt["totalSizeBytes"], second.len());
    let requests = server.finish();
    assert_eq!(
        request_body(&requests[1]),
        serde_json::json!({"members": ["inputs/second"]})
    );
    storage.finish();
    assert_no_secret_output(
        &output,
        &[
            TOKEN,
            "unique-selected-capability-sentinel",
            std::str::from_utf8(first).unwrap(),
            std::str::from_utf8(second).unwrap(),
        ],
    );
}

#[test]
fn retained_inputs_download_accepts_a_relative_destination() {
    let input_bytes = b"relative destination bytes";
    let storage = OneShotServer::respond("200 OK", Some("text/plain"), input_bytes);
    let signed_url = format!("{}/object?signature=relative", storage.api_url);
    let (server, _credentials, credential_path) = prepared_run(vec![
        retained_inputs_response(input_bytes),
        download_capability_response(input_bytes, &signed_url),
    ]);
    let environment = deployment_environment(&server.api_url, &credential_path);
    let current_directory = tempfile::tempdir().unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_um"));
    command
        .args([
            "run",
            "input",
            "download",
            ORGANIZATION,
            RUN_ID,
            "--output",
            "retained-inputs",
            "--json",
            "--allow-insecure-http",
        ])
        .current_dir(current_directory.path())
        .env_remove(CREDENTIALS_FILE_VARIABLE);
    for variable in DEPLOYMENT_VARIABLES {
        command.env_remove(variable);
    }
    for (name, value) in environment {
        command.env(name, value);
    }

    let output = command.output().unwrap();

    assert!(output.status.success());
    assert_eq!(
        fs::read(
            current_directory
                .path()
                .join("retained-inputs/inputs/request")
        )
        .unwrap(),
        input_bytes
    );
    server.finish();
    storage.finish();
}

#[cfg(target_os = "linux")]
#[test]
fn retained_inputs_json_download_rejects_an_unrepresentable_destination_before_transfer() {
    let (server, _credentials, credential_path) = prepared_run(Vec::new());
    let environment = deployment_environment(&server.api_url, &credential_path);
    let destination_parent = tempfile::tempdir().unwrap();
    let destination = destination_parent
        .path()
        .join(std::ffi::OsString::from_vec(b"download-\xff".to_vec()));
    let mut command = Command::new(env!("CARGO_BIN_EXE_um"));
    command
        .args(["run", "input", "download", ORGANIZATION, RUN_ID, "--output"])
        .arg(&destination)
        .args(["--json", "--allow-insecure-http"])
        .env_remove(CREDENTIALS_FILE_VARIABLE);
    for variable in DEPLOYMENT_VARIABLES {
        command.env_remove(variable);
    }
    for (name, value) in environment {
        command.env(name, value);
    }

    let output = command.output().unwrap();

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert!(!destination.exists());
    assert!(server.finish().is_empty());
}

// REQ-RUNINPUT-66, REQ-RUNINPUT-75
#[test]
fn retained_input_integrity_failure_leaves_no_destination() {
    let expected = b"expected retained bytes";
    let storage = OneShotServer::respond("200 OK", Some("text/plain"), b"different retained bytes");
    let signed_url = format!("{}/object?signature=integrity-mismatch", storage.api_url);
    let (server, _credentials, credential_path) = prepared_run(vec![
        retained_inputs_response(expected),
        download_capability_response(expected, &signed_url),
    ]);
    let environment = deployment_environment(&server.api_url, &credential_path);
    let destination_parent = tempfile::tempdir().unwrap();
    let destination = destination_parent.path().join("must-not-exist");

    let output = run_with_env(
        &[
            "run",
            "input",
            "download",
            ORGANIZATION,
            RUN_ID,
            "--output",
            destination.to_str().unwrap(),
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert_eq!(output.status.code(), Some(1));
    assert!(!destination.exists());
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "integrity_mismatch");
    assert_no_secret_output(&output, &[TOKEN, "integrity-mismatch"]);
    server.finish();
    storage.finish();
}

// REQ-RUNINPUT-66
#[cfg(target_os = "linux")]
#[test]
fn interrupted_retained_input_download_removes_verified_private_staging() {
    let first = b"already downloaded private input\n";
    let second = b"paused private input\n";
    let manifest = serde_json::json!({
        "schemaVersion": 1,
        "inputs": {
            "first": {
                "kind": "text",
                "sizeBytes": first.len(),
                "sha256": hex_digest(first)
            },
            "second": {
                "kind": "text",
                "sizeBytes": second.len(),
                "sha256": hex_digest(second)
            }
        }
    });
    let first_storage = OneShotServer::respond("200 OK", Some("text/plain"), first);
    let mut second_storage =
        ScriptedServer::respond_with_paused_first_response(vec![http_response_with_headers(
            "200 OK",
            Some("text/plain"),
            &[],
            second,
        )]);
    let first_url = format!("{}/first?signature=first-private", first_storage.api_url);
    let second_url = format!(
        "{}/second?signature=interrupted-retained-input",
        second_storage.api_url
    );
    let capabilities = download_capability_response_for_members(serde_json::json!([
        {
            "memberId": "inputs/first",
            "attachmentIndex": null,
            "displayName": null,
            "mediaType": "text/plain; charset=utf-8",
            "sizeBytes": first.len(),
            "sha256": hex_digest(first),
            "url": first_url
        },
        {
            "memberId": "inputs/second",
            "attachmentIndex": null,
            "displayName": null,
            "mediaType": "text/plain; charset=utf-8",
            "sizeBytes": second.len(),
            "sha256": hex_digest(second),
            "url": second_url
        }
    ]));
    let (server, _credentials, credential_path) = prepared_run(vec![
        retained_inputs_response_for_manifest(&manifest, 2, 0, first.len() + second.len()),
        capabilities,
    ]);
    let environment = deployment_environment(&server.api_url, &credential_path);
    let destination_parent = tempfile::tempdir().unwrap();
    let destination = destination_parent.path().join("must-not-be-committed");
    let mut command = Command::new(env!("CARGO_BIN_EXE_um"));
    command
        .args([
            "run",
            "input",
            "download",
            ORGANIZATION,
            RUN_ID,
            "--output",
            destination.to_str().unwrap(),
            "--json",
            "--allow-insecure-http",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env_remove(CREDENTIALS_FILE_VARIABLE);
    for variable in DEPLOYMENT_VARIABLES {
        command.env_remove(variable);
    }
    for (name, value) in environment {
        command.env(name, value);
    }
    let child = command.spawn().unwrap();
    let request = second_storage.next_request();
    assert!(request.starts_with("GET /api/second?signature="));

    rustix::process::kill_process(
        rustix::process::Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap(),
        rustix::process::Signal::INT,
    )
    .unwrap();
    let output = child.wait_with_output().unwrap();
    second_storage.release_paused_response();

    assert_eq!(output.status.code(), Some(130));
    assert!(output.stderr.is_empty());
    assert!(!destination.exists());
    assert!(
        fs::read_dir(destination_parent.path())
            .unwrap()
            .all(|entry| !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".scherzo-input-download-"))
    );
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "interrupted");
    assert_eq!(result["runId"], RUN_ID);
    assert_no_secret_output(
        &output,
        &[
            TOKEN,
            "first-private",
            "interrupted-retained-input",
            std::str::from_utf8(first).unwrap(),
            std::str::from_utf8(second).unwrap(),
        ],
    );
    assert_eq!(server.finish().len(), 2);
    first_storage.finish();
    second_storage.finish();
}

// REQ-RUNINPUT-67
#[test]
fn retained_input_deletion_requires_confirmation_and_sends_one_idempotent_request() {
    let unconfirmed = Command::new(env!("CARGO_BIN_EXE_um"))
        .args(["run", "input", "delete", ORGANIZATION, RUN_ID])
        .output()
        .unwrap();
    assert_eq!(unconfirmed.status.code(), Some(2));

    let response = http_response_with_headers(
        "204 No Content",
        None,
        &[("Idempotency-Key", ECHO_IDEMPOTENCY_KEY)],
        b"",
    );
    let (server, _credentials, credential_path) = prepared_run(vec![response]);
    let environment = deployment_environment(&server.api_url, &credential_path);
    let output = run_with_env(
        &[
            "run",
            "input",
            "delete",
            ORGANIZATION,
            RUN_ID,
            "--yes",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(output.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["outcome"],
        "deleted"
    );
    let request = server.finish().remove(0);
    assert!(request.starts_with(&format!(
        "DELETE /api/v1/organizations/{ORGANIZATION}/runs/{RUN_ID}/inputs HTTP/1.1"
    )));
    assert_eq!(header_value(&request, "idempotency-key").len(), 64);
}

// REQ-RUNINPUT-45
// REQ-WORKSPACE-12
// REQ-RUN-59
#[test]
fn run_show_reports_the_complete_projection_in_plain_and_json_modes() {
    for json in [false, true] {
        let mut body = run_body();
        body["observation"] = serde_json::json!({
            "observedAt": "2026-08-03T12:00:00Z",
            "placement": {"runnerId": "runner-b", "runnerName": "second",
                "poolId": "pool-b", "poolName": "work"},
            "assignment": {"id": "assignment-b", "state": "active", "runnerId": "runner-b",
                "bootId": "boot-b", "presenceGeneration": 2, "leaseSequence": 9,
                "leaseExpiresAt": "2026-08-03T13:00:00Z", "leaseValid": false,
                "runnerConnected": true, "runnerLastSeenAt": null},
            "lastTransition": {"attemptId": "attempt-a", "eventSequence": 3,
                "transitionSequence": 2, "recordedAt": "2026-08-03T11:00:00Z",
                "kind": "step_state_changed", "stepId": "build", "targetState": "running"},
            "lastDecline": null
        });
        let (server, _directory, credential_path) = prepared_run(vec![run_response(body.clone())]);
        let environment = deployment_environment(&server.api_url, &credential_path);
        let mut args = vec!["run", "show", ORGANIZATION, RUN_ID];
        if json {
            args.push("--json");
        }
        args.push("--allow-insecure-http");

        let output = run_with_env(&args, &environment);

        assert!(output.status.success());
        assert!(output.stderr.is_empty());
        if json {
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(),
                serde_json::json!({
                    "schemaVersion": 1,
                    "operation": "show",
                    "deployment": server.api_url,
                    "organizationRef": ORGANIZATION,
                    "runId": RUN_ID,
                    "outcome": "found",
                    "run": body,
                    "publication": null,
                    "cancellationRequest": null,
                    "replayed": null,
                    "error": null
                })
            );
        } else {
            let stdout = String::from_utf8(output.stdout).unwrap();
            let issue_context = stdout
                .find("issueId: issue-private-context-sentinel")
                .expect("plain projection should expose the complete integration context");
            let source_context = stdout
                .find("source: linear")
                .expect("plain projection should expose the complete integration context");
            assert!(
                issue_context < source_context,
                "integration context should be sorted"
            );
            for field in [
                format!("run: {RUN_ID}"),
                "state: running".to_owned(),
                "version: 7".to_owned(),
                format!("attempt: {ATTEMPT_ID} (number 2)"),
                "source branch: release/next".to_owned(),
                "  workflow: workflows/build.yaml".to_owned(),
                "  provider: github".to_owned(),
                format!("  input set: {INPUT_SET_ID}"),
                "  availability: available".to_owned(),
                "created: 2026-08-10T12:00:00Z".to_owned(),
                "updated: 2026-08-10T12:05:00Z".to_owned(),
                "  runner: second (runner-b)".to_owned(),
                "  assignment: assignment-b (active)".to_owned(),
                "  recorded: 2026-08-03T11:00:00Z (attempt attempt-a)".to_owned(),
            ] {
                assert!(
                    stdout.lines().any(|line| line == field),
                    "missing {field:?} in {stdout:?}"
                );
            }
        }
        let request = server.finish().pop().unwrap();
        assert!(request.starts_with(&format!(
            "GET /api/v1/organizations/{ORGANIZATION}/runs/{RUN_ID} HTTP/1.1\r\n"
        )));
    }
}

const LIFECYCLE_TRIGGER_ID: &str = "trg_01k0z6r1w8f4jy2m7q9v3x5abc";
const LIFECYCLE_CONNECTION_ID: &str = "lcn_01k0z6r1w8f4jy2m7q9v3x5abc";
const LIFECYCLE_WORKSPACE_ID: &str = "a3b2c1d0-1111-4222-8333-944455556666";
const LIFECYCLE_ISSUE_ID: &str = "b4c3d2e1-2222-4333-8444-955566667777";

fn linear_lifecycle_body(state: &str, code: Option<&str>) -> serde_json::Value {
    let mut view = serde_json::json!({
        "triggerId": LIFECYCLE_TRIGGER_ID,
        "configurationVersion": 1,
        "connectionId": LIFECYCLE_CONNECTION_ID,
        "workspaceId": LIFECYCLE_WORKSPACE_ID,
        "issueId": LIFECYCLE_ISSUE_ID,
        "ownershipState": state,
    });
    if let Some(code) = code {
        view["reasonCode"] = serde_json::json!(code);
    }
    view
}

fn linear_lifecycle_command(responses: Vec<Vec<u8>>, args: &[&str]) -> (Output, ScriptedServer) {
    let (server, _directory, credential_path) = prepared_run(responses);
    let environment = deployment_environment(&server.api_url, &credential_path);
    let mut command = vec!["run", "linear"];
    command.extend_from_slice(args);
    command.push("--allow-insecure-http");
    (run_with_env(&command, &environment), server)
}

fn linear_lifecycle_show(response: Vec<u8>, json: bool) -> (Output, ScriptedServer) {
    let mut args = vec!["show", ORGANIZATION, RUN_ID];
    if json {
        args.push("--json");
    }
    linear_lifecycle_command(vec![response], &args)
}

fn lifecycle_identity_lines(indent: &str) -> Vec<String> {
    vec![
        format!("{indent}trigger: {LIFECYCLE_TRIGGER_ID}"),
        format!("{indent}configuration version: 1"),
        format!("{indent}connection: {LIFECYCLE_CONNECTION_ID}"),
        format!("{indent}workspace: {LIFECYCLE_WORKSPACE_ID}"),
        format!("{indent}issue: {LIFECYCLE_ISSUE_ID}"),
    ]
}

// REQ-TRIGGER-145
#[test]
fn linear_lifecycle_show_writes_the_view_unchanged_and_one_line_per_member() {
    let view = linear_lifecycle_body("skipped", Some("team_unmapped"));
    let (output, server) = linear_lifecycle_show(run_response(view.clone()), true);
    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(),
        serde_json::json!({
            "schemaVersion": 1,
            "operation": "show",
            "deployment": server.api_url,
            "organizationRef": ORGANIZATION,
            "runId": RUN_ID,
            "outcome": "found",
            "linearLifecycle": view,
            "error": null
        })
    );
    let request = server.finish().pop().unwrap();
    assert!(request.starts_with(&format!(
        "GET /api/v1/organizations/{ORGANIZATION}/runs/{RUN_ID}/linear-lifecycle HTTP/1.1\r\n"
    )));

    let (output, server) = linear_lifecycle_show(
        run_response(linear_lifecycle_body("awaiting_event", None)),
        false,
    );
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let mut expected = lifecycle_identity_lines("");
    expected.push("ownership state: awaiting_event".to_owned());
    for field in expected {
        assert!(
            stdout.lines().any(|line| line == field),
            "missing {field:?} in {stdout:?}"
        );
    }
    assert!(!stdout.contains("reason code"), "{stdout:?}");
    server.finish();
}

// REQ-TRIGGER-145
#[test]
fn linear_lifecycle_show_writes_the_latest_update_under_the_view() {
    let mut view = linear_lifecycle_body("awaiting_event", None);
    view["latestUpdate"] = serde_json::json!({
        "id": "llu_01k0z6r1w8f4jy2m7q9v3x5abd",
        "attemptId": "atm_01k0z6r1w8f4jy2m7q9v3x5abc",
        "attemptNumber": 2,
        "eventId": "evt-7",
        "eventType": "step.failed",
        "eventSequence": 7,
        "eventOccurredAt": "2026-10-26T12:07:00Z",
        "node": {"scope": ["deploy"], "role": "step", "id": "build"},
        "createdAt": "2026-10-26T12:07:01Z",
        "updatedAt": "2026-10-26T12:08:00Z",
        "state": "retry_waiting",
        "reasonCode": "outcome_unresolved",
        "cycleNumber": 1,
        "cycleDeadline": "2026-10-27T12:07:00Z",
        "requestCount": 1,
        "retryable": true,
        "desiredStateId": "state-blocked",
        "observedStateId": "state-progress",
        "nextAttemptAt": "2026-10-26T12:09:00Z",
        "blockingUpdate": {
            "runId": "run_01k0z6r1w8f4jy2m7q9v3x5abe",
            "updateId": "llu_01k0z6r1w8f4jy2m7q9v3x5abf"
        }
    });
    let (output, server) = linear_lifecycle_show(run_response(view.clone()), true);
    assert!(output.status.success());
    let shown: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(shown["linearLifecycle"], view);
    server.finish();

    let (output, server) = linear_lifecycle_show(run_response(view), false);
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let lines: Vec<&str> = stdout.lines().collect();
    let block = lines
        .iter()
        .position(|line| *line == "latest update:")
        .unwrap_or_else(|| panic!("no latest update block in {stdout:?}"));
    for field in [
        "  id: llu_01k0z6r1w8f4jy2m7q9v3x5abd",
        "  attempt: atm_01k0z6r1w8f4jy2m7q9v3x5abc",
        "  attempt number: 2",
        "  event: evt-7",
        "  event type: step.failed",
        "  event sequence: 7",
        "  event occurred: 2026-10-26T12:07:00Z",
        "  node: step deploy/build",
        "  created: 2026-10-26T12:07:01Z",
        "  updated: 2026-10-26T12:08:00Z",
        "  state: retry_waiting",
        "  reason code: outcome_unresolved",
        "  cycle number: 1",
        "  cycle deadline: 2026-10-27T12:07:00Z",
        "  request count: 1",
        "  retryable: true",
        "  desired state: state-blocked",
        "  observed state: state-progress",
        "  next attempt: 2026-10-26T12:09:00Z",
        "  blocking update: llu_01k0z6r1w8f4jy2m7q9v3x5abf (run run_01k0z6r1w8f4jy2m7q9v3x5abe)",
    ] {
        assert!(
            lines[block + 1..].contains(&field),
            "missing {field:?} under the latest update in {stdout:?}"
        );
    }
    assert!(!stdout.contains("confirmed state"), "{stdout:?}");
    server.finish();
}

// REQ-TRIGGER-145
#[test]
fn linear_lifecycle_show_reports_absence_and_refuses_a_wrong_pair() {
    let (output, server) = linear_lifecycle_show(
        problem_http_response(
            "404 Not Found",
            serde_json::json!({
                "type": "https://api.usefulmachinery.com/problems/not-found",
                "title": "Not found",
                "status": 404
            }),
        ),
        true,
    );
    assert_eq!(output.status.code(), Some(1));
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["operation"], "show");
    assert_eq!(result["outcome"], "not_found");
    assert_eq!(result["error"]["code"], "not_found");
    assert!(result["linearLifecycle"].is_null());
    server.finish();

    let (output, server) = linear_lifecycle_show(
        run_response(linear_lifecycle_body(
            "awaiting_event",
            Some("team_unmapped"),
        )),
        true,
    );
    assert_invalid_response(&output);
    server.finish();
}

const LIFECYCLE_UPDATE_ID: &str = "llu_01k0z6r1w8f4jy2m7q9v3x5abd";
const LIFECYCLE_OLDER_UPDATE_ID: &str = "llu_01k0z6r1w8f4jy2m7q9v3x5abe";

fn lifecycle_update(id: &str, state: &str, code: Option<&str>) -> serde_json::Value {
    let mut update = serde_json::json!({
        "id": id,
        "attemptId": ATTEMPT_ID,
        "attemptNumber": 1,
        "eventId": "evt-9",
        "eventType": "run.succeeded",
        "eventSequence": 9,
        "eventOccurredAt": "2026-10-26T12:09:00Z",
        "createdAt": "2026-10-26T12:09:01Z",
        "updatedAt": "2026-10-27T12:09:00Z",
        "state": state,
        "cycleNumber": 1,
        "cycleDeadline": "2026-10-27T12:09:00Z",
        "requestCount": 3,
        "retryable": false,
        "desiredStateId": "state-done"
    });
    if let Some(code) = code {
        update["reasonCode"] = serde_json::json!(code);
    }
    update
}

fn lifecycle_history_page() -> serde_json::Value {
    let mut exhausted = lifecycle_update(LIFECYCLE_UPDATE_ID, "failed", Some("retry_exhausted"));
    exhausted["retryable"] = serde_json::json!(true);
    exhausted["diagnostic"] =
        serde_json::json!({"cause": "provider_unavailable", "providerStatus": 503});
    serde_json::json!({
        "items": [exhausted, lifecycle_update(LIFECYCLE_OLDER_UPDATE_ID, "applied", None)],
        "nextCursor": "page-3"
    })
}

fn lifecycle_problem(status: &str, problem: &str) -> Vec<u8> {
    let number: u16 = status.split(' ').next().unwrap().parse().unwrap();
    problem_http_response(
        status,
        serde_json::json!({
            "type": format!("https://api.usefulmachinery.com/problems/{problem}"),
            "title": "Problem",
            "status": number
        }),
    )
}

// REQ-TRIGGER-145
#[test]
fn linear_lifecycle_history_writes_the_page_unchanged_and_each_update_as_show_does() {
    let page = lifecycle_history_page();
    let (output, server) = linear_lifecycle_command(
        vec![run_response(page.clone())],
        &[
            "history",
            ORGANIZATION,
            RUN_ID,
            "--limit",
            "2",
            "--cursor",
            "page-2",
            "--json",
        ],
    );
    assert!(output.status.success(), "{output:?}");
    assert!(output.stderr.is_empty());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(),
        serde_json::json!({
            "schemaVersion": 1,
            "operation": "history",
            "deployment": server.api_url,
            "organizationRef": ORGANIZATION,
            "runId": RUN_ID,
            "outcome": "found",
            "updates": page,
            "error": null
        })
    );
    let request = server.finish().pop().unwrap();
    assert!(
        request.starts_with(&format!(
            "GET /api/v1/organizations/{ORGANIZATION}/runs/{RUN_ID}/linear-lifecycle/updates?limit=2&cursor=page-2 HTTP/1.1\r\n"
        )),
        "{request:?}"
    );

    let (output, server) =
        linear_lifecycle_command(vec![run_response(page)], &["history", ORGANIZATION, RUN_ID]);
    assert!(output.status.success(), "{output:?}");
    let request = server.finish().pop().unwrap();
    assert!(request.starts_with(&format!(
        "GET /api/v1/organizations/{ORGANIZATION}/runs/{RUN_ID}/linear-lifecycle/updates HTTP/1.1\r\n"
    )));
    let stdout = String::from_utf8(output.stdout).unwrap();
    let blocks: Vec<Vec<&str>> = stdout
        .split("\n\n")
        .map(|block| block.lines().collect())
        .collect();
    assert_eq!(blocks.len(), 3, "{stdout:?}");
    for field in [
        format!("id: {LIFECYCLE_UPDATE_ID}"),
        "state: failed".to_owned(),
        "reason code: retry_exhausted".to_owned(),
        "retryable: true".to_owned(),
        "diagnostic: cause provider_unavailable, provider status 503".to_owned(),
    ] {
        assert!(
            blocks[0].contains(&field.as_str()),
            "{field:?} in {stdout:?}"
        );
    }
    for field in [
        format!("id: {LIFECYCLE_OLDER_UPDATE_ID}"),
        "state: applied".to_owned(),
        "request count: 3".to_owned(),
    ] {
        assert!(
            blocks[1].contains(&field.as_str()),
            "{field:?} in {stdout:?}"
        );
    }
    assert_eq!(blocks[2], ["next cursor: page-3"]);

    let mut last = lifecycle_history_page();
    last.as_object_mut().unwrap().remove("nextCursor");
    last["items"].as_array_mut().unwrap().truncate(1);
    let (output, server) =
        linear_lifecycle_command(vec![run_response(last)], &["history", ORGANIZATION, RUN_ID]);
    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(!stdout.contains("next cursor"), "{stdout:?}");
    assert!(!stdout.contains("\n\n"), "{stdout:?}");
    server.finish();
}

// REQ-TRIGGER-145
#[test]
fn linear_lifecycle_history_reports_absence_and_refuses_invalid_updates() {
    let (output, server) = linear_lifecycle_command(
        vec![lifecycle_problem("404 Not Found", "not-found")],
        &["history", ORGANIZATION, RUN_ID, "--json"],
    );
    assert_eq!(output.status.code(), Some(1));
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["operation"], "history");
    assert_eq!(result["outcome"], "not_found");
    assert_eq!(result["error"]["code"], "not_found");
    assert!(result["updates"].is_null());
    server.finish();

    let wrong_pair = |page: &mut serde_json::Value| {
        page["items"][1]["reasonCode"] = serde_json::json!("retry_exhausted");
    };
    let empty_diagnostic = |page: &mut serde_json::Value| {
        page["items"][0]["diagnostic"] = serde_json::json!({});
    };
    for change in [
        &wrong_pair as &dyn Fn(&mut serde_json::Value),
        &empty_diagnostic,
    ] {
        let mut page = lifecycle_history_page();
        change(&mut page);
        let (output, server) = linear_lifecycle_command(
            vec![run_response(page)],
            &["history", ORGANIZATION, RUN_ID, "--json"],
        );
        assert_invalid_response(&output);
        server.finish();
    }

    for limit in ["0", "101"] {
        let output = run(&[
            "run",
            "linear",
            "history",
            ORGANIZATION,
            RUN_ID,
            "--limit",
            limit,
        ]);
        assert_eq!(output.status.code(), Some(2), "--limit {limit}");
        assert!(output.stdout.is_empty());
    }
}

// REQ-RUN-59, REQ-TRIGGER-145
#[test]
fn run_show_carries_the_linear_lifecycle_binding_only_when_present() {
    let mut body = run_body();
    body["linearLifecycle"] = linear_lifecycle_body("awaiting_event", None);
    let (output, server) = run_show_with_response(run_response(body.clone()));
    assert!(output.status.success());
    let shown: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(shown["outcome"], "found");
    assert_eq!(shown["run"], body);
    server.finish();

    for with_binding in [true, false] {
        let mut body = run_body();
        if with_binding {
            body["linearLifecycle"] = linear_lifecycle_body("awaiting_event", None);
        }
        let (server, _directory, credential_path) = prepared_run(vec![run_response(body)]);
        let environment = deployment_environment(&server.api_url, &credential_path);
        let output = run_with_env(
            &["run", "show", ORGANIZATION, RUN_ID, "--allow-insecure-http"],
            &environment,
        );
        assert!(output.status.success());
        let stdout = String::from_utf8(output.stdout).unwrap();
        assert_eq!(
            stdout.lines().any(|line| line == "linear lifecycle:"),
            with_binding,
            "{stdout:?}"
        );
        if with_binding {
            let mut expected = lifecycle_identity_lines("  ");
            expected.push("  ownership state: awaiting_event".to_owned());
            for field in expected {
                assert!(
                    stdout.lines().any(|line| line == field),
                    "missing {field:?} in {stdout:?}"
                );
            }
        }
        server.finish();
    }
}

#[test]
fn run_show_displays_terminal_failure_and_rejection() {
    for (state, field, evidence, expected) in [
        (
            "failed",
            "failure",
            serde_json::json!({
                "node": {"id": "build", "role": "step"}, "state": "failed",
                "detail": {"code": "command_exit", "exitCode": 23, "input": {"name": "source"}, "output": {"name": "result"}}
            }),
            "command_exit",
        ),
        (
            "rejected",
            "rejection",
            serde_json::json!({"reason": "source_commit_unavailable"}),
            "source_commit_unavailable",
        ),
    ] {
        let mut body = run_body_with_state(state);
        body[field] = evidence;
        let (server, _directory, credential_path) = prepared_run(vec![run_response(body)]);
        let environment = deployment_environment(&server.api_url, &credential_path);
        let output = run_with_env(
            &["run", "show", ORGANIZATION, RUN_ID, "--allow-insecure-http"],
            &environment,
        );
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let text = String::from_utf8(output.stdout).unwrap();
        assert!(text.contains(expected), "{text}");
        assert_eq!(server.finish().len(), 1);
    }
}

// REQ-DIAG-29, REQ-DIAG-30
#[test]
fn run_show_json_keeps_harness_failure_and_input_size_diagnostic() {
    let mut body = run_body_with_state("failed");
    body["failure"] = serde_json::json!({
        "node": {"id": "agent", "role": "step"}, "state": "failed",
        "detail": {"phase": "start", "code": "harness_input_too_large"},
        "diagnostic": {"harnessFailure": "diagnostic_failure_member",
            "harnessInput": "diagnostic_input_member", "limitBytes": 1048576,
            "observedBytes": 1048577}
    });
    for json in [true, false] {
        let (server, _directory, credential_path) = prepared_run(vec![run_response(body.clone())]);
        let environment = deployment_environment(&server.api_url, &credential_path);
        let mut arguments = vec!["run", "show", ORGANIZATION, RUN_ID];
        if json {
            arguments.push("--json");
        }
        arguments.push("--allow-insecure-http");
        let output = run_with_env(&arguments, &environment);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        if json {
            let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(result["run"]["failure"], body["failure"]);
        } else {
            let text = String::from_utf8(output.stdout).unwrap();
            assert!(text.contains("harness_input_too_large"), "{text}");
            for member in ["diagnostic_", "1048576", "1048577"] {
                assert!(
                    !text.contains(member),
                    "runner diagnostic rendered in {text}"
                );
            }
        }
        assert_eq!(server.finish().len(), 1);
    }
}

// REQ-RUN-60
#[test]
fn run_show_distinguishes_pending_creation_and_creation_rejection() {
    let pending_response = http_response_with_headers(
        "202 Accepted",
        Some("application/json"),
        &[("Cache-Control", "private, no-store")],
        &serde_json::to_vec(&serde_json::json!({"runId": RUN_ID})).unwrap(),
    );
    let (output, server) = run_show_with_response(pending_response);
    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    let pending: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(pending["outcome"], "accepted");
    assert!(pending["run"].is_null());
    assert_eq!(pending["organizationRef"], ORGANIZATION);
    assert_eq!(pending["runId"], RUN_ID);
    server.finish();

    let rejected_response = http_response_with_headers(
        "409 Conflict",
        Some("application/problem+json"),
        &[("Cache-Control", "private, no-store")],
        &serde_json::to_vec(&serde_json::json!({
            "type": "https://api.usefulmachinery.com/problems/run-creation-rejected",
            "title": "Run creation rejected",
            "status": 409
        }))
        .unwrap(),
    );
    let (output, server) = run_show_with_response(rejected_response);
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stderr.is_empty());
    let rejected: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(rejected["outcome"], "error");
    assert_eq!(rejected["error"]["code"], "creation_rejected");
    assert_eq!(rejected["runId"], RUN_ID);
    server.finish();
}

// REQ-CANCEL-52
// REQ-ARTIFACT-12
#[test]
fn run_show_renders_cancellation_interruption_and_artifact_delivery() {
    let mut body = run_body_with_state("interrupted");
    body["cancellation"] = serde_json::json!({
        "mode": "force",
        "gracefulRequestId": "cmd_01k0z6r1w8f4jy2m7q9v3x5abc",
        "forceRequestId": "cmd_01k0z6r1w8f4jy2m7q9v3x5abd"
    });
    body["interruption"] = serde_json::json!({
        "phase": "running",
        "cause": "executor_fault",
        "executorFault": "runner_internal_failure",
        "stopConfirmed": true
    });
    body["artifactDelivery"] = serde_json::json!({
        "state": "failed",
        "phase": "upload",
        "code": "carrier_upload_failed"
    });
    let (server, _directory, credential_path) = prepared_run(vec![run_response(body)]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(
        &["run", "show", ORGANIZATION, RUN_ID, "--allow-insecure-http"],
        &environment,
    );

    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    let stdout = String::from_utf8(output.stdout).unwrap();
    for field in [
        "  mode: force",
        "  force request: cmd_01k0z6r1w8f4jy2m7q9v3x5abd",
        "  cause: executor_fault",
        "  executor fault: runner_internal_failure",
        "  stop confirmed: yes",
        "  phase: upload",
        "  code: carrier_upload_failed",
    ] {
        assert!(
            stdout.lines().any(|line| line == field),
            "missing {field:?} in {stdout:?}"
        );
    }
    server.finish();
}

// REQ-RUN-45, REQ-RUN-61
#[test]
fn show_wait_emits_terminal_json_for_every_execution_outcome() {
    for state in [
        "succeeded",
        "failed",
        "cancelled",
        "interrupted",
        "rejected",
    ] {
        let (server, _directory, credential_path) =
            prepared_run(vec![run_response(run_body_with_state(state))]);
        let environment = deployment_environment(&server.api_url, &credential_path);

        let output = run_with_env(
            &[
                "run",
                "show",
                ORGANIZATION,
                RUN_ID,
                "--wait",
                "--json",
                "--allow-insecure-http",
            ],
            &environment,
        );

        assert_eq!(output.status.code(), Some(0));
        assert!(!output.stderr.is_empty());
        let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(result["schemaVersion"], 1);
        assert_eq!(result["deployment"], server.api_url);
        assert_eq!(result["operation"], "show");
        assert_eq!(result["outcome"], "settled");
        assert_eq!(result["run"]["id"], RUN_ID);
        assert_eq!(result["run"]["state"], state);
        let requests = server.finish();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].starts_with(&format!(
            "GET /api/v1/organizations/{ORGANIZATION}/runs/{RUN_ID} HTTP/1.1\r\n"
        )));
    }
}

// REQ-RUN-61
#[test]
fn show_wait_emits_the_terminal_plain_projection() {
    let (server, _directory, credential_path) =
        prepared_run(vec![run_response(run_body_with_state("failed"))]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(
        &[
            "run",
            "show",
            ORGANIZATION,
            RUN_ID,
            "--wait",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert_eq!(output.status.code(), Some(0));
    assert!(!output.stderr.is_empty());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.lines().any(|line| line == "✓ Run observed."));
    assert!(stdout.lines().any(|line| line == format!("run: {RUN_ID}")));
    assert!(stdout.lines().any(|line| line == "state: failed"));
    server.finish();
}

// REQ-RUN-62
#[test]
fn show_wait_bounds_expiring_and_rejected_credential_refresh() {
    for expiring in [true, false] {
        let refresh = json_http_response(
            "200 OK",
            serde_json::json!({
                "access_token": REFRESHED_TOKEN,
                "refresh_token": "unique-bounded-refresh-token",
                "token_type": "Bearer",
                "expires_in": 3600
            }),
        );
        let responses = if expiring {
            vec![refresh]
        } else {
            vec![
                problem_http_response(
                    "401 Unauthorized",
                    serde_json::json!({
                        "type": "https://api.usefulmachinery.com/problems/unauthorized",
                        "title": "Unauthorized",
                        "status": 401
                    }),
                ),
                refresh,
            ]
        };
        let mut server = ScriptedServer::respond_with_paused_last_response(responses);
        let directory = private_credential_directory();
        let path = directory.path().join("credentials.json");
        write_credential_fixture_with_refresh_token(
            &path,
            &server.api_url,
            &server.issuer,
            TOKEN,
            if expiring {
                "2000-01-01T00:00:00Z"
            } else {
                "2999-01-01T00:00:00Z"
            },
            "unique-original-refresh-token",
        );
        let api_url = server.api_url.clone();
        let issuer = server.issuer.clone();
        let environment =
            deployment_environment_with_issuer(&api_url, &issuer, path.to_str().unwrap());
        let (output, requests) = std::thread::scope(|scope| {
            let invocation = scope.spawn(|| {
                run_with_env(
                    &[
                        "run",
                        "show",
                        ORGANIZATION,
                        RUN_ID,
                        "--wait",
                        "--json",
                        "--timeout",
                        "1s",
                        "--allow-insecure-http",
                    ],
                    &environment,
                )
            });
            let mut requests = Vec::new();
            if !expiring {
                requests.push(server.wait_for_request());
            }
            // The request is the synchronization point; a wall-clock receive
            // deadline can race the child scheduler before it reaches the
            // refresh endpoint.
            requests.push(server.wait_for_request());
            (invocation.join().unwrap(), requests)
        });
        server.release_paused_response();
        assert_eq!(output.status.code(), Some(1));
        let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(result["outcome"], "timed_out");
        assert_eq!(result["error"]["code"], "wait_timed_out");
        assert_no_secret_output(&output, &[TOKEN, REFRESHED_TOKEN]);
        assert!(server.finish().is_empty());
        assert_eq!(requests.len(), if expiring { 1 } else { 2 });
        assert!(
            requests
                .last()
                .unwrap()
                .starts_with("POST /auth/oauth/token HTTP/1.1")
        );
    }
}

#[test]
fn show_wait_refreshes_authentication_and_recovers_from_one_server_failure() {
    let server = ScriptedServer::respond(vec![
        problem_http_response(
            "401 Unauthorized",
            serde_json::json!({
                "type": "https://api.usefulmachinery.com/problems/unauthorized",
                "title": "Unauthorized",
                "status": 401
            }),
        ),
        json_http_response(
            "200 OK",
            serde_json::json!({
                "access_token": REFRESHED_TOKEN,
                "refresh_token": "unique-cloud-run-observation-refreshed-refresh-token",
                "token_type": "Bearer",
                "expires_in": 3600
            }),
        ),
        problem_http_response(
            "500 Internal Server Error",
            serde_json::json!({
                "type": "https://api.usefulmachinery.com/problems/internal-server-error",
                "title": "Internal Server Error",
                "status": 500
            }),
        ),
        run_response(run_body_with_state("succeeded")),
    ]);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture_for_deployment(
        &credential_path,
        &server.api_url,
        &server.issuer,
        TOKEN,
        "2999-01-01T00:00:00Z",
    );
    let environment = deployment_environment_with_issuer(
        &server.api_url,
        &server.issuer,
        credential_path.to_str().unwrap(),
    );

    let output = run_with_env(
        &[
            "run",
            "show",
            ORGANIZATION,
            RUN_ID,
            "--wait",
            "--json",
            "--timeout",
            "10s",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(output.status.success());
    assert!(!output.stderr.is_empty());
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "settled");
    assert_eq!(result["run"]["id"], RUN_ID);
    assert_no_secret_output(&output, &[TOKEN, REFRESHED_TOKEN]);
    let requests = server.finish();
    assert_eq!(requests.len(), 4);
    assert!(requests[0].starts_with("GET /api/v1/organizations/"));
    assert!(requests[1].starts_with("POST /auth/oauth/token HTTP/1.1\r\n"));
    assert!(requests[2].starts_with("GET /api/v1/organizations/"));
    assert!(requests[3].starts_with("GET /api/v1/organizations/"));
    assert_eq!(
        header_value(&requests[3], "authorization"),
        format!("Bearer {REFRESHED_TOKEN}")
    );
}

#[test]
fn show_wait_preserves_fatal_response_classifications() {
    let unauthenticated = run(&["run", "show", ORGANIZATION, RUN_ID, "--wait", "--json"]);
    assert_eq!(unauthenticated.status.code(), Some(3));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&unauthenticated.stdout).unwrap()["error"]["code"],
        "authentication_required"
    );

    let cases = [
        (
            problem_http_response(
                "403 Forbidden",
                serde_json::json!({
                    "type": "https://api.usefulmachinery.com/problems/forbidden",
                    "title": "Forbidden",
                    "status": 403
                }),
            ),
            "forbidden",
        ),
        (
            problem_http_response(
                "404 Not Found",
                serde_json::json!({
                    "type": "https://api.usefulmachinery.com/problems/not-found",
                    "title": "Not Found",
                    "status": 404
                }),
            ),
            "not_found",
        ),
        (
            json_http_response("200 OK", serde_json::json!({"state": "running"})),
            "protocol_error",
        ),
    ];

    for (response, expected) in cases {
        let (server, _directory, credential_path) = prepared_run(vec![response]);
        let environment = deployment_environment(&server.api_url, &credential_path);
        let output = run_with_env(
            &[
                "run",
                "show",
                ORGANIZATION,
                RUN_ID,
                "--wait",
                "--json",
                "--allow-insecure-http",
            ],
            &environment,
        );

        assert_eq!(output.status.code(), Some(1));
        assert!(output.stderr.is_empty());
        let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(result["outcome"], "error");
        assert_eq!(result["error"]["code"], expected);
        assert_eq!(result["runId"], RUN_ID);
        server.finish();
    }
}

// REQ-RUN-60
#[test]
fn run_show_rejects_a_projection_for_a_different_run() {
    let mut response_body = run_body();
    response_body["id"] = serde_json::json!("run_01k0z6r1w8f4jy2m7q9v3x5abd");

    let (output, server) = run_show_with_response(run_response(response_body));

    assert_invalid_response(&output);
    server.finish();
}

#[test]
fn pre_dispatch_refresh_outage_is_not_run_acceptance_unknown() {
    for operation in ["create", "cancel"] {
        for json in [false, true] {
            let server =
                ScriptedServer::respond(vec![http_response("503 Service Unavailable", None, &[])]);
            let credential_directory = private_credential_directory();
            let credential_path = credential_directory.path().join("credentials.json");
            write_credential_fixture_with_refresh_token(
                &credential_path,
                &server.api_url,
                &server.issuer,
                TOKEN,
                "2000-01-01T00:00:00Z",
                "unique-run-refresh-token",
            );
            let environment = deployment_environment_with_issuer(
                &server.api_url,
                &server.issuer,
                credential_path.to_str().unwrap(),
            );
            let mut args = if operation == "create" {
                create_args(json)
            } else {
                let mut args = vec![
                    "run",
                    "cancel",
                    ORGANIZATION,
                    RUN_ID,
                    "--idempotency-key",
                    "pre-refresh-key",
                    "--allow-insecure-http",
                ];
                if json {
                    args.push("--json");
                }
                args
            };
            if operation == "create" && !json {
                args.retain(|arg| *arg != "--json");
            }
            let output = run_with_env(&args, &environment);
            assert_eq!(output.status.code(), Some(4));
            if json {
                let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
                assert_eq!(result["outcome"], "error");
                assert_eq!(result["error"]["code"], "unavailable");
            } else {
                assert!(!String::from_utf8_lossy(&output.stderr).contains("acceptance_unknown"));
            }
            let requests = server.finish();
            assert_eq!(requests.len(), 1);
            assert!(requests[0].starts_with("POST /auth/oauth/token HTTP/1.1"));
        }
    }
}

#[test]
fn run_operations_reject_responses_larger_than_the_api_limit() {
    let mut create_body = acceptance_body(RUN_ID, false);
    create_body.extend(std::iter::repeat_n(b' ', 1024 * 1024));
    let create_response = http_response_with_headers(
        "202 Accepted",
        Some("application/json"),
        &[
            ("Idempotency-Key", ECHO_IDEMPOTENCY_KEY),
            (
                "Location",
                "/v1/organizations/acme-research/runs/run_01k0z6r1w8f4jy2m7q9v3x5abc",
            ),
        ],
        &create_body,
    );
    let (server, _directory, credential_path) = prepared_run(vec![create_response]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(&create_args(true), &environment);

    assert_invalid_response(&output);
    server.finish();

    let mut show_body = serde_json::to_vec(&run_body()).unwrap();
    show_body.extend(std::iter::repeat_n(b' ', 1024 * 1024));
    let show_response = chunked_json_response("200 OK", &show_body);
    let (output, server) = run_show_with_response(show_response);

    assert_invalid_response(&output);
    server.finish();
}

#[test]
fn run_show_requires_confirmed_stops_for_contained_interruptions() {
    for (cause, executor_fault) in [
        ("executor_shutdown", serde_json::Value::Null),
        (
            "executor_fault",
            serde_json::json!("runner_internal_failure"),
        ),
    ] {
        let mut body = run_body_with_state("interrupted");
        body["interruption"] = serde_json::json!({
            "phase": "running",
            "cause": cause,
            "executorFault": executor_fault,
            "stopConfirmed": false
        });

        let (output, server) = run_show_with_response(run_response(body));

        assert_invalid_response(&output);
        server.finish();
    }
}

#[test]
fn run_show_accepts_lease_expiry_with_either_stop_confirmation() {
    for stop_confirmed in [false, true] {
        let mut body = run_body_with_state("interrupted");
        body["interruption"] = serde_json::json!({
            "phase": "running",
            "cause": "execution_lease_expired",
            "executorFault": null,
            "stopConfirmed": stop_confirmed
        });

        let (output, server) = run_show_with_response(run_response(body));

        assert!(output.status.success());
        let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(result["outcome"], "found");
        assert_eq!(
            result["run"]["interruption"]["stopConfirmed"],
            stop_confirmed
        );
        server.finish();
    }
}

#[test]
fn run_semantic_response_validation_rejects_contract_invalid_values() {
    let invalid_run_id = "run_invalid";
    let create_response = acceptance_response_for(
        "202 Accepted",
        invalid_run_id,
        false,
        &[
            ("Idempotency-Key", ECHO_IDEMPOTENCY_KEY),
            (
                "Location",
                "/v1/organizations/acme-research/runs/run_invalid",
            ),
        ],
    );
    let (server, _directory, credential_path) = prepared_run(vec![create_response]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(&create_args(true), &environment);

    assert_invalid_response(&output);
    server.finish();

    let mut invalid_projection = run_body();
    invalid_projection["updatedAt"] = serde_json::json!("not-a-timestamp");
    let (output, server) = run_show_with_response(run_response(invalid_projection));

    assert_invalid_response(&output);
    server.finish();
}

#[test]
fn run_failures_use_registered_outcomes_without_exposing_secrets() {
    let unauthenticated = run(&["run", "show", ORGANIZATION, RUN_ID, "--json"]);
    assert_eq!(unauthenticated.status.code(), Some(3));
    let diagnostic: serde_json::Value = serde_json::from_slice(&unauthenticated.stdout).unwrap();
    assert_eq!(diagnostic["outcome"], "error");
    assert_eq!(diagnostic["error"]["code"], "authentication_required");
    assert_no_secret_output(&unauthenticated, &[TOKEN]);

    let cases = [
        (
            "403 Forbidden",
            serde_json::json!({
                "type": "https://api.usefulmachinery.com/problems/forbidden",
                "title": "Forbidden",
                "status": 403,
                "detail": "unique-response-capability-material"
            }),
            "forbidden",
        ),
        (
            "404 Not Found",
            serde_json::json!({
                "type": "https://api.usefulmachinery.com/problems/not-found",
                "title": "Not found",
                "status": 404
            }),
            "not_found",
        ),
    ];
    for (status, problem, expected) in cases {
        let (server, _directory, credential_path) =
            prepared_run(vec![problem_http_response(status, problem)]);
        let environment = deployment_environment(&server.api_url, &credential_path);
        let output = run_with_env(
            &[
                "run",
                "show",
                ORGANIZATION,
                RUN_ID,
                "--json",
                "--allow-insecure-http",
            ],
            &environment,
        );
        assert_eq!(output.status.code(), Some(1));
        let diagnostic: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(diagnostic["outcome"], "error");
        assert_eq!(diagnostic["error"]["code"], expected);
        assert_no_secret_output(&output, &[TOKEN, "unique-response-capability-material"]);
        server.finish();
    }

    let (conflict, _directory, credential_path) = prepared_run(vec![problem_http_response(
        "409 Conflict",
        serde_json::json!({
            "type": "https://api.usefulmachinery.com/problems/project-not-ready",
            "title": "Project not ready",
            "status": 409,
            "blockers": ["runner_pool_unassigned"]
        }),
    )]);
    let environment = deployment_environment(&conflict.api_url, &credential_path);
    let output = run_with_env(&create_args(true), &environment);
    assert_eq!(output.status.code(), Some(1));
    let diagnostic: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(diagnostic["outcome"], "error");
    assert_eq!(diagnostic["error"]["code"], "submission_failed");
    assert_no_secret_output(&output, &[TOKEN]);
    conflict.finish();

    let (malformed, _directory, credential_path) = prepared_run(vec![json_http_response(
        "200 OK",
        serde_json::json!({"state": "running"}),
    )]);
    let environment = deployment_environment(&malformed.api_url, &credential_path);
    let output = run_with_env(
        &[
            "run",
            "show",
            ORGANIZATION,
            RUN_ID,
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert_eq!(output.status.code(), Some(1));
    let diagnostic: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(diagnostic["outcome"], "error");
    assert_eq!(diagnostic["error"]["code"], "protocol_error");
    assert_no_secret_output(&output, &[TOKEN]);
    malformed.finish();

    let refused = api_test_support::RefusedPort::hold();
    let api_url = format!("http://{}/api", refused.address());
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture(&credential_path, &api_url, TOKEN, "2999-01-01T00:00:00Z");
    let environment = deployment_environment(&api_url, credential_path.to_str().unwrap());
    let output = run_with_env(
        &[
            "run",
            "show",
            ORGANIZATION,
            RUN_ID,
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert_eq!(output.status.code(), Some(4));
    let diagnostic: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(diagnostic["outcome"], "error");
    assert_eq!(diagnostic["error"]["code"], "unavailable");
    assert_no_secret_output(&output, &[TOKEN]);
}

// REQ-RUN-62
#[test]
fn show_wait_bounds_transport_retries_and_emits_one_unavailable_result() {
    let (server, _directory, credential_path) = prepared_run(vec![Vec::new(), Vec::new()]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(
        &[
            "run",
            "show",
            ORGANIZATION,
            RUN_ID,
            "--wait",
            "--json",
            "--timeout",
            "10s",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert_eq!(output.status.code(), Some(4));
    assert!(output.stderr.is_empty());
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "error");
    assert_eq!(result["error"]["code"], "observation_failed");
    assert_eq!(result["runId"], RUN_ID);
    assert_eq!(server.finish().len(), 2);
}

// REQ-RUN-62
#[test]
fn signals_stop_only_active_cloud_run_show_observation() {
    for (signal, expected_exit) in [
        (rustix::process::Signal::INT, 130),
        (rustix::process::Signal::TERM, 143),
    ] {
        let mut server =
            ScriptedServer::respond_with_paused_first_response(vec![run_response(run_body())]);
        let credential_directory = private_credential_directory();
        let credential_path = credential_directory.path().join("credentials.json");
        write_credential_fixture(
            &credential_path,
            &server.api_url,
            TOKEN,
            "2999-01-01T00:00:00Z",
        );
        let environment =
            deployment_environment(&server.api_url, credential_path.to_str().unwrap());
        let args = [
            "run",
            "show",
            ORGANIZATION,
            RUN_ID,
            "--wait",
            "--json",
            "--allow-insecure-http",
        ];
        let mut command = Command::new(env!("CARGO_BIN_EXE_um"));
        command
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .env_remove(CREDENTIALS_FILE_VARIABLE);
        for variable in DEPLOYMENT_VARIABLES {
            command.env_remove(variable);
        }
        for (name, value) in environment {
            command.env(name, value);
        }
        let child = command.spawn().unwrap();
        let request = server.wait_for_request();
        assert!(request.starts_with(&format!(
            "GET /api/v1/organizations/{ORGANIZATION}/runs/{RUN_ID} HTTP/1.1\r\n"
        )));

        rustix::process::kill_process(
            rustix::process::Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap(),
            signal,
        )
        .unwrap();
        let output = child.wait_with_output().unwrap();
        server.release_paused_response();

        assert_eq!(output.status.code(), Some(expected_exit));
        assert!(output.stderr.is_empty());
        let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(result["outcome"], "observation_stopped");
        assert_eq!(result["error"]["code"], "observation_stopped");
        assert_eq!(result["organizationRef"], ORGANIZATION);
        assert_eq!(result["runId"], RUN_ID);
        assert_no_secret_output(&output, &[TOKEN]);
        assert!(server.finish().is_empty());
    }
}

#[cfg(target_os = "linux")]
#[test]
fn signalled_input_set_create_reports_unknown_without_an_invented_id() {
    let input_text = "input set interrupted before its identifier is known";
    let input_bytes = input_text.as_bytes();
    let input_directory = tempfile::tempdir().unwrap();
    let input_path = input_directory.path().join("request.txt");
    fs::write(&input_path, input_bytes).unwrap();
    let mut server =
        ScriptedServer::respond_with_paused_first_response(vec![create_input_set_response(
            input_bytes,
            false,
        )]);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture(
        &credential_path,
        &server.api_url,
        TOKEN,
        "2999-01-01T00:00:00Z",
    );
    let environment = deployment_environment(&server.api_url, credential_path.to_str().unwrap());
    let mut command = Command::new(env!("CARGO_BIN_EXE_um"));
    command
        .args([
            "run",
            "input-set",
            "create",
            ORGANIZATION,
            "--project-id",
            PROJECT_ID,
            "--input-text-file",
            "request",
            input_path.to_str().unwrap(),
            "--json",
            "--allow-insecure-http",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env_remove(CREDENTIALS_FILE_VARIABLE);
    for variable in DEPLOYMENT_VARIABLES {
        command.env_remove(variable);
    }
    for (name, value) in environment {
        command.env(name, value);
    }
    let child = command.spawn().unwrap();
    assert!(server.next_request().contains("/run-input-sets HTTP/1.1"));

    rustix::process::kill_process(
        rustix::process::Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap(),
        rustix::process::Signal::INT,
    )
    .unwrap();
    let output = child.wait_with_output().unwrap();
    server.release_paused_response();

    assert_eq!(output.status.code(), Some(130));
    assert!(output.stderr.is_empty());
    let receipt: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(receipt["outcome"], "unknown");
    assert_eq!(receipt["commitment"], "unknown");
    assert_eq!(receipt["organizationRef"], ORGANIZATION);
    assert_eq!(receipt["resourceKind"], "input set");
    assert!(receipt.get("resourceId").is_none());
    assert_no_secret_output(&output, &[TOKEN, input_text]);
    assert!(server.finish().is_empty());
}

#[cfg(target_os = "linux")]
#[test]
fn signalled_input_set_seal_reports_one_unknown_receipt() {
    let input_bytes = b"sealed input";
    let mut server = ScriptedServer::respond_with_paused_last_response(vec![
        http_response_with_headers(
            "200 OK",
            Some("application/json"),
            &[("Cache-Control", "private, no-store")],
            &serde_json::to_vec(&scalar_input_set_body(
                input_bytes,
                "text",
                "open",
                true,
                false,
            ))
            .unwrap(),
        ),
        seal_input_set_response(input_bytes, false),
    ]);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture(
        &credential_path,
        &server.api_url,
        TOKEN,
        "2999-01-01T00:00:00Z",
    );
    let environment = deployment_environment(&server.api_url, credential_path.to_str().unwrap());
    let mut command = Command::new(env!("CARGO_BIN_EXE_um"));
    command
        .args([
            "run",
            "input-set",
            "seal",
            ORGANIZATION,
            INPUT_SET_ID,
            "--json",
            "--allow-insecure-http",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env_remove(CREDENTIALS_FILE_VARIABLE);
    for variable in DEPLOYMENT_VARIABLES {
        command.env_remove(variable);
    }
    for (name, value) in environment {
        command.env(name, value);
    }
    let child = command.spawn().unwrap();
    assert!(
        server
            .next_request()
            .contains(&format!("/run-input-sets/{INPUT_SET_ID} HTTP/1.1"))
    );
    assert!(
        server
            .next_request()
            .contains(&format!("/run-input-sets/{INPUT_SET_ID}/seal HTTP/1.1"))
    );

    rustix::process::kill_process(
        rustix::process::Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap(),
        rustix::process::Signal::INT,
    )
    .unwrap();
    let output = child.wait_with_output().unwrap();
    server.release_paused_response();

    assert_eq!(output.status.code(), Some(130));
    assert!(output.stderr.is_empty());
    let receipt: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(receipt["outcome"], "unknown");
    assert_eq!(receipt["commitment"], "unknown");
    assert_eq!(receipt["resourceKind"], "input set");
    assert_eq!(receipt["resourceId"], INPUT_SET_ID);
    assert!(server.finish().is_empty());
}

/// `um workflow run show RUN --cloud ORGANIZATION`, `um workflow run release RUN --cloud
/// ORGANIZATION` and the control leaves with `--cloud` against the scripted Gateway the `um
/// run` tests use: `RUN` is a Cloud run id, and nothing under the `um` home is read or
/// written.
mod workflow_run_cloud {
    use super::*;

    const DEFINITION_ID: &str = "def_01k0z6r1w8f4jy2m7q9v3x5abc";
    const CLAIM_ID: &str = "wsc_01k0z6r1w8f4jy2m7q9v3x5abc";
    const APPLIED_DELIVERY: &str = "wtd_01k0z6r1w8f4jy2m7q9v3x5abd";
    const SKIPPED_DELIVERY: &str = "wtd_01k0z6r1w8f4jy2m7q9v3x5abc";
    const TREE_DIGEST: &str =
        "sha256:89abcdef0123456789abcdef0123456789abcdef0123456789abcdef01234567";

    /// A definition-backed run whose current attempt exited `planned` after it waited on an
    /// answer, holding its subject's claim.
    fn definition_run() -> serde_json::Value {
        let mut run = run_body_with_state("succeeded");
        run["definition"] = serde_json::json!({
            "definitionId": DEFINITION_ID, "revision": 3,
            "treeDigest": TREE_DIGEST, "workflowPath": "workflow.yaml",
        });
        run["subjectClaim"] = serde_json::json!({
            "claimId": CLAIM_ID, "subject": "AUTO-7", "state": "held", "releaseCause": null,
        });
        run["workflowState"] = serde_json::json!("exited");
        run["workflowOutcome"] = serde_json::json!("planned");
        run["workflowProgress"] = serde_json::json!({
            "attempt": 2,
            "position": ["design", {"item": 1}, "review"],
            "wait": {"kind": "answer", "since": "2026-08-10T12:03:00Z", "step": ["design", {"item": 1}, "review"]},
            "reportedAt": "2026-08-10T12:03:01Z",
        });
        run["exits"] = serde_json::json!([{
            "attemptNumber": 2,
            "workflowOutcome": "planned",
            "mirror": [
                {"hook": "outcome", "at": "2026-08-10T12:04:00Z", "issue": "AUTO-7",
                 "action": {"kind": "comment", "body": "Planned"}, "outcome": "written"},
                {"hook": "outcome", "at": "2026-08-10T12:04:01Z", "issue": "AUTO-7",
                 "action": {"kind": "state", "state": "Planned", "binding": "plannedState"},
                 "outcome": "failed", "code": "mirror_write_rejected"},
            ],
            "claimRelease": false,
        }]);
        run
    }

    fn delivery(id: &str, state: &str) -> serde_json::Value {
        let applied = state == "applied";
        serde_json::json!({
            "id": id, "definitionId": DEFINITION_ID, "revision": 3,
            "triggerName": if applied { "cancel-on-close" } else { "withdraw-on-hold" },
            "connectionId": "lnc_01k0z6r1w8f4jy2m7q9v3x5abc",
            "eventName": "issue.state_entered",
            "event": {
                "issue": {"id": "issue-7", "identifier": "AUTO-7"},
                "issueUpdatedAt": "2026-08-10T12:04:30Z", "team": "team-1",
                "state": "state-closed", "actor": null, "at": "2026-08-10T12:04:30Z",
            },
            "claim": {"claimId": CLAIM_ID, "issueId": "issue-7",
                      "observation": {"teamId": "team-1", "stateId": "state-todo", "updatedAt": null}},
            "state": state,
            "action": if applied { serde_json::json!("cancel") } else { serde_json::Value::Null },
            "reason": if applied { serde_json::Value::Null } else { serde_json::json!("run_exited") },
            "observation": null,
            "acceptedAt": "2026-08-10T12:04:31Z",
            "deliveredAt": "2026-08-10T12:04:32Z",
            "settledAt": "2026-08-10T12:04:33Z",
        })
    }

    fn deliveries_response(more: bool) -> Vec<u8> {
        let mut page = serde_json::json!({
            "items": [delivery(APPLIED_DELIVERY, "applied"), delivery(SKIPPED_DELIVERY, "skipped")],
        });
        if more {
            page["nextCursor"] = serde_json::json!("older-deliveries");
        }
        run_response(page)
    }

    fn released_claim() -> Vec<u8> {
        http_response_with_headers(
            "200 OK",
            Some("application/json"),
            &[
                ("Cache-Control", "private, no-store"),
                (
                    "Idempotency-Key",
                    api_test_support::REQUEST_IDEMPOTENCY_KEY_ECHO,
                ),
            ],
            &serde_json::to_vec(&serde_json::json!({
                "claimId": CLAIM_ID, "subject": "AUTO-7", "state": "released",
                "releaseCause": "owner",
            }))
            .unwrap(),
        )
    }

    fn conflict(slug: &str) -> Vec<u8> {
        problem_http_response(
            "409 Conflict",
            serde_json::json!({
                "type": format!("https://api.usefulmachinery.com/problems/{slug}"),
                "title": "Conflict", "status": 409,
            }),
        )
    }

    /// `um workflow run LEAF RUN --cloud ORGANIZATION ARGS --allow-insecure-http` against a
    /// Gateway answering `responses` in order, with `UM_HOME` at `home`: the output and the
    /// requests served.
    fn cloud(
        leaf: &str,
        run: &str,
        args: &[&str],
        responses: Vec<Vec<u8>>,
        home: &std::path::Path,
    ) -> (Output, Vec<String>) {
        let (server, _directory, credential_path) = prepared_run(responses);
        let mut environment = deployment_environment(&server.api_url, &credential_path).to_vec();
        environment.push(("UM_HOME", home.to_str().unwrap()));
        let mut all = vec!["workflow", "run", leaf, run, "--cloud", ORGANIZATION];
        all.extend_from_slice(args);
        all.push("--allow-insecure-http");
        let output = run_with_env(&all, &environment);
        assert_no_secret_output(&output, &[TOKEN]);
        (output, server.finish())
    }

    fn document(output: &Output) -> serde_json::Value {
        serde_json::from_slice(&output.stdout).unwrap_or_else(|_| panic!("{output:?}"))
    }

    fn route(request: &str) -> &str {
        request
            .lines()
            .next()
            .unwrap()
            .trim_end_matches(" HTTP/1.1")
    }

    // REQ-RUN-96, REQ-RUN-87, REQ-RUN-88, REQ-LOCALRUN-189, REQ-LOCALRUN-180: Cloud show
    // reads the run and the newest page of its deliveries, and reports the definition
    // identity, claim, workflow state and outcome, progress, exits and deliveries.
    #[test]
    fn cloud_show_reports_a_definition_runs_identity_observations_and_deliveries() {
        let home = tempfile::tempdir().unwrap();
        let (output, requests) = cloud(
            "show",
            RUN_ID,
            &["--json"],
            vec![run_response(definition_run()), deliveries_response(true)],
            home.path(),
        );
        assert_eq!(output.status.code(), Some(0), "{output:?}");
        assert_eq!(
            requests
                .iter()
                .map(|request| route(request))
                .collect::<Vec<_>>(),
            [
                format!("GET /api/v1/organizations/{ORGANIZATION}/runs/{RUN_ID}"),
                format!(
                    "GET /api/v1/organizations/{ORGANIZATION}/runs/{RUN_ID}/trigger-deliveries?limit=100"
                ),
            ]
        );
        let shown = document(&output);
        assert_eq!(shown["command"], "um workflow run show");
        assert_eq!(shown["outcome"], "shown");
        assert_eq!(shown["exitStatus"], 0);
        assert_eq!(shown["organizationRef"], ORGANIZATION);
        assert!(shown.get("runDirectory").is_none(), "{shown}");
        let fixture = definition_run();
        let run = &shown["run"];
        assert_eq!(run["id"], RUN_ID);
        assert_eq!(run["state"], "succeeded");
        for field in [
            "definition",
            "subjectClaim",
            "workflowState",
            "workflowOutcome",
            "workflowProgress",
            "exits",
        ] {
            assert_eq!(run[field], fixture[field], "{field}");
        }
        let deliveries = shown["deliveries"].as_array().unwrap();
        assert_eq!(
            deliveries
                .iter()
                .map(|delivery| (&delivery["id"], &delivery["state"], &delivery["action"]))
                .collect::<Vec<_>>(),
            [
                (
                    &serde_json::json!(APPLIED_DELIVERY),
                    &serde_json::json!("applied"),
                    &serde_json::json!("cancel")
                ),
                (
                    &serde_json::json!(SKIPPED_DELIVERY),
                    &serde_json::json!("skipped"),
                    &serde_json::Value::Null
                ),
            ]
        );
        assert_eq!(shown["moreDeliveries"], true);
        assert_eq!(std::fs::read_dir(home.path()).unwrap().count(), 0);

        let (output, _) = cloud(
            "show",
            RUN_ID,
            &[],
            vec![run_response(definition_run()), deliveries_response(false)],
            home.path(),
        );
        assert_eq!(output.status.code(), Some(0), "{output:?}");
        let text = String::from_utf8(output.stdout).unwrap();
        for fact in [
            RUN_ID,
            DEFINITION_ID,
            TREE_DIGEST,
            "workflow.yaml",
            CLAIM_ID,
            "AUTO-7",
            "held",
            "exited",
            "planned",
            "answer",
            "design.1.review",
            "mirror_write_rejected",
            APPLIED_DELIVERY,
            "cancel-on-close",
            "cancel",
            SKIPPED_DELIVERY,
            "run_exited",
            "2026-08-10T12:04:31Z",
        ] {
            assert!(text.contains(fact), "{fact} missing from {text}");
        }
    }

    const OWNER: &str = "prn_01k0z6r1w8f4jy2m7q9v3x5abd";
    const PERSONAL_POOL: &str = "rpl_01k0z6r1w8f4jy2m7q9v3x5abd";
    const LAST_SEEN: &str = "2026-08-10T11:40:00Z";

    /// A queued run its owner's trigger started, routed to her personal pool, whose only
    /// runner Cloud last saw at `LAST_SEEN`.
    fn queued_owned_run(waiting: serde_json::Value) -> serde_json::Value {
        let mut run = run_body_with_state("queued");
        run["owner"] = serde_json::json!(OWNER);
        run["routing"] = serde_json::json!({
            "policyVersion": 1, "rule": "owner_personal_pool",
            "pools": [{"id": PERSONAL_POOL, "name": "ada-laptop"}],
        });
        run["observation"] = serde_json::json!({
            "observedAt": "2026-08-10T12:05:00Z", "placement": null, "assignment": null,
            "lastTransition": null, "lastDecline": null, "waiting": waiting,
        });
        run
    }

    fn offline_since_last_seen() -> serde_json::Value {
        serde_json::json!({"reason": "runners_offline", "since": LAST_SEEN})
    }

    /// The value of the human report's `key: value` line, if it printed one.
    fn reported<'a>(text: &'a str, key: &str) -> Option<&'a str> {
        text.lines()
            .find_map(|line| line.trim_start().strip_prefix(key)?.strip_prefix(": "))
    }

    /// Asserts the report names the queued owned run's owner, rule, pool and, in words,
    /// that its runners are offline since `LAST_SEEN`.
    fn assert_reports_owned_route(text: &str) {
        assert_eq!(reported(text, "owner"), Some(OWNER), "{text}");
        let routing = reported(text, "routing").unwrap_or_else(|| panic!("{text}"));
        assert!(routing.contains("owner_personal_pool"), "{text}");
        let pools = reported(text, "pools").unwrap_or_else(|| panic!("{text}"));
        assert!(
            pools.contains(PERSONAL_POOL) && pools.contains("ada-laptop"),
            "{text}"
        );
        let waiting = reported(text, "waiting").unwrap_or_else(|| panic!("{text}"));
        assert!(waiting.contains(LAST_SEEN), "{text}");
        assert!(!waiting.contains("runners_offline"), "{text}");
    }

    // REQ-RUN-101, SC-RUN-7: Cloud show names a run's owner, the rule that routed it, the
    // pools it may use and, while it is queued, what it waits for in words; it says when a
    // run has no owner or route, shows an unrecognized reason as given, and passes all
    // three through under --json as received.
    #[test]
    fn cloud_show_reports_a_runs_owner_route_and_what_it_waits_for() {
        let home = tempfile::tempdir().unwrap();
        let fixture = queued_owned_run(offline_since_last_seen());
        let (output, _) = cloud(
            "show",
            RUN_ID,
            &["--json"],
            vec![run_response(fixture.clone())],
            home.path(),
        );
        assert_eq!(output.status.code(), Some(0), "{output:?}");
        let run = &document(&output)["run"];
        assert_eq!(run["owner"], fixture["owner"]);
        assert_eq!(run["routing"], fixture["routing"]);
        assert_eq!(
            run["observation"]["waiting"],
            fixture["observation"]["waiting"]
        );

        let (output, _) = cloud(
            "show",
            RUN_ID,
            &[],
            vec![run_response(fixture)],
            home.path(),
        );
        assert_eq!(output.status.code(), Some(0), "{output:?}");
        assert_reports_owned_route(&String::from_utf8(output.stdout).unwrap());

        let mut unowned = queued_owned_run(serde_json::Value::Null);
        unowned["owner"] = serde_json::Value::Null;
        unowned["routing"] = serde_json::Value::Null;
        let (output, _) = cloud(
            "show",
            RUN_ID,
            &["--json"],
            vec![run_response(unowned.clone())],
            home.path(),
        );
        assert_eq!(output.status.code(), Some(0), "{output:?}");
        let run = &document(&output)["run"];
        for field in ["owner", "routing"] {
            assert_eq!(run.get(field), Some(&serde_json::Value::Null), "{field}");
        }
        assert_eq!(
            run["observation"].get("waiting"),
            Some(&serde_json::Value::Null)
        );
        let (output, _) = cloud(
            "show",
            RUN_ID,
            &[],
            vec![run_response(unowned)],
            home.path(),
        );
        assert_eq!(output.status.code(), Some(0), "{output:?}");
        let text = String::from_utf8(output.stdout).unwrap();
        assert_eq!(reported(&text, "owner"), Some("none"), "{text}");
        assert_eq!(reported(&text, "routing"), Some("none"), "{text}");
        assert_eq!(reported(&text, "waiting"), None, "{text}");

        let unrecognized = queued_owned_run(serde_json::json!({"reason": "runners_asleep"}));
        let (output, _) = cloud(
            "show",
            RUN_ID,
            &[],
            vec![run_response(unrecognized)],
            home.path(),
        );
        assert_eq!(output.status.code(), Some(0), "{output:?}");
        let text = String::from_utf8(output.stdout).unwrap();
        assert_eq!(reported(&text, "waiting"), Some("runners_asleep"), "{text}");
    }

    // REQ-RUN-101: `um run show` names the same owner, route and waiting reason.
    #[test]
    fn run_show_reports_a_runs_owner_route_and_what_it_waits_for() {
        let (server, _directory, credential_path) = prepared_run(vec![run_response(
            queued_owned_run(offline_since_last_seen()),
        )]);
        let environment = deployment_environment(&server.api_url, &credential_path);
        let output = run_with_env(
            &["run", "show", ORGANIZATION, RUN_ID, "--allow-insecure-http"],
            &environment,
        );
        assert_eq!(output.status.code(), Some(0), "{output:?}");
        assert_reports_owned_route(&String::from_utf8(output.stdout).unwrap());
        server.finish();
    }

    // REQ-RUN-96, REQ-RUN-87, REQ-RUN-88: a run by `workflowPath` reads no deliveries, and
    // the observations its read leaves out are null, not absent.
    #[test]
    fn cloud_show_of_a_path_run_reads_no_deliveries_and_reports_unknown_fields_as_null() {
        let home = tempfile::tempdir().unwrap();
        let (output, requests) = cloud(
            "show",
            RUN_ID,
            &["--json"],
            vec![run_response(run_body())],
            home.path(),
        );
        assert_eq!(output.status.code(), Some(0), "{output:?}");
        assert_eq!(requests.len(), 1);
        let shown = document(&output);
        for field in [
            "definition",
            "subjectClaim",
            "workflowState",
            "workflowOutcome",
            "workflowProgress",
            "exits",
        ] {
            let value = shown["run"].get(field);
            assert_eq!(value, Some(&serde_json::Value::Null), "{field}: {shown}");
        }
        assert_eq!(shown["deliveries"], serde_json::Value::Null);
        assert_eq!(shown["moreDeliveries"], false);

        let (output, _) = cloud(
            "show",
            RUN_ID,
            &[],
            vec![run_response(run_body())],
            home.path(),
        );
        assert_eq!(output.status.code(), Some(0), "{output:?}");
        assert!(String::from_utf8(output.stdout).unwrap().contains(RUN_ID));
    }

    // REQ-LOCALRUN-189: a run Cloud does not find is refused without reading any local run.
    #[test]
    fn cloud_show_reports_a_missing_run_and_a_rejected_session() {
        let home = tempfile::tempdir().unwrap();
        let (output, _) = cloud(
            "show",
            RUN_ID,
            &["--json"],
            vec![problem_http_response(
                "404 Not Found",
                serde_json::json!({"type": "https://api.usefulmachinery.com/problems/not-found", "title": "Not Found", "status": 404}),
            )],
            home.path(),
        );
        assert_eq!(output.status.code(), Some(1), "{output:?}");
        let refused = document(&output);
        assert_eq!(refused["outcome"], "refused");
        assert_eq!(refused["exitStatus"], 1);
        assert_eq!(refused["diagnostics"][0]["code"], "not_found");
    }

    // REQ-CLOUDDEF-19, REQ-LOCALRUN-187: Cloud release asks Cloud to release the run's
    // current claim under one fresh key, sent again when the response is lost, and reports
    // the released claim.
    #[test]
    fn cloud_release_sends_one_keyed_claim_release_and_reports_the_claim() {
        let home = tempfile::tempdir().unwrap();
        let (output, requests) = cloud(
            "release",
            RUN_ID,
            &["--json"],
            vec![Vec::new(), released_claim()],
            home.path(),
        );
        assert_eq!(output.status.code(), Some(0), "{output:?}");
        assert_eq!(requests.len(), 2);
        for request in &requests {
            assert_eq!(
                route(request),
                format!("POST /api/v1/organizations/{ORGANIZATION}/runs/{RUN_ID}/claim-release")
            );
            assert_eq!(request.split("\r\n\r\n").nth(1), Some("{}"));
        }
        let key = header_value(&requests[0], "idempotency-key");
        assert!(!key.is_empty());
        assert_eq!(header_value(&requests[1], "idempotency-key"), key);
        let released = document(&output);
        assert_eq!(released["command"], "um workflow run release");
        assert_eq!(released["outcome"], "released");
        assert_eq!(released["exitStatus"], 0);
        assert_eq!(released["runId"], RUN_ID);
        assert_eq!(released["claim"]["claimId"], CLAIM_ID);
        assert_eq!(released["claim"]["state"], "released");
        assert_eq!(released["claim"]["releaseCause"], "owner");
        assert_eq!(std::fs::read_dir(home.path()).unwrap().count(), 0);

        let (output, requests) = cloud("release", RUN_ID, &[], vec![released_claim()], home.path());
        assert_eq!(output.status.code(), Some(0), "{output:?}");
        assert_eq!(requests.len(), 1);
        let text = String::from_utf8(output.stdout).unwrap();
        assert!(text.contains(CLAIM_ID) && text.contains("owner"), "{text}");
    }

    // REQ-CLOUDDEF-19, REQ-LOCALRUN-187, REQ-LOCALRUN-189: a live run's claim, a changed
    // claim, a reused key and a member's release are refused with exit 1, and show or release
    // without a session exits 3.
    #[test]
    fn cloud_release_refusals_name_their_cause() {
        let home = tempfile::tempdir().unwrap();
        for (response, code) in [
            (conflict("claim-live"), "claim_live"),
            (conflict("claim-changed"), "claim_changed"),
            (conflict("idempotency-conflict"), "idempotency_conflict"),
            (
                problem_http_response(
                    "403 Forbidden",
                    serde_json::json!({"type": "https://api.usefulmachinery.com/problems/forbidden", "title": "Forbidden", "status": 403}),
                ),
                "forbidden",
            ),
        ] {
            let (output, requests) = cloud(
                "release",
                RUN_ID,
                &["--json"],
                vec![response.clone()],
                home.path(),
            );
            assert_eq!(output.status.code(), Some(1), "{code}: {output:?}");
            assert_eq!(requests.len(), 1);
            let refused = document(&output);
            assert_eq!(refused["outcome"], "refused", "{refused}");
            assert_eq!(refused["exitStatus"], 1);
            assert_eq!(refused["diagnostics"][0]["code"], code, "{refused}");

            let (output, _) = cloud("release", RUN_ID, &[], vec![response], home.path());
            assert_eq!(output.status.code(), Some(1), "{code}: {output:?}");
            assert!(output.stdout.is_empty());
            assert!(String::from_utf8(output.stderr).unwrap().contains(code));
        }
        for leaf in ["show", "release"] {
            let output = run(&[
                "workflow",
                "run",
                leaf,
                RUN_ID,
                "--cloud",
                ORGANIZATION,
                "--json",
            ]);
            assert_eq!(output.status.code(), Some(3), "{leaf}: {output:?}");
            assert_eq!(
                document(&output)["diagnostics"][0]["code"],
                "authentication_required"
            );
        }
        assert_eq!(std::fs::read_dir(home.path()).unwrap().count(), 0);
    }

    const CONTROL_ID: &str = "rct_01k0z6r1w8f4jy2m7q9v3x5abc";
    const WAIT_SINCE: &str = "2026-08-10T12:03:00Z";

    /// The definition-backed run above while its current attempt waits on a `kind` wait, or
    /// runs without one for `None`.
    fn waiting_run(kind: Option<&str>) -> serde_json::Value {
        let mut run = definition_run();
        run["state"] = serde_json::json!("running");
        run["workflowState"] =
            serde_json::json!(if kind.is_some() { "waiting" } else { "running" });
        run["workflowOutcome"] = serde_json::Value::Null;
        run["exits"] = serde_json::json!([]);
        run["workflowProgress"]["wait"] = kind.map_or(serde_json::Value::Null, |kind| {
            serde_json::json!({"kind": kind, "since": WAIT_SINCE, "step": ["design", {"item": 1}, "review"]})
        });
        run
    }

    /// Cloud's `202` for an accepted control of `kind` carrying the request's members.
    fn accepted_control(kind: &str, request: &serde_json::Value) -> Vec<u8> {
        let member = |name: &str| {
            request
                .get(name)
                .cloned()
                .unwrap_or(serde_json::Value::Null)
        };
        http_response_with_headers(
            "202 Accepted",
            Some("application/json"),
            &[
                ("Cache-Control", "private, no-store"),
                (
                    "Idempotency-Key",
                    api_test_support::REQUEST_IDEMPOTENCY_KEY_ECHO,
                ),
            ],
            &serde_json::to_vec(&serde_json::json!({
                "id": CONTROL_ID, "runId": RUN_ID, "attemptNumber": 2, "kind": kind,
                "wait": member("wait"), "answers": member("answers"), "notes": member("notes"),
                "before": member("before"), "reason": member("reason"),
                "state": "accepted", "code": null, "actionOutcome": null,
                "requestedBy": "prn_01k0z6r1w8f4jy2m7q9v3x5abc",
                "acceptedAt": "2026-08-10T12:05:00Z",
                "deliveredAt": null, "settledAt": null,
            }))
            .unwrap(),
        )
    }

    fn body(request: &str) -> serde_json::Value {
        serde_json::from_str(request.split("\r\n\r\n").nth(1).unwrap())
            .unwrap_or_else(|_| panic!("{request}"))
    }

    fn controls_route() -> String {
        format!("POST /api/v1/organizations/{ORGANIZATION}/runs/{RUN_ID}/controls")
    }

    // REQ-LOCALRUN-210, REQ-RUN-91: Cloud answer reads the run, sends one control naming the
    // run's current wait under one fresh key, sent again when the response is lost, and
    // reports the accepted control; nothing under the `um` home is read or written.
    #[test]
    fn cloud_answer_sends_the_runs_current_wait_under_one_key_and_reports_the_control() {
        let home = tempfile::tempdir().unwrap();
        let request = serde_json::json!({
            "kind": "answer",
            "wait": {"kind": "answer", "since": WAIT_SINCE},
            "answers": [{"question": "scope", "text": "Only the API"}],
        });
        let (output, requests) = cloud(
            "answer",
            RUN_ID,
            &["--question", "scope", "--text", "Only the API", "--json"],
            vec![
                run_response(waiting_run(Some("answer"))),
                Vec::new(),
                accepted_control("answer", &request),
            ],
            home.path(),
        );
        assert_eq!(output.status.code(), Some(0), "{output:?}");
        assert_eq!(requests.len(), 3);
        assert_eq!(
            route(&requests[0]),
            format!("GET /api/v1/organizations/{ORGANIZATION}/runs/{RUN_ID}")
        );
        for request_sent in &requests[1..] {
            assert_eq!(route(request_sent), controls_route());
            assert_eq!(body(request_sent), request);
        }
        let key = header_value(&requests[1], "idempotency-key");
        assert!(!key.is_empty());
        assert_eq!(header_value(&requests[2], "idempotency-key"), key);
        let accepted = document(&output);
        assert_eq!(accepted["command"], "um workflow run answer");
        assert_eq!(accepted["outcome"], "accepted");
        assert_eq!(accepted["exitStatus"], 0);
        assert_eq!(accepted["organizationRef"], ORGANIZATION);
        assert_eq!(accepted["runId"], RUN_ID);
        assert_eq!(accepted["control"]["id"], CONTROL_ID);
        assert_eq!(accepted["control"]["state"], "accepted");
        assert_eq!(accepted["control"]["wait"], request["wait"]);
        assert_eq!(std::fs::read_dir(home.path()).unwrap().count(), 0);

        let (output, requests) = cloud(
            "answer",
            RUN_ID,
            &["--question", "scope", "--text", "Only the API"],
            vec![
                run_response(waiting_run(Some("answer"))),
                accepted_control("answer", &request),
            ],
            home.path(),
        );
        assert_eq!(output.status.code(), Some(0), "{output:?}");
        assert_eq!(requests.len(), 2);
        let text = String::from_utf8(output.stdout).unwrap();
        assert!(
            text.contains(CONTROL_ID) && text.contains("accepted"),
            "{text}"
        );
    }

    // REQ-LOCALRUN-210, REQ-RUN-91: approve, hand-back, retry and a lift name the run's
    // current wait with their kinds; a lift of a run that does not wait on a stop, and a
    // stop, name none, and a stop does not read the run.
    #[test]
    fn cloud_control_leaves_send_their_kinds() {
        let home = tempfile::tempdir().unwrap();
        let wait = |kind: &str| serde_json::json!({"kind": kind, "since": WAIT_SINCE});
        for (leaf, args, waiting, request) in [
            (
                "approve",
                &[][..],
                Some("approval"),
                serde_json::json!({"kind": "approve", "wait": wait("approval")}),
            ),
            (
                "hand-back",
                &["--notes", "Split the migration"][..],
                Some("approval"),
                serde_json::json!({"kind": "hand_back", "wait": wait("approval"), "notes": "Split the migration"}),
            ),
            (
                "retry",
                &[][..],
                Some("repair"),
                serde_json::json!({"kind": "retry", "wait": wait("repair")}),
            ),
            (
                "lift",
                &["--before", "review"][..],
                Some("stop"),
                serde_json::json!({"kind": "lift", "wait": wait("stop"), "before": "review"}),
            ),
            ("lift", &[][..], None, serde_json::json!({"kind": "lift"})),
            (
                "stop",
                &["--before", "review", "--reason", "Hold for review"][..],
                None,
                serde_json::json!({"kind": "stop", "before": "review", "reason": "Hold for review"}),
            ),
        ] {
            let kind = request["kind"].as_str().unwrap();
            let mut responses = Vec::new();
            if leaf != "stop" {
                responses.push(run_response(waiting_run(waiting)));
            }
            responses.push(accepted_control(kind, &request));
            let mut all = args.to_vec();
            all.push("--json");
            let (output, requests) = cloud(leaf, RUN_ID, &all, responses, home.path());
            assert_eq!(output.status.code(), Some(0), "{leaf}: {output:?}");
            let posted = requests.last().unwrap();
            assert_eq!(requests.len(), if leaf == "stop" { 1 } else { 2 }, "{leaf}");
            assert_eq!(route(posted), controls_route(), "{leaf}");
            assert_eq!(body(posted), request, "{leaf}");
            let accepted = document(&output);
            assert_eq!(accepted["command"], format!("um workflow run {leaf}"));
            assert_eq!(accepted["outcome"], "accepted", "{leaf}");
            assert_eq!(accepted["control"]["kind"], kind, "{leaf}");
        }
        assert_eq!(std::fs::read_dir(home.path()).unwrap().count(), 0);
    }

    // REQ-LOCALRUN-210, REQ-RUN-91: Cloud's refusals exit 1 with their slugs, a run that does
    // not wait on the leaf's kind is refused before any control is sent, and a leaf without a
    // session exits 3.
    #[test]
    fn cloud_control_refusals_name_their_cause() {
        let home = tempfile::tempdir().unwrap();
        let answer = ["--question", "scope", "--text", "Only the API", "--json"];
        for (response, code) in [
            (conflict("wait-not-current"), "wait_not_current"),
            (conflict("run-not-live"), "run_not_live"),
            (conflict("idempotency-conflict"), "idempotency_conflict"),
            (
                problem_http_response(
                    "422 Unprocessable Entity",
                    serde_json::json!({"type": "https://api.usefulmachinery.com/problems/control-too-large", "title": "Unprocessable Entity", "status": 422}),
                ),
                "control_too_large",
            ),
        ] {
            let (output, requests) = cloud(
                "answer",
                RUN_ID,
                &answer,
                vec![run_response(waiting_run(Some("answer"))), response],
                home.path(),
            );
            assert_eq!(output.status.code(), Some(1), "{code}: {output:?}");
            assert_eq!(requests.len(), 2, "{code}");
            let refused = document(&output);
            assert_eq!(refused["outcome"], "refused", "{refused}");
            assert_eq!(refused["exitStatus"], 1);
            assert_eq!(refused["diagnostics"][0]["code"], code, "{refused}");
        }
        for (waiting, code) in [
            (Some("approval"), "wait_kind_mismatch"),
            (None, "run_not_waiting"),
        ] {
            let (output, requests) = cloud(
                "answer",
                RUN_ID,
                &answer,
                vec![run_response(waiting_run(waiting))],
                home.path(),
            );
            assert_eq!(output.status.code(), Some(1), "{code}: {output:?}");
            assert_eq!(requests.len(), 1, "{code}");
            assert_eq!(document(&output)["diagnostics"][0]["code"], code);
        }
        let (output, _) = cloud(
            "approve",
            RUN_ID,
            &[],
            vec![run_response(waiting_run(Some("answer")))],
            home.path(),
        );
        assert_eq!(output.status.code(), Some(1), "{output:?}");
        assert!(output.stdout.is_empty());
        assert!(
            String::from_utf8(output.stderr)
                .unwrap()
                .contains("wait_kind_mismatch")
        );
        let output = run(&[
            "workflow",
            "run",
            "approve",
            RUN_ID,
            "--cloud",
            ORGANIZATION,
            "--json",
        ]);
        assert_eq!(output.status.code(), Some(3), "{output:?}");
        assert_eq!(
            document(&output)["diagnostics"][0]["code"],
            "authentication_required"
        );
        assert_eq!(std::fs::read_dir(home.path()).unwrap().count(), 0);
    }

    // REQ-LOCALRUN-210: a Cloud retry takes no local execution root or replacement profiles.
    #[test]
    fn cloud_retry_refuses_its_local_options() {
        for local in [
            &["--execution-root", "root"][..],
            &["--agent-profiles", "profiles.json"][..],
        ] {
            let mut all = vec!["workflow", "run", "retry", RUN_ID, "--cloud", ORGANIZATION];
            all.extend_from_slice(local);
            let output = run(&all);
            assert_eq!(output.status.code(), Some(2), "{local:?}: {output:?}");
            assert!(output.stdout.is_empty(), "{local:?}");
        }
    }

    // REQ-LOCALRUN-180, REQ-LOCALRUN-181, REQ-LOCALRUN-187, REQ-LOCALRUN-189: with
    // `--cloud`, a local run's id is sent to Cloud as typed and the local run is neither read
    // nor changed; without it, show and release keep resolving and acting on the local run.
    #[test]
    fn cloud_show_and_release_leave_a_same_named_local_run_unchanged() {
        let published = super::super::workflow_run_control::PublishedWaitTree::new();
        let (directory, id, terminal) = published.run("blocked");
        assert_eq!(terminal["wait"]["kind"], "answer", "{terminal}");
        let before = std::fs::read(directory.join("run.json")).unwrap();
        let controls = |directory: &std::path::Path| {
            std::fs::read_dir(directory.join("controls")).map_or(0, |entries| entries.count())
        };
        let controls_before = controls(&directory);

        let mut cloud_run = definition_run();
        cloud_run["id"] = serde_json::json!(id);
        let (output, requests) = cloud(
            "show",
            &id,
            &["--json"],
            vec![run_response(cloud_run), deliveries_response(false)],
            std::path::Path::new(published.home.path()),
        );
        assert_eq!(output.status.code(), Some(0), "{output:?}");
        assert_eq!(
            route(&requests[0]),
            format!("GET /api/v1/organizations/{ORGANIZATION}/runs/{id}")
        );
        let shown = document(&output);
        assert!(shown.get("runDirectory").is_none(), "{shown}");
        assert_eq!(shown["run"]["definition"]["definitionId"], DEFINITION_ID);

        let (output, requests) = cloud(
            "release",
            &id,
            &["--json"],
            vec![released_claim()],
            std::path::Path::new(published.home.path()),
        );
        assert_eq!(output.status.code(), Some(0), "{output:?}");
        assert_eq!(
            route(&requests[0]),
            format!("POST /api/v1/organizations/{ORGANIZATION}/runs/{id}/claim-release")
        );
        assert_eq!(std::fs::read(directory.join("run.json")).unwrap(), before);
        assert_eq!(controls(&directory), controls_before);

        let home = Some(&published.home);
        let local = super::super::workflow_run_control::run_control(home, "show", &id, &[]);
        let local = document(&local);
        assert_eq!(local["outcome"], "shown", "{local}");
        assert_eq!(local["runDirectory"], directory.to_str().unwrap());
        let refused = super::super::workflow_run_control::refused(
            &super::super::workflow_run_control::run_control(home, "release", &id, &[]),
            "release",
            "run_live",
        );
        assert_eq!(refused["runDirectory"], directory.to_str().unwrap());
        assert_eq!(controls(&directory), controls_before);
    }
}
