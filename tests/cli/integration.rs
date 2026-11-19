//! `useful integration` beside `useful grant`, its earlier spelling (REQ-INTEGRATION-13,
//! REQ-INTEGRATION-15). Each case runs a leaf under both spellings against the same scripted
//! answers and compares what each sent and printed; the grant cases elsewhere read the leaves'
//! behavior through the earlier spelling.

use super::*;

use super::agent_logins::private_file;
use super::grant::{
    ALPHA, BRAVO, CHARLIE, ENDED_RUNNER_ID, LIST_ACME_RUNNERS, LIST_EVERY_KIND, LIST_FIRST_PAGE,
    LIST_MEMBERSHIPS, LIST_OAUTH, NEW_RUNNER_ID, OAUTH_GRANT_ID, ORGANIZATION_ID, Opener, Place,
    READ_IDENTITIES, READ_SERVED, RUNNER_ID, SENTINEL, START_CONSENT, TOKEN, ada, after_notice,
    answered, assert_stated_by_the_refusal, authorized, consent_of, deleted, family_on_terminal,
    holding, identities, json_output, memberships, memberships_as, nothing_held, oauth_grant,
    oauth_grant_unauthorized, on_terminal, one, opener_command, organization, page,
    predating_served, prepared, problem, registration, registrations, request_body, started,
    statement, stderr, two_pages, uses,
};

/// `useful integration` first, then the earlier spelling it is compared with.
const SPELLINGS: [&str; 2] = ["integration", "grant"];

/// Runs `useful FAMILY args` against `server` with the member's session.
fn run_under(family: &str, server: &ScriptedServer, credential: &Path, args: &[&str]) -> Output {
    let mut arguments = vec![family];
    arguments.extend_from_slice(args);
    arguments.push("--allow-insecure-http");
    let environment = deployment_environment_with_issuer(
        &server.api_url,
        &server.issuer,
        credential.to_str().unwrap(),
    );
    let output = run_with_env(&arguments, &environment);
    assert_no_secret_output(&output, &[SENTINEL, TOKEN]);
    output
}

/// Runs `args` under each spelling, `useful integration` first, against the same server.
fn under_both(server: &ScriptedServer, credential: &Path, args: &[&str]) -> [Output; 2] {
    SPELLINGS.map(|family| run_under(family, server, credential, args))
}

/// What each request asked: its request line and its body. The idempotency key a mutation
/// generates differs between invocations, so the headers are not compared.
fn asked(requests: &[String]) -> Vec<(String, String)> {
    requests
        .iter()
        .map(|request| {
            let (head, body) = request.split_once("\r\n\r\n").unwrap_or((request, ""));
            (head.lines().next().unwrap().to_owned(), body.to_owned())
        })
        .collect()
}

/// The requests of the server's two runs, the first and the second half, which must ask the
/// same; the first run's.
fn the_same_requests(server: ScriptedServer) -> Vec<(String, String)> {
    let requests = asked(&server.finish());
    assert!(!requests.is_empty());
    let (integration, grant) = requests.split_at(requests.len() / 2);
    assert_eq!(integration, grant);
    integration.to_vec()
}

/// `earlier` exited as `integration` did and printed the same standard output, and its standard
/// error is one line naming `replacement` and then what `integration` printed there, which
/// begins with no such line (REQ-INTEGRATION-15).
fn assert_the_same_but_the_notice(integration: &Output, earlier: &Output, replacement: &str) {
    assert_eq!(earlier.status.code(), integration.status.code());
    assert_eq!(
        String::from_utf8_lossy(&earlier.stdout),
        String::from_utf8_lossy(&integration.stdout)
    );
    let stderr = String::from_utf8(integration.stderr.clone()).unwrap();
    assert!(
        !stderr
            .lines()
            .next()
            .unwrap_or_default()
            .contains(replacement),
        "{stderr}"
    );
    assert_eq!(after_notice(earlier, replacement), stderr);
}

// REQ-INTEGRATION-13
// REQ-INTEGRATION-15
// REQ-INTEGRATION-22
#[test]
fn list_states_each_grants_side_and_the_earlier_list_states_none() {
    // The member is an active member of no organization, so `useful integration list` reads
    // the memberships after what the earlier list reads, and nothing after them.
    let answers = |integration: bool| {
        let mut answers = two_pages().to_vec();
        answers.extend([predating_served(), organization(), identities(ada())]);
        if integration {
            answers.push(memberships(&[]));
        }
        answers
    };
    let responses = [answers(true), answers(false), answers(true), answers(false)].concat();
    let earlier_reads = answers(false).len();
    let (server, _directory, credential) = prepared(responses);
    let [integration, grant] = under_both(&server, &credential, &["list", "--json"]);
    let [integration_human, grant_human] = under_both(&server, &credential, &["list"]);
    let requests = asked(&server.finish());
    let (json_requests, human_requests) = requests.split_at(requests.len() / 2);
    for requests in [json_requests, human_requests] {
        let (first, second) = requests.split_at(earlier_reads + 1);
        assert_eq!(first[..earlier_reads], *second);
        assert_eq!(first[earlier_reads].0, LIST_MEMBERSHIPS);
        assert_eq!(first[0].0, LIST_FIRST_PAGE);
    }

    assert_eq!(integration.status.code(), Some(0));
    assert_eq!(grant.status.code(), Some(0));
    let mut document = json_output(&integration);
    let earlier = json_output(&grant);
    let grants = document["grants"].as_array_mut().unwrap();
    assert_eq!(grants.len(), 2, "{earlier}");
    for grant in grants {
        assert_eq!(grant["side"], "member", "{grant}");
        grant.as_object_mut().unwrap().remove("side");
    }
    let not_connected = document["notConnected"].as_array().unwrap();
    assert!(!not_connected.is_empty());
    for entry in not_connected {
        assert!(entry.get("side").is_none(), "{entry}");
    }
    for grant in earlier["grants"].as_array().unwrap() {
        assert!(grant.get("side").is_none(), "{grant}");
    }
    // A deployment that predates the served read serves no token service, so the list's
    // `tokens` is empty, a member of no organization has no connection to list, and the
    // earlier list has neither (REQ-INTEGRATION-19, REQ-INTEGRATION-22).
    for member in ["tokens", "connections"] {
        let listed = document.as_object_mut().unwrap().remove(member);
        assert_eq!(
            listed,
            Some(serde_json::json!([])),
            "{member} in {document}"
        );
    }
    assert_eq!(document, earlier);
    assert!(integration.stderr.is_empty());
    assert_eq!(after_notice(&grant, "useful integration list"), "");

    // The human listing is the earlier one with a line stating the side of each grant.
    assert_eq!(integration_human.status.code(), Some(0));
    assert_eq!(grant_human.status.code(), Some(0));
    let listing = String::from_utf8(integration_human.stdout.clone()).unwrap();
    let earlier_listing = String::from_utf8(grant_human.stdout.clone()).unwrap();
    let side = |line: &&str| *line == "side: member";
    assert_eq!(listing.lines().filter(side).count(), 2, "{listing}");
    assert_eq!(
        listing
            .lines()
            .filter(|line| !side(line))
            .collect::<Vec<_>>(),
        earlier_listing.lines().collect::<Vec<_>>()
    );
    assert!(integration_human.stderr.is_empty());
    assert_eq!(after_notice(&grant_human, "useful integration list"), "");
}

// REQ-INTEGRATION-13
// REQ-INTEGRATION-15
#[test]
fn list_of_one_grant_states_its_side_and_the_earlier_list_states_none() {
    let grant = oauth_grant(OAUTH_GRANT_ID, "github", "active");
    let mut responses = Vec::new();
    for _ in SPELLINGS {
        responses.extend([one(&grant), uses(&[]), identities(ada())]);
    }
    let (server, _directory, credential) = prepared(responses);
    let [integration, earlier] =
        under_both(&server, &credential, &["list", OAUTH_GRANT_ID, "--json"]);
    the_same_requests(server);

    assert_eq!(integration.status.code(), Some(0));
    assert_eq!(earlier.status.code(), Some(0));
    let mut document = json_output(&integration);
    let earlier_document = json_output(&earlier);
    assert_eq!(document["grant"]["side"], "member", "{document}");
    assert!(earlier_document["grant"].get("side").is_none());
    document["grant"].as_object_mut().unwrap().remove("side");
    assert_eq!(document, earlier_document);
    assert!(integration.stderr.is_empty());
    assert_eq!(after_notice(&earlier, "useful integration list"), "");
}

/// A leaf's arguments, the answers to one run of it and the command its notice names.
type Acting = (&'static [&'static str], Vec<Vec<u8>>, &'static str);

// REQ-INTEGRATION-13
// REQ-INTEGRATION-15
#[test]
fn authorize_deauthorize_and_revoke_act_and_print_as_their_earlier_spelling_does() {
    let held = || oauth_grant(OAUTH_GRANT_ID, "github", "active");
    let cases: [Acting; 3] = [
        (
            &["authorize", OAUTH_GRANT_ID, NEW_RUNNER_ID, "--yes"],
            vec![one(&held()), authorized(OAUTH_GRANT_ID, NEW_RUNNER_ID)],
            "useful integration authorize",
        ),
        (
            &["deauthorize", "github", RUNNER_ID, "--yes"],
            vec![page(&[held()], None), deleted()],
            "useful integration deauthorize",
        ),
        (
            &["revoke", OAUTH_GRANT_ID, "--yes"],
            vec![one(&held()), deleted()],
            "useful integration revoke",
        ),
    ];
    for (args, answers, replacement) in cases {
        let (server, _directory, credential) = prepared([answers.clone(), answers].concat());
        let [integration, earlier] = under_both(&server, &credential, args);
        let requests = the_same_requests(server);

        assert_eq!(requests.len(), 2, "{args:?}");
        assert_eq!(integration.status.code(), Some(0), "{args:?}");
        assert!(!integration.stdout.is_empty(), "{args:?}");
        assert_the_same_but_the_notice(&integration, &earlier, replacement);
    }
}

// REQ-INTEGRATION-13
// REQ-INTEGRATION-15
#[test]
fn connect_begins_the_consent_create_begins() {
    let (server, _directory, credential) =
        prepared(vec![nothing_held(), started(), nothing_held(), started()]);
    let opener = Opener::new(0);
    let [integration, earlier] = [["integration", "connect"], ["grant", "create"]].map(|leaf| {
        let output = opener_command(
            &server,
            &credential,
            &opener,
            Place::Remote,
            &[&leaf[..], &["linear", "--no-wait", "--json"]].concat(),
        )
        .stdin(Stdio::null())
        .output()
        .unwrap();
        assert_no_secret_output(&output, &[SENTINEL, TOKEN]);
        output
    });
    let requests = the_same_requests(server);

    assert_eq!(
        requests
            .iter()
            .map(|(line, _)| line.as_str())
            .collect::<Vec<_>>(),
        [LIST_OAUTH, START_CONSENT]
    );
    assert_eq!(integration.status.code(), Some(0));
    assert_the_same_but_the_notice(&integration, &earlier, "useful integration connect");
}

// REQ-INTEGRATION-15
#[test]
fn every_next_action_and_refusal_names_the_integration_commands_under_either_spelling() {
    // An active GitHub grant no runner is authorized for, and nothing of Linear, which is served;
    // `useful integration list` also reads the memberships of a member of no organization.
    let mut responses = Vec::new();
    for integration in [true, false, true, false] {
        responses.extend([
            page(&[oauth_grant_unauthorized(OAUTH_GRANT_ID)], None),
            predating_served(),
            identities(ada()),
        ]);
        if integration {
            responses.push(memberships(&[]));
        }
    }
    let (server, _directory, credential) = prepared(responses);
    let json = under_both(&server, &credential, &["list", "--json"]);
    let human = under_both(&server, &credential, &["list"]);
    server.finish();

    for output in &json {
        assert_eq!(output.status.code(), Some(0));
        let document = json_output(output);
        assert_eq!(document["grants"][0]["id"], OAUTH_GRANT_ID);
        assert_eq!(document["grants"][0]["nextAction"], "authorize_runner");
        let linear = document["notConnected"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["provider"] == "linear")
            .unwrap_or_else(|| panic!("no linear in {document}"));
        assert_eq!(linear["nextAction"], "connect");
    }
    for output in &human {
        assert_eq!(output.status.code(), Some(0));
        let text = String::from_utf8(output.stdout.clone()).unwrap();
        let commands: Vec<_> = text
            .lines()
            .filter(|line| line.contains("next action:"))
            .flat_map(|line| line.split('`').skip(1).step_by(2))
            .collect();
        for named in [
            format!("useful integration authorize {OAUTH_GRANT_ID} RUNNER --yes"),
            "useful integration connect linear".to_owned(),
        ] {
            assert!(commands.contains(&named.as_str()), "{named} not in\n{text}");
        }
        assert!(!text.contains("useful grant "), "{text}");
    }

    // The earlier spelling's refusal, made before the leaf runs, names the command a member is
    // to run.
    let (server, _directory, credential) = prepared(vec![]);
    let refused = run_under("grant", &server, &credential, &["revoke", OAUTH_GRANT_ID]);
    server.finish();
    assert_eq!(refused.status.code(), Some(2));
    assert!(refused.stdout.is_empty());
    let refusal = String::from_utf8(refused.stderr).unwrap();
    assert!(refusal.contains("--yes"), "{refusal}");
    assert!(refusal.contains("useful integration revoke"), "{refusal}");
    assert!(!refusal.contains("useful grant "), "{refusal}");
}

// `useful integration connect` for a token service: the token read from a file, standard input
// or a prompt that does not echo, for the member or with `--org` for an organization.

/// The token the member gives in these cases, which no output may carry.
const KNOWN_TOKEN: &str = "mergify-known-token-7f3a9c";
const TOKEN_ID: &str = "itk_01k0z6r1w8f4jy2m7q9v3x5abc";
const PRINCIPAL_ID: &str = "prn_01k0z6r1w8f4jy2m7q9v3x5abc";
const LIST_MEMBER_TOKENS: &str = "GET /api/v1/me/integration-tokens?limit=200 HTTP/1.1";
const STORE_MEMBER_TOKEN: &str = "POST /api/v1/me/integration-tokens HTTP/1.1";
const LIST_ACME_TOKENS: &str =
    "GET /api/v1/organizations/acme/integration-tokens?limit=200 HTTP/1.1";
const STORE_ACME_TOKEN: &str = "POST /api/v1/organizations/acme/integration-tokens HTTP/1.1";
/// What the prompt for a token shows.
const HIDDEN: &str = "(input is hidden)";

/// The answer of a deployment that serves the five, the OAuth provider `example` and, with
/// `token`, `mergify` as a token service titled Mergify (REQ-GRANT-78).
fn serving_with(token: bool) -> Vec<u8> {
    let mut items = vec![
        serde_json::json!({"provider": "github", "kind": "oauth"}),
        serde_json::json!({"provider": "gitlab.com", "kind": "oauth"}),
        serde_json::json!({"provider": "linear", "kind": "oauth"}),
        serde_json::json!({"provider": "example", "kind": "oauth"}),
        serde_json::json!({"provider": "claude_code", "kind": "native"}),
        serde_json::json!({"provider": "codex", "kind": "native"}),
    ];
    if token {
        items.push(serde_json::json!({
            "provider": "mergify",
            "kind": "token",
            "displayName": "Mergify"
        }));
    }
    json_http_response("200 OK", serde_json::json!({"items": items}))
}

/// The `mergify` token of `side` at `version`, carrying in a member no model names what no
/// answer may carry.
fn mergify_token(side: &str, version: i64) -> serde_json::Value {
    let mut token = serde_json::json!({
        "id": TOKEN_ID,
        "provider": "mergify",
        "side": side,
        "version": version,
        "connectedBy": PRINCIPAL_ID,
        "connectedAt": "2026-11-18T09:00:00Z",
        "createdAt": "2026-11-17T09:00:00Z",
        "authorizations": [],
        "token": SENTINEL
    });
    if side == "org" {
        token["organizationId"] = ORGANIZATION_ID.into();
    }
    token
}

fn tokens(items: &[serde_json::Value]) -> Vec<u8> {
    json_http_response("200 OK", serde_json::json!({"items": items}))
}

/// The answer to a store, which echoes the request's key.
fn stored_token(status: &str, side: &str, version: i64) -> Vec<u8> {
    http_response_with_headers(
        status,
        Some("application/json"),
        &[("Idempotency-Key", ECHO_IDEMPOTENCY_KEY)],
        &serde_json::to_vec(&mergify_token(side, version)).unwrap(),
    )
}

/// Runs `useful integration connect args` with `input` on standard input, which is no terminal.
fn connect_with_input(
    server: &ScriptedServer,
    credential: &Path,
    args: &[&str],
    input: &[u8],
) -> Output {
    let mut arguments = vec!["integration", "connect"];
    arguments.extend_from_slice(args);
    arguments.push("--allow-insecure-http");
    let environment = deployment_environment_with_issuer(
        &server.api_url,
        &server.issuer,
        credential.to_str().unwrap(),
    );
    let output = run_with_stdin(&arguments, &environment, input);
    assert_no_secret_output(&output, &[SENTINEL, TOKEN, KNOWN_TOKEN]);
    output
}

/// `useful integration connect args`, for a pseudo-terminal to run.
fn connect_on_terminal(server: &ScriptedServer, credential: &Path, args: &[&str]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_useful"));
    command
        .args(["integration", "connect"])
        .args(args)
        .arg("--allow-insecure-http");
    for variable in DEPLOYMENT_VARIABLES
        .into_iter()
        .chain(RUNNER_TELEMETRY_VARIABLES)
    {
        command.env_remove(variable);
    }
    command.envs(deployment_environment_with_issuer(
        &server.api_url,
        &server.issuer,
        credential.to_str().unwrap(),
    ));
    command
}

/// A connect refused once the owner's tokens are read: its arguments, its standard input and
/// the variables set besides the deployment's.
type Refused<'a> = (&'a [&'a str], &'a [u8], &'a [(&'a str, &'a str)]);

/// The request lines of `requests`.
fn lines(requests: &[(String, String)]) -> Vec<&str> {
    requests.iter().map(|(line, _)| line.as_str()).collect()
}

/// The one JSON document of a command's standard output.
fn one_document(output: &Output) -> serde_json::Value {
    let text = String::from_utf8(output.stdout.clone()).unwrap();
    assert_eq!(text.trim_end().lines().count(), 1, "{text}");
    serde_json::from_str(&text).unwrap()
}

/// Whether a human report names the token's ID and `version`, as its data fields do.
fn reports(output: &Output, version: i64) -> bool {
    let text = String::from_utf8_lossy(&output.stdout);
    text.lines()
        .any(|line| line == format!("token: {TOKEN_ID}"))
        && text
            .lines()
            .any(|line| line == format!("version: {version}"))
}

// REQ-INTEGRATION-16
#[test]
fn connect_stores_a_token_given_in_a_file_on_standard_input_or_at_a_prompt_that_does_not_echo() {
    let directory = tempfile::tempdir().unwrap();
    let file = private_file(
        directory.path(),
        "mergify.token",
        format!("{KNOWN_TOKEN}\n").as_bytes(),
    );
    let file = file.to_str().unwrap();
    let typed = format!("{KNOWN_TOKEN}\n");
    let answers = || {
        vec![
            serving_with(true),
            tokens(&[]),
            stored_token("201 Created", "member", 1),
        ]
    };
    let cases: [(&[&str], Option<&[u8]>); 4] = [
        (&["mergify", "--from-file", file], Some(b"")),
        (
            &["mergify", "--from-file", "-", "--json"],
            Some(typed.as_bytes()),
        ),
        (&["mergify"], Some(typed.as_bytes())),
        (&["mergify", "--json"], None),
    ];
    for (args, input) in cases {
        let (server, _credentials, credential) = prepared(answers());
        let output = match input {
            Some(input) => connect_with_input(&server, &credential, args, input),
            None => {
                let command = connect_on_terminal(&server, &credential, args);
                let (transcript, output) = on_terminal(command, &[(HIDDEN, typed.as_str())]);
                assert!(!transcript.contains(KNOWN_TOKEN), "{transcript}");
                assert_no_secret_output(&output, &[SENTINEL, TOKEN, KNOWN_TOKEN]);
                output
            }
        };
        let requests = server.finish();

        assert_eq!(
            output.status.code(),
            Some(0),
            "{args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let asked = asked(&requests);
        assert_eq!(
            lines(&asked),
            [READ_SERVED, LIST_MEMBER_TOKENS, STORE_MEMBER_TOKEN],
            "{args:?}"
        );
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&asked[2].1).unwrap(),
            serde_json::json!({"provider": "mergify", "token": KNOWN_TOKEN}),
            "{args:?}"
        );
        assert!(!header_value(&requests[2], "idempotency-key").is_empty());
        if args.contains(&"--json") {
            let document = one_document(&output);
            assert_eq!(document["schemaVersion"], 1, "{document}");
            assert_eq!(document["outcome"], "connected", "{document}");
            assert_eq!(document["token"]["id"], TOKEN_ID, "{document}");
            assert_eq!(document["token"]["service"], "mergify", "{document}");
            assert_eq!(document["token"]["side"], "member", "{document}");
            assert_eq!(document["token"]["version"], 1, "{document}");
        } else {
            assert!(reports(&output, 1), "{args:?}");
        }
    }
}

// REQ-INTEGRATION-16
#[test]
fn connect_refuses_a_token_service_it_cannot_take_before_anything_is_read_or_stored() {
    // A deployment that serves no token service, or predates the read, is asked that alone,
    // and nothing is asked on a terminal.
    for served in [serving_with(false), predating_served()] {
        let (server, _credentials, credential) = prepared(vec![served.clone(), served]);
        let refused = connect_with_input(&server, &credential, &["mergify"], b"");
        let command = connect_on_terminal(&server, &credential, &["mergify"]);
        let (transcript, on_a_terminal) = on_terminal(command, &[]);
        let requests = asked(&server.finish());

        assert_eq!(refused.status.code(), Some(2));
        assert!(refused.stdout.is_empty());
        assert_eq!(on_a_terminal.status.code(), Some(2));
        assert!(!transcript.contains(HIDDEN), "{transcript}");
        assert_eq!(lines(&requests), [READ_SERVED, READ_SERVED]);
    }

    // A token outside the form, or a file another user could read, is refused once the owner's
    // tokens are read, and nothing is stored; a token in the environment is not read.
    let directory = tempfile::tempdir().unwrap();
    let readable = private_file(directory.path(), "readable.token", KNOWN_TOKEN.as_bytes());
    fs::set_permissions(&readable, Permissions::from_mode(0o644)).unwrap();
    let readable = readable.to_str().unwrap();
    let too_long = "a".repeat(4097);
    let cases: [Refused; 5] = [
        (&["mergify"], b"", &[]),
        (&["mergify"], b"mergify token\n", &[]),
        (&["mergify"], too_long.as_bytes(), &[]),
        (&["mergify", "--from-file", readable], b"", &[]),
        (&["mergify"], b"", &[("MERGIFY_TOKEN", KNOWN_TOKEN)]),
    ];
    for (args, input, variables) in cases {
        let (server, _credentials, credential) = prepared(vec![serving_with(true), tokens(&[])]);
        let mut arguments = vec!["integration", "connect"];
        arguments.extend_from_slice(args);
        arguments.push("--allow-insecure-http");
        let mut environment = deployment_environment_with_issuer(
            &server.api_url,
            &server.issuer,
            credential.to_str().unwrap(),
        )
        .to_vec();
        environment.extend_from_slice(variables);
        let output = run_with_stdin(&arguments, &environment, input);
        let requests = asked(&server.finish());

        assert_no_secret_output(&output, &[SENTINEL, TOKEN, KNOWN_TOKEN, too_long.as_str()]);
        assert_eq!(output.status.code(), Some(2), "{args:?} {variables:?}");
        assert!(output.stdout.is_empty(), "{args:?}");
        assert_eq!(
            lines(&requests),
            [READ_SERVED, LIST_MEMBER_TOKENS],
            "{args:?}"
        );
    }

    // The options of a sign-in are refused once the deployment is found to serve a token
    // service, before its tokens or the token are read.
    let token_file = private_file(directory.path(), "token", KNOWN_TOKEN.as_bytes());
    let token_file = token_file.to_str().unwrap();
    for option in [
        &["--login"][..],
        &["--device"],
        &["--token-file", token_file],
        &["--no-wait"],
        &["--timeout", "30s"],
        &["--organization", "acme"],
    ] {
        let (server, _credentials, credential) = prepared(vec![serving_with(true)]);
        let args = [&["mergify"][..], option].concat();
        let output = connect_with_input(&server, &credential, &args, KNOWN_TOKEN.as_bytes());

        assert_eq!(output.status.code(), Some(2), "{option:?}");
        assert!(output.stdout.is_empty(), "{option:?}");
        assert_eq!(lines(&asked(&server.finish())), [READ_SERVED], "{option:?}");
    }

    // The earlier spelling takes no token service and sends nothing.
    let (server, _credentials, credential) = prepared(vec![]);
    let earlier = run_under(
        "grant",
        &server,
        &credential,
        &["create", "mergify", "--from-file", token_file],
    );
    assert_eq!(earlier.status.code(), Some(2));
    assert!(server.finish().is_empty());

    // Signing in to each provider the member lacks selects no token service.
    for every in [&["connect", "--json"][..], &["connect", "--all", "--json"]] {
        let (server, _credentials, credential) = prepared(vec![
            holding(&["github", "gitlab.com", "linear", "example"], true),
            serving_with(true),
        ]);
        let output = run_under("integration", &server, &credential, every);
        let requests = asked(&server.finish());

        assert_eq!(output.status.code(), Some(0), "{every:?}");
        assert_eq!(
            lines(&requests),
            [LIST_EVERY_KIND, READ_SERVED],
            "{every:?}"
        );
        let text = String::from_utf8(output.stdout).unwrap();
        assert!(!text.contains("mergify"), "{text}");
    }
}

// REQ-INTEGRATION-13
// REQ-INTEGRATION-16
// Under `useful integration` a name outside the five is read for among what the deployment
// serves before the leaf acts, where the earlier spelling judges its options first.
#[test]
fn connect_of_a_served_oauth_provider_begins_the_consent_create_begins() {
    let consent = || json_http_response("201 Created", consent_of("example", "pending"));
    let (server, _directory, credential) = prepared(vec![
        serving_with(true),
        nothing_held(),
        consent(),
        serving_with(true),
        nothing_held(),
        consent(),
    ]);
    let opener = Opener::new(0);
    let [integration, earlier] = [["integration", "connect"], ["grant", "create"]].map(|leaf| {
        let output = opener_command(
            &server,
            &credential,
            &opener,
            Place::Remote,
            &[&leaf[..], &["example", "--no-wait", "--json"]].concat(),
        )
        .stdin(Stdio::null())
        .output()
        .unwrap();
        assert_no_secret_output(&output, &[SENTINEL, TOKEN]);
        output
    });
    let requests = the_same_requests(server);

    assert_eq!(lines(&requests), [READ_SERVED, LIST_OAUTH, START_CONSENT]);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&requests[2].1).unwrap(),
        serde_json::json!({"provider": "example"})
    );
    assert_eq!(
        integration.status.code(),
        Some(0),
        "{}",
        stderr(&integration)
    );
    assert_the_same_but_the_notice(&integration, &earlier, "useful integration connect");

    // An option an OAuth provider does not take is refused once the read names it one, and a
    // name the deployment serves as nothing is refused naming the token service it could be.
    let directory = tempfile::tempdir().unwrap();
    let file = private_file(directory.path(), "example.token", KNOWN_TOKEN.as_bytes());
    let (server, _credentials, credential) = prepared(vec![serving_with(true), serving_with(true)]);
    let unsuited = connect_with_input(
        &server,
        &credential,
        &["example", "--from-file", file.to_str().unwrap()],
        b"",
    );
    let unserved = connect_with_input(&server, &credential, &["bitbucket"], b"");
    let requests = asked(&server.finish());

    for refused in [&unsuited, &unserved] {
        assert_eq!(refused.status.code(), Some(2), "{}", stderr(refused));
        assert!(refused.stdout.is_empty());
    }
    let refusal = stderr(&unserved);
    for name in ["example", "mergify"] {
        assert!(refusal.contains(name), "{name}: {refusal}");
    }
    assert_eq!(lines(&requests), [READ_SERVED, READ_SERVED]);
}

// REQ-INTEGRATION-18
#[test]
fn connect_settles_replacing_a_held_token_before_it_reads_the_new_one() {
    let held = || tokens(&[mergify_token("member", 1)]);

    // Off a terminal, a held token is replaced only with --replace.
    let (server, _credentials, credential) = prepared(vec![
        serving_with(true),
        held(),
        serving_with(true),
        held(),
        stored_token("200 OK", "member", 2),
    ]);
    let input = format!("{KNOWN_TOKEN}\n");
    let refused = connect_with_input(&server, &credential, &["mergify"], input.as_bytes());
    let replaced = connect_with_input(
        &server,
        &credential,
        &["mergify", "--replace", "--json"],
        input.as_bytes(),
    );
    let requests = asked(&server.finish());

    assert_eq!(refused.status.code(), Some(2));
    assert!(refused.stdout.is_empty());
    let refusal = String::from_utf8_lossy(&refused.stderr);
    assert!(refusal.contains(TOKEN_ID), "{refusal}");
    assert!(refusal.contains("--replace"), "{refusal}");
    assert_eq!(replaced.status.code(), Some(0));
    let document = one_document(&replaced);
    assert_eq!(document["outcome"], "replaced", "{document}");
    assert_eq!(document["token"]["id"], TOKEN_ID, "{document}");
    assert_eq!(document["token"]["version"], 2, "{document}");
    assert_eq!(
        lines(&requests),
        [
            READ_SERVED,
            LIST_MEMBER_TOKENS,
            READ_SERVED,
            LIST_MEMBER_TOKENS,
            STORE_MEMBER_TOKEN
        ]
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&requests[4].1).unwrap(),
        serde_json::json!({"provider": "mergify", "token": KNOWN_TOKEN, "replace": true})
    );

    // On a terminal it asks first: no keeps the token and asks for no other, yes asks for the
    // new token and replaces it.
    let (server, _credentials, credential) = prepared(vec![serving_with(true), held()]);
    let (question, kept) = answered(
        connect_on_terminal(&server, &credential, &["mergify"]),
        "n\n",
    );
    assert_eq!(kept.status.code(), Some(2));
    assert!(question.contains(TOKEN_ID), "{question}");
    assert_eq!(
        lines(&asked(&server.finish())),
        [READ_SERVED, LIST_MEMBER_TOKENS]
    );
    let (server, _credentials, credential) = prepared(vec![serving_with(true), held()]);
    let (transcript, kept) = on_terminal(
        connect_on_terminal(&server, &credential, &["mergify"]),
        &[("[y/N]", "n\n")],
    );
    server.finish();
    assert_eq!(kept.status.code(), Some(2));
    assert!(!transcript.contains(HIDDEN), "{transcript}");

    let (server, _credentials, credential) = prepared(vec![
        serving_with(true),
        held(),
        stored_token("200 OK", "member", 2),
    ]);
    let (transcript, replaced) = on_terminal(
        connect_on_terminal(&server, &credential, &["mergify"]),
        &[("[y/N]", "y\n"), (HIDDEN, input.as_str())],
    );
    assert_no_secret_output(&replaced, &[SENTINEL, TOKEN, KNOWN_TOKEN]);
    assert!(!transcript.contains(KNOWN_TOKEN), "{transcript}");
    assert_eq!(replaced.status.code(), Some(0), "{transcript}");
    assert!(reports(&replaced, 2));
    let requests = asked(&server.finish());
    assert_eq!(
        lines(&requests),
        [READ_SERVED, LIST_MEMBER_TOKENS, STORE_MEMBER_TOKEN]
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&requests[2].1).unwrap()["replace"],
        true
    );
}

// REQ-INTEGRATION-17
#[test]
fn connect_with_org_stores_the_organizations_token_and_is_refused_for_a_grants_provider() {
    let directory = tempfile::tempdir().unwrap();
    let file = private_file(directory.path(), "mergify.token", KNOWN_TOKEN.as_bytes());
    let file = file.to_str().unwrap();
    let args = ["mergify", "--org", "acme", "--from-file", file];

    let (server, _credentials, credential) = prepared(vec![
        serving_with(true),
        tokens(&[]),
        stored_token("201 Created", "org", 1),
    ]);
    let stored = connect_with_input(
        &server,
        &credential,
        &[&args[..], &["--json"]].concat(),
        b"",
    );
    let requests = asked(&server.finish());
    assert_eq!(
        stored.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&stored.stderr)
    );
    let document = one_document(&stored);
    assert_eq!(document["outcome"], "connected", "{document}");
    assert_eq!(document["token"]["side"], "org", "{document}");
    assert_eq!(
        document["token"]["organization"]["id"], ORGANIZATION_ID,
        "{document}"
    );
    assert_eq!(
        lines(&requests),
        [READ_SERVED, LIST_ACME_TOKENS, STORE_ACME_TOKEN]
    );

    // A member who is not an owner is refused by Cloud, and nothing follows the refusal.
    let (server, _credentials, credential) = prepared(vec![
        serving_with(true),
        tokens(&[]),
        problem("403 Forbidden", 403, "forbidden"),
    ]);
    let forbidden = connect_with_input(&server, &credential, &args, b"");
    let requests = asked(&server.finish());
    assert_eq!(forbidden.status.code(), Some(1));
    assert!(forbidden.stdout.is_empty());
    assert_eq!(
        lines(&requests),
        [READ_SERVED, LIST_ACME_TOKENS, STORE_ACME_TOKEN]
    );

    // A grant's provider takes no --org: a coding agent and GitLab.com before any request,
    // another OAuth provider once the served read names it one. GitHub and Linear connect the
    // organization (REQ-INTEGRATION-21).
    let (server, _credentials, credential) = prepared(vec![serving_with(true)]);
    for provider in ["codex", "claude_code", "gitlab.com"] {
        let output = connect_with_input(&server, &credential, &[provider, "--org", "acme"], b"");
        assert_eq!(output.status.code(), Some(2), "{provider}");
        assert!(output.stdout.is_empty(), "{provider}");
    }
    let example = connect_with_input(&server, &credential, &["example", "--org", "acme"], b"");
    assert_eq!(example.status.code(), Some(2));
    let without_provider = connect_with_input(&server, &credential, &["--org", "acme"], b"");
    assert_eq!(without_provider.status.code(), Some(2));
    assert_eq!(lines(&asked(&server.finish())), [READ_SERVED]);
}

// `useful integration connect github --org` and `connect linear --org`, which connect the
// organization through the `useful connection` commands (REQ-INTEGRATION-21).

const BEGIN_ACME_SETUP: &str = "POST /api/v1/organizations/acme/github/setup-sessions HTTP/1.1";
const START_ACME_AUTHORIZATION: &str =
    "POST /api/v1/organizations/acme/connections/linear/authorization-sessions HTTP/1.1";

/// Runs `useful integration connect args` and then the `useful connection` command `connection`
/// against one server that gives each the same `answers`: the two exit alike and print the same
/// on standard output, and send the same requests, which are returned and of which none is the
/// member's (REQ-INTEGRATION-21).
fn beside_connection(
    answers: &[Vec<u8>],
    args: &[&str],
    connection: &[&str],
) -> (Output, Vec<(String, String)>) {
    let (server, _directory, credential) = prepared([answers, answers].concat());
    let integration = run_under(
        "integration",
        &server,
        &credential,
        &[&["connect"], args].concat(),
    );
    let connected = run_under("connection", &server, &credential, connection);
    assert_eq!(
        integration.status.code(),
        connected.status.code(),
        "{}",
        stderr(&integration)
    );
    assert_eq!(
        String::from_utf8_lossy(&integration.stdout),
        String::from_utf8_lossy(&connected.stdout)
    );
    let requests = the_same_requests(server);
    assert!(
        requests
            .iter()
            .all(|(line, _)| !line.contains(" /api/v1/me/")),
        "{requests:?}"
    );
    (integration, requests)
}

// REQ-INTEGRATION-21
#[test]
fn connect_github_with_org_begins_the_setup_connection_github_setup_begin_begins() {
    let begun = || json_http_response("201 Created", super::github::setup_session());
    for json in [&[][..], &["--json"]] {
        let (output, requests) = beside_connection(
            &[begun()],
            &[&["github", "--org", "acme"], json].concat(),
            &[&["github", "setup", "begin", "acme"], json].concat(),
        );
        assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));
        assert!(!output.stdout.is_empty());
        assert_eq!(lines(&requests), [BEGIN_ACME_SETUP]);
    }

    // Cloud's refusal of a member who is not an owner is the setup command's.
    let forbidden = || problem("403 Forbidden", 403, "forbidden");
    for json in [&[][..], &["--json"]] {
        let (output, requests) = beside_connection(
            &[forbidden()],
            &[&["github", "--org", "acme"], json].concat(),
            &[&["github", "setup", "begin", "acme"], json].concat(),
        );
        assert_ne!(output.status.code(), Some(0));
        assert_eq!(lines(&requests), [BEGIN_ACME_SETUP]);
    }
}

// REQ-INTEGRATION-21
#[test]
fn connect_linear_with_org_starts_the_authorization_of_a_new_workspace() {
    use super::connection::{completed_session, session};

    let pending = || json_http_response("201 Created", session("pending"));
    let (output, requests) = beside_connection(
        &[pending()],
        &["linear", "--org", "acme", "--no-wait", "--json"],
        &[
            "linear",
            "authorization",
            "create",
            "acme",
            "--no-wait",
            "--json",
        ],
    );
    assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));
    assert_eq!(lines(&requests), [START_ACME_AUTHORIZATION]);
    let body = serde_json::from_str::<serde_json::Value>(&requests[0].1).unwrap();
    assert_eq!(body["operation"], "connect", "{body}");
    assert!(body.get("connectionId").is_none(), "{body}");
    assert!(body.get("enableStateUpdates").is_none(), "{body}");

    // Without --no-wait it waits for the session as the authorization command does.
    for json in [&[][..], &["--json"]] {
        let (output, requests) = beside_connection(
            &[pending(), json_http_response("200 OK", completed_session())],
            &[&["linear", "--org", "acme"], json].concat(),
            &[&["linear", "authorization", "create", "acme"], json].concat(),
        );
        assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));
        assert_eq!(requests.len(), 2, "{requests:?}");
        assert_eq!(requests[0].0, START_ACME_AUTHORIZATION);
        assert!(requests[1].0.starts_with("GET "), "{requests:?}");
        if !json.is_empty() {
            let outcomes: Vec<_> = String::from_utf8_lossy(&output.stdout)
                .lines()
                .map(|line| {
                    serde_json::from_str::<serde_json::Value>(line).unwrap()["outcome"].clone()
                })
                .collect();
            assert_eq!(outcomes, ["pending", "completed"]);
        }
    }

    // Cloud's refusal of a member who is not an owner is the authorization command's, and
    // nothing follows it.
    for json in [&[][..], &["--json"]] {
        let (output, requests) = beside_connection(
            &[problem("403 Forbidden", 403, "forbidden")],
            &[&["linear", "--org", "acme"], json].concat(),
            &[&["linear", "authorization", "create", "acme"], json].concat(),
        );
        assert_ne!(output.status.code(), Some(0));
        assert_eq!(lines(&requests), [START_ACME_AUTHORIZATION]);
    }
}

// REQ-INTEGRATION-21
#[test]
fn connect_with_org_refuses_a_members_sign_in_option_and_gitlab_before_any_request() {
    let missing = tempfile::tempdir().unwrap();
    let missing = missing.path().join("absent");
    let missing = missing.to_str().unwrap();
    let (server, _directory, credential) = prepared(vec![]);
    let refused = |args: &[&str]| {
        let output = connect_with_input(&server, &credential, args, b"");
        assert_eq!(
            output.status.code(),
            Some(2),
            "{args:?} {}",
            stderr(&output)
        );
        assert!(output.stdout.is_empty(), "{args:?}");
        stderr(&output)
    };
    let sign_in: [&[&str]; 6] = [
        &["--organization", "acme"],
        &["--login"],
        &["--device"],
        &["--from-file", missing],
        &["--token-file", missing],
        &["--replace"],
    ];
    for provider in ["github", "linear"] {
        let connects = format!("useful integration connect {provider} --org ORGANIZATION");
        for option in sign_in {
            let error = refused(&[&[provider, "--org", "acme"], option].concat());
            assert!(error.contains(&connects), "{option:?} {error}");
        }
    }
    let replace = refused(&["linear", "--org", "acme", "--replace"]);
    assert!(
        replace.contains(
            "useful connection linear authorization create ORGANIZATION --connection-id CONNECTION"
        ),
        "{replace}"
    );
    for option in [&["--no-wait"][..], &["--timeout", "30s"]] {
        let error = refused(&[&["github", "--org", "acme"], option].concat());
        assert!(
            error.contains("useful integration connect github --org ORGANIZATION"),
            "{option:?} {error}"
        );
    }
    let gitlab = refused(&["gitlab.com", "--org", "acme"]);
    assert!(gitlab.contains("useful connection gitlab"), "{gitlab}");
    for provider in ["codex", "claude_code"] {
        assert!(refused(&[provider, "--org", "acme"]).contains("--organization"));
    }

    // An organization's connection is authorized for no runner, and revoke names the command
    // that removes it.
    for (provider, removes) in [
        ("github", "useful connection github installation remove"),
        ("gitlab.com", "useful connection gitlab remove"),
        ("linear", "useful connection linear remove"),
    ] {
        for args in [
            &["revoke", provider, "--yes"][..],
            &["authorize", provider, NEW_RUNNER_ID, "--yes"],
            &["deauthorize", provider, RUNNER_ID, "--yes"],
        ] {
            let output = for_acme(&server, &credential, args);
            assert_eq!(output.status.code(), Some(2), "{args:?}");
            assert!(output.stdout.is_empty(), "{args:?}");
            if args[0] == "revoke" {
                assert!(
                    stderr(&output).contains(removes),
                    "{args:?} {}",
                    stderr(&output)
                );
            }
        }
    }
    assert!(server.finish().is_empty());
}

// `useful integration list` with the member's and their organizations' tokens.

const BUILDKITE_TOKEN_ID: &str = "itk_01k0z6r1w8f4jy2m7q9v3x5abd";
/// Another member of acme, who connected its Buildkite token.
const OTHER_PRINCIPAL_ID: &str = "prn_01k0z6r1w8f4jy2m7q9v3x5abd";
const OTHER_PRINCIPAL_NAME: &str = "Grace Hopper";
const RUNNER_NAME: &str = "build-1";
const LIST_ORGANIZATION_TOKENS: &str = "GET /api/v1/organizations/org_01k0z6r1w8f4jy2m7q9v3x5abc/integration-tokens?limit=200 HTTP/1.1";
const READ_DIRECTORY: &str =
    "GET /api/v1/organizations/org_01k0z6r1w8f4jy2m7q9v3x5abc/memberships?limit=200 HTTP/1.1";
const READ_ACME_DIRECTORY: &str = "GET /api/v1/organizations/acme/memberships?limit=200 HTTP/1.1";

/// The answer of a deployment that serves GitHub, Codex and the token services `mergify` and
/// `buildkite` (REQ-GRANT-78).
fn serving_tokens() -> Vec<u8> {
    json_http_response(
        "200 OK",
        serde_json::json!({"items": [
            {"provider": "github", "kind": "oauth"},
            {"provider": "codex", "kind": "native"},
            {"provider": "mergify", "kind": "token", "displayName": "Mergify"},
            {"provider": "buildkite", "kind": "token", "displayName": "Buildkite"}
        ]}),
    )
}

/// The member's `mergify` token, authorized for one runner of acme.
fn authorized_mergify() -> serde_json::Value {
    let mut token = mergify_token("member", 1);
    token["authorizations"] = serde_json::json!([{
        "id": "ita_01k0z6r1w8f4jy2m7q9v3x5abc",
        "runnerId": RUNNER_ID,
        "runnerName": RUNNER_NAME,
        "organizationId": ORGANIZATION_ID,
        "createdAt": "2026-11-18T09:05:00Z"
    }]);
    token
}

/// Acme's `buildkite` token at version 2, which another member connected, carrying in a member
/// no model names what no answer may carry.
fn acme_buildkite() -> serde_json::Value {
    serde_json::json!({
        "id": BUILDKITE_TOKEN_ID,
        "provider": "buildkite",
        "side": "org",
        "organizationId": ORGANIZATION_ID,
        "version": 2,
        "connectedBy": OTHER_PRINCIPAL_ID,
        "connectedAt": "2026-11-16T09:00:00Z",
        "createdAt": "2026-11-15T09:00:00Z",
        "authorizations": [],
        "token": SENTINEL
    })
}

/// Acme's directory: the member, and the member who connected its Buildkite token.
fn directory() -> Vec<u8> {
    let entry = |id: &str, principal: &str, name: &str, role: &str| {
        serde_json::json!({
            "id": id,
            "principalId": principal,
            "principalType": "human",
            "principalState": "active",
            "displayName": name,
            "state": "active",
            "role": role
        })
    };
    json_http_response(
        "200 OK",
        serde_json::json!({"items": [
            entry("mem_01k0z6r1w8f4jy2m7q9v3x5abc", PRINCIPAL_ID, "Ada", "owner"),
            entry("mem_01k0z6r1w8f4jy2m7q9v3x5abd", OTHER_PRINCIPAL_ID, OTHER_PRINCIPAL_NAME, "member"),
        ]}),
    )
}

/// The answers to `useful integration list` for a member who holds a GitHub grant and a
/// `mergify` token and whose one organization, acme, holds a `buildkite` token: their
/// membership of acme is in `role`, and `connections` answers acme's connection reads.
fn every_side_with(role: &str, connections: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
    let mut answers = vec![
        page(&[oauth_grant(OAUTH_GRANT_ID, "github", "active")], None),
        serving_tokens(),
        identities(ada()),
        tokens(&[authorized_mergify()]),
        memberships_as(&[(ORGANIZATION_ID, "acme")], role),
        tokens(&[acme_buildkite()]),
        directory(),
    ];
    answers.extend(connections);
    answers
}

/// The answers of [`every_side_with`] for an owner of acme, which also holds a GitHub App
/// installation, a Linear connection and a GitLab connection.
fn every_side() -> Vec<Vec<u8>> {
    every_side_with("owner", connections_of_acme())
}

/// What `useful integration list` sends over [`every_side_with`] before acme's connection reads.
const EVERY_SIDE: [&str; 7] = [
    LIST_FIRST_PAGE,
    READ_SERVED,
    READ_IDENTITIES,
    LIST_MEMBER_TOKENS,
    LIST_MEMBERSHIPS,
    LIST_ORGANIZATION_TOKENS,
    READ_DIRECTORY,
];

const INSTALLATION_ID: &str = "ghi_01k0z6r1w8f4jy2m7q9v3x5abc";
const LINEAR_CONNECTION_ID: &str = "lcn_01k0z6r1w8f4jy2m7q9v3x5abc";
const GITLAB_CONNECTION_ID: &str = "glc_01k0z6r1w8f4jy2m7q9v3x5abc";
const LIST_INSTALLATIONS: &str =
    "GET /api/v1/organizations/org_01k0z6r1w8f4jy2m7q9v3x5abc/github/installations HTTP/1.1";
const LIST_LINEAR: &str = "GET /api/v1/organizations/org_01k0z6r1w8f4jy2m7q9v3x5abc/connections/linear?limit=100 HTTP/1.1";
const LIST_GITLAB: &str = "GET /api/v1/organizations/org_01k0z6r1w8f4jy2m7q9v3x5abc/connections/gitlab?limit=200 HTTP/1.1";
const LIST_ACME_INSTALLATIONS: &str =
    "GET /api/v1/organizations/acme/github/installations HTTP/1.1";
const LIST_ACME_LINEAR: &str =
    "GET /api/v1/organizations/acme/connections/linear?limit=100 HTTP/1.1";
const LIST_ACME_GITLAB: &str =
    "GET /api/v1/organizations/acme/connections/gitlab?limit=200 HTTP/1.1";

/// One page of a connection read holding `item`, carrying in a member no model names what no
/// output may carry.
fn holding_connection(mut item: serde_json::Value) -> Vec<u8> {
    item["token"] = SENTINEL.into();
    json_http_response("200 OK", serde_json::json!({"items": [item]}))
}

/// Acme's GitHub App installation on the account `acme`.
fn installations() -> Vec<u8> {
    holding_connection(super::github::installation_body("active"))
}

/// Acme's Linear connection of the workspace `Research`.
fn linear_connections() -> Vec<u8> {
    holding_connection(super::connection::connection())
}

/// Acme's GitLab connection at `https://gitlab.com`, whose bot user is `automation-bot`.
fn gitlab_connections() -> Vec<u8> {
    holding_connection(super::gitlab::active_connection())
}

/// The answers to an owner's three connection reads of acme, in the order they are sent.
fn connections_of_acme() -> Vec<Vec<u8>> {
    vec![installations(), linear_connections(), gitlab_connections()]
}

/// The services of the connections a `useful integration list` document carries, in order.
fn services(document: &serde_json::Value) -> Vec<&str> {
    document["connections"]
        .as_array()
        .unwrap_or_else(|| panic!("no connections in {document}"))
        .iter()
        .map(|connection| connection["service"].as_str().unwrap())
        .collect()
}

/// The position of the line `line` in `text`.
fn line_at(text: &str, line: &str) -> usize {
    text.lines()
        .position(|candidate| candidate == line)
        .unwrap_or_else(|| panic!("{line} not in\n{text}"))
}

// REQ-INTEGRATION-19
#[test]
fn list_prints_the_members_tokens_then_each_organizations_after_the_grants() {
    let (server, _directory, credential) = prepared([every_side(), every_side()].concat());
    let listed = run_under("integration", &server, &credential, &["list", "--json"]);
    let human = run_under("integration", &server, &credential, &["list"]);
    let requests = asked(&server.finish());

    // The organization's connection reads follow its tokens (REQ-INTEGRATION-22).
    let expected = [
        &EVERY_SIDE[..],
        &[LIST_INSTALLATIONS, LIST_LINEAR, LIST_GITLAB],
    ]
    .concat();
    assert_eq!(lines(&requests), [&expected[..], &expected[..]].concat());
    assert_eq!(
        listed.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&listed.stderr)
    );
    let document = json_output(&listed);
    assert_eq!(
        document["grants"].as_array().unwrap().len(),
        1,
        "{document}"
    );
    let listed_tokens = document["tokens"].as_array().unwrap();
    assert_eq!(listed_tokens.len(), 2, "{document}");
    let (member, org) = (&listed_tokens[0], &listed_tokens[1]);
    assert_eq!(member["id"], TOKEN_ID, "{member}");
    assert_eq!(member["service"], "mergify", "{member}");
    assert_eq!(member["side"], "member", "{member}");
    assert!(member.get("organization").is_none(), "{member}");
    assert_eq!(member["version"], 1, "{member}");
    assert_eq!(member["connectedBy"], PRINCIPAL_ID, "{member}");
    assert_eq!(member["connectedAt"], "2026-11-18T09:00:00Z", "{member}");
    assert_eq!(
        member["authorizations"][0]["runnerId"], RUNNER_ID,
        "{member}"
    );
    assert_eq!(
        member["authorizations"][0]["runnerName"], RUNNER_NAME,
        "{member}"
    );
    assert_eq!(org["id"], BUILDKITE_TOKEN_ID, "{org}");
    assert_eq!(org["service"], "buildkite", "{org}");
    assert_eq!(org["side"], "org", "{org}");
    assert_eq!(org["organization"]["id"], ORGANIZATION_ID, "{org}");
    assert_eq!(org["organization"]["slug"], "acme", "{org}");
    assert_eq!(org["version"], 2, "{org}");
    assert_eq!(org["connectedBy"], OTHER_PRINCIPAL_ID, "{org}");
    assert_eq!(org["connectedByName"], OTHER_PRINCIPAL_NAME, "{org}");
    assert_eq!(org["connectedAt"], "2026-11-16T09:00:00Z", "{org}");
    assert_eq!(org["authorizations"], serde_json::json!([]), "{org}");

    // The report prints the same, the tokens after the grant.
    assert_eq!(human.status.code(), Some(0));
    let text = String::from_utf8(human.stdout).unwrap();
    let grant = line_at(&text, &format!("grant: {OAUTH_GRANT_ID}"));
    let first = line_at(&text, &format!("token: {TOKEN_ID}"));
    let second = line_at(&text, &format!("token: {BUILDKITE_TOKEN_ID}"));
    assert!(grant < first && first < second, "{text}");
    for line in [
        format!("runner: {RUNNER_NAME} ({RUNNER_ID})"),
        format!("connected by: {OTHER_PRINCIPAL_NAME} ({OTHER_PRINCIPAL_ID})"),
        "version: 2".to_owned(),
        "service: buildkite".to_owned(),
    ] {
        assert!(line_at(&text, &line) > first, "{line} in\n{text}");
    }
}

// REQ-INTEGRATION-19
// REQ-INTEGRATION-22
#[test]
fn list_reads_no_token_unless_the_deployment_serves_a_token_service() {
    // A deployment that serves none, or predates the read, is sent what the earlier list sends,
    // and then the memberships and each organization's connection reads, but no token list.
    let earlier_reads = [LIST_FIRST_PAGE, READ_SERVED, READ_IDENTITIES];
    for served in [serving_with(false), predating_served()] {
        let answers = || {
            vec![
                page(&[oauth_grant(OAUTH_GRANT_ID, "github", "active")], None),
                served.clone(),
                identities(ada()),
            ]
        };
        let mut integration_answers = answers();
        integration_answers.extend([
            memberships(&[(ORGANIZATION_ID, "acme")]),
            linear_connections(),
            gitlab_connections(),
        ]);
        let (server, _directory, credential) = prepared([integration_answers, answers()].concat());
        let [integration, earlier] = under_both(&server, &credential, &["list", "--json"]);
        let requests = asked(&server.finish());

        assert_eq!(
            lines(&requests),
            [
                &earlier_reads[..],
                &[LIST_MEMBERSHIPS, LIST_LINEAR, LIST_GITLAB],
                &earlier_reads[..]
            ]
            .concat()
        );
        assert_eq!(
            integration.status.code(),
            Some(0),
            "{}",
            stderr(&integration)
        );
        let document = json_output(&integration);
        assert_eq!(document["tokens"], serde_json::json!([]));
        assert_eq!(services(&document), ["linear", "gitlab.com"]);
        let earlier = json_output(&earlier);
        assert!(earlier.get("tokens").is_none(), "{earlier}");
        assert!(earlier.get("connections").is_none(), "{earlier}");
    }

    // The earlier list reads no token where the deployment serves token services.
    let (server, _directory, credential) = prepared(every_side()[..3].to_vec());
    let earlier = run_under("grant", &server, &credential, &["list", "--json"]);
    let requests = asked(&server.finish());
    assert_eq!(earlier.status.code(), Some(0));
    assert_eq!(lines(&requests), earlier_reads);
    let earlier = json_output(&earlier);
    assert!(earlier.get("tokens").is_none(), "{earlier}");
    assert!(earlier.get("connections").is_none(), "{earlier}");
}

// REQ-INTEGRATION-19
// REQ-INTEGRATION-22
#[test]
fn list_with_org_shows_that_side_alone_and_list_of_a_service_shows_its_token() {
    // --org: the organization's tokens and connections, read by the reference as given with no
    // membership read, and no grant; the installations read Cloud refuses is passed over.
    let (server, _directory, credential) = prepared(vec![
        tokens(&[acme_buildkite()]),
        directory(),
        problem("403 Forbidden", 403, "forbidden"),
        linear_connections(),
        gitlab_connections(),
    ]);
    let alone = run_under(
        "integration",
        &server,
        &credential,
        &["list", "--org", "acme", "--json"],
    );
    assert_eq!(
        lines(&asked(&server.finish())),
        [
            LIST_ACME_TOKENS,
            READ_ACME_DIRECTORY,
            LIST_ACME_INSTALLATIONS,
            LIST_ACME_LINEAR,
            LIST_ACME_GITLAB
        ]
    );
    assert_eq!(alone.status.code(), Some(0), "{}", stderr(&alone));
    assert!(alone.stderr.is_empty(), "{}", stderr(&alone));
    let document = json_output(&alone);
    assert!(document.get("grants").is_none(), "{document}");
    assert_eq!(services(&document), ["linear", "gitlab.com"]);
    for connection in document["connections"].as_array().unwrap() {
        assert_eq!(
            connection["organization"],
            serde_json::json!({"id": ORGANIZATION_ID, "slug": "acme"}),
            "{connection}"
        );
    }
    let listed_tokens = document["tokens"].as_array().unwrap();
    assert_eq!(listed_tokens.len(), 1, "{document}");
    assert_eq!(listed_tokens[0]["side"], "org", "{document}");
    assert_eq!(
        listed_tokens[0]["connectedByName"], OTHER_PRINCIPAL_NAME,
        "{document}"
    );

    // A service named: the member's token once no grant of that name is found.
    let (server, _directory, credential) = prepared(vec![
        page(&[oauth_grant(OAUTH_GRANT_ID, "github", "active")], None),
        serving_tokens(),
        tokens(&[authorized_mergify()]),
    ]);
    let shown = run_under(
        "integration",
        &server,
        &credential,
        &["list", "mergify", "--json"],
    );
    assert_eq!(
        lines(&asked(&server.finish())),
        [LIST_FIRST_PAGE, READ_SERVED, LIST_MEMBER_TOKENS]
    );
    assert_eq!(shown.status.code(), Some(0));
    let document = json_output(&shown);
    assert_eq!(document["token"]["id"], TOKEN_ID, "{document}");
    assert_eq!(document["token"]["side"], "member", "{document}");

    // With --org, the organization's token of the service.
    let (server, _directory, credential) = prepared(vec![tokens(&[acme_buildkite()]), directory()]);
    let shown = run_under(
        "integration",
        &server,
        &credential,
        &["list", "buildkite", "--org", "acme", "--json"],
    );
    assert_eq!(
        lines(&asked(&server.finish())),
        [LIST_ACME_TOKENS, READ_ACME_DIRECTORY]
    );
    assert_eq!(shown.status.code(), Some(0));
    let document = json_output(&shown);
    assert_eq!(document["token"]["id"], BUILDKITE_TOKEN_ID, "{document}");
    assert_eq!(document["token"]["version"], 2, "{document}");
    assert_eq!(
        document["token"]["connectedByName"], OTHER_PRINCIPAL_NAME,
        "{document}"
    );

    // A service the organization holds no token of is refused under a code of its own.
    let (server, _directory, credential) = prepared(vec![tokens(&[acme_buildkite()])]);
    let missing = run_under(
        "integration",
        &server,
        &credential,
        &["list", "mergify", "--org", "acme", "--json"],
    );
    assert_eq!(lines(&asked(&server.finish())), [LIST_ACME_TOKENS]);
    assert_eq!(missing.status.code(), Some(1));
    assert_eq!(json_output(&missing)["outcome"], "token_not_found");

    // A name that names neither a grant nor a token of the member is refused as today.
    let (server, _directory, credential) = prepared(vec![
        page(&[oauth_grant(OAUTH_GRANT_ID, "github", "active")], None),
        serving_tokens(),
        tokens(&[]),
    ]);
    let neither = run_under(
        "integration",
        &server,
        &credential,
        &["list", "buildkite", "--json"],
    );
    assert_eq!(
        lines(&asked(&server.finish())),
        [LIST_FIRST_PAGE, READ_SERVED, LIST_MEMBER_TOKENS]
    );
    assert_eq!(neither.status.code(), Some(1));
    assert_eq!(json_output(&neither)["outcome"], "grant_not_found");
}

// REQ-INTEGRATION-19
#[test]
fn list_fails_on_a_failed_token_read_and_leaves_the_id_on_a_failed_directory_read() {
    // A token list that fails fails the command as a failed read of the grants does.
    let refused = || problem("403 Forbidden", 403, "forbidden");
    let mut answers = every_side()[..3].to_vec();
    answers.push(refused());
    answers.push(refused());
    let (server, _directory, credential) = prepared(answers);
    let failed = run_under("integration", &server, &credential, &["list", "--json"]);
    let grants_failed = run_under("grant", &server, &credential, &["list", "--json"]);
    let requests = asked(&server.finish());
    assert_eq!(
        lines(&requests),
        [
            LIST_FIRST_PAGE,
            READ_SERVED,
            READ_IDENTITIES,
            LIST_MEMBER_TOKENS,
            LIST_FIRST_PAGE
        ]
    );
    assert_ne!(failed.status.code(), Some(0));
    assert_eq!(failed.status.code(), grants_failed.status.code());
    assert_eq!(
        json_output(&failed)["outcome"],
        json_output(&grants_failed)["outcome"]
    );

    // A directory that cannot be read names nobody, and the principal is shown by ID.
    let mut answers = every_side();
    answers[6] = problem("404 Not Found", 404, "not-found");
    let (server, _directory, credential) = prepared(answers);
    let listed = run_under("integration", &server, &credential, &["list", "--json"]);
    server.finish();
    assert_eq!(listed.status.code(), Some(0));
    let document = json_output(&listed);
    let org = &document["tokens"][1];
    assert_eq!(org["connectedBy"], OTHER_PRINCIPAL_ID, "{document}");
    assert!(org.get("connectedByName").is_none(), "{document}");
}

// REQ-INTEGRATION-22
#[test]
fn list_prints_each_organizations_connections_after_the_tokens() {
    let (server, _directory, credential) = prepared([every_side(), every_side()].concat());
    let listed = run_under("integration", &server, &credential, &["list", "--json"]);
    let human = run_under("integration", &server, &credential, &["list"]);
    let requests = asked(&server.finish());

    let expected = [
        &EVERY_SIDE[..],
        &[LIST_INSTALLATIONS, LIST_LINEAR, LIST_GITLAB],
    ]
    .concat();
    assert_eq!(lines(&requests), [&expected[..], &expected[..]].concat());
    assert_eq!(listed.status.code(), Some(0), "{}", stderr(&listed));
    assert!(listed.stderr.is_empty());
    let document = json_output(&listed);
    assert_eq!(
        document["tokens"].as_array().unwrap().len(),
        2,
        "{document}"
    );
    assert_eq!(services(&document), ["github", "linear", "gitlab.com"]);
    let shown = [
        (INSTALLATION_ID, "acme", None),
        (LINEAR_CONNECTION_ID, "Research", None),
        (
            GITLAB_CONNECTION_ID,
            "automation-bot",
            Some("https://gitlab.com"),
        ),
    ];
    for (connection, (id, account, instance)) in document["connections"]
        .as_array()
        .unwrap()
        .iter()
        .zip(shown)
    {
        assert_eq!(connection["id"], id, "{connection}");
        assert_eq!(connection["side"], "org", "{connection}");
        assert_eq!(
            connection["organization"],
            serde_json::json!({"id": ORGANIZATION_ID, "slug": "acme"}),
            "{connection}"
        );
        assert_eq!(connection["account"], account, "{connection}");
        assert_eq!(
            connection
                .get("instanceUrl")
                .and_then(serde_json::Value::as_str),
            instance,
            "{connection}"
        );
        assert_eq!(connection["state"], "active", "{connection}");
    }

    // The report prints one section for each, after the tokens and before what is not
    // connected.
    assert_eq!(human.status.code(), Some(0), "{}", stderr(&human));
    let text = String::from_utf8(human.stdout).unwrap();
    let token = line_at(&text, &format!("token: {BUILDKITE_TOKEN_ID}"));
    let sections: Vec<_> = [INSTALLATION_ID, LINEAR_CONNECTION_ID, GITLAB_CONNECTION_ID]
        .iter()
        .map(|id| line_at(&text, &format!("connection: {id}")))
        .collect();
    let not_connected = line_at(&text, "── not connected ──");
    assert!(token < sections[0], "{text}");
    assert!(sections.is_sorted(), "{text}");
    assert!(sections[2] < not_connected, "{text}");
    assert_eq!(
        text.lines()
            .filter(|line| *line == "── connection ──")
            .count(),
        3,
        "{text}"
    );
    for line in [
        "service: gitlab.com",
        "side: org · acme (org_01k0z6r1w8f4jy2m7q9v3x5abc)",
        "account: automation-bot",
        "state: active",
    ] {
        assert!(
            text.lines().skip(sections[2]).any(|shown| shown == line),
            "{line} in\n{text}"
        );
    }
}

// REQ-INTEGRATION-22
#[test]
fn list_reads_the_installations_for_an_owner_alone_and_passes_over_a_read_cloud_refuses_or_cannot_serve()
 {
    // A member who is not an owner is not sent the installations read.
    let (server, _directory, credential) = prepared(every_side_with(
        "member",
        vec![linear_connections(), gitlab_connections()],
    ));
    let listed = run_under("integration", &server, &credential, &["list", "--json"]);
    assert_eq!(
        lines(&asked(&server.finish())),
        [&EVERY_SIDE[..], &[LIST_LINEAR, LIST_GITLAB]].concat()
    );
    assert_eq!(listed.status.code(), Some(0), "{}", stderr(&listed));
    assert_eq!(services(&json_output(&listed)), ["linear", "gitlab.com"]);

    // A read Cloud refuses, or answers that it cannot serve, is passed over and the other reads
    // of the organization are listed.
    for (read, answer, left) in [
        (
            0,
            problem("403 Forbidden", 403, "forbidden"),
            ["linear", "gitlab.com"],
        ),
        (
            0,
            problem("500 Internal Server Error", 500, "internal"),
            ["linear", "gitlab.com"],
        ),
        (
            1,
            problem("503 Service Unavailable", 503, "provider-unavailable"),
            ["github", "gitlab.com"],
        ),
    ] {
        let mut connections = connections_of_acme();
        connections[read] = answer;
        let (server, _directory, credential) = prepared(every_side_with("owner", connections));
        let listed = run_under("integration", &server, &credential, &["list", "--json"]);
        assert_eq!(
            lines(&asked(&server.finish())),
            [
                &EVERY_SIDE[..],
                &[LIST_INSTALLATIONS, LIST_LINEAR, LIST_GITLAB]
            ]
            .concat()
        );
        assert_eq!(listed.status.code(), Some(0), "{}", stderr(&listed));
        assert!(listed.stderr.is_empty(), "{}", stderr(&listed));
        assert_eq!(services(&json_output(&listed)), left);
    }

    // A dropped connection, or an answer outside the contract, fails the command as a failed
    // token read does.
    let outside: fn() -> Vec<u8> = || {
        json_http_response(
            "200 OK",
            serde_json::json!({"items": [{"id": "not-a-connection"}]}),
        )
    };
    for failed in [Vec::new as fn() -> Vec<u8>, outside] {
        let mut answers = every_side()[..3].to_vec();
        answers.push(failed());
        let (server, _directory, credential) = prepared(answers);
        let token_failed = run_under("integration", &server, &credential, &["list", "--json"]);
        server.finish();

        let (server, _directory, credential) =
            prepared(every_side_with("owner", vec![installations(), failed()]));
        let listed = run_under("integration", &server, &credential, &["list", "--json"]);
        assert_eq!(
            lines(&asked(&server.finish())),
            [&EVERY_SIDE[..], &[LIST_INSTALLATIONS, LIST_LINEAR]].concat()
        );
        assert_ne!(listed.status.code(), Some(0));
        assert_eq!(listed.status.code(), token_failed.status.code());
        assert_eq!(
            json_output(&listed)["outcome"],
            json_output(&token_failed)["outcome"]
        );
    }
}

// `useful integration authorize`, `deauthorize` and `revoke` acting on a token service's token,
// the member's or with `--org` the organization's.

const REVOKE_MEMBER_TOKEN: &str =
    "DELETE /api/v1/me/integration-tokens/itk_01k0z6r1w8f4jy2m7q9v3x5abc HTTP/1.1";
const AUTHORIZE_MEMBER_TOKEN: &str =
    "POST /api/v1/me/integration-tokens/itk_01k0z6r1w8f4jy2m7q9v3x5abc/authorizations HTTP/1.1";
const DEAUTHORIZE_MEMBER_TOKEN: &str = "DELETE /api/v1/me/integration-tokens/itk_01k0z6r1w8f4jy2m7q9v3x5abc/authorizations/ita_01k0z6r1w8f4jy2m7q9v3x5abc HTTP/1.1";
const REVOKE_ACME_TOKEN: &str =
    "DELETE /api/v1/organizations/acme/integration-tokens/itk_01k0z6r1w8f4jy2m7q9v3x5abc HTTP/1.1";
const AUTHORIZE_ACME_TOKEN: &str = "POST /api/v1/organizations/acme/integration-tokens/itk_01k0z6r1w8f4jy2m7q9v3x5abc/authorizations HTTP/1.1";
const DEAUTHORIZE_ACME_TOKEN: &str = "DELETE /api/v1/organizations/acme/integration-tokens/itk_01k0z6r1w8f4jy2m7q9v3x5abc/authorizations/ita_01k0z6r1w8f4jy2m7q9v3x5abc HTTP/1.1";
const REVOKE_GITHUB: &str = "DELETE /api/v1/me/grants/pgr_01k0z6r1w8f4jy2m7q9v3x5abc HTTP/1.1";
const AUTHORIZE_GITHUB: &str =
    "POST /api/v1/me/grants/pgr_01k0z6r1w8f4jy2m7q9v3x5abc/authorizations HTTP/1.1";
/// The authorization Cloud makes of a token for a runner.
const NEW_AUTHORIZATION_ID: &str = "ita_01k0z6r1w8f4jy2m7q9v3x5abf";

/// A member who holds a GitHub grant and no grant of `mergify`: the grant list, then the served
/// read naming `mergify` as a token service, then the member's tokens, `held`.
fn holding_token(held: serde_json::Value) -> Vec<Vec<u8>> {
    vec![
        page(&[oauth_grant(OAUTH_GRANT_ID, "github", "active")], None),
        serving_tokens(),
        tokens(&[held]),
    ]
}

/// The `mergify` token of `side` authorized for one runner of acme.
fn mergify_authorized_on(side: &str) -> serde_json::Value {
    let mut token = authorized_mergify();
    if side == "org" {
        token["side"] = "org".into();
        token["organizationId"] = ORGANIZATION_ID.into();
    }
    token
}

/// The authorization Cloud makes of a token for `runner_id`, which echoes the request's key.
fn token_authorized(runner_id: &str) -> Vec<u8> {
    http_response_with_headers(
        "201 Created",
        Some("application/json"),
        &[("Idempotency-Key", ECHO_IDEMPOTENCY_KEY)],
        &serde_json::to_vec(&serde_json::json!({
            "id": NEW_AUTHORIZATION_ID,
            "runnerId": runner_id,
            "runnerName": "bravo",
            "organizationId": ORGANIZATION_ID,
            "createdAt": "2026-11-18T10:00:00Z"
        }))
        .unwrap(),
    )
}

/// Runs `useful integration args` without `--yes`, which is refused with exit 2 before anything
/// is sent or printed on standard output, naming the option; its standard error.
fn refused_without_yes(server: &ScriptedServer, credential: &Path, args: &[&str]) -> String {
    let output = run_under("integration", server, credential, args);
    assert_eq!(output.status.code(), Some(2), "{args:?}");
    assert!(output.stdout.is_empty(), "{args:?}");
    let text = stderr(&output);
    assert!(text.contains("--yes"), "{args:?}: {text}");
    text
}

// REQ-INTEGRATION-20
// REQ-INTEGRATION-15
#[test]
fn revoke_erases_the_members_token_once_no_grant_of_its_service_is_found() {
    let mut answers = holding_token(mergify_token("member", 1));
    answers.push(deleted());
    // A GitHub grant revoked by its provider name, whose statement is a grant's.
    answers.push(page(
        &[oauth_grant(OAUTH_GRANT_ID, "github", "active")],
        None,
    ));
    answers.push(deleted());
    // The earlier spelling reaches no token.
    answers.push(page(
        &[oauth_grant(OAUTH_GRANT_ID, "github", "active")],
        None,
    ));
    let (server, _directory, credential) = prepared(answers);
    let revoked = run_under(
        "integration",
        &server,
        &credential,
        &["revoke", "mergify", "--yes", "--json"],
    );
    let refusal = refused_without_yes(&server, &credential, &["revoke", "mergify"]);
    let grant_revoked = run_under(
        "integration",
        &server,
        &credential,
        &["revoke", "github", "--yes"],
    );
    let earlier = run_under(
        "grant",
        &server,
        &credential,
        &["revoke", "mergify", "--yes", "--json"],
    );
    let requests = server.finish();

    assert_eq!(
        lines(&asked(&requests)),
        [
            LIST_EVERY_KIND,
            READ_SERVED,
            LIST_MEMBER_TOKENS,
            REVOKE_MEMBER_TOKEN,
            LIST_EVERY_KIND,
            REVOKE_GITHUB,
            LIST_EVERY_KIND,
        ]
    );
    assert!(!header_value(&requests[3], "idempotency-key").is_empty());
    assert_eq!(revoked.status.code(), Some(0), "{}", stderr(&revoked));
    let document = json_output(&revoked);
    assert_eq!(document["outcome"], "revoked", "{document}");
    assert_eq!(document["integrationTokenId"], TOKEN_ID, "{document}");
    assert_eq!(document["service"], "mergify", "{document}");
    assert_eq!(document["side"], "member", "{document}");

    // Before it acts it states what revoking a token does, as the refusal without --yes does,
    // and that is not what revoking a grant does.
    let token_statement = statement(&revoked, TOKEN_ID);
    let grant_statement = statement(&grant_revoked, OAUTH_GRANT_ID);
    assert_stated_by_the_refusal(&token_statement, &refusal);
    assert_stated_by_the_refusal(&grant_statement, &refusal);
    assert!(
        token_statement
            .iter()
            .all(|line| !grant_statement.contains(line)),
        "{token_statement:?} {grant_statement:?}"
    );

    // `useful grant revoke` finds no grant of mergify, as before, having read the grants alone.
    assert_eq!(earlier.status.code(), Some(1));
    assert_eq!(json_output(&earlier)["outcome"], "grant_not_found");
}

// REQ-INTEGRATION-20
#[test]
fn authorize_and_deauthorize_act_on_the_members_token_by_its_service() {
    let mut answers = holding_token(mergify_authorized_on("member"));
    answers.push(token_authorized(NEW_RUNNER_ID));
    answers.extend(holding_token(mergify_authorized_on("member")));
    answers.push(deleted());
    // The token lists no authorization for the new runner.
    answers.extend(holding_token(mergify_authorized_on("member")));
    let (server, _directory, credential) = prepared(answers);
    let authorized_run = run_under(
        "integration",
        &server,
        &credential,
        &["authorize", "mergify", NEW_RUNNER_ID, "--yes", "--json"],
    );
    let deauthorized = run_under(
        "integration",
        &server,
        &credential,
        &["deauthorize", "mergify", RUNNER_ID, "--yes", "--json"],
    );
    let not_found = run_under(
        "integration",
        &server,
        &credential,
        &["deauthorize", "mergify", NEW_RUNNER_ID, "--yes", "--json"],
    );
    // Without --yes and with RUNNER each is refused before anything is sent, stating the
    // token's lines.
    let authorize_refusal = refused_without_yes(
        &server,
        &credential,
        &["authorize", "mergify", NEW_RUNNER_ID],
    );
    let deauthorize_refusal =
        refused_without_yes(&server, &credential, &["deauthorize", "mergify", RUNNER_ID]);
    let requests = server.finish();

    let reads = [LIST_EVERY_KIND, READ_SERVED, LIST_MEMBER_TOKENS];
    assert_eq!(
        lines(&asked(&requests)),
        [
            &reads[..],
            &[AUTHORIZE_MEMBER_TOKEN],
            &reads,
            &[DEAUTHORIZE_MEMBER_TOKEN],
            &reads
        ]
        .concat()
    );
    assert_eq!(
        request_body(&requests[3]),
        serde_json::json!({"runnerId": NEW_RUNNER_ID})
    );
    for request in [&requests[3], &requests[7]] {
        assert!(!header_value(request, "idempotency-key").is_empty());
    }

    assert_eq!(
        authorized_run.status.code(),
        Some(0),
        "{}",
        stderr(&authorized_run)
    );
    let document = json_output(&authorized_run);
    assert_eq!(document["outcome"], "authorized", "{document}");
    assert_eq!(document["integrationTokenId"], TOKEN_ID, "{document}");
    assert!(document.get("grantId").is_none(), "{document}");
    let authorization = &document["authorization"];
    assert_eq!(authorization["id"], NEW_AUTHORIZATION_ID, "{document}");
    assert_eq!(authorization["runnerId"], NEW_RUNNER_ID, "{document}");
    assert_eq!(authorization["runnerName"], "bravo", "{document}");
    assert_eq!(
        authorization["organizationId"], ORGANIZATION_ID,
        "{document}"
    );
    assert_stated_by_the_refusal(&statement(&authorized_run, TOKEN_ID), &authorize_refusal);

    assert_eq!(
        deauthorized.status.code(),
        Some(0),
        "{}",
        stderr(&deauthorized)
    );
    let document = json_output(&deauthorized);
    assert_eq!(document["outcome"], "deauthorized", "{document}");
    assert_eq!(document["integrationTokenId"], TOKEN_ID, "{document}");
    assert_eq!(
        document["authorizationId"], "ita_01k0z6r1w8f4jy2m7q9v3x5abc",
        "{document}"
    );
    assert_eq!(document["runnerId"], RUNNER_ID, "{document}");
    assert_stated_by_the_refusal(&statement(&deauthorized, TOKEN_ID), &deauthorize_refusal);

    assert_eq!(not_found.status.code(), Some(1));
    assert_eq!(
        json_output(&not_found)["outcome"],
        "authorization_not_found"
    );
}

// REQ-INTEGRATION-20
// INV-CLI-16
#[test]
fn authorize_on_a_terminal_offers_the_runners_the_members_token_is_not_authorized_for() {
    let mut answers = holding_token(mergify_authorized_on("member"));
    answers.extend([
        memberships(&[(ORGANIZATION_ID, "acme")]),
        // The token is authorized for charlie already, so alpha and bravo are offered.
        registrations(&[
            registration(ALPHA, "offline", "enabled"),
            registration(BRAVO, "online", "enabled"),
            registration(CHARLIE, "online", "enabled"),
        ]),
        token_authorized(NEW_RUNNER_ID),
    ]);
    answers.extend(holding_token(mergify_authorized_on("member")));
    answers.push(token_authorized(NEW_RUNNER_ID));
    let (server, _directory, credential) = prepared(answers);
    // Bravo, online and enabled, is offered first; the list is shown once alpha is.
    let (shown, chosen) = on_terminal(
        family_on_terminal(
            "integration",
            &server,
            &credential,
            &["authorize", "mergify"],
        ),
        &[(ENDED_RUNNER_ID, "\r"), ("[y/N]", "y\n")],
    );
    let named = run_under(
        "integration",
        &server,
        &credential,
        &["authorize", "mergify", NEW_RUNNER_ID, "--yes"],
    );
    let requests = server.finish();

    assert_eq!(chosen.status.code(), Some(0), "{shown}");
    assert!(!shown.contains("charlie"), "{shown}");
    assert!(
        shown.find(ENDED_RUNNER_ID).unwrap() < shown.find("[y/N]").unwrap(),
        "{shown}"
    );
    let asked = asked(&requests);
    assert_eq!(
        lines(&asked),
        [
            LIST_EVERY_KIND,
            READ_SERVED,
            LIST_MEMBER_TOKENS,
            LIST_MEMBERSHIPS,
            LIST_ACME_RUNNERS,
            AUTHORIZE_MEMBER_TOKEN,
            LIST_EVERY_KIND,
            READ_SERVED,
            LIST_MEMBER_TOKENS,
            AUTHORIZE_MEMBER_TOKEN,
        ]
    );
    // The choice sends what the command line naming that runner with --yes sends, and prints
    // what it prints.
    assert_eq!(asked[5], asked[9]);
    assert_eq!(named.status.code(), Some(0));
    assert_eq!(chosen.stdout, named.stdout);
}

/// The outcome of each entry of `entries` of an `authorize --all` document, by `id`.
fn outcomes_by(entries: &serde_json::Value, id: &str) -> Vec<(String, String)> {
    entries
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| {
            (
                entry[id].as_str().unwrap().to_owned(),
                entry["outcome"].as_str().unwrap().to_owned(),
            )
        })
        .collect()
}

// REQ-INTEGRATION-20
// REQ-INTEGRATION-15
#[test]
fn authorize_all_includes_the_members_tokens_and_the_earlier_spelling_does_not() {
    let grant = || page(&[oauth_grant_unauthorized(OAUTH_GRANT_ID)], None);
    let (server, _directory, credential) = prepared(vec![
        grant(),
        serving_tokens(),
        tokens(&[mergify_token("member", 1)]),
        authorized(OAUTH_GRANT_ID, NEW_RUNNER_ID),
        token_authorized(NEW_RUNNER_ID),
        grant(),
        authorized(OAUTH_GRANT_ID, NEW_RUNNER_ID),
    ]);
    let args = ["authorize", "--all", NEW_RUNNER_ID, "--yes", "--json"];
    let every = run_under("integration", &server, &credential, &args);
    let earlier = run_under("grant", &server, &credential, &args);
    let requests = asked(&server.finish());

    assert_eq!(
        lines(&requests),
        [
            LIST_EVERY_KIND,
            READ_SERVED,
            LIST_MEMBER_TOKENS,
            AUTHORIZE_GITHUB,
            AUTHORIZE_MEMBER_TOKEN,
            LIST_EVERY_KIND,
            AUTHORIZE_GITHUB,
        ]
    );
    assert_eq!(every.status.code(), Some(0), "{}", stderr(&every));
    let document = json_output(&every);
    assert_eq!(document["outcome"], "authorized", "{document}");
    assert_eq!(
        outcomes_by(&document["grants"], "grantId"),
        [(OAUTH_GRANT_ID.to_owned(), "authorized".to_owned())]
    );
    assert_eq!(
        outcomes_by(&document["tokens"], "integrationTokenId"),
        [(TOKEN_ID.to_owned(), "authorized".to_owned())]
    );
    assert_eq!(document["tokens"][0]["service"], "mergify", "{document}");

    assert_eq!(earlier.status.code(), Some(0));
    let document = json_output(&earlier);
    assert!(document.get("tokens").is_none(), "{document}");
    assert_eq!(
        outcomes_by(&document["grants"], "grantId"),
        [(OAUTH_GRANT_ID.to_owned(), "authorized".to_owned())]
    );
}

// REQ-INTEGRATION-19
// REQ-INTEGRATION-20
// A deployment that serves no token service, or predates the served read, may have no token
// route: a name that names no grant and `authorize --all` ask it for no token list.
#[test]
fn a_leaf_reads_no_token_list_unless_the_deployment_serves_a_token_service() {
    let grant = || page(&[oauth_grant_unauthorized(OAUTH_GRANT_ID)], None);
    for served in [serving_with(false), predating_served()] {
        let (server, _directory, credential) = prepared(vec![
            grant(),
            served.clone(),
            grant(),
            served.clone(),
            grant(),
            served,
            authorized(OAUTH_GRANT_ID, NEW_RUNNER_ID),
        ]);
        let run = |args: &[&str]| run_under("integration", &server, &credential, args);
        let revoked = run(&["revoke", "mergify", "--yes", "--json"]);
        let shown = run(&["list", "mergify", "--json"]);
        let every = run(&["authorize", "--all", NEW_RUNNER_ID, "--yes", "--json"]);
        let requests = asked(&server.finish());

        assert_eq!(
            lines(&requests),
            [
                LIST_EVERY_KIND,
                READ_SERVED,
                LIST_EVERY_KIND,
                READ_SERVED,
                LIST_EVERY_KIND,
                READ_SERVED,
                AUTHORIZE_GITHUB,
            ]
        );
        for not_found in [&revoked, &shown] {
            assert_eq!(not_found.status.code(), Some(1));
            assert_eq!(json_output(not_found)["outcome"], "grant_not_found");
        }
        assert_eq!(every.status.code(), Some(0), "{}", stderr(&every));
        let document = json_output(&every);
        assert_eq!(
            outcomes_by(&document["grants"], "grantId"),
            [(OAUTH_GRANT_ID.to_owned(), "authorized".to_owned())]
        );
        assert_eq!(document["tokens"], serde_json::json!([]), "{document}");
    }
}

/// Runs `useful integration args --org acme`.
fn for_acme(server: &ScriptedServer, credential: &Path, args: &[&str]) -> Output {
    run_under(
        "integration",
        server,
        credential,
        &[args, &["--org", "acme"]].concat(),
    )
}

// REQ-INTEGRATION-17
// REQ-INTEGRATION-20
#[test]
fn with_org_each_leaf_acts_on_the_organizations_token_through_its_routes() {
    let held = || tokens(&[mergify_authorized_on("org")]);
    let (server, _directory, credential) = prepared(vec![
        held(),
        deleted(),
        held(),
        token_authorized(NEW_RUNNER_ID),
        held(),
        deleted(),
        // No runner named: the organization's runners alone are offered.
        held(),
        registrations(&[
            registration(ALPHA, "offline", "enabled"),
            registration(BRAVO, "online", "enabled"),
            registration(CHARLIE, "online", "enabled"),
        ]),
        // --all ranges over the organization's tokens alone.
        tokens(&[mergify_token("org", 1)]),
        token_authorized(NEW_RUNNER_ID),
        // The member's own token, whose statement is compared with the organization's.
        page(&[oauth_grant(OAUTH_GRANT_ID, "github", "active")], None),
        serving_tokens(),
        tokens(&[mergify_token("member", 1)]),
        token_authorized(NEW_RUNNER_ID),
    ]);
    let org = |args: &[&str]| for_acme(&server, &credential, args);
    let revoked = org(&["revoke", "mergify", "--yes", "--json"]);
    let authorized_run = org(&["authorize", "mergify", NEW_RUNNER_ID, "--yes"]);
    let deauthorized = org(&["deauthorize", "mergify", RUNNER_ID, "--yes", "--json"]);
    let offered = org(&["authorize", "mergify", "--json"]);
    let every = org(&["authorize", "--all", NEW_RUNNER_ID, "--yes", "--json"]);
    let member = run_under(
        "integration",
        &server,
        &credential,
        &["authorize", "mergify", NEW_RUNNER_ID, "--yes"],
    );
    let refusal = refused_without_yes(
        &server,
        &credential,
        &["authorize", "mergify", NEW_RUNNER_ID, "--org", "acme"],
    );
    let requests = asked(&server.finish());

    assert_eq!(
        lines(&requests),
        [
            LIST_ACME_TOKENS,
            REVOKE_ACME_TOKEN,
            LIST_ACME_TOKENS,
            AUTHORIZE_ACME_TOKEN,
            LIST_ACME_TOKENS,
            DEAUTHORIZE_ACME_TOKEN,
            LIST_ACME_TOKENS,
            LIST_ACME_RUNNERS,
            LIST_ACME_TOKENS,
            AUTHORIZE_ACME_TOKEN,
            LIST_EVERY_KIND,
            READ_SERVED,
            LIST_MEMBER_TOKENS,
            AUTHORIZE_MEMBER_TOKEN,
        ]
    );
    assert_eq!(revoked.status.code(), Some(0), "{}", stderr(&revoked));
    let document = json_output(&revoked);
    assert_eq!(document["outcome"], "revoked", "{document}");
    assert_eq!(document["side"], "org", "{document}");
    assert_eq!(
        authorized_run.status.code(),
        Some(0),
        "{}",
        stderr(&authorized_run)
    );
    assert_eq!(
        deauthorized.status.code(),
        Some(0),
        "{}",
        stderr(&deauthorized)
    );
    assert_eq!(json_output(&deauthorized)["outcome"], "deauthorized");

    // Off a terminal, the leaf given no runner names the organization's runners it offers.
    assert_eq!(offered.status.code(), Some(2), "{}", stderr(&offered));
    let document = json_output(&offered);
    assert_eq!(document["outcome"], "runner_required", "{document}");
    let runners: Vec<_> = document["runners"]
        .as_array()
        .unwrap()
        .iter()
        .map(|runner| runner["id"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(runners, [NEW_RUNNER_ID, ENDED_RUNNER_ID], "{document}");

    assert_eq!(every.status.code(), Some(0), "{}", stderr(&every));
    let document = json_output(&every);
    assert_eq!(document["grants"], serde_json::json!([]), "{document}");
    assert_eq!(
        outcomes_by(&document["tokens"], "integrationTokenId"),
        [(TOKEN_ID.to_owned(), "authorized".to_owned())]
    );
    assert_eq!(document["tokens"][0]["side"], "org", "{document}");

    // Who can use the organization's token is stated before acting, as the refusal states it,
    // and is not who can use the member's.
    let org_statement = statement(&authorized_run, TOKEN_ID);
    let member_statement = statement(&member, TOKEN_ID);
    assert_stated_by_the_refusal(&org_statement, &refusal);
    assert_stated_by_the_refusal(&member_statement, &refusal);
    assert!(
        org_statement
            .iter()
            .all(|line| !member_statement.contains(line)),
        "{org_statement:?} {member_statement:?}"
    );
}

// REQ-INTEGRATION-17
#[test]
fn with_org_a_refusal_ends_the_leaf_and_a_grants_provider_is_refused() {
    let forbidden = || problem("403 Forbidden", 403, "forbidden");
    let (server, _directory, credential) = prepared(vec![
        forbidden(),
        forbidden(),
        forbidden(),
        forbidden(),
        tokens(&[mergify_token("org", 1)]),
        forbidden(),
    ]);
    let refused = [
        for_acme(&server, &credential, &["revoke", "mergify", "--yes"]),
        for_acme(
            &server,
            &credential,
            &["authorize", "mergify", NEW_RUNNER_ID, "--yes"],
        ),
        for_acme(
            &server,
            &credential,
            &["deauthorize", "mergify", RUNNER_ID, "--yes"],
        ),
        for_acme(
            &server,
            &credential,
            &["authorize", "--all", NEW_RUNNER_ID, "--yes"],
        ),
        for_acme(&server, &credential, &["revoke", "mergify", "--yes"]),
    ];
    assert_eq!(
        lines(&asked(&server.finish())),
        [
            LIST_ACME_TOKENS,
            LIST_ACME_TOKENS,
            LIST_ACME_TOKENS,
            LIST_ACME_TOKENS,
            LIST_ACME_TOKENS,
            REVOKE_ACME_TOKEN,
        ]
    );
    for output in &refused {
        assert_eq!(output.status.code(), Some(1), "{}", stderr(output));
        assert!(output.stdout.is_empty(), "{}", stderr(output));
    }

    // A grant's provider takes no --org, refused before any request; a coding agent's refusal
    // names the option of its login.
    let (server, _directory, credential) = prepared(vec![]);
    for provider in ["github", "codex", "claude_code"] {
        for args in [
            &["revoke", provider, "--yes"][..],
            &["authorize", provider, NEW_RUNNER_ID, "--yes"],
            &["deauthorize", provider, RUNNER_ID, "--yes"],
        ] {
            let output = for_acme(&server, &credential, args);
            assert_eq!(output.status.code(), Some(2), "{args:?}");
            assert!(output.stdout.is_empty(), "{args:?}");
            if provider != "github" {
                assert!(stderr(&output).contains("--organization"), "{args:?}");
            }
        }
    }
    assert!(server.finish().is_empty());

    // A name the organization holds no token of is read for among the served providers: an OAuth
    // provider is refused with exit 2, any other is no token of the organization's.
    let (server, _directory, credential) = prepared(vec![
        tokens(&[]),
        serving_with(true),
        tokens(&[]),
        serving_with(true),
    ]);
    let oauth = for_acme(&server, &credential, &["revoke", "example", "--yes"]);
    let missing = for_acme(
        &server,
        &credential,
        &["revoke", "mergify", "--yes", "--json"],
    );
    assert_eq!(
        lines(&asked(&server.finish())),
        [LIST_ACME_TOKENS, READ_SERVED, LIST_ACME_TOKENS, READ_SERVED]
    );
    assert_eq!(oauth.status.code(), Some(2), "{}", stderr(&oauth));
    assert_eq!(missing.status.code(), Some(1), "{}", stderr(&missing));
    assert_eq!(json_output(&missing)["outcome"], "token_not_found");
}
