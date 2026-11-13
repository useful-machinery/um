use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use serde_json::Value;
use tempfile::TempDir;

use super::workflow_definition::{Home, Tree, read_json, schema};
use super::workflow_run::{
    assert_usage_error, fixture_path_with_host_tools, isolated_command, succeeded, tree_rejection,
};

const STAGE_PROGRAM: &str = "#!/bin/sh\nexit 0\n";

/// The engineering tree published to a fresh home, with an execution root and a `PATH`
/// that satisfies the root's `requires.tools`.
pub(super) struct Published {
    pub(super) home: Home,
    pub(super) tree: Tree,
    pub(super) id: String,
    digest: String,
    pub(super) scratch: TempDir,
}

impl Published {
    pub(super) fn engineering() -> Self {
        Self::publish(Tree::engineering())
    }

    /// `tree`, an engineering tree whose files a test may have rewritten, published as
    /// `engineering` publishes it.
    pub(super) fn publish(tree: Tree) -> Self {
        // The `design` stage runs `design.py` from the tree's `programs` directory.
        tree.write("scripts/design.py", STAGE_PROGRAM);
        executable(&Path::new(tree.path()).join("scripts/design.py"));
        let home = Home::new();
        let report = home.publish(&tree, &["--bindings", &tree.engineering_bindings()]);
        let id = report["definition"]["id"].as_str().unwrap().to_owned();
        let digest = report["revision"]["tree"].as_str().unwrap().to_owned();
        let scratch = tempfile::tempdir().unwrap();
        fs::create_dir(scratch.path().join("execution")).unwrap();
        fs::create_dir(scratch.path().join("tools")).unwrap();
        // `gh` is a declared tool that a test host need not have.
        fs::write(scratch.path().join("tools/gh"), STAGE_PROGRAM).unwrap();
        executable(&scratch.path().join("tools/gh"));
        Self {
            home,
            tree,
            id,
            digest,
            scratch,
        }
    }

    pub(super) fn execution_root(&self) -> PathBuf {
        self.scratch.path().join("execution")
    }

    /// The home's copy of the current revision's tree.
    fn copy(&self) -> PathBuf {
        Path::new(self.home.path())
            .join("trees/sha256")
            .join(self.digest.trim_start_matches("sha256:"))
    }

    /// `um workflow run ARGS` against the home.
    fn run(&self, args: &[&str]) -> Output {
        self.command(args).output().unwrap()
    }

    /// The `um workflow run ARGS` command against the home. The root mirrors to Linear
    /// (REQ-LOCALRUN-191), so the command carries no Linear key or endpoint unless the test
    /// sets its own.
    pub(super) fn command(&self, args: &[&str]) -> Command {
        let mut full = vec!["workflow".to_owned(), "run".to_owned()];
        full.extend(args.iter().map(|arg| (*arg).to_owned()));
        let path = fixture_path_with_host_tools(&[&self.scratch.path().join("tools")]);
        let mut command = isolated_command(&full);
        command.env("UM_HOME", self.home.path()).env("PATH", path);
        command
    }

    /// `um workflow run create ID --execution-root E --json OPTIONS`.
    fn create(&self, id: &str, options: &[&str]) -> Output {
        let execution_root = self.execution_root();
        let mut args = vec![
            "create",
            id,
            "--execution-root",
            execution_root.to_str().unwrap(),
            "--json",
        ];
        args.extend_from_slice(options);
        self.run(&args)
    }

    /// The canonical `workflows/<id>/runs/` the definition's runs are created under.
    pub(super) fn runs_directory(&self) -> PathBuf {
        self.home
            .definition_directory(&self.id)
            .canonicalize()
            .unwrap()
            .join("runs")
    }

    /// A run created by `options`, its run directory checked to sit under the home.
    fn created(&self, options: &[&str]) -> (PathBuf, Value) {
        let terminal = succeeded(&self.create(&self.id, options));
        assert_eq!(terminal["command"], "um workflow run create");
        let run_directory = PathBuf::from(terminal["runDirectory"].as_str().unwrap());
        assert_eq!(
            run_directory.parent(),
            Some(self.runs_directory().as_path())
        );
        let record = read_json(&run_directory.join("run.json"));
        assert!(
            schema("workflow-run-v1.schema.json").is_valid(&record),
            "{record}"
        );
        assert_eq!(
            run_directory.file_name().and_then(|name| name.to_str()),
            record["id"].as_str()
        );
        (run_directory, record)
    }

    /// A refusal at phase `definition` with the one diagnostic `code`, which creates no run
    /// directory; that diagnostic.
    fn refused(&self, id: &str, options: &[&str], code: &str) -> Value {
        let runs = self.runs();
        let rejection = tree_rejection(&self.create(id, options));
        assert_eq!(rejection["command"], "um workflow run create");
        assert_eq!(rejection["phase"], "definition", "{rejection}");
        let diagnostics = rejection["diagnostics"].as_array().unwrap();
        assert_eq!(diagnostics.len(), 1, "{rejection}");
        assert_eq!(diagnostics[0]["code"], code, "{rejection}");
        if runs.is_empty() {
            self.home.assert_no_runs();
        } else {
            assert_eq!(self.runs(), runs);
        }
        diagnostics[0].clone()
    }

    /// The entries of the definition's `runs/`, none before the first run.
    fn runs(&self) -> Vec<String> {
        let runs = self.home.definition_directory(&self.id).join("runs");
        let Ok(entries) = fs::read_dir(runs) else {
            return Vec::new();
        };
        let mut names = entries
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    /// The definition's slot file for `subject` (REQ-WFDEF-28).
    fn slot(&self, subject: &str) -> PathBuf {
        self.home
            .definition_directory(&self.id)
            .join("subjects")
            .join(subject)
    }
}

fn executable(path: &Path) {
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
}

fn writable(path: &Path) {
    let mode = if path.is_dir() { 0o755 } else { 0o644 };
    fs::set_permissions(path, fs::Permissions::from_mode(mode)).unwrap();
    if path.is_dir() {
        for entry in fs::read_dir(path).unwrap() {
            writable(&entry.unwrap().path());
        }
    }
}

/// The retained bytes of the root input `name` of the run's first attempt.
fn retained_input(run_directory: &Path, name: &str) -> Vec<u8> {
    let manifest = read_json(&run_directory.join("workflow/manifest.json"));
    let input = &manifest["inputs"][name];
    let file = input["relativeFile"]
        .as_str()
        .unwrap_or_else(|| panic!("{name} should be retained: {manifest}"));
    fs::read(run_directory.join("workflow").join(file)).unwrap()
}

// REQ-LOCALRUN-173, REQ-LOCALRUN-175, SC-LOCALRUN-9
#[test]
fn a_definition_run_starts_the_current_revision_under_the_home() {
    let published = Published::engineering();

    let (run_directory, record) = published.created(&["--input-text", "ticket", "AUTO-1"]);
    assert_eq!(record["definition"], published.id.as_str());
    assert_eq!(record["revision"], 1);
    assert_eq!(record["tree"], published.digest.as_str());
    assert_eq!(record["workflow"], "workflow.yaml");
    assert_eq!(record["parent"], Value::Null);
    assert_eq!(record["subject"], "AUTO-1");
    assert_eq!(record["state"], "exited");
    assert_eq!(record["attempts"], 1);

    let status = published.run(&["status", run_directory.to_str().unwrap(), "--json"]);
    assert_eq!(status.status.code(), Some(0), "{status:?}");

    // A stage file run by hand is an ordinary run that names no parent.
    let (_, stage) = published.created(&[
        "--workflow",
        "design.yaml",
        "--input-text",
        "ticket",
        "AUTO-1",
    ]);
    assert_eq!(stage["workflow"], "design.yaml");
    assert_eq!(stage["parent"], Value::Null);
    assert_eq!(stage["definition"], published.id.as_str());

    // The first run still holds the slot for AUTO-1 (REQ-WFDEF-28).
    published.home.json(&["disable", &published.id]);
    let (_, disabled) = published.created(&["--input-text", "ticket", "AUTO-2"]);
    assert_eq!(disabled["revision"], 1);

    let execution_root = published.execution_root();
    assert_usage_error(
        &[
            "workflow",
            "run",
            "create",
            &published.id,
            "--execution-root",
            execution_root.to_str().unwrap(),
            "--run-dir",
            published.scratch.path().join("elsewhere").to_str().unwrap(),
        ]
        .map(str::to_owned),
    );
    assert!(!published.scratch.path().join("elsewhere").exists());
}

// INV-TESTING-20
#[test]
fn the_um_binary_of_a_test_build_is_its_own_worker_when_none_is_supplied() {
    let published = Published::engineering();
    let execution_root = published.execution_root();

    let output = published
        .command(&[
            "create",
            &published.id,
            "--execution-root",
            execution_root.to_str().unwrap(),
            "--json",
            "--workflow",
            "design.yaml",
            "--input-text",
            "ticket",
            "AUTO-1",
        ])
        .env_remove("UM_TEST_INTERNAL_WORKER_EXECUTABLE")
        .output()
        .unwrap();
    let terminal = succeeded(&output);
    assert_eq!(terminal["outcome"], "succeeded", "{terminal}");
    assert_eq!(terminal["result"]["steps"][0]["id"], "design", "{terminal}");
    assert_eq!(
        terminal["result"]["steps"][0]["state"], "succeeded",
        "{terminal}"
    );
}

// REQ-LOCALRUN-173
#[test]
fn a_definition_run_is_refused_at_phase_definition_without_a_run_directory() {
    let published = Published::engineering();
    let ticket = ["--input-text", "ticket", "AUTO-1"];

    let unknown = "def_01jzzzzzzzzzzzzzzzzzzzzzzz";
    let not_found = published.refused(unknown, &ticket, "definition_not_found");
    assert_eq!(not_found["definition"], unknown);
    published.refused("engineering", &ticket, "definition_id_invalid");

    // A file the tree does not name as a workflow file is refused as the path form does.
    let rejection = tree_rejection(&published.create(
        &published.id,
        &[
            "--workflow",
            "missing.yaml",
            "--input-text",
            "ticket",
            "AUTO-1",
        ],
    ));
    assert_eq!(rejection["phase"], "tree");
    assert_eq!(rejection["workflow"]["path"], "missing.yaml");
    assert_eq!(rejection["diagnostics"][0]["code"], "workflow_not_in_tree");
    published.home.assert_no_runs();

    let copy = published.copy();
    writable(&copy);
    let mut design = fs::read(copy.join("design.yaml")).unwrap();
    design.push(b'\n');
    fs::write(copy.join("design.yaml"), design).unwrap();
    let modified = published.refused(&published.id, &ticket, "tree_retained_modified");
    assert_eq!(modified["definition"], published.id.as_str());
    assert_eq!(modified["revision"], 1);

    fs::remove_dir_all(&copy).unwrap();
    let missing = published.refused(&published.id, &ticket, "tree_not_retained");
    assert_eq!(missing["definition"], published.id.as_str());
    assert_eq!(missing["revision"], 1);
}

// REQ-LOCALRUN-176, REQ-LOCALRUN-91, SC-LOCALRUN-9
#[test]
fn a_definition_run_completes_omitted_inputs_from_the_published_tree() {
    let published = Published::engineering();
    let policy =
        fs::read(Path::new(published.tree.path()).join("policy/engineering.json")).unwrap();
    let policy: Value = serde_json::from_slice(&policy).unwrap();
    let ticket = ["--input-text", "ticket", "AUTO-1"];

    let (run_directory, _) = published.created(&ticket);
    assert_eq!(retained_input(&run_directory, "baseBranch"), b"main");
    let retained: Value =
        serde_json::from_slice(&retained_input(&run_directory, "policy")).unwrap();
    assert_eq!(retained, policy);

    // The default is read from the home's copy, never from the source checkout. The first
    // run holds AUTO-1's slot (REQ-WFDEF-28), so this run is for another ticket.
    fs::remove_file(Path::new(published.tree.path()).join("policy/engineering.json")).unwrap();
    let (run_directory, _) = published.created(&["--input-text", "ticket", "AUTO-2"]);
    let retained: Value =
        serde_json::from_slice(&retained_input(&run_directory, "policy")).unwrap();
    assert_eq!(retained, policy);

    let runs = fs::read_dir(published.runs_directory()).unwrap().count();
    let rejection = tree_rejection(&published.create(&published.id, &[]));
    assert_eq!(rejection["phase"], "admission");
    assert_eq!(
        rejection["diagnostics"][0]["code"],
        "missing_required_input"
    );
    assert_eq!(rejection["diagnostics"][0]["location"]["input"], "ticket");
    assert_eq!(
        fs::read_dir(published.runs_directory()).unwrap().count(),
        runs
    );
}

// REQ-LOCALRUN-173, REQ-LOCALRUN-175, REQ-LOCALRUN-187, REQ-WFDEF-28, SC-LOCALRUN-11
#[test]
fn a_definition_run_holds_its_subjects_slot_until_released() {
    let published = Published::engineering();
    let ticket = ["--input-text", "ticket", "AUTO-1"];

    let (_, first) = published.created(&ticket);
    let first = first["id"].as_str().unwrap().to_owned();
    assert_eq!(
        fs::read_to_string(published.slot("AUTO-1")).unwrap(),
        format!("{first}\n")
    );

    // The first run exited and holds no release, so the subject is settled.
    let settled = published.refused(&published.id, &ticket, "subject_settled");
    assert_eq!(settled["run"], first.as_str(), "{settled}");
    assert_eq!(settled["definition"], published.id.as_str());
    assert_eq!(settled["revision"], 1);

    let invalid = published.refused(
        &published.id,
        &["--input-text", "ticket", "AUTO 1"],
        "subject_invalid",
    );
    assert!(invalid.get("run").is_none(), "{invalid}");

    let released = published.run(&["release", &first, "--json"]);
    assert_eq!(released.status.code(), Some(0), "{released:?}");
    let (_, second) = published.created(&ticket);
    let second = second["id"].as_str().unwrap().to_owned();
    assert_eq!(
        fs::read_to_string(published.slot("AUTO-1")).unwrap(),
        format!("{second}\n")
    );

    // A stage file declares no subject: the run records none and takes no slot, so the
    // settled second run does not refuse it.
    let (_, stage) = published.created(&[
        "--workflow",
        "design.yaml",
        "--input-text",
        "ticket",
        "AUTO-1",
    ]);
    assert_eq!(stage["subject"], Value::Null);
    assert_eq!(
        fs::read_to_string(published.slot("AUTO-1")).unwrap(),
        format!("{second}\n")
    );

    // A `--path` run of the tree records the subject and takes no slot.
    let run_directory = published.scratch.path().join("path-run");
    let execution_root = published.execution_root();
    let output = published.run(&[
        "create",
        "--path",
        published.tree.path(),
        "--run-dir",
        run_directory.to_str().unwrap(),
        "--execution-root",
        execution_root.to_str().unwrap(),
        "--json",
        "--input-text",
        "ticket",
        "AUTO-1",
    ]);
    succeeded(&output);
    let record = read_json(&run_directory.join("run.json"));
    assert_eq!(record["subject"], "AUTO-1");
    assert_eq!(record["definition"], Value::Null);
    assert_eq!(
        fs::read_dir(published.slot("AUTO-1").parent().unwrap())
            .unwrap()
            .filter_map(|entry| entry.unwrap().file_name().into_string().ok())
            .filter(|name| !name.starts_with('.'))
            .collect::<Vec<_>>(),
        ["AUTO-1"]
    );
    assert_eq!(
        fs::read_to_string(published.slot("AUTO-1")).unwrap(),
        format!("{second}\n")
    );
}

/// A workflow file whose source root is not a tree: it sits at `main.yaml`, with no
/// `workflow.yaml` beside it.
const CLOSURE_WORKFLOW: &str =
    "schemaVersion: 1\nsteps:\n  work:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";

// REQ-LOCALRUN-209, REQ-LOCALRUN-175, REQ-LOCALRUN-80, REQ-WFDEF-4
#[test]
fn a_created_run_records_the_owner_and_external_references_it_is_given() {
    let published = Published::engineering();

    let (_, record) = published.created(&[
        "--input-text",
        "ticket",
        "AUTO-1",
        "--owner",
        "alice",
        "--external",
        "url=https://cloud/runs/run_x",
        "--external",
        "cloud.run=run_x",
    ]);
    assert_eq!(record["owner"], "alice");
    assert_eq!(
        record["external"],
        serde_json::json!({"url": "https://cloud/runs/run_x", "cloud.run": "run_x"})
    );

    // A `--path` run of the tree takes them alike, under `--plain`; a value is split at
    // its first `=`.
    let run_directory = published.scratch.path().join("path-run");
    let execution_root = published.execution_root();
    let output = published.run(&[
        "create",
        "--path",
        published.tree.path(),
        "--run-dir",
        run_directory.to_str().unwrap(),
        "--execution-root",
        execution_root.to_str().unwrap(),
        "--plain",
        "--input-text",
        "ticket",
        "AUTO-2",
        "--owner",
        "bob",
        "--external",
        "query=a=b",
    ]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let record = read_json(&run_directory.join("run.json"));
    assert!(
        schema("workflow-run-v1.schema.json").is_valid(&record),
        "{record}"
    );
    assert_eq!(record["owner"], "bob");
    assert_eq!(record["external"], serde_json::json!({"query": "a=b"}));

    // Each refusal is a usage error that creates no run directory.
    let runs = published.runs();
    for options in [
        &["--external", "url=a", "--external", "url=b"][..],
        &["--external", "Url=a"],
        &["--external", "1url=a"],
        &["--external", "pull-request=a"],
        &["--external", "url"],
        &["--owner", ""],
    ] {
        let mut args = vec!["--input-text", "ticket", "AUTO-3"];
        args.extend_from_slice(options);
        let output = published.create(&published.id, &args);
        assert_eq!(output.status.code(), Some(2), "{options:?}: {output:?}");
        assert!(output.stdout.is_empty(), "{options:?}: {output:?}");
        assert_eq!(published.runs(), runs, "{options:?}");
    }

    // A run of a workflow file's closure has no run record to carry either.
    let bundle = super::workflow_run::RunBundle::new(CLOSURE_WORKFLOW);
    for options in [["--owner", "alice"], ["--external", "url=a"]] {
        let destination = bundle.result("closure");
        let mut args = bundle.args(&destination);
        args.extend(options.map(str::to_owned));
        assert_usage_error(&args);
        assert!(!destination.exists(), "{options:?}");
    }
}

/// `um workflow run create ID --cloud ORGANIZATION --project PROJECT` against a scripted Cloud
/// (REQ-RUN-94).
mod cloud {
    use std::path::Path;
    use std::process::Output;

    use tempfile::TempDir;

    use super::super::{
        ECHO_IDEMPOTENCY_KEY, ScriptedServer, deployment_environment, header_value,
        http_response_with_headers, private_credential_directory, run_with_env,
        write_credential_fixture,
    };

    const TOKEN: &str = "unique-workflow-run-create-cloud-token-sentinel";
    const ORGANIZATION: &str = "acme-research";
    const PROJECT: &str = "prj_01k0z6r1w8f4jy2m7q9v3x5abc";
    const DEFINITION: &str = "def_01k0z6r1w8f4jy2m7q9v3x5abc";
    const RUN_ID: &str = "run_01k0z6r1w8f4jy2m7q9v3x5abc";
    const INPUT_SET_ID: &str = "ris_01k0z6r1w8f4jy2m7q9v3x5abc";
    const COMMIT: &str = "0123456789abcdef0123456789abcdef01234567";

    /// A Cloud answering `responses` in turn, a credential for it, and an empty home.
    struct Cloud {
        server: ScriptedServer,
        _credentials: TempDir,
        credential_path: String,
        home: TempDir,
    }

    impl Cloud {
        fn respond(responses: Vec<Vec<u8>>) -> Self {
            let server = ScriptedServer::respond(responses);
            let credentials = private_credential_directory();
            let credential_path = credentials.path().join("credentials.json");
            write_credential_fixture(
                &credential_path,
                &server.api_url,
                TOKEN,
                "2999-01-01T00:00:00Z",
            );
            Self {
                server,
                credential_path: credential_path.to_str().unwrap().to_owned(),
                _credentials: credentials,
                home: tempfile::tempdir().unwrap(),
            }
        }

        /// `um workflow run create ARGS --allow-insecure-http` with the home as `UM_HOME`.
        fn create(&self, args: &[&str]) -> Output {
            let mut full = vec!["workflow", "run", "create"];
            full.extend_from_slice(args);
            full.push("--allow-insecure-http");
            let mut environment =
                deployment_environment(&self.server.api_url, &self.credential_path).to_vec();
            environment.push(("UM_HOME", self.home.path().to_str().unwrap()));
            run_with_env(&full, &environment)
        }

        /// The requests Cloud received; the home is still empty, so nothing local was read
        /// into being or written.
        fn finish(self) -> Vec<String> {
            assert_eq!(empty_directory(self.home.path()), Vec::<String>::new());
            self.server.finish()
        }
    }

    fn empty_directory(path: &Path) -> Vec<String> {
        std::fs::read_dir(path)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect()
    }

    fn accepted() -> Vec<u8> {
        http_response_with_headers(
            "202 Accepted",
            Some("application/json"),
            &[
                ("Idempotency-Key", ECHO_IDEMPOTENCY_KEY),
                (
                    "Location",
                    "/v1/organizations/acme-research/runs/run_01k0z6r1w8f4jy2m7q9v3x5abc",
                ),
            ],
            &serde_json::to_vec(&serde_json::json!({"runId": RUN_ID, "replayed": false})).unwrap(),
        )
    }

    fn body(request: &str) -> serde_json::Value {
        serde_json::from_str(request.split_once("\r\n\r\n").unwrap().1).unwrap()
    }

    fn definition_args<'a>(options: &[&'a str]) -> Vec<&'a str> {
        let mut args = vec![DEFINITION, "--cloud", ORGANIZATION, "--project", PROJECT];
        args.extend_from_slice(options);
        args
    }

    // REQ-RUN-94, REQ-RUN-55, REQ-RUN-56
    #[test]
    fn a_definition_run_posts_one_definition_request_and_prints_the_acceptance() {
        let cloud = Cloud::respond(vec![accepted()]);
        let output = cloud.create(&definition_args(&["--json"]));
        assert_eq!(output.status.code(), Some(0), "{output:?}");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(),
            serde_json::json!({
                "schemaVersion": 1,
                "operation": "create",
                "deployment": cloud.server.api_url,
                "outcome": "accepted",
                "organizationRef": ORGANIZATION,
                "runId": RUN_ID,
                "run": null,
                "publication": null,
                "cancellationRequest": null,
                "replayed": false,
                "error": null
            })
        );
        let requests = cloud.finish();
        assert_eq!(requests.len(), 1);
        assert!(
            requests[0].starts_with(&format!(
                "POST /api/v1/organizations/{ORGANIZATION}/runs HTTP/1.1\r\n"
            )),
            "{}",
            requests[0]
        );
        assert_eq!(
            body(&requests[0]),
            serde_json::json!({"projectId": PROJECT, "definition": {"definitionId": DEFINITION}})
        );
        let key = header_value(&requests[0], "idempotency-key");
        assert_eq!(key.len(), 64);
        assert!(key.bytes().all(|byte| byte.is_ascii_hexdigit()));

        let cloud = Cloud::respond(vec![accepted()]);
        let output = cloud.create(&definition_args(&[]));
        assert_eq!(output.status.code(), Some(0), "{output:?}");
        let stdout = String::from_utf8(output.stdout).unwrap();
        for field in [
            format!("run: {RUN_ID}"),
            "replayed: no".to_owned(),
            format!("organization: {ORGANIZATION}"),
            format!("deployment: {}", cloud.server.api_url),
        ] {
            assert!(stdout.lines().any(|line| line == field), "{stdout}");
        }
        assert_eq!(cloud.finish().len(), 1);
    }

    // REQ-RUN-94, REQ-RUN-84, REQ-WORKSPACE-7
    #[test]
    fn the_workflow_file_and_the_cloud_options_reach_the_request_as_given() {
        let cloud = Cloud::respond(vec![accepted()]);
        let output = cloud.create(&definition_args(&[
            "--workflow",
            "flows/review.yaml",
            "--source-branch",
            "release/next",
            "--workspace-commit",
            COMMIT,
            "--display-name",
            "Release checks",
            "--input-set-id",
            INPUT_SET_ID,
            "--max-parallel",
            "4",
            "--json",
        ]));
        assert_eq!(output.status.code(), Some(0), "{output:?}");
        let request = cloud.finish().pop().unwrap();
        assert_eq!(
            body(&request),
            serde_json::json!({
                "projectId": PROJECT,
                "definition": {"definitionId": DEFINITION, "workflowPath": "flows/review.yaml"},
                "sourceBranch": "release/next",
                "workspaceSource": {"kind": "commit", "commitOid": COMMIT},
                "displayName": "Release checks",
                "inputSetId": INPUT_SET_ID,
                "maximumParallelSteps": 4
            })
        );
    }

    // REQ-RUN-94, REQ-RUN-99
    #[test]
    fn no_owner_asks_for_an_unowned_run_and_its_absence_leaves_the_owner_to_cloud() {
        for (options, owner) in [
            (
                &["--no-owner", "--json"][..],
                Some(serde_json::json!("none")),
            ),
            (&["--json"], None),
        ] {
            let cloud = Cloud::respond(vec![accepted()]);
            let output = cloud.create(&definition_args(options));
            assert_eq!(output.status.code(), Some(0), "{options:?}: {output:?}");
            let request = cloud.finish().pop().unwrap();
            assert_eq!(body(&request).get("owner"), owner.as_ref(), "{options:?}");
        }
    }

    // REQ-RUN-94, REQ-LOCALRUN-80
    #[test]
    fn a_local_option_beside_cloud_or_a_cloud_option_without_it_is_a_usage_error() {
        let scratch = tempfile::tempdir().unwrap();
        let scratch = scratch.path().to_str().unwrap();
        let cloud_with = |options: &[&'static str]| -> Vec<String> {
            let mut args = definition_args(options)
                .into_iter()
                .map(str::to_owned)
                .collect::<Vec<_>>();
            args.push("--json".to_owned());
            args
        };
        let mut refused: Vec<Vec<String>> = [
            &["--path", "."][..],
            &["--run-dir", "run"],
            &["--agent-profiles", "profiles.json"],
            &["--stop-before", "build"],
            &["--owner", "alice"],
            &["--no-owner", "--owner", "alice"],
            &["--external", "url=a"],
            &["--max-parallel", "65"],
            &["--max-parallel", "0"],
            &["--workflow", "../review.yaml"],
        ]
        .into_iter()
        .map(cloud_with)
        .collect();
        refused.push({
            let mut args = cloud_with(&[]);
            args.extend(["--execution-root".to_owned(), scratch.to_owned()]);
            args
        });
        // `--cloud` without `--project`.
        refused.push(
            [DEFINITION, "--cloud", ORGANIZATION, "--json"]
                .map(str::to_owned)
                .to_vec(),
        );
        for options in [
            &["--project", PROJECT][..],
            &["--source-branch", "main"],
            &["--workspace-branch", "main"],
            &["--workspace-commit", COMMIT],
            &["--display-name", "Release checks"],
            &["--input-set-id", INPUT_SET_ID],
            &["--integration-context-file", "context.json"],
            &["--wait"],
            &["--no-owner"],
        ] {
            let mut args = [DEFINITION, "--execution-root", scratch, "--json"]
                .map(str::to_owned)
                .to_vec();
            args.extend(options.iter().map(|option| (*option).to_owned()));
            refused.push(args);
        }
        for args in refused {
            let cloud = Cloud::respond(Vec::new());
            let args = args.iter().map(String::as_str).collect::<Vec<_>>();
            let output = cloud.create(&args);
            assert_eq!(output.status.code(), Some(2), "{args:?}: {output:?}");
            assert!(output.stdout.is_empty(), "{args:?}: {output:?}");
            assert!(cloud.finish().is_empty(), "{args:?}");
        }
    }

    // REQ-LOCALRUN-76
    #[test]
    fn without_cloud_the_execution_root_is_still_required() {
        let cloud = Cloud::respond(Vec::new());
        let output = cloud.create(&[DEFINITION, "--json"]);
        assert_eq!(output.status.code(), Some(2), "{output:?}");
        assert!(output.stdout.is_empty(), "{output:?}");
        assert!(cloud.finish().is_empty());
    }
}

/// What `um workflow run create --cloud` does around acceptance, driven against the scripted
/// Cloud of `cli/tests/cli/run.rs`: one idempotency key per invocation and the single resend
/// (REQ-RUN-55 to REQ-RUN-57), `--wait` (REQ-RUN-58), the definite source-admission rejection
/// (REQ-RUN-80), the named inputs staged and sealed before creation (REQ-RUNINPUT-53,
/// REQ-RUNINPUT-54, REQ-RUNINPUT-76) and the integration context (REQ-TRIGGER-54).
mod cloud_acceptance {
    use base64::Engine as _;

    use super::super::run_command::{
        DEFINITION_ID, INPUT_SET_ID, ORGANIZATION, ORGANIZATION_ID, PROJECT_ID, REFRESHED_TOKEN,
        RUN_ID, TOKEN, acceptance_response, acceptance_response_for, assert_invalid_response,
        create_args, create_input_set_response, create_scalar_input_set_response, hex_digest,
        prepared_run, request_body, run_body_with_state, run_response, scalar_input_set_body,
        scalar_upload_capability_response, seal_input_set_response, seal_scalar_input_set_response,
        sha256, upload_capability_response,
    };
    use super::super::*;

    fn create_args_with_scalar_input<'a>(
        flag: &'a str,
        input_name: &'a str,
        input_source: &'a str,
        json: bool,
    ) -> Vec<&'a str> {
        let mut args: Vec<&'a str> = create_args(json);
        let insertion = args.len() - 1;
        args.insert(insertion, flag);
        args.insert(insertion + 1, input_name);
        args.insert(insertion + 2, input_source);
        args
    }

    fn create_args_with_text_input<'a>(
        input_name: &'a str,
        input_file: &'a str,
        json: bool,
    ) -> Vec<&'a str> {
        create_args_with_scalar_input("--input-text-file", input_name, input_file, json)
    }

    fn create_args_with_file_input<'a>(
        input_name: &'a str,
        media_type: &'a str,
        input_file: &'a str,
        json: bool,
    ) -> Vec<&'a str> {
        let mut args: Vec<&'a str> = create_args(json);
        let insertion = args.len() - 1;
        args.splice(
            insertion..insertion,
            ["--input-file", input_name, media_type, input_file],
        );
        args
    }

    fn file_manifest_digest(bytes: &[u8], media_type: &str) -> String {
        let canonical = format!(
            "{{\"inputs\":{{\"request\":{{\"kind\":\"file\",\"mediaType\":{},\"sha256\":\"{}\",\"sizeBytes\":{}}}}},\"schemaVersion\":1}}",
            serde_json::to_string(media_type).unwrap(),
            hex_digest(bytes),
            bytes.len()
        );
        hex_digest(canonical.as_bytes())
    }

    fn file_input_set_body(
        bytes: &[u8],
        media_type: &str,
        state: &str,
        uploaded: bool,
    ) -> serde_json::Value {
        let mut body = scalar_input_set_body(bytes, "file", state, uploaded, false);
        body["manifest"]["inputs"]["request"]["mediaType"] = serde_json::json!(media_type);
        body["manifestDigest"]["value"] =
            serde_json::json!(file_manifest_digest(bytes, media_type));
        body
    }

    fn create_file_input_set_response(bytes: &[u8], media_type: &str) -> Vec<u8> {
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
            &serde_json::to_vec(&file_input_set_body(bytes, media_type, "open", false)).unwrap(),
        )
    }

    fn seal_file_input_set_response(bytes: &[u8], media_type: &str) -> Vec<u8> {
        http_response_with_headers(
            "200 OK",
            Some("application/json"),
            &[("Idempotency-Key", ECHO_IDEMPOTENCY_KEY)],
            &serde_json::to_vec(&file_input_set_body(bytes, media_type, "sealed", true)).unwrap(),
        )
    }

    fn assert_create_invalid_input(output: &Output) {
        assert_eq!(output.status.code(), Some(1));
        let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(result["operation"], "create");
        assert_eq!(result["outcome"], "error");
        assert_eq!(result["error"]["code"], "invalid_input");
    }

    // REQ-RUN-62
    #[cfg(target_os = "linux")]
    #[test]
    fn create_signal_after_acceptance_stops_only_observation_and_keeps_replay() {
        let mut server = ScriptedServer::respond_with_paused_last_response(vec![
            acceptance_response(true),
            run_response(run_body_with_state("succeeded")),
        ]);
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
        let mut args = create_args(true);
        args.insert(args.len() - 1, "--wait");
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
        assert!(server.wait_for_request().contains("/runs HTTP/1.1"));
        assert!(
            server
                .wait_for_request()
                .contains(&format!("/runs/{RUN_ID} HTTP/1.1"))
        );
        rustix::process::kill_process(
            rustix::process::Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap(),
            rustix::process::Signal::INT,
        )
        .unwrap();
        let output = child.wait_with_output().unwrap();
        server.release_paused_response();
        assert_eq!(output.status.code(), Some(130));
        let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(result["operation"], "create");
        assert_eq!(result["outcome"], "observation_stopped");
        assert_eq!(result["replayed"], true);
        assert_eq!(result["runId"], RUN_ID);
        assert_eq!(result["error"]["code"], "observation_stopped");
        assert_eq!(server.finish().len(), 0);
    }

    // REQ-RUN-58
    #[test]
    fn create_wait_preserves_authoritative_replay_through_failed_execution() {
        let mut body = run_body_with_state("failed");
        body["failure"] = serde_json::json!({
            "node": {"id": "build", "role": "step"}, "state": "failed",
            "detail": {"phase": "execution", "code": "command_exit", "exitCode": 23}
        });
        let (server, _directory, credential_path) =
            prepared_run(vec![acceptance_response(true), run_response(body.clone())]);
        let environment = deployment_environment(&server.api_url, &credential_path);
        let mut arguments = create_args(true);
        arguments.insert(arguments.len() - 1, "--wait");
        let output = run_with_env(&arguments, &environment);
        assert_eq!(output.status.code(), Some(1));
        let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(result["operation"], "create");
        assert_eq!(result["outcome"], "settled");
        assert_eq!(result["run"]["state"], "failed");
        assert_eq!(result["run"]["failure"], body["failure"]);
        assert_eq!(result["replayed"], true);
        assert_eq!(result["error"], serde_json::Value::Null);
        let requests = server.finish();
        assert_eq!(requests.len(), 2);
        assert!(requests[0].contains("/runs HTTP/1.1"));
        assert!(requests[1].contains(&format!("/runs/{RUN_ID} HTTP/1.1")));
    }

    // REQ-RUN-58
    #[test]
    fn create_wait_displays_failed_primary_issue() {
        let mut body = run_body_with_state("failed");
        body["failure"] = serde_json::json!({
            "node": {"id": "build", "role": "step"}, "state": "failed",
            "detail": {"code": "command_exit", "exitCode": 23}
        });
        let (server, _directory, credential_path) =
            prepared_run(vec![acceptance_response(false), run_response(body)]);
        let environment = deployment_environment(&server.api_url, &credential_path);
        let mut arguments = create_args(false);
        arguments.insert(arguments.len() - 1, "--wait");
        let output = run_with_env(&arguments, &environment);
        assert_eq!(output.status.code(), Some(1));
        let text = String::from_utf8(output.stdout).unwrap();
        for field in ["build", "command_exit", "exitCode"] {
            assert!(text.contains(field), "missing {field} in {text}");
        }
        assert_eq!(server.finish().len(), 2);
    }

    // REQ-WORKSPACE-7
    // REQ-RUN-55
    #[test]
    fn run_create_sends_inputless_request_and_reports_plain_and_json_receipts() {
        for json in [false, true] {
            let (server, _directory, credential_path) =
                prepared_run(vec![acceptance_response(false)]);
            let environment = deployment_environment(&server.api_url, &credential_path);

            let output = run_with_env(&create_args(json), &environment);

            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(output.stderr.is_empty());
            if json {
                assert_eq!(
                    serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(),
                    serde_json::json!({
                        "schemaVersion": 1,
                        "operation": "create",
                        "deployment": server.api_url,
                        "outcome": "accepted",
                        "organizationRef": ORGANIZATION,
                        "runId": RUN_ID,
                        "run": null,
                        "publication": null,
                        "cancellationRequest": null,
                        "replayed": false,
                        "error": null
                    })
                );
            } else {
                let stdout = String::from_utf8(output.stdout).unwrap();
                for field in [
                    format!("run: {RUN_ID}"),
                    "replayed: no".to_owned(),
                    format!("organization: {ORGANIZATION}"),
                    format!("deployment: {}", server.api_url),
                ] {
                    assert!(stdout.lines().any(|line| line == field));
                }
            }
            let request = server.finish().pop().unwrap();
            assert!(request.starts_with(&format!(
                "POST /api/v1/organizations/{ORGANIZATION}/runs HTTP/1.1\r\n"
            )));
            assert_eq!(
                request_body(&request),
                serde_json::json!({
                    "projectId": PROJECT_ID,
                    "definition": {"definitionId": DEFINITION_ID},
                    "sourceBranch": "release/next",
                    "displayName": "Release checks"
                })
            );
            assert_eq!(
                header_value(&request, "authorization"),
                format!("Bearer {TOKEN}")
            );
            let key = header_value(&request, "idempotency-key");
            assert_eq!(key.len(), 64);
            assert!(key.bytes().all(|byte| byte.is_ascii_hexdigit()));
        }
    }

    // REQ-TRIGGER-54
    #[test]
    fn run_create_sends_integration_context_from_regular_file_and_standard_input() {
        for source in ["file", "stdin"] {
            let context = br#"{"empty":"","issueId":"abc-123","source":"linear"}"#;
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("context.json");
            fs::write(&path, context).unwrap();
            let operand = if source == "file" {
                path.to_str().unwrap()
            } else {
                "-"
            };
            let (server, _credentials, credential_path) =
                prepared_run(vec![acceptance_response(false)]);
            let environment = deployment_environment(&server.api_url, &credential_path);
            let mut arguments = create_args(true);
            let insertion = arguments.len() - 1;
            arguments.splice(
                insertion..insertion,
                ["--integration-context-file", operand],
            );

            let output = if source == "stdin" {
                run_with_stdin(&arguments, &environment, context)
            } else {
                run_with_env(&arguments, &environment)
            };

            assert!(
                output.status.success(),
                "stdout: {}\nstderr: {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert_no_secret_output(&output, &["abc-123"]);
            let requests = server.finish();
            assert_eq!(requests.len(), 1);
            assert_eq!(
                request_body(&requests[0])["integrationContext"],
                serde_json::json!({"empty": "", "issueId": "abc-123", "source": "linear"})
            );
        }
    }

    // REQ-RUNINPUT-53
    #[test]
    fn run_create_reads_named_text_and_json_from_standard_input() {
        for (flag, kind, input_bytes) in [
            ("--input-text-file", "text", b"stdin text".as_slice()),
            ("--input-json-file", "json", br#"{"stdin":true}"#.as_slice()),
        ] {
            let storage = OneShotServer::respond("204 No Content", None, b"");
            let signed_url = format!("{}/private/request?signature=stdin", storage.api_url);
            let (server, _credentials, credential_path) = prepared_run(vec![
                create_scalar_input_set_response(input_bytes, kind, false),
                scalar_upload_capability_response(
                    input_bytes,
                    if kind == "text" {
                        "text/plain; charset=utf-8"
                    } else {
                        "application/json"
                    },
                    &signed_url,
                ),
                seal_scalar_input_set_response(input_bytes, kind, false),
                acceptance_response(false),
            ]);
            let environment = deployment_environment(&server.api_url, &credential_path);
            let arguments = create_args_with_scalar_input(flag, "request", "-", true);

            let output = run_with_stdin(&arguments, &environment, input_bytes);

            assert!(
                output.status.success(),
                "stdout: {}\nstderr: {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert_eq!(server.finish().len(), 4);
            assert_eq!(
                storage
                    .finish()
                    .split_once("\r\n\r\n")
                    .unwrap()
                    .1
                    .as_bytes(),
                input_bytes
            );
        }
    }

    // REQ-RUNINPUT-56
    #[test]
    fn competing_standard_input_claims_reject_before_cloud_access() {
        for input_flag in ["--input-text-file", "--input-json-file"] {
            let (server, _credentials, credential_path) = prepared_run(Vec::new());
            let environment = deployment_environment(&server.api_url, &credential_path);
            let mut arguments = create_args(true);
            let insertion = arguments.len() - 1;
            arguments.splice(
                insertion..insertion,
                [
                    "--integration-context-file",
                    "-",
                    input_flag,
                    "request",
                    "-",
                ],
            );

            let output = run_with_env(&arguments, &environment);

            assert_create_invalid_input(&output);
            assert!(server.finish().is_empty());
        }
    }

    // REQ-TRIGGER-54
    #[test]
    fn invalid_integration_context_rejects_before_cloud_access() {
        let mut too_many = serde_json::Map::new();
        for index in 0..33 {
            too_many.insert(
                format!("key{index:02}"),
                serde_json::Value::String(String::new()),
            );
        }
        let invalid_documents = [
            br#"{"duplicate":"first","duplicate":"second"}"#.to_vec(),
            serde_json::to_vec(&too_many).unwrap(),
            serde_json::to_vec(&serde_json::json!({"": "value"})).unwrap(),
            serde_json::to_vec(&serde_json::json!({"key": "v".repeat(1025)})).unwrap(),
            br#"{"key":"\u0000"}"#.to_vec(),
        ];
        for document in invalid_documents {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("context.json");
            fs::write(&path, document).unwrap();
            let (server, _credentials, credential_path) = prepared_run(Vec::new());
            let environment = deployment_environment(&server.api_url, &credential_path);
            let mut arguments = create_args(true);
            let insertion = arguments.len() - 1;
            arguments.splice(
                insertion..insertion,
                ["--integration-context-file", path.to_str().unwrap()],
            );

            let output = run_with_env(&arguments, &environment);

            assert_create_invalid_input(&output);
            assert!(server.finish().is_empty());
        }
    }

    // REQ-TRIGGER-54
    #[test]
    fn invalid_integration_context_plain_output_redacts_caller_values() {
        const PRIVATE_VALUE: &str = "private-parser-value-sentinel";
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("context.json");
        let encoded_object = serde_json::json!({"token": PRIVATE_VALUE}).to_string();
        fs::write(&path, serde_json::to_vec(&encoded_object).unwrap()).unwrap();
        let (server, _credentials, credential_path) = prepared_run(Vec::new());
        let environment = deployment_environment(&server.api_url, &credential_path);
        let mut arguments = create_args(false);
        let insertion = arguments.len() - 1;
        arguments.splice(
            insertion..insertion,
            ["--integration-context-file", path.to_str().unwrap()],
        );

        let output = run_with_env(&arguments, &environment);

        assert_eq!(output.status.code(), Some(1));
        assert_no_secret_output(&output, &[PRIVATE_VALUE]);
        assert!(server.finish().is_empty());
    }

    // REQ-TRIGGER-54
    #[test]
    fn oversized_integration_context_standard_input_rejects_before_cloud_access() {
        let (server, _credentials, credential_path) = prepared_run(Vec::new());
        let environment = deployment_environment(&server.api_url, &credential_path);
        let mut arguments = create_args(true);
        let insertion = arguments.len() - 1;
        arguments.splice(insertion..insertion, ["--integration-context-file", "-"]);
        let oversized_source = vec![b' '; 128 * 1024 + 1];

        let output = run_with_stdin(&arguments, &environment, &oversized_source);

        assert_create_invalid_input(&output);
        assert!(server.finish().is_empty());
    }

    // REQ-TRIGGER-54
    #[test]
    fn unreadable_integration_context_rejects_before_cloud_access() {
        let directory = tempfile::tempdir().unwrap();
        let missing = directory.path().join("missing.json");
        let (server, _credentials, credential_path) = prepared_run(Vec::new());
        let environment = deployment_environment(&server.api_url, &credential_path);
        let mut arguments = create_args(true);
        let insertion = arguments.len() - 1;
        arguments.splice(
            insertion..insertion,
            ["--integration-context-file", missing.to_str().unwrap()],
        );

        let output = run_with_env(&arguments, &environment);

        assert_eq!(output.status.code(), Some(1));
        assert!(server.finish().is_empty());
    }

    // REQ-RUNINPUT-57
    #[test]
    fn run_create_consumes_an_explicit_sealed_input_set_without_restaging() {
        let (server, _directory, credential_path) = prepared_run(vec![acceptance_response(false)]);
        let environment = deployment_environment(&server.api_url, &credential_path);
        let mut arguments = create_args(true);
        let insertion = arguments.len() - 1;
        arguments.splice(insertion..insertion, ["--input-set-id", INPUT_SET_ID]);

        let output = run_with_env(&arguments, &environment);

        assert!(output.status.success());
        assert!(output.stderr.is_empty());
        let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(result["outcome"], "accepted");
        assert_eq!(result["runId"], RUN_ID);
        assert!(result["run"].is_null());
        let request = server.finish().remove(0);
        assert!(request.contains("/runs HTTP/1.1"));
        assert_eq!(request_body(&request)["inputSetId"], INPUT_SET_ID);
    }

    // REQ-RUNINPUT-59, REQ-RUNINPUT-72
    #[test]
    fn create_json_input_rejected_by_file_workflow_reports_safe_binding_diagnostic() {
        let directory = tempfile::tempdir().unwrap();
        let input = directory.path().join("request.json");
        let bytes = b"{\"private\":\"ticket sentinel\"}";
        fs::write(&input, bytes).unwrap();
        let storage = OneShotServer::respond("204 No Content", None, b"");
        let signed_url = format!(
            "{}/private/request?signature=capability-sentinel",
            storage.api_url
        );
        let rejection = problem_http_response(
            "409 Conflict",
            serde_json::json!({
                "type": "https://api.usefulmachinery.com/problems/run-input-bindings-invalid",
                "title": "Run Input bindings invalid", "status": 409,
                "detail": "ticket sentinel must not be displayed",
                "diagnostic": { "code": "input_kind_mismatch", "input": "request" }
            }),
        );
        let (server, _credentials, credential_path) = prepared_run(vec![
            create_scalar_input_set_response(bytes, "json", false),
            scalar_upload_capability_response(bytes, "application/json", &signed_url),
            seal_scalar_input_set_response(bytes, "json", false),
            rejection,
        ]);
        let environment = deployment_environment(&server.api_url, &credential_path);
        let output = run_with_env(
            &create_args_with_scalar_input(
                "--input-json-file",
                "request",
                input.to_str().unwrap(),
                false,
            ),
            &environment,
        );
        assert_eq!(output.status.code(), Some(1));
        let diagnostic = String::from_utf8_lossy(&output.stderr);
        for fact in [
            "HTTP 409",
            "run-input-bindings-invalid",
            "diagnostic.code: input_kind_mismatch",
            "diagnostic.input: request",
            INPUT_SET_ID,
        ] {
            assert!(diagnostic.contains(fact), "missing {fact}: {diagnostic}");
        }
        assert_no_secret_output(&output, &["ticket sentinel", "capability-sentinel"]);
        let requests = server.finish();
        assert_eq!(requests.len(), 4);
        assert_eq!(request_body(&requests[3])["inputSetId"], INPUT_SET_ID);
        storage.finish();
    }

    // REQ-RUNINPUT-59
    #[test]
    fn create_input_admission_json_keeps_cloud_envelope_and_safe_diagnostic() {
        let (server, _directory, credential_path) = prepared_run(vec![problem_http_response(
            "409 Conflict",
            serde_json::json!({
                "type": "https://api.usefulmachinery.com/problems/run-input-bindings-invalid",
                "title": "Run Input bindings invalid", "status": 409,
                "detail": "ticket sentinel",
                "diagnostic": { "code": "input_kind_mismatch", "input": "request" }
            }),
        )]);
        let environment = deployment_environment(&server.api_url, &credential_path);
        let mut arguments = create_args(true);
        let insertion = arguments.len() - 1;
        arguments.splice(insertion..insertion, ["--input-set-id", INPUT_SET_ID]);
        let output = run_with_env(&arguments, &environment);
        assert_eq!(output.status.code(), Some(1));
        let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(result["operation"], "create");
        assert_eq!(result["outcome"], "error");
        assert_eq!(result["inputSetId"], INPUT_SET_ID);
        assert_eq!(result["error"]["httpStatus"], 409);
        assert_eq!(
            result["error"]["problemType"],
            "https://api.usefulmachinery.com/problems/run-input-bindings-invalid"
        );
        assert_eq!(result["error"]["diagnostic"]["code"], "input_kind_mismatch");
        assert_eq!(result["error"]["diagnostic"]["input"], "request");
        assert!(result["error"]["idempotencyKey"].as_str().is_some());
        assert_no_secret_output(&output, &["ticket sentinel"]);
        assert_eq!(server.finish().len(), 1);
    }

    // REQ-RUNINPUT-59, REQ-RUNINPUT-76, REQ-RUNINPUT-77
    #[test]
    fn create_input_set_rejections_without_diagnostic_retain_safe_http_facts() {
        for (status, kind, remedy) in [
            (
                "409 Conflict",
                "run-input-set-not-sealed",
                "Seal the retained input set",
            ),
            (
                "409 Conflict",
                "run-input-set-consumed",
                "Create and seal a new input set",
            ),
            (
                "410 Gone",
                "run-input-set-expired",
                "Create and seal a new input set",
            ),
            (
                "410 Gone",
                "run-input-content-gone",
                "Create and seal a new input set",
            ),
        ] {
            for json in [false, true] {
                let (server, _directory, credential_path) = prepared_run(vec![
                    problem_http_response(
                        status,
                        serde_json::json!({
                            "type": format!("https://api.usefulmachinery.com/problems/{kind}"),
                            "title": "Input set not ready", "status": if status == "410 Gone" { 410 } else { 409 },
                            "detail": "ticket sentinel"
                        }),
                    ),
                ]);
                let environment = deployment_environment(&server.api_url, &credential_path);
                let mut arguments = create_args(json);
                let insertion = arguments.len() - 1;
                arguments.splice(insertion..insertion, ["--input-set-id", INPUT_SET_ID]);
                let output = run_with_env(&arguments, &environment);
                assert_eq!(output.status.code(), Some(1), "{kind}");
                if json {
                    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
                    assert_eq!(
                        result["error"]["httpStatus"],
                        if status == "410 Gone" { 410 } else { 409 }
                    );
                    assert_eq!(
                        result["error"]["problemType"],
                        format!("https://api.usefulmachinery.com/problems/{kind}")
                    );
                    assert!(result["error"].get("diagnostic").is_none());
                    assert_eq!(result["inputSetId"], INPUT_SET_ID);
                } else {
                    let diagnostic = String::from_utf8_lossy(&output.stderr);
                    assert!(
                        diagnostic.contains(kind)
                            && diagnostic.contains(INPUT_SET_ID)
                            && diagnostic.contains(remedy)
                            && diagnostic.contains(if status == "410 Gone" {
                                "HTTP 410"
                            } else {
                                "HTTP 409"
                            }),
                        "{diagnostic}"
                    );
                    assert!(!diagnostic.contains("diagnostic.code"));
                }
                assert_no_secret_output(&output, &["ticket sentinel"]);
                assert_eq!(server.finish().len(), 1);
            }
        }
    }

    // REQ-RUNINPUT-57
    #[test]
    fn explicit_input_set_conflicts_with_acquired_inputs_before_cloud_access() {
        let output = Command::new(env!("CARGO_BIN_EXE_um"))
            .args([
                "workflow",
                "run",
                "create",
                DEFINITION_ID,
                "--cloud",
                ORGANIZATION,
                "--project",
                PROJECT_ID,
                "--input-set-id",
                INPUT_SET_ID,
                "--input-text",
                "request",
                "private argument sentinel",
            ])
            .output()
            .unwrap();

        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        assert!(!String::from_utf8_lossy(&output.stderr).contains("private argument sentinel"));
    }

    // REQ-RUNINPUT-54
    #[test]
    fn run_create_stages_present_named_text_input_files_before_binding_the_sealed_set() {
        for input_bytes in [b"".as_slice(), b"Write a concise limerick.\n".as_slice()] {
            let input_directory = tempfile::tempdir().unwrap();
            let input_path = input_directory.path().join("request.txt");
            fs::write(&input_path, input_bytes).unwrap();
            let input_path = input_path.to_str().unwrap();
            let storage = OneShotServer::respond("204 No Content", None, b"");
            let signed_url = format!(
                "{}/private/request?signature=unique-input-capability-sentinel",
                storage.api_url
            );
            let (server, _credential_directory, credential_path) = prepared_run(vec![
                create_input_set_response(input_bytes, false),
                upload_capability_response(input_bytes, &signed_url),
                seal_input_set_response(input_bytes, false),
                acceptance_response(false),
            ]);
            let environment = deployment_environment(&server.api_url, &credential_path);

            let output = run_with_env(
                &create_args_with_text_input("request", input_path, true),
                &environment,
            );

            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(output.stderr.is_empty());
            let receipt: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(receipt["outcome"], "accepted");
            assert_eq!(receipt["runId"], RUN_ID);
            let mut secrets = vec![TOKEN, "unique-input-capability-sentinel"];
            if !input_bytes.is_empty() {
                secrets.push(std::str::from_utf8(input_bytes).unwrap());
            }
            assert_no_secret_output(&output, &secrets);

            let requests = server.finish();
            assert_eq!(requests.len(), 4);
            assert!(requests[0].starts_with(&format!(
                "POST /api/v1/organizations/{ORGANIZATION}/run-input-sets HTTP/1.1\r\n"
            )));
            assert_eq!(
                request_body(&requests[0]),
                serde_json::json!({
                    "projectId": PROJECT_ID,
                    "schemaVersion": 1,
                    "inputs": {
                        "request": {
                            "kind": "text",
                            "sizeBytes": input_bytes.len(),
                            "sha256": hex_digest(input_bytes)
                        }
                    }
                })
            );
            assert!(requests[1].starts_with(&format!(
                "POST /api/v1/organizations/{ORGANIZATION}/run-input-sets/{INPUT_SET_ID}/upload-capabilities HTTP/1.1\r\n"
            )));
            assert_eq!(
                request_body(&requests[1]),
                serde_json::json!({"members": ["inputs/request"]})
            );
            assert!(requests[2].starts_with(&format!(
                "POST /api/v1/organizations/{ORGANIZATION}/run-input-sets/{INPUT_SET_ID}/seal HTTP/1.1\r\n"
            )));
            assert!(requests[3].starts_with(&format!(
                "POST /api/v1/organizations/{ORGANIZATION}/runs HTTP/1.1\r\n"
            )));
            assert_eq!(
                request_body(&requests[3]),
                serde_json::json!({
                    "projectId": PROJECT_ID,
                    "definition": {"definitionId": DEFINITION_ID},
                    "sourceBranch": "release/next",
                    "displayName": "Release checks",
                    "inputSetId": INPUT_SET_ID
                })
            );
            for request in &requests {
                assert_eq!(
                    header_value(request, "authorization"),
                    format!("Bearer {TOKEN}")
                );
            }
            let mutation_keys = [&requests[0], &requests[2], &requests[3]]
                .map(|request| header_value(request, "idempotency-key"));
            assert!(mutation_keys.iter().all(|key| key.len() == 64));
            assert_ne!(mutation_keys[0], mutation_keys[1]);
            assert_ne!(mutation_keys[1], mutation_keys[2]);
            assert!(!requests[1].contains("idempotency-key:"));

            let upload = storage.finish();
            assert!(upload.starts_with("PUT /api/private/request?signature="));
            assert_eq!(
                upload.split_once("\r\n\r\n").unwrap().1.as_bytes(),
                input_bytes
            );
            assert_eq!(
                header_value(&upload, "content-length"),
                input_bytes.len().to_string()
            );
            assert_eq!(
                header_value(&upload, "content-type"),
                "text/plain; charset=utf-8"
            );
            assert_eq!(header_value(&upload, "if-none-match"), "*");
            assert_eq!(
                header_value(&upload, "x-amz-checksum-sha256"),
                base64::engine::general_purpose::STANDARD.encode(sha256(input_bytes))
            );
            let mut header_names = upload
                .split_once("\r\n\r\n")
                .unwrap()
                .0
                .lines()
                .skip(1)
                .map(|line| line.split_once(':').unwrap().0.to_ascii_lowercase())
                .collect::<Vec<_>>();
            header_names.sort();
            assert_eq!(
                header_names,
                [
                    "content-length",
                    "content-type",
                    "host",
                    "if-none-match",
                    "x-amz-checksum-sha256",
                ]
                .map(str::to_owned)
            );
        }
    }

    // REQ-RUNINPUT-54
    #[test]
    fn run_create_stages_named_json_sources_without_rewriting_bytes() {
        let input_bytes = b"{\n  \"enabled\": true, \"ratio\": 1.00\n}\n";
        let input_directory = tempfile::tempdir().unwrap();
        let input_path = input_directory.path().join("request.json");
        fs::write(&input_path, input_bytes).unwrap();
        let input_path = input_path.to_str().unwrap();

        for (flag, source) in [
            ("--input-json", std::str::from_utf8(input_bytes).unwrap()),
            ("--input-json-file", input_path),
        ] {
            let storage = OneShotServer::respond("204 No Content", None, b"");
            let signed_url = format!(
                "{}/private/request?signature=unique-json-capability-sentinel",
                storage.api_url
            );
            let (server, _credential_directory, credential_path) = prepared_run(vec![
                create_scalar_input_set_response(input_bytes, "json", false),
                scalar_upload_capability_response(input_bytes, "application/json", &signed_url),
                seal_scalar_input_set_response(input_bytes, "json", false),
                acceptance_response(false),
            ]);
            let environment = deployment_environment(&server.api_url, &credential_path);

            let output = run_with_env(
                &create_args_with_scalar_input(flag, "request", source, true),
                &environment,
            );

            assert!(
                output.status.success(),
                "stdout: {}\nstderr: {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(output.stderr.is_empty());
            assert_no_secret_output(
                &output,
                &[
                    TOKEN,
                    "unique-json-capability-sentinel",
                    std::str::from_utf8(input_bytes).unwrap(),
                ],
            );
            let requests = server.finish();
            assert_eq!(
                request_body(&requests[0]),
                serde_json::json!({
                    "projectId": PROJECT_ID,
                    "schemaVersion": 1,
                    "inputs": {
                        "request": {
                            "kind": "json",
                            "sizeBytes": input_bytes.len(),
                            "sha256": hex_digest(input_bytes)
                        }
                    }
                })
            );
            assert_eq!(
                request_body(&requests[1]),
                serde_json::json!({"members": ["inputs/request"]})
            );
            assert_eq!(request_body(&requests[3])["inputSetId"], INPUT_SET_ID);

            let upload = storage.finish();
            assert_eq!(
                upload.split_once("\r\n\r\n").unwrap().1.as_bytes(),
                input_bytes
            );
            assert_eq!(header_value(&upload, "content-type"), "application/json");
            assert_eq!(
                header_value(&upload, "x-amz-checksum-sha256"),
                base64::engine::general_purpose::STANDARD.encode(sha256(input_bytes))
            );
        }
    }

    // REQ-RUNINPUT-53, REQ-RUNINPUT-54
    #[test]
    fn run_create_stages_named_file_with_exact_media_type_and_bytes() {
        let input_bytes = *b"exact file bytes";
        let media_type = "application/octet-stream; version=1";
        let input_directory = tempfile::tempdir().unwrap();
        let input_path = input_directory.path().join("request.bin");
        fs::write(&input_path, input_bytes).unwrap();
        let input_path = input_path.to_str().unwrap();
        let storage = OneShotServer::respond("204 No Content", None, b"");
        let signed_url = format!(
            "{}/private/request?signature=unique-file-capability-sentinel",
            storage.api_url
        );
        let (server, _credential_directory, credential_path) = prepared_run(vec![
            create_file_input_set_response(&input_bytes, media_type),
            scalar_upload_capability_response(&input_bytes, media_type, &signed_url),
            seal_file_input_set_response(&input_bytes, media_type),
            acceptance_response(false),
        ]);
        let environment = deployment_environment(&server.api_url, &credential_path);

        let output = run_with_env(
            &create_args_with_file_input("request", media_type, input_path, true),
            &environment,
        );

        assert!(
            output.status.success(),
            "stdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(output.stderr.is_empty());
        assert_no_secret_output(&output, &[TOKEN, "unique-file-capability-sentinel"]);
        let requests = server.finish();
        assert_eq!(
            request_body(&requests[0]),
            serde_json::json!({
                "projectId": PROJECT_ID,
                "schemaVersion": 1,
                "inputs": {
                    "request": {
                        "kind": "file",
                        "mediaType": media_type,
                        "sizeBytes": input_bytes.len(),
                        "sha256": hex_digest(&input_bytes)
                    }
                }
            })
        );
        assert_eq!(
            request_body(&requests[1]),
            serde_json::json!({"members": ["inputs/request"]})
        );
        assert_eq!(request_body(&requests[3])["inputSetId"], INPUT_SET_ID);

        let upload = storage.finish();
        assert_eq!(
            upload.split_once("\r\n\r\n").unwrap().1.as_bytes(),
            input_bytes
        );
        assert_eq!(header_value(&upload, "content-type"), media_type);
    }

    // REQ-RUNINPUT-53, REQ-RUNINPUT-54, REQ-RUNINPUT-71
    #[test]
    fn run_create_stages_mixed_values_ordered_attachments_and_present_empty_collections() {
        let directory = tempfile::tempdir().unwrap();
        let first_path = directory.path().join("first.txt");
        let second_path = directory.path().join("second.bin");
        fs::write(&first_path, b"first attachment").unwrap();
        fs::write(&second_path, b"second attachment").unwrap();
        let json_bytes = b"{\"enabled\":true}";
        let values: [&[u8]; 4] = [b"", b"first attachment", b"second attachment", json_bytes];
        let media_types = [
            "text/plain; charset=utf-8",
            "text/plain",
            "application/octet-stream",
            "application/json",
        ];
        let storages = values
            .iter()
            .map(|_| OneShotServer::respond("204 No Content", None, b""))
            .collect::<Vec<_>>();
        let urls = storages
            .iter()
            .enumerate()
            .map(|(index, storage)| format!("{}/member-{index}?signature=mixed", storage.api_url))
            .collect::<Vec<_>>();
        let manifest = serde_json::json!({
            "schemaVersion": 1,
            "inputs": {
                "emptyText": {
                    "kind": "text",
                    "sizeBytes": 0,
                    "sha256": hex_digest(b"")
                },
                "evidence": {
                    "kind": "attachments",
                    "items": [
                        {
                            "index": 0,
                            "displayName": null,
                            "mediaType": "text/plain",
                            "sizeBytes": values[1].len(),
                            "sha256": hex_digest(values[1])
                        },
                        {
                            "index": 1,
                            "displayName": null,
                            "mediaType": "application/octet-stream",
                            "sizeBytes": values[2].len(),
                            "sha256": hex_digest(values[2])
                        }
                    ]
                },
                "nothing": {"kind": "attachments", "items": []},
                "settings": {
                    "kind": "json",
                    "sizeBytes": json_bytes.len(),
                    "sha256": hex_digest(json_bytes)
                }
            }
        });
        let canonical = serde_json::to_vec(&manifest).unwrap();
        let member_ids = [
            "inputs/emptyText",
            "inputs/evidence/000000",
            "inputs/evidence/000001",
            "inputs/settings",
        ];
        let set_body = |state: &str, uploaded: bool| {
            let mut body = serde_json::json!({
                "id": INPUT_SET_ID,
                "organizationId": ORGANIZATION_ID,
                "projectId": PROJECT_ID,
                "boundsProfile": 1,
                "manifest": manifest.clone(),
                "manifestDigest": {"algorithm": "sha256", "value": hex_digest(&canonical)},
                "inputCount": 4,
                "attachmentCount": 2,
                "aggregateSizeBytes": values.iter().map(|value| value.len()).sum::<usize>(),
                "state": state,
                "createdAt": "2026-08-24T01:00:00Z",
                "openDeadlineAt": "2999-08-25T01:00:00Z",
                "members": member_ids.iter().map(|member| serde_json::json!({
                    "memberId": member,
                    "uploadConfirmed": uploaded
                })).collect::<Vec<_>>(),
                "replayed": false
            });
            if state == "sealed" {
                body["sealedAt"] = serde_json::json!("2026-08-24T01:02:00Z");
                body["sealedDeadlineAt"] = serde_json::json!("2026-08-25T01:02:00Z");
            }
            body
        };
        let create_response = http_response_with_headers(
            "201 Created",
            Some("application/json"),
            &[
                ("Idempotency-Key", ECHO_IDEMPOTENCY_KEY),
                (
                    "Location",
                    "/v1/organizations/acme-research/run-input-sets/ris_01k0z6r1w8f4jy2m7q9v3x5abc",
                ),
            ],
            &serde_json::to_vec(&set_body("open", false)).unwrap(),
        );
        let capability_response = http_response_with_headers(
            "200 OK",
            Some("application/json"),
            &[("Cache-Control", "private, no-store")],
            &serde_json::to_vec(&serde_json::json!({
                "inputSetId": INPUT_SET_ID,
                "capabilityExpiresAt": "2998-08-24T01:05:00Z",
                "members": member_ids.iter().enumerate().map(|(index, member)| serde_json::json!({
                    "memberId": member,
                    "url": urls[index],
                    "requiredHeaders": {
                        "contentLength": values[index].len().to_string(),
                        "contentType": media_types[index],
                        "ifNoneMatch": "*",
                        "xAmzChecksumSha256": base64::engine::general_purpose::STANDARD.encode(sha256(values[index]))
                    }
                })).collect::<Vec<_>>()
            }))
            .unwrap(),
        );
        let sealed_response = http_response_with_headers(
            "200 OK",
            Some("application/json"),
            &[("Idempotency-Key", ECHO_IDEMPOTENCY_KEY)],
            &serde_json::to_vec(&set_body("sealed", true)).unwrap(),
        );
        let (server, _credentials, credential_path) = prepared_run(vec![
            create_response,
            capability_response,
            sealed_response,
            acceptance_response(false),
        ]);
        let environment = deployment_environment(&server.api_url, &credential_path);
        let mut arguments = create_args(true);
        let insertion = arguments.len() - 1;
        arguments.splice(
            insertion..insertion,
            [
                "--input-text",
                "emptyText",
                "",
                "--input-json",
                "settings",
                std::str::from_utf8(json_bytes).unwrap(),
                "--input-attachment",
                "evidence",
                "text/plain",
                first_path.to_str().unwrap(),
                "--input-attachment",
                "evidence",
                "application/octet-stream",
                second_path.to_str().unwrap(),
                "--input-attachments-empty",
                "nothing",
            ],
        );

        let output = run_with_env(&arguments, &environment);

        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_no_secret_output(&output, &[TOKEN, "first attachment", "second attachment"]);
        let requests = server.finish();
        assert_eq!(request_body(&requests[0])["inputs"], manifest["inputs"]);
        assert_eq!(
            request_body(&requests[1]),
            serde_json::json!({"members": member_ids})
        );
        assert_eq!(request_body(&requests[3])["inputSetId"], INPUT_SET_ID);
        for (storage, expected) in storages.into_iter().zip(values) {
            assert_eq!(
                storage
                    .finish()
                    .split_once("\r\n\r\n")
                    .unwrap()
                    .1
                    .as_bytes(),
                expected
            );
        }
    }

    // REQ-RUNINPUT-55
    #[test]
    fn named_file_parameter_controls_stop_before_cloud_input_set_allocation() {
        let input_directory = tempfile::tempdir().unwrap();
        let input_path = input_directory.path().join("request.bin");
        fs::write(&input_path, b"private file input").unwrap();
        let input_path = input_path.to_str().unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let api_url = format!("http://{}/api", listener.local_addr().unwrap());
        let credential_directory = private_credential_directory();
        let credential_path = credential_directory.path().join("credentials.json");
        write_credential_fixture(&credential_path, &api_url, TOKEN, "2999-01-01T00:00:00Z");
        let environment = deployment_environment(&api_url, credential_path.to_str().unwrap());

        for control in ['\u{000b}', '\u{000c}', '\u{007f}'] {
            let media_type = format!("application/octet-stream;version=one{control}two");
            let output = run_with_env(
                &create_args_with_file_input("request", &media_type, input_path, true),
                &environment,
            );

            assert_eq!(output.status.code(), Some(1));
            assert!(output.stderr.is_empty());
            let failure: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(failure["outcome"], "error");
            assert_eq!(failure["error"]["code"], "invalid_input");
            assert_no_secret_output(&output, &[TOKEN, "private file input"]);
        }
        assert!(matches!(
            listener.accept(),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock
        ));
    }

    #[test]
    fn invalid_json_and_scalar_binding_conflicts_stop_before_cloud_access() {
        let input_directory = tempfile::tempdir().unwrap();
        let text_path = input_directory.path().join("request.txt");
        fs::write(&text_path, b"private text input").unwrap();
        let text_path = text_path.to_str().unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let api_url = format!("http://{}/api", listener.local_addr().unwrap());
        let credential_directory = private_credential_directory();
        let credential_path = credential_directory.path().join("credentials.json");
        write_credential_fixture(&credential_path, &api_url, TOKEN, "2999-01-01T00:00:00Z");
        let environment = deployment_environment(&api_url, credential_path.to_str().unwrap());

        let malformed = run_with_env(
            &create_args_with_scalar_input(
                "--input-json",
                "request",
                "{\"secret\":1,\"secret\":2}",
                true,
            ),
            &environment,
        );
        assert_eq!(malformed.status.code(), Some(1));
        assert_no_secret_output(&malformed, &[TOKEN, "secret"]);

        let malformed_file = run_with_env(
            &create_args_with_file_input("request", "not a media type", text_path, true),
            &environment,
        );
        assert_eq!(malformed_file.status.code(), Some(1));
        assert_no_secret_output(&malformed_file, &[TOKEN, "private text input"]);

        let mut conflicting = create_args_with_text_input("request", text_path, true);
        let insertion = conflicting.len() - 1;
        conflicting.splice(insertion..insertion, ["--input-json", "request", "null"]);
        let conflict = run_with_env(&conflicting, &environment);
        assert_eq!(conflict.status.code(), Some(1));
        assert_no_secret_output(&conflict, &[TOKEN, "private text input"]);
        assert!(matches!(
            listener.accept(),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock
        ));
    }

    // REQ-RUNINPUT-55, REQ-RUNINPUT-72
    #[test]
    fn invalid_and_oversized_named_text_input_files_stop_before_cloud_access() {
        let input_directory = tempfile::tempdir().unwrap();
        let invalid_path = input_directory.path().join("invalid.txt");
        fs::write(&invalid_path, b"secret-valid-prefix\xff").unwrap();
        let oversized_path = input_directory.path().join("oversized.txt");
        fs::write(&oversized_path, vec![b'x'; 1024 * 1024 + 1]).unwrap();
        let valid_path = input_directory.path().join("valid.txt");
        fs::write(&valid_path, b"private valid input sentinel").unwrap();

        for (name, path) in [
            ("request", invalid_path.as_path()),
            ("request", oversized_path.as_path()),
            ("Request", valid_path.as_path()),
            ("request", input_directory.path()),
        ] {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let api_url = format!("http://{}/api", listener.local_addr().unwrap());
            let credential_directory = private_credential_directory();
            let credential_path = credential_directory.path().join("credentials.json");
            write_credential_fixture(&credential_path, &api_url, TOKEN, "2999-01-01T00:00:00Z");
            let environment = deployment_environment(&api_url, credential_path.to_str().unwrap());

            let output = run_with_env(
                &create_args_with_text_input(name, path.to_str().unwrap(), true),
                &environment,
            );

            assert_eq!(output.status.code(), Some(1));
            assert!(output.stderr.is_empty());
            let failure: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(failure["outcome"], "error");
            assert_eq!(failure["error"]["code"], "invalid_input");
            assert_no_secret_output(
                &output,
                &[TOKEN, "secret-valid-prefix", "private valid input sentinel"],
            );
            assert!(matches!(
                listener.accept(),
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock
            ));
        }
    }

    // REQ-RUNINPUT-55
    #[test]
    fn exactly_one_mib_text_input_reaches_input_set_creation() {
        let input_directory = tempfile::tempdir().unwrap();
        let input_path = input_directory.path().join("boundary.txt");
        let input_bytes = vec![b'z'; 1024 * 1024];
        fs::write(&input_path, &input_bytes).unwrap();
        let (server, _credential_directory, credential_path) =
            prepared_run(vec![problem_http_response(
                "403 Forbidden",
                serde_json::json!({
                    "type": "https://api.usefulmachinery.com/problems/forbidden",
                    "title": "Forbidden",
                    "status": 403
                }),
            )]);
        let environment = deployment_environment(&server.api_url, &credential_path);

        let output = run_with_env(
            &create_args_with_text_input("request", input_path.to_str().unwrap(), true),
            &environment,
        );

        assert_eq!(output.status.code(), Some(1));
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["error"]["code"],
            "forbidden"
        );
        let request = server.finish().pop().unwrap();
        assert!(request.contains("/run-input-sets HTTP/1.1"));
        assert_eq!(
            request_body(&request)["inputs"]["request"]["sizeBytes"],
            1024 * 1024
        );
        assert_eq!(
            request_body(&request)["inputs"]["request"]["sha256"],
            hex_digest(&input_bytes)
        );
    }

    // REQ-RUN-56
    #[test]
    fn credential_rejection_reuses_the_create_key_and_reports_the_server_replay() {
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
                    "refresh_token": "unique-cloud-run-refreshed-refresh-token",
                    "token_type": "Bearer",
                    "expires_in": 3600
                }),
            ),
            acceptance_response(true),
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

        let output = run_with_env(&create_args(true), &environment);

        assert!(output.status.success());
        let receipt: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(receipt["runId"], RUN_ID);
        assert_eq!(receipt["replayed"], true);
        assert_no_secret_output(&output, &[TOKEN, REFRESHED_TOKEN]);
        let requests = server.finish();
        assert_eq!(requests.len(), 3);
        assert!(requests[0].starts_with("POST /api/v1/organizations/"));
        assert!(requests[1].starts_with("POST /auth/oauth/token HTTP/1.1\r\n"));
        assert!(requests[2].starts_with("POST /api/v1/organizations/"));
        assert_eq!(
            header_value(&requests[0], "idempotency-key"),
            header_value(&requests[2], "idempotency-key")
        );
        assert_eq!(request_body(&requests[0]), request_body(&requests[2]));
        assert_eq!(
            header_value(&requests[2], "authorization"),
            format!("Bearer {REFRESHED_TOKEN}")
        );
    }

    #[test]
    fn malformed_unauthorized_response_still_refreshes_the_human_session() {
        let server = ScriptedServer::respond(vec![
            http_response_with_headers(
                "401 Unauthorized",
                Some("application/problem+json"),
                &[],
                b"not-json",
            ),
            json_http_response(
                "200 OK",
                serde_json::json!({
                    "access_token": REFRESHED_TOKEN,
                    "refresh_token": "unique-cloud-run-refreshed-refresh-token",
                    "token_type": "Bearer",
                    "expires_in": 3600
                }),
            ),
            acceptance_response(true),
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

        let output = run_with_env(&create_args(true), &environment);

        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stdout)
        );
        let receipt: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(receipt["runId"], RUN_ID);
        assert_no_secret_output(&output, &[TOKEN, REFRESHED_TOKEN]);
        let requests = server.finish();
        assert_eq!(requests.len(), 3);
        assert!(requests[1].starts_with("POST /auth/oauth/token HTTP/1.1\r\n"));
    }

    #[test]
    fn interrupted_create_without_an_idempotency_echo_is_invalid_response() {
        let truncated_response = format!(
            "HTTP/1.1 202 Accepted\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: 4096\r\nLocation: /v1/organizations/{ORGANIZATION}/runs/{RUN_ID}\r\n\r\n{{\"runId\":\"{RUN_ID}\""
        )
        .into_bytes();
        let (server, _directory, credential_path) = prepared_run(vec![truncated_response]);
        let environment = deployment_environment(&server.api_url, &credential_path);

        let output = run_with_env(&create_args(true), &environment);

        assert_invalid_response(&output);
        assert_no_secret_output(&output, &[TOKEN]);
        server.finish();
    }

    // REQ-RUN-56
    #[test]
    fn ambiguous_transport_retry_reuses_the_create_key_and_request() {
        let directory = tempfile::tempdir().unwrap();
        let context_path = directory.path().join("context.json");
        fs::write(&context_path, br#"{"issueId":"retry-context"}"#).unwrap();
        let (server, _credential_directory, credential_path) =
            prepared_run(vec![Vec::new(), acceptance_response(true)]);
        let environment = deployment_environment(&server.api_url, &credential_path);
        let mut arguments = create_args(true);
        let insertion = arguments.len() - 1;
        arguments.splice(
            insertion..insertion,
            ["--integration-context-file", context_path.to_str().unwrap()],
        );

        let output = run_with_env(&arguments, &environment);

        assert!(output.status.success());
        let receipt: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(receipt["runId"], RUN_ID);
        assert_eq!(receipt["replayed"], true);
        let requests = server.finish();
        assert_eq!(requests.len(), 2);
        assert_eq!(
            header_value(&requests[0], "idempotency-key"),
            header_value(&requests[1], "idempotency-key")
        );
        assert_eq!(request_body(&requests[0]), request_body(&requests[1]));
    }

    #[test]
    fn text_input_sequence_refreshes_authority_but_does_not_treat_network_failure_as_upload_evidence()
     {
        let input_bytes = b"Explain why 2 + 3 = 5.\n";
        let input_directory = tempfile::tempdir().unwrap();
        let input_path = input_directory.path().join("request.txt");
        fs::write(&input_path, input_bytes).unwrap();
        let storage = OneShotServer::respond("", None, b"");
        let signed_url = format!(
            "{}/private/request?signature=unique-refreshed-capability-sentinel",
            storage.api_url
        );
        let server = ScriptedServer::respond(vec![
            create_input_set_response(input_bytes, false),
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
                    "refresh_token": "unique-input-refreshed-refresh-token",
                    "token_type": "Bearer",
                    "expires_in": 3600
                }),
            ),
            upload_capability_response(input_bytes, &signed_url),
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
            &create_args_with_text_input("request", input_path.to_str().unwrap(), true),
            &environment,
        );

        assert_eq!(output.status.code(), Some(4));
        let receipt: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(receipt["outcome"], "error");
        assert_eq!(receipt["error"]["code"], "unavailable");
        assert!(receipt["runId"].is_null());
        assert_no_secret_output(
            &output,
            &[
                TOKEN,
                REFRESHED_TOKEN,
                "unique-refreshed-capability-sentinel",
                std::str::from_utf8(input_bytes).unwrap(),
            ],
        );
        let requests = server.finish();
        assert_eq!(requests.len(), 4);
        assert!(requests[1].contains("/upload-capabilities HTTP/1.1"));
        assert!(requests[2].starts_with("POST /auth/oauth/token HTTP/1.1\r\n"));
        assert!(requests[3].contains("/upload-capabilities HTTP/1.1"));
        assert!(!requests[1].contains("idempotency-key:"));
        assert!(!requests[3].contains("idempotency-key:"));
        assert_eq!(
            header_value(&requests[1], "authorization"),
            format!("Bearer {TOKEN}")
        );
        assert_eq!(
            header_value(&requests[3], "authorization"),
            format!("Bearer {REFRESHED_TOKEN}")
        );
        let upload = storage.finish();
        assert!(upload.starts_with("PUT "));
        assert!(!upload.contains("authorization:"));
        assert_eq!(
            upload.split_once("\r\n\r\n").unwrap().1.as_bytes(),
            input_bytes
        );
    }

    #[test]
    fn definite_upload_rejection_with_truncated_body_never_reaches_seal_or_run() {
        let input_bytes = b"definite upload rejection sentinel\n";
        let input_directory = tempfile::tempdir().unwrap();
        let input_path = input_directory.path().join("request.txt");
        fs::write(&input_path, input_bytes).unwrap();
        let storage = api_test_support::ScriptedHttpServer::respond(
            b"HTTP/1.1 403 Forbidden\r\nConnection: close\r\nContent-Length: 100\r\n\r\nshort"
                .to_vec(),
        );
        let signed_url = format!("{}private/request?signature=private", storage.api_url);
        let (server, _credential_directory, credential_path) = prepared_run(vec![
            create_input_set_response(input_bytes, false),
            upload_capability_response(input_bytes, &signed_url),
            seal_input_set_response(input_bytes, false),
            acceptance_response(false),
        ]);
        let environment = deployment_environment(&server.api_url, &credential_path);

        let output = run_with_env(
            &create_args_with_text_input("request", input_path.to_str().unwrap(), true),
            &environment,
        );
        let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();

        // A corrected command consumes only the first two scripted responses. Drain the fixture's
        // deliberately unreachable responses after the command so the same test terminates on both
        // sides of the assertion without making those requests part of command behavior.
        if !output.status.success() {
            let authority = server
                .api_url
                .strip_prefix("http://")
                .unwrap()
                .split('/')
                .next()
                .unwrap();
            for index in 0..2 {
                let mut stream = std::net::TcpStream::connect(authority).unwrap();
                write!(
                    stream,
                    "GET /review-fixture-drain/{index} HTTP/1.1\r\nHost: {authority}\r\nIdempotency-Key: fixture-drain-{index}\r\nConnection: close\r\n\r\n"
                )
                .unwrap();
                let mut ignored = Vec::new();
                stream.read_to_end(&mut ignored).unwrap();
            }
        }
        let requests = server.finish();
        storage.finish_one();
        let command_continued = !requests[2].starts_with("GET /review-fixture-drain/");

        assert!(
            !output.status.success()
                && result["outcome"] != "accepted"
                && result["runId"].is_null()
                && !command_continued,
            "a definite 403 must abort before seal and CreateRun; observed status={:?}, outcome={:?}, run_id_present={}, continued={command_continued}",
            output.status.code(),
            result["outcome"],
            !result["runId"].is_null(),
        );
    }

    #[test]
    fn text_input_upload_redirect_is_not_followed_or_reported_as_acceptance() {
        let input_bytes = b"redirect safety sentinel\n";
        let input_directory = tempfile::tempdir().unwrap();
        let input_path = input_directory.path().join("request.txt");
        fs::write(&input_path, input_bytes).unwrap();
        let redirect_secret = "unique-redirect-target-sentinel";
        let storage = api_test_support::ScriptedHttpServer::respond(http_response_with_headers(
            "307 Temporary Redirect",
            None,
            &[(
                "Location",
                "https://storage.invalid/private/redirect?signature=unique-redirect-target-sentinel",
            )],
            b"storage response details",
        ));
        let signed_url = format!(
            "{}private/request?signature=unique-original-capability-sentinel",
            storage.api_url
        );
        let (server, _credential_directory, credential_path) = prepared_run(vec![
            create_input_set_response(input_bytes, false),
            upload_capability_response(input_bytes, &signed_url),
        ]);
        let environment = deployment_environment(&server.api_url, &credential_path);

        let output = run_with_env(
            &create_args_with_text_input("request", input_path.to_str().unwrap(), true),
            &environment,
        );

        assert_eq!(output.status.code(), Some(1));
        let diagnostic = String::from_utf8_lossy(&output.stderr);
        assert!(diagnostic.contains(&format!("input set: {INPUT_SET_ID}")));
        assert!(diagnostic.contains("run input-set show"));
        let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(result["outcome"], "error");
        assert_eq!(result["error"]["code"], "submission_failed");
        assert!(result["runId"].is_null());
        assert!(result.get("inputSetId").is_none());
        assert_no_secret_output(
            &output,
            &[
                TOKEN,
                redirect_secret,
                "unique-original-capability-sentinel",
                std::str::from_utf8(input_bytes).unwrap(),
                "storage response details",
            ],
        );
        assert_eq!(server.finish().len(), 2);
        let upload = storage.finish_one();
        assert!(upload.starts_with("PUT "));
        assert!(!upload.contains("authorization:"));
    }

    // REQ-RUNINPUT-58
    #[test]
    fn failed_staging_reports_allocated_input_set_in_human_mode() {
        let input_directory = tempfile::tempdir().unwrap();
        let input_path = input_directory.path().join("request.txt");
        let input_bytes = b"staging rejection sentinel\n";
        fs::write(&input_path, input_bytes).unwrap();
        let (server, _credential_directory, credential_path) = prepared_run(vec![
            create_input_set_response(input_bytes, false),
            problem_http_response(
                "403 Forbidden",
                serde_json::json!({
                    "type": "https://api.usefulmachinery.com/problems/forbidden",
                    "title": "Forbidden",
                    "status": 403
                }),
            ),
        ]);
        let environment = deployment_environment(&server.api_url, &credential_path);

        let output = run_with_env(
            &create_args_with_text_input("request", input_path.to_str().unwrap(), false),
            &environment,
        );

        assert_eq!(output.status.code(), Some(1));
        assert!(output.stdout.is_empty());
        let diagnostic = String::from_utf8_lossy(&output.stderr);
        assert!(diagnostic.contains(&format!("input set: {INPUT_SET_ID}")));
        assert!(diagnostic.contains("run input-set show"));
        assert_no_secret_output(&output, &[TOKEN, std::str::from_utf8(input_bytes).unwrap()]);
        let requests = server.finish();
        assert_eq!(requests.len(), 2);
        assert!(requests[1].contains(&format!(
            "/run-input-sets/{INPUT_SET_ID}/upload-capabilities"
        )));
    }

    // REQ-RUNINPUT-60
    #[test]
    fn precondition_replay_is_resolved_by_authoritative_sealing() {
        let input_bytes = b"already uploaded named input sentinel\n";
        let input_directory = tempfile::tempdir().unwrap();
        let input_path = input_directory.path().join("request.txt");
        fs::write(&input_path, input_bytes).unwrap();
        let storage = OneShotServer::respond("412 Precondition Failed", None, b"");
        let signed_url = format!("{}/private/request?signature=private", storage.api_url);
        let (server, _credential_directory, credential_path) = prepared_run(vec![
            create_input_set_response(input_bytes, false),
            upload_capability_response(input_bytes, &signed_url),
            seal_input_set_response(input_bytes, false),
            acceptance_response(false),
        ]);
        let environment = deployment_environment(&server.api_url, &credential_path);

        let output = run_with_env(
            &create_args_with_text_input("request", input_path.to_str().unwrap(), true),
            &environment,
        );

        assert!(
            output.status.success(),
            "a 412 replay must be resolved by sealing: {}",
            String::from_utf8_lossy(&output.stdout)
        );
        let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(result["outcome"], "accepted");
        assert_eq!(result["runId"], RUN_ID);
        assert_eq!(server.finish().len(), 4);
        storage.finish();
    }

    const WORKSPACE_COMMIT: &str = "a3d0e2d3abc0bf0742be096155f683bf985e485a";
    const CREATE_REQUEST_ID: &str = "req_01m622k6qsf09npetkmdkdj51j";

    fn create_args_with_workspace_commit(json: bool) -> Vec<&'static str> {
        let mut args = create_args(json);
        let insertion = args.len() - 1;
        args.splice(
            insertion..insertion,
            ["--workspace-commit", WORKSPACE_COMMIT],
        );
        args
    }

    fn create_problem_response(status: &str, kind: &str, request_id: Option<&str>) -> Vec<u8> {
        let status_number: u16 = status.split(' ').next().unwrap().parse().unwrap();
        let body = serde_json::json!({
            "type": format!("https://api.usefulmachinery.com/problems/{kind}"),
            "title": "server title sentinel",
            "status": status_number,
            "detail": "server detail sentinel"
        });
        let headers: Vec<(&str, &str)> = request_id
            .map(|id| ("X-Request-ID", id))
            .into_iter()
            .collect();
        http_response_with_headers(
            status,
            Some("application/problem+json"),
            &headers,
            &serde_json::to_vec(&body).unwrap(),
        )
    }

    // REQ-RUN-80
    #[test]
    fn unsupported_workspace_selection_is_a_definite_admission_rejection() {
        for json in [false, true] {
            let (server, _directory, credential_path) =
                prepared_run(vec![create_problem_response(
                    "409 Conflict",
                    "workspace-selection-unsupported",
                    Some(CREATE_REQUEST_ID),
                )]);
            let output = run_with_env(
                &create_args_with_workspace_commit(json),
                &deployment_environment(&server.api_url, &credential_path),
            );
            assert_eq!(output.status.code(), Some(1));
            if json {
                let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
                assert_eq!(result["operation"], "create");
                assert_eq!(result["outcome"], "error");
                assert!(result["runId"].is_null());
                assert_eq!(result["error"]["code"], "admission_rejected");
                assert_eq!(result["error"]["httpStatus"], 409);
                assert_eq!(
                    result["error"]["problemType"],
                    "https://api.usefulmachinery.com/problems/workspace-selection-unsupported"
                );
                assert_eq!(result["error"]["correlationId"], CREATE_REQUEST_ID);
            } else {
                assert!(output.stdout.is_empty());
                let diagnostic = String::from_utf8_lossy(&output.stderr);
                assert!(
                    diagnostic.contains(
                        "https://api.usefulmachinery.com/problems/workspace-selection-unsupported"
                    ) && diagnostic.contains(CREATE_REQUEST_ID)
                        && diagnostic.contains("HTTP 409"),
                    "{diagnostic}"
                );
                assert!(!diagnostic.contains("acceptance_unknown"), "{diagnostic}");
            }
            assert_no_secret_output(
                &output,
                &["server title sentinel", "server detail sentinel"],
            );
            let requests = server.finish();
            assert_eq!(requests.len(), 1);
            assert!(requests[0].contains(WORKSPACE_COMMIT));
        }
    }

    // REQ-RUN-80
    #[test]
    fn admission_rejection_without_request_id_omits_correlation_id() {
        let (server, _directory, credential_path) = prepared_run(vec![create_problem_response(
            "422 Unprocessable Entity",
            "workflow-source-invalid",
            None,
        )]);
        let output = run_with_env(
            &create_args(true),
            &deployment_environment(&server.api_url, &credential_path),
        );
        assert_eq!(output.status.code(), Some(1));
        let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(result["outcome"], "error");
        assert_eq!(result["error"]["code"], "admission_rejected");
        assert_eq!(result["error"]["httpStatus"], 422);
        assert_eq!(
            result["error"]["problemType"],
            "https://api.usefulmachinery.com/problems/workflow-source-invalid"
        );
        // The member is omitted, not null, like `httpStatus` and `problemType` outside a rejection.
        assert!(result["error"].get("correlationId").is_none());
        assert_eq!(server.finish().len(), 1);
    }

    // REQ-RUN-57
    #[test]
    fn create_server_error_after_dispatch_remains_acceptance_unknown() {
        let (server, _directory, credential_path) = prepared_run(vec![create_problem_response(
            "503 Service Unavailable",
            "source-resolution-unavailable",
            Some(CREATE_REQUEST_ID),
        )]);
        let output = run_with_env(
            &create_args_with_workspace_commit(true),
            &deployment_environment(&server.api_url, &credential_path),
        );
        assert_eq!(output.status.code(), Some(4));
        let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(result["outcome"], "acceptance_unknown");
        assert_eq!(result["error"]["code"], "acceptance_unknown");
        let requests = server.finish();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            result["error"]["idempotencyKey"],
            header_value(&requests[0], "idempotency-key")
        );
    }

    // REQ-RUN-57
    #[test]
    fn invalid_create_acceptance_in_plain_mode_retains_reconciliation_key() {
        let (server, _directory, credential_path) = prepared_run(vec![acceptance_response_for(
            "202 Accepted",
            RUN_ID,
            false,
            &[],
        )]);
        let output = run_with_env(
            &create_args(false),
            &deployment_environment(&server.api_url, &credential_path),
        );
        assert_eq!(output.status.code(), Some(4));
        assert!(output.stdout.is_empty());
        let requests = server.finish();
        assert_eq!(requests.len(), 1);
        let key = header_value(&requests[0], "idempotency-key");
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert!(stderr.contains("acceptance_unknown"));
        assert!(stderr.contains(&format!("idempotency key: {key}")));
    }

    #[test]
    fn run_create_rejects_malformed_success_envelopes() {
        let cases = [
            acceptance_response_for("202 Accepted", RUN_ID, false, &[]),
            acceptance_response_for(
                "201 Created",
                RUN_ID,
                false,
                &[
                    ("Idempotency-Key", ECHO_IDEMPOTENCY_KEY),
                    (
                        "Location",
                        "/v1/organizations/acme-research/runs/run_01k0z6r1w8f4jy2m7q9v3x5abc",
                    ),
                ],
            ),
            acceptance_response_for(
                "202 Accepted",
                RUN_ID,
                false,
                &[
                    ("Idempotency-Key", "mismatched-request-key"),
                    (
                        "Location",
                        "/v1/organizations/acme-research/runs/run_01k0z6r1w8f4jy2m7q9v3x5abc",
                    ),
                ],
            ),
            acceptance_response_for(
                "202 Accepted",
                RUN_ID,
                false,
                &[
                    ("Idempotency-Key", ECHO_IDEMPOTENCY_KEY),
                    ("Idempotency-Key", ECHO_IDEMPOTENCY_KEY),
                    (
                        "Location",
                        "/v1/organizations/acme-research/runs/run_01k0z6r1w8f4jy2m7q9v3x5abc",
                    ),
                ],
            ),
            acceptance_response_for(
                "202 Accepted",
                RUN_ID,
                false,
                &[
                    ("Idempotency-Key", ECHO_IDEMPOTENCY_KEY),
                    (
                        "Location",
                        "/v1/organizations/acme-research/runs/run_01k0z6r1w8f4jy2m7q9v3x5abd",
                    ),
                ],
            ),
        ];
        for response in cases {
            let (server, _directory, credential_path) = prepared_run(vec![response]);
            let environment = deployment_environment(&server.api_url, &credential_path);

            let output = run_with_env(&create_args(true), &environment);

            assert_invalid_response(&output);
            server.finish();
        }
    }

    // REQ-RUN-57
    #[cfg(target_os = "linux")]
    #[test]
    fn signalled_create_reports_unknown_commitment_without_exposing_credentials() {
        for (signal, expected_exit) in [
            (rustix::process::Signal::INT, 130),
            (rustix::process::Signal::TERM, 143),
        ] {
            for json in [true, false] {
                let mut server =
                    ScriptedServer::respond_with_paused_first_response(vec![acceptance_response(
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
                let environment =
                    deployment_environment(&server.api_url, credential_path.to_str().unwrap());
                let mut command = Command::new(env!("CARGO_BIN_EXE_um"));
                command
                    .args(create_args(json))
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
                assert!(request.starts_with("POST /api/v1/organizations/"));

                rustix::process::kill_process(
                    rustix::process::Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap(),
                    signal,
                )
                .unwrap();
                let output = child.wait_with_output().unwrap();
                server.release_paused_response();

                assert_eq!(output.status.code(), Some(expected_exit));
                let key = header_value(&request, "idempotency-key");
                if json {
                    assert!(output.stderr.is_empty());
                    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
                    assert_eq!(result["outcome"], "acceptance_unknown");
                    assert_eq!(result["error"]["code"], "acceptance_unknown");
                    assert_eq!(result["error"]["idempotencyKey"], key);
                    assert_eq!(result["organizationRef"], ORGANIZATION);
                    assert!(result["runId"].is_null());
                } else {
                    assert!(output.stdout.is_empty());
                    let diagnostic = String::from_utf8_lossy(&output.stderr);
                    assert!(
                        diagnostic
                            .lines()
                            .any(|line| line == format!("idempotency key: {key}"))
                    );
                    assert!(diagnostic.contains("commitment: unknown"));
                }
                assert_no_secret_output(&output, &[TOKEN]);
                assert!(server.finish().is_empty());
            }
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn interruption_during_text_input_upload_never_claims_run_acceptance() {
        for json in [true, false] {
            let input_bytes = b"private interrupted named input sentinel\n";
            let input_directory = tempfile::tempdir().unwrap();
            let input_path = input_directory.path().join("request.txt");
            fs::write(&input_path, input_bytes).unwrap();
            let mut storage = ScriptedServer::respond_with_paused_first_response(vec![
                http_response_with_headers("204 No Content", None, &[], b""),
            ]);
            let signed_url = format!(
                "{}/private/request?signature=unique-interrupted-capability-sentinel",
                storage.api_url
            );
            let (server, _credential_directory, credential_path) = prepared_run(vec![
                create_input_set_response(input_bytes, false),
                upload_capability_response(input_bytes, &signed_url),
            ]);
            let environment = deployment_environment(&server.api_url, &credential_path);
            let mut command = Command::new(env!("CARGO_BIN_EXE_um"));
            command
                .args(create_args_with_text_input(
                    "request",
                    input_path.to_str().unwrap(),
                    json,
                ))
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
            let upload = storage.next_request();
            assert!(upload.starts_with("PUT "));
            assert!(!upload.contains("authorization:"));

            rustix::process::kill_process(
                rustix::process::Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap(),
                rustix::process::Signal::INT,
            )
            .unwrap();
            let output = child.wait_with_output().unwrap();
            storage.release_paused_response();

            assert_eq!(output.status.code(), Some(130));
            let diagnostic = String::from_utf8_lossy(&output.stderr);
            assert!(diagnostic.contains(&format!("input set: {INPUT_SET_ID}")));
            assert!(diagnostic.contains("run input-set show"));
            if json {
                let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
                assert_eq!(result["outcome"], "acceptance_unknown");
                assert_eq!(result["error"]["code"], "acceptance_unknown");
                assert!(result["runId"].is_null());
                assert!(result.get("inputSetId").is_none());
                assert_eq!(result["organizationRef"], ORGANIZATION);
            } else {
                assert!(output.stdout.is_empty());
            }
            assert_no_secret_output(
                &output,
                &[
                    TOKEN,
                    "unique-interrupted-capability-sentinel",
                    std::str::from_utf8(input_bytes).unwrap(),
                ],
            );
            assert_eq!(server.finish().len(), 2);
            assert!(storage.finish().is_empty());
        }
    }
}
