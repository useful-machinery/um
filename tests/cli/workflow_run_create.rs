use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use serde_json::Value;
use tempfile::TempDir;

use super::workflow_definition::{Home, Tree, read_json, schema};
use super::workflow_run::{assert_usage_error, isolated_command, succeeded, tree_rejection};

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
        let inherited = std::env::var_os("PATH").unwrap_or_default();
        let path = std::env::join_paths(
            std::iter::once(self.scratch.path().join("tools"))
                .chain(std::env::split_paths(&inherited)),
        )
        .unwrap();
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
    fn runs_directory(&self) -> PathBuf {
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
