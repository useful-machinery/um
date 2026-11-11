use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;
use std::sync::Arc;

use serde_json::{Map, Value, json};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use super::*;
use crate::workflow::admission::{ResolvedInput, ResolvedInputs};
use crate::workflow::local_run::LocalRunDirectoryError;
use crate::workflow::runtime::EnteredWait;

const RUN: &str = "run_01jcf9a4r7t2m8k3v6x1z5n0pq";
const OTHER_RUN: &str = "run_01jcfa3m6p9q2r5s8t1v4w7x0y";
const SINCE: &str = "2026-11-07T21:41:58Z";

fn validator(schema: &str) -> jsonschema::Validator {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../schemas")
        .join(schema);
    let schema = serde_json::from_slice::<Value>(&fs::read(path).unwrap()).unwrap();
    jsonschema::draft202012::new(&schema).unwrap()
}

fn assert_valid(validator: &jsonschema::Validator, document: &Value) {
    assert!(
        validator.is_valid(document),
        "{document}: {:?}",
        validator.iter_errors(document).collect::<Vec<_>>()
    );
}

fn since() -> OffsetDateTime {
    OffsetDateTime::parse(SINCE, &Rfc3339).unwrap()
}

fn entered(kind: WaitKind, step: &str, then: &str, on_hand_back: Option<&str>) -> EnteredWait {
    EnteredWait {
        step: step.to_owned(),
        outcome: (kind != WaitKind::Stop).then(|| "blocked".to_owned()),
        kind,
        then: then.to_owned(),
        on_hand_back: on_hand_back.map(str::to_owned),
        rule: None,
    }
}

fn object(value: Value) -> Map<String, Value> {
    value.as_object().unwrap().clone()
}

fn control(event: &str, run: &str, wait: Option<&str>, payload: Option<Value>) -> ControlEvent {
    ControlEvent {
        source: "um".to_owned(),
        event: event.to_owned(),
        run: run.to_owned(),
        by: "saeed".to_owned(),
        at: "2026-11-07T22:00:00Z".to_owned(),
        wait: wait.map(str::to_owned),
        payload: payload.map(object),
    }
}

fn record(kind: WaitKind, step: &str, then: &str, on_hand_back: Option<&str>) -> WaitRecord {
    WaitRecord::entered(
        &entered(kind, step, then, on_hand_back),
        None,
        Map::new(),
        since(),
        &ResolvedInputs::default(),
        &BTreeMap::new(),
    )
    .unwrap()
}

// REQ-WFDEF-5, REQ-LOCALRUN-178, REQ-WFEXEC-61, REQ-WFEXEC-62
#[test]
fn an_entered_wait_derives_its_rule_from_the_kind_and_writes_the_published_record() {
    let validator = validator("workflow-run-wait-v1.schema.json");
    let correlated = "event.run == run.id and event.wait == wait.id";
    for (kind, name, step, then, on_hand_back, events, where_) in [
        (
            WaitKind::Answer,
            "answer",
            "design",
            "design",
            None,
            json!(["control.answer"]),
            "event.run == run.id and event.wait == wait.id and event.payload.complete == true",
        ),
        (
            WaitKind::Approval,
            "approval",
            "review",
            "merge",
            Some("fix"),
            json!(["control.approve", "control.hand_back"]),
            correlated,
        ),
        (
            WaitKind::Repair,
            "repair",
            "build",
            "build",
            None,
            json!(["control.retry", "control.withdraw"]),
            correlated,
        ),
        (
            WaitKind::Stop,
            "stop",
            "merge",
            "merge",
            None,
            json!(["control.lift"]),
            "event.run == run.id and event.payload.before == wait.then and event.payload.stop == wait.payload.at",
        ),
    ] {
        let payload = object(json!({"outcome": "blocked", "child": "children/design/000001"}));
        let record = WaitRecord::entered(
            &entered(kind, step, then, on_hand_back),
            None,
            payload.clone(),
            since(),
            &ResolvedInputs::default(),
            &BTreeMap::new(),
        )
        .unwrap();
        let document = serde_json::to_value(&record).unwrap();
        assert_valid(&validator, &document);
        let mut expected = json!({
            "kind": name,
            "since": SINCE,
            "step": [step],
            "then": then,
            "rule": {"source": "um", "event": events, "where": where_},
            "payload": payload,
        });
        if let Some(on_hand_back) = on_hand_back {
            expected["onHandBack"] = json!(on_hand_back);
        }
        assert_eq!(document, expected, "{name}");
        assert_eq!(record.id(), format!("{name}@{SINCE}"));
        assert!(record.is_valid());
        let parsed = serde_json::from_value::<WaitRecord>(document).unwrap();
        assert_eq!(parsed, record);
    }
}

// REQ-WFDEF-5
#[test]
fn a_wait_entered_at_a_fan_out_element_names_the_element_in_its_step() {
    let validator = validator("workflow-run-wait-v1.schema.json");
    for (item, step, path) in [
        (Some(2), json!(["build", {"item": 2}]), "build[2]"),
        (None, json!(["build"]), "build"),
    ] {
        let record = WaitRecord::entered(
            &entered(WaitKind::Answer, "build", "build", None),
            item,
            Map::new(),
            since(),
            &ResolvedInputs::default(),
            &BTreeMap::new(),
        )
        .unwrap();
        let document = serde_json::to_value(&record).unwrap();
        assert_valid(&validator, &document);
        assert_eq!(document["step"], step);
        assert_eq!(record.step_path(), path);
        assert!(record.is_valid());
        assert_eq!(
            serde_json::from_value::<WaitRecord>(document).unwrap(),
            record
        );
    }
    // An element below 1, or a first position element that is not a step, is no record.
    let mut record = record(WaitKind::Answer, "build", "build", None);
    for step in [
        json!(["build", {"item": 0}]),
        json!([{"item": 2}]),
        json!(["build", {"item": 2, "index": 1}]),
    ] {
        record.step = step.as_array().unwrap().clone();
        assert!(!record.is_valid(), "{step}");
    }
}

// REQ-WFDEF-5, REQ-WFEXEC-68, REQ-LOCALRUN-199
#[test]
fn a_position_names_a_fan_out_element_only_as_its_step_and_one_item() {
    let mut record = record(WaitKind::Answer, "build", "build", None);
    for (position, expected) in [
        (json!(["build", {"item": 2}]), Some(("build", 2))),
        (json!(["build"]), None),
        (json!(["build", {"item": 2}, {"item": 3}]), None),
        (json!(["build", {"item": "2"}]), None),
        (json!(["build", {}]), None),
        (json!([{"item": 2}]), None),
    ] {
        let position = position.as_array().unwrap().clone();
        assert_eq!(fan_out_element(&position), expected, "{position:?}");
        record.step = position;
        assert_eq!(record.fan_out_element(), expected, "{:?}", record.step);
    }
}

/// A run directory holding `run.json` and attempt 1's copy of `wait`, the wait an answer
/// with `answers` and the string field `summary` resolves.
fn answered_run(wait: &WaitRecord) -> tempfile::TempDir {
    resolved_run(
        wait,
        "control.answer",
        json!({"complete": true, "answers": {"q1": "yes"}, "summary": "ship it"}),
    )
}

/// A run directory holding `run.json` and attempt 1's copy of `wait`, resolved by a control
/// of `event` with `payload`.
fn resolved_run(wait: &WaitRecord, event: &str, payload: Value) -> tempfile::TempDir {
    let root = run_root();
    let record = crate::workflow::run_record::WorkflowRunRecord::new(
        None,
        &format!("sha256:{}", "a".repeat(64)),
        "workflow.yaml",
        "saeed",
    );
    fs::write(
        root.path().join("run.json"),
        serde_json::to_vec(&record).unwrap(),
    )
    .unwrap();
    fs::create_dir_all(root.path().join("attempts/000001")).unwrap();
    fs::write(
        root.path().join("attempts/000001/wait.json"),
        serde_json::to_vec(wait).unwrap(),
    )
    .unwrap();
    append_control(
        root.path(),
        &control(event, &record.id, Some(&wait.id()), Some(payload)),
    )
    .unwrap();
    root
}

// REQ-WFEXEC-63, REQ-CONTINUE-75, REQ-AGENT-59
#[test]
fn attempt_controls_supply_wait_fields_only_to_the_attempt_continuing_the_wait() {
    let wait = record(WaitKind::Answer, "review", "build", None);
    let run = answered_run(&wait);
    // The attempt that continues the resolved wait reads the control payload's string fields.
    let controls = crate::workflow::local_run::attempt_controls(run.path(), 2)
        .unwrap()
        .controls;
    assert_eq!(
        controls.wait,
        Some((
            "build".to_owned(),
            BTreeMap::from([("summary".to_owned(), "ship it".to_owned())])
        ))
    );
    // An attempt after one that ended without a wait (a backward or limited edge ending
    // `continued`, an abandoned attempt) does not continue from that wait: `build` reached
    // again has no `wait.<field>`, while `controls.answers` still carries the latest answer.
    fs::create_dir_all(run.path().join("attempts/000002")).unwrap();
    let controls = crate::workflow::local_run::attempt_controls(run.path(), 3)
        .unwrap()
        .controls;
    assert_eq!(controls.wait, None);
    assert_eq!(
        controls
            .answers
            .get("build")
            .map(|answers| (**answers).clone()),
        Some(json!({"q1": "yes"}))
    );
    // The agent-profile replacement a `control.retry` carried is retained for every later
    // attempt (REQ-AGENT-59), however many attempts back the wait it resolved is.
    let wait = record(WaitKind::Repair, "build", "build", None);
    let profiles = json!({"builder": "claude-code"});
    let run = resolved_run(
        &wait,
        "control.retry",
        json!({"agentProfiles": profiles, "note": "again"}),
    );
    fs::create_dir_all(run.path().join("attempts/000002")).unwrap();
    let resolved = crate::workflow::local_run::attempt_controls(run.path(), 3).unwrap();
    assert_eq!(resolved.controls.wait, None);
    assert_eq!(resolved.replacement, Some(("build".to_owned(), profiles)));
}

// REQ-WFDEF-5, REQ-CONTINUE-75, REQ-WFEXEC-68
#[test]
fn attempt_controls_resume_the_element_of_a_wait_continuing_at_the_waiting_step() {
    for (then, item, expected) in [
        ("build", Some(2), Some(("build".to_owned(), 2))),
        ("design", Some(2), None),
        ("build", None, None),
    ] {
        let wait = WaitRecord::entered(
            &entered(WaitKind::Answer, "build", then, None),
            item,
            Map::new(),
            since(),
            &ResolvedInputs::default(),
            &BTreeMap::new(),
        )
        .unwrap();
        let run = answered_run(&wait);
        let controls = crate::workflow::local_run::attempt_controls(run.path(), 2)
            .unwrap()
            .controls;
        assert_eq!(controls.resume_item, expected, "{then} {item:?}");
        assert_eq!(
            controls.wait.map(|(target, _)| target).as_deref(),
            Some(then)
        );
    }
    // Only the attempt that continues the wait resumes at its element: an attempt that
    // reaches the step after a later attempt ended without a wait (a backward edge, an
    // abandoned attempt) starts at the first element (REQ-WFEXEC-68).
    let wait = WaitRecord::entered(
        &entered(WaitKind::Answer, "build", "build", None),
        Some(2),
        Map::new(),
        since(),
        &ResolvedInputs::default(),
        &BTreeMap::new(),
    )
    .unwrap();
    let run = answered_run(&wait);
    fs::create_dir_all(run.path().join("attempts/000002")).unwrap();
    let controls = crate::workflow::local_run::attempt_controls(run.path(), 3)
        .unwrap()
        .controls;
    assert_eq!(controls.resume_item, None);
}

// REQ-WFDEF-5, REQ-WFEXEC-59
#[test]
fn a_wait_without_a_derived_rule_or_continuing_at_an_exit_has_no_record() {
    // A `children` wait, and an `external` wait without its authored rule, have no rule.
    assert_eq!(
        WaitRecord::entered(
            &entered(WaitKind::External, "merge", "merge", None),
            None,
            Map::new(),
            since(),
            &ResolvedInputs::default(),
            &BTreeMap::new()
        ),
        Err(LocalRunDirectoryError::WaitRecordInvalid)
    );
    // The published record's `then` is a step identifier (`wait/invalid/then-exit.json`).
    assert_eq!(
        WaitRecord::entered(
            &entered(WaitKind::Answer, "design", "exit.landed", None),
            None,
            Map::new(),
            since(),
            &ResolvedInputs::default(),
            &BTreeMap::new()
        ),
        Err(LocalRunDirectoryError::WaitRecordInvalid)
    );
}

const PULL_REQUEST: &str = "https://github.com/useful-machinery/platform/pull/3958";

/// The `merge` step's `admitted` edge: an `external` wait on the queue of the pull request
/// `design` exported, continuing at `merge`.
fn external_wait(where_: &str) -> EnteredWait {
    EnteredWait {
        rule: Some(WaitRule {
            source: "github".to_owned(),
            event: vec!["pull_request.queue_settled".to_owned()],
            where_: where_.to_owned(),
        }),
        ..entered(WaitKind::External, "merge", "merge", None)
    }
}

/// The run's step states: `design` succeeded exporting text `pullRequest`.
fn design_outputs() -> BTreeMap<String, StepState<CapturedValue>> {
    BTreeMap::from([(
        "design".to_owned(),
        StepState::Succeeded {
            outputs: BTreeMap::from([(
                "pullRequest".to_owned(),
                CapturedValue::text(Arc::from(PULL_REQUEST)),
            )]),
        },
    )])
}

fn external_record(where_: &str) -> Result<WaitRecord, LocalRunDirectoryError> {
    WaitRecord::entered(
        &external_wait(where_),
        None,
        object(json!({"outcome": "admitted"})),
        since(),
        &ResolvedInputs::new(BTreeMap::from([(
            "ticket".to_owned(),
            ResolvedInput::Text(Arc::from("AUTO-208")),
        )])),
        &design_outputs(),
    )
}

fn queue_settled(pull_request: &str, state: &str) -> ExternalEvent {
    ExternalEvent {
        source: "github".to_owned(),
        event: "pull_request.queue_settled".to_owned(),
        at: "2026-11-07T22:30:00Z".to_owned(),
        payload: object(json!({"pullRequest": pull_request, "state": state, "headSha": "4f2a"})),
    }
}

// REQ-WFEXEC-65, REQ-WFDEF-5: an `external` wait's record carries the authored rule with its
// run-value operands resolved to quoted literals, so the matcher reads the rule alone.
#[test]
fn an_external_wait_writes_its_authored_rule_with_run_values_resolved() {
    let record = external_record(
        "event.pullRequest == outputs.design.pullRequest and event.state == \"merged\"",
    )
    .unwrap();
    let document = serde_json::to_value(&record).unwrap();
    assert_valid(&validator("workflow-run-wait-v1.schema.json"), &document);
    assert_eq!(
        document["rule"],
        json!({
            "source": "github",
            "event": ["pull_request.queue_settled"],
            "where": format!(
                "event.pullRequest == \"{PULL_REQUEST}\" and event.state == \"merged\""
            ),
        })
    );
    assert_eq!(document["kind"], "external");
    assert_eq!(document["then"], "merge");
    assert_eq!(
        serde_json::from_value::<WaitRecord>(document).unwrap(),
        record
    );

    // An input operand resolves the same way, on either side of an equality.
    let record = external_record("inputs.ticket == event.ticket").unwrap();
    assert_eq!(record.rule.where_, "\"AUTO-208\" == event.ticket");

    // An operand whose value the run does not hold (its producer did not run, or the input
    // was omitted) leaves no record.
    for where_ in [
        "event.pullRequest == outputs.review.pullRequest",
        "event.pullRequest == outputs.design.branch",
        "event.ticket == inputs.title",
    ] {
        assert_eq!(
            external_record(where_),
            Err(LocalRunDirectoryError::WaitRecordInvalid),
            "{where_}"
        );
    }
}

// REQ-CONTINUE-74, INV-ENGINE-21: the one matcher resolves an `external` wait against the
// events its condition observed, as it resolves a local wait against the controls.
#[test]
fn evaluate_matches_observed_external_events_through_the_rule() {
    let record = external_record(
        "event.pullRequest == outputs.design.pullRequest and event.state == \"merged\"",
    )
    .unwrap();
    let merged = queue_settled(PULL_REQUEST, "merged");
    assert_eq!(
        evaluate(&record.rule, RUN, &record, std::slice::from_ref(&merged)),
        Ok(Some(&merged))
    );
    let other = queue_settled(
        "https://github.com/useful-machinery/platform/pull/1",
        "merged",
    );
    let dequeued = queue_settled(PULL_REQUEST, "dequeued");
    assert_eq!(
        evaluate(&record.rule, RUN, &record, &[other, dequeued]),
        Ok(None)
    );
    let mut elsewhere = merged.clone();
    elsewhere.source = "gitlab".to_owned();
    assert_eq!(evaluate(&record.rule, RUN, &record, &[elsewhere]), Ok(None));
    let no_events: [ExternalEvent; 0] = [];
    assert_eq!(evaluate(&record.rule, RUN, &record, &no_events), Ok(None));
}

/// A directory shaped like a run root as far as controls need: `.private/` beside them.
fn run_root() -> tempfile::TempDir {
    let root = tempfile::tempdir().unwrap();
    fs::create_dir(root.path().join(".private")).unwrap();
    fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
    root
}

fn mode(path: &Path) -> u32 {
    fs::symlink_metadata(path).unwrap().permissions().mode() & 0o7777
}

fn assert_control_invalid(root: &Path, name: &str) {
    match read_controls(root) {
        Err(LocalRunDirectoryError::StateFile { path, source, .. })
            if path.ends_with(Path::new("controls").join(name))
                && matches!(*source, LocalRunDirectoryError::ControlEventInvalid) => {}
        other => panic!("{name} was not refused as a control event: {other:?}"),
    }
}

// REQ-WFDEF-6, REQ-LOCALRUN-178
#[test]
fn append_control_writes_ordinal_files_that_read_controls_returns_in_order() {
    let validator = validator("workflow-run-control-v1.schema.json");
    let root = run_root();
    assert_eq!(read_controls(root.path()).unwrap(), []);

    let wait = format!("approval@{SINCE}");
    let approve = control("control.approve", RUN, Some(&wait), None);
    let hand_back = control(
        "control.hand_back",
        RUN,
        Some(&wait),
        Some(json!({"notes": "tighten the test"})),
    );
    let first = append_control(root.path(), &approve).unwrap();
    let second = append_control(root.path(), &hand_back).unwrap();

    let controls = root.path().join("controls");
    assert_eq!(
        first,
        controls.canonicalize().unwrap().join("0001-approve.json")
    );
    assert_eq!(
        second,
        controls.canonicalize().unwrap().join("0002-hand_back.json")
    );
    assert_eq!(mode(&controls), 0o700);
    for path in [&first, &second] {
        assert_eq!(mode(path) & 0o077, 0, "{}", path.display());
        assert_valid(
            &validator,
            &serde_json::from_slice::<Value>(&fs::read(path).unwrap()).unwrap(),
        );
    }
    assert_eq!(
        fs::read_dir(root.path().join(".private")).unwrap().count(),
        0
    );
    assert_eq!(
        read_controls(root.path()).unwrap(),
        [approve.clone(), hand_back]
    );

    // `ControlEvent::new` stamps `at` with the current UTC time and refuses an event the
    // published schema does not name.
    let release = ControlEvent::new(CONTROL_RELEASE, RUN, "saeed", None, None).unwrap();
    assert!(
        parse_canonical_utc_timestamp(&release.at).is_some(),
        "{}",
        release.at
    );
    assert_eq!(
        ControlEvent::new("control.approved", RUN, "saeed", Some(wait.clone()), None),
        Err(LocalRunDirectoryError::ControlEventInvalid)
    );

    // An event the published schema does not name is refused and writes nothing.
    let mut approved = approve.clone();
    approved.event = "control.approved".to_owned();
    assert_eq!(
        append_control(root.path(), &approved),
        Err(LocalRunDirectoryError::ControlEventInvalid)
    );
    assert_eq!(fs::read_dir(&controls).unwrap().count(), 2);

    // A file under `controls/` that is not a control event makes the controls unreadable.
    fs::write(controls.join("0003-lift.json"), b"not json").unwrap();
    assert_control_invalid(root.path(), "0003-lift.json");
    fs::remove_file(controls.join("0003-lift.json")).unwrap();
    fs::write(
        controls.join("0003-lift.json"),
        serde_json::to_vec(&approve).unwrap(),
    )
    .unwrap();
    assert_control_invalid(root.path(), "0003-lift.json");
    fs::remove_file(controls.join("0003-lift.json")).unwrap();
    fs::write(controls.join("notes.txt"), b"").unwrap();
    assert_control_invalid(root.path(), "notes.txt");
}

// REQ-LOCALRUN-178, REQ-CONTINUE-74, INV-ENGINE-21
#[test]
fn evaluate_resolves_a_wait_only_through_its_rule() {
    let approval = record(WaitKind::Approval, "review", "merge", Some("fix"));
    let wait = approval.id();
    let approve = control("control.approve", RUN, Some(&wait), None);
    assert_eq!(
        evaluate(
            &approval.rule,
            RUN,
            &approval,
            std::slice::from_ref(&approve)
        ),
        Ok(Some(&approve))
    );
    // Another run, another wait identity and an event the rule does not name never match;
    // the first control that does, in order, resolves the wait.
    let hand_back = control(
        "control.hand_back",
        RUN,
        Some(&wait),
        Some(json!({"notes": "keep it"})),
    );
    let events = [
        control("control.approve", OTHER_RUN, Some(&wait), None),
        control(
            "control.approve",
            RUN,
            Some("approval@2026-11-06T13:40:11Z"),
            None,
        ),
        control("control.answer", RUN, Some(&wait), None),
        hand_back.clone(),
        approve,
    ];
    assert_eq!(
        evaluate(&approval.rule, RUN, &approval, &events),
        Ok(Some(&hand_back))
    );
    assert_eq!(
        evaluate(&approval.rule, RUN, &approval, &events[..3]),
        Ok(None)
    );

    let stop = WaitRecord::entered(
        &entered(WaitKind::Stop, "design", "design", None),
        None,
        object(json!({"by": "saeed", "at": "2026-11-07T21:00:00Z"})),
        since(),
        &ResolvedInputs::default(),
        &BTreeMap::new(),
    )
    .unwrap();
    let lift_design = control(
        "control.lift",
        RUN,
        None,
        Some(json!({"before": "design", "stop": "2026-11-07T21:00:00Z"})),
    );
    let lift_build = control(
        "control.lift",
        RUN,
        None,
        Some(json!({"before": "build", "stop": "2026-11-07T21:00:00Z"})),
    );
    assert_eq!(
        evaluate(
            &stop.rule,
            RUN,
            &stop,
            &[lift_build.clone(), lift_design.clone()]
        ),
        Ok(Some(&lift_design))
    );
    assert_eq!(evaluate(&stop.rule, RUN, &stop, &[lift_build]), Ok(None));

    // Quoted literals and every `wait.<field>` are operands.
    let mut literal = stop.rule.clone();
    literal.where_ = "event.payload.before == \"design\" and wait.kind == \"stop\" and wait.step == wait.then and wait.since == \"2026-11-07T21:41:58Z\"".to_owned();
    assert_eq!(
        evaluate(&literal, RUN, &stop, std::slice::from_ref(&lift_design)),
        Ok(Some(&lift_design))
    );

    // A `where` outside the grammar is an error, never a match or a miss.
    for where_ in [
        "event.payload.complete == maybe",
        "wait.payload == \"merge\"",
        "wait.payload.payload..before == \"merge\"",
        "event.run != run.id",
        "event.run == run.id or event.wait == wait.id",
        "event.pullRequest == steps.design.outputs.pullRequest",
        "event.run == wait.owner",
        "",
    ] {
        let mut rule = stop.rule.clone();
        rule.where_ = where_.to_owned();
        assert!(
            evaluate(&rule, RUN, &stop, std::slice::from_ref(&lift_design)).is_err(),
            "{where_:?} parsed"
        );
        assert!(
            evaluate::<ControlEvent>(&rule, RUN, &stop, &[]).is_err(),
            "{where_:?} parsed"
        );
    }
}

// REQ-WFFORMAT-97
#[test]
fn run_value_operands_parse_and_an_unresolved_one_never_matches() {
    let conjuncts = parse_where(
        "event.ticket == inputs.ticket and event.pullRequest == outputs.design.pullRequest",
    )
    .unwrap();
    assert!(
        matches!(
            conjuncts.as_slice(),
            [
                (Operand::Event(_), Operand::Inputs(input)),
                (Operand::Event(_), Operand::Outputs { step, output }),
            ] if input == "ticket" && step == "design" && output == "pullRequest"
        ),
        "{conjuncts:?}"
    );
    for operand in [
        "inputs.",
        "inputs.ticket.id",
        "outputs.design",
        "outputs.design.pullRequest.url",
        "outputs..pullRequest",
    ] {
        assert!(
            parse_where(&format!("event.ticket == {operand}")).is_err(),
            "{operand:?} parsed"
        );
    }

    // Validation admits these operands and the record substitutes them, so the matcher
    // never resolves one: a conjunct naming it never holds.
    let repair = record(WaitKind::Repair, "merge", "merge", None);
    let retry = control(
        "control.retry",
        RUN,
        Some(&repair.id()),
        Some(json!({"ticket": "AUTO-208"})),
    );
    let mut rule = repair.rule.clone();
    rule.where_ = "event.run == run.id and event.payload.ticket == \"AUTO-208\"".to_owned();
    assert_eq!(
        evaluate(&rule, RUN, &repair, std::slice::from_ref(&retry)),
        Ok(Some(&retry))
    );
    for unresolved in ["inputs.ticket", "outputs.design.ticket"] {
        rule.where_ = format!("event.run == run.id and event.payload.ticket == {unresolved}");
        assert_eq!(
            evaluate(&rule, RUN, &repair, std::slice::from_ref(&retry)),
            Ok(None),
            "{unresolved} matched"
        );
    }
}

// REQ-CONTINUE-74, REQ-WFEXEC-61, REQ-WFEXEC-62, INV-ENGINE-21
#[test]
fn an_answer_resolves_only_when_complete_and_a_lift_only_for_the_stop_in_force() {
    let answer = record(WaitKind::Answer, "design", "design", None);
    let wait = answer.id();
    let answered = |complete: bool| {
        control(
            "control.answer",
            RUN,
            Some(&wait),
            Some(json!({
                "question": "q1",
                "text": "yes",
                "answers": {"q1": "yes"},
                "complete": complete,
            })),
        )
    };
    let (open, complete) = (answered(false), answered(true));
    assert_eq!(
        evaluate(&answer.rule, RUN, &answer, std::slice::from_ref(&open)),
        Ok(None)
    );
    assert_eq!(
        evaluate(
            &answer.rule,
            RUN,
            &answer,
            &[open.clone(), complete.clone()]
        ),
        Ok(Some(&complete))
    );
    // A quoted `"true"` is a string and never equals the boolean `true`.
    let mut quoted = answer.rule.clone();
    quoted.where_ = "event.payload.complete == \"true\"".to_owned();
    assert_eq!(
        evaluate(&quoted, RUN, &answer, std::slice::from_ref(&complete)),
        Ok(None)
    );
    let mut literal = answer.rule.clone();
    literal.where_ = "event.payload.complete == true and false == false".to_owned();
    assert_eq!(
        evaluate(&literal, RUN, &answer, &[open, complete.clone()]),
        Ok(Some(&complete))
    );
    let mut bare = answer.rule.clone();
    bare.where_ = "event.payload.complete == maybe".to_owned();
    assert!(matches!(
        evaluate(&bare, RUN, &answer, std::slice::from_ref(&complete)),
        Err(error) if error.clause() == "event.payload.complete == maybe"
    ));

    let stop = WaitRecord::entered(
        &entered(WaitKind::Stop, "merge", "merge", None),
        None,
        object(json!({
            "by": "saeed",
            "at": "2026-11-06T16:01:17Z",
            "payload": {"before": "merge", "reason": "hold for the release"},
        })),
        since(),
        &ResolvedInputs::default(),
        &BTreeMap::new(),
    )
    .unwrap();
    let lift = |at: &str| {
        control(
            "control.lift",
            RUN,
            None,
            Some(json!({"before": "merge", "stop": at})),
        )
    };
    let (earlier, current) = (lift("2026-11-06T15:00:00Z"), lift("2026-11-06T16:01:17Z"));
    assert_eq!(
        evaluate(&stop.rule, RUN, &stop, std::slice::from_ref(&earlier)),
        Ok(None)
    );
    assert_eq!(
        evaluate(&stop.rule, RUN, &stop, &[earlier, current.clone()]),
        Ok(Some(&current))
    );
    // `wait.payload.<path>` folds every segment over the record's payload.
    let mut nested = stop.rule.clone();
    nested.where_ = "wait.payload.payload.before == \"merge\" and event.payload.before == wait.payload.payload.before".to_owned();
    assert_eq!(
        evaluate(&nested, RUN, &stop, std::slice::from_ref(&current)),
        Ok(Some(&current))
    );
    nested.where_ = "wait.payload.payload.reason == \"merge\"".to_owned();
    assert_eq!(
        evaluate(&nested, RUN, &stop, std::slice::from_ref(&current)),
        Ok(None)
    );
}

// REQ-WFEXEC-62, INV-ENGINE-21
#[test]
fn a_stop_is_in_force_until_a_later_lift_for_the_same_step() {
    let stop = |before: &str| {
        control(
            "control.stop",
            RUN,
            None,
            Some(json!({"before": before, "reason": "hold"})),
        )
    };
    let lift = |before: &str| control("control.lift", RUN, None, Some(json!({"before": before})));

    assert_eq!(stop_in_force(&[], "design"), None);
    let mut events = vec![stop("design")];
    assert_eq!(stop_in_force(&events, "design"), Some(&events[0]));
    events.push(lift("design"));
    assert_eq!(stop_in_force(&events, "design"), None);
    events.push(stop("design"));
    assert_eq!(stop_in_force(&events, "design"), Some(&events[2]));

    let build = [stop("build"), lift("design")];
    assert_eq!(stop_in_force(&build, "design"), None);
    assert_eq!(stop_in_force(&build, "build"), Some(&build[0]));
}
