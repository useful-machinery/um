//! The run record `run.json` (REQ-WFDEF-4, REQ-LOCALRUN-175): the shape published as
//! `contracts/jsonschema/workflow-run-v1.schema.json`. A run admitted from a tree holds one
//! beside the engine's identity record `local-run.json`; `local_run` writes it into the
//! staging root and rewrites its `state`, `attempts` and `outcome` as attempts start and settle,
//! and its `edges` as a counted edge starts an attempt (REQ-WFEXEC-64).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use super::schema_common::{
    digest_hex, is_canonical_relative_path, is_identifier, is_prefixed_ulid, utc_timestamp,
};
use super::subject_index::is_subject;

const SCHEMA_VERSION: u64 = 1;
const RUN_ID_PREFIX: &str = "run_";
const DEFINITION_ID_PREFIX: &str = "def_";

/// One run's record. Every field is present on disk; an absent value is written as `null`
/// and a missing key is refused on read.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkflowRunRecord {
    schema_version: u64,
    /// `run_` and a lowercase ULID.
    pub id: String,
    /// The definition id, or null for a run started by path.
    #[serde(deserialize_with = "Option::deserialize")]
    pub definition: Option<String>,
    /// The definition revision, null exactly when `definition` is.
    #[serde(deserialize_with = "Option::deserialize")]
    pub revision: Option<u64>,
    /// The pinned tree's digest, `sha256:<hex>`.
    pub tree: String,
    /// The tree-relative workflow file the run executes.
    pub workflow: String,
    #[serde(deserialize_with = "Option::deserialize")]
    pub subject: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub parent: Option<RunParent>,
    pub owner: String,
    pub state: RunState,
    pub position: Vec<Value>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub outcome: Option<String>,
    pub started_at: String,
    pub attempts: u64,
    #[serde(deserialize_with = "Option::deserialize")]
    pub predecessor: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub successor: Option<String>,
    pub external: BTreeMap<String, String>,
    /// The continuations each limited edge started, by `<step>.<outcome>` (REQ-WFDEF-4,
    /// REQ-WFEXEC-64); empty for a run that took none. A record written before the field
    /// existed reads as empty.
    #[serde(default)]
    pub edges: BTreeMap<String, u64>,
}

/// The parent run and step of a stage run.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RunParent {
    pub run: String,
    pub step: String,
    /// The one-based element of a fan-out (`forEach`) step the child runs for.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item: Option<u64>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RunState {
    Running,
    Waiting,
    Exited,
}

impl WorkflowRunRecord {
    /// A fresh record for a run starting now: a new `run_` id, `running`, one attempt, and
    /// no subject, parent, outcome, predecessor, successor, position or external references.
    /// `definition` is the definition id and revision, or `None` for a run started by path.
    pub fn new(definition: Option<(String, u64)>, tree: &str, workflow: &str, owner: &str) -> Self {
        let (definition, revision) = definition.unzip();
        Self {
            schema_version: SCHEMA_VERSION,
            id: format!(
                "{RUN_ID_PREFIX}{}",
                ulid::Ulid::generate().to_string().to_ascii_lowercase()
            ),
            definition,
            revision,
            tree: tree.to_owned(),
            workflow: workflow.to_owned(),
            subject: None,
            parent: None,
            owner: owner.to_owned(),
            state: RunState::Running,
            position: Vec::new(),
            outcome: None,
            // A clock outside the RFC 3339 range leaves this empty, which `is_valid` refuses.
            started_at: utc_timestamp(um_support::utc_now()).unwrap_or_default(),
            attempts: 1,
            predecessor: None,
            successor: None,
            external: BTreeMap::new(),
            edges: BTreeMap::new(),
        }
    }

    /// A fresh record for the child run a `workflow` step of `parent` starts
    /// (REQ-LOCALRUN-177): as `new`, with the parent's `definition`, `revision`, `tree`,
    /// `owner` and `subject`, `workflow` the child file, and `parent` naming the parent run,
    /// `step` and, for a fan-out child, `item`, the one-based element it runs for.
    pub fn child_of(parent: &Self, step: &str, workflow: &str, item: Option<u64>) -> Self {
        let mut child = Self::new(None, &parent.tree, workflow, &parent.owner);
        child.definition.clone_from(&parent.definition);
        child.revision = parent.revision;
        child.subject.clone_from(&parent.subject);
        child.parent = Some(RunParent {
            run: parent.id.clone(),
            step: step.to_owned(),
            item,
        });
        child
    }

    /// The facts this crate writes and relies on: the ids, the definition and revision
    /// present together, the tree digest, a tree-relative workflow, a subject of the subject
    /// form (REQ-WFDEF-28), a parent naming a run, a step and an element of at least 1, a
    /// position of step identifiers and elements of at least 1, an owner, a start time, at
    /// least one attempt and positive edge counts under `<step>.<outcome>` keys.
    pub fn is_valid(&self) -> bool {
        self.schema_version == SCHEMA_VERSION
            && is_prefixed_ulid(&self.id, RUN_ID_PREFIX)
            && self.parent.as_ref().is_none_or(|parent| {
                is_prefixed_ulid(&parent.run, RUN_ID_PREFIX)
                    && is_identifier(&parent.step)
                    && parent.item.is_none_or(|item| item >= 1)
            })
            && self.position.iter().all(is_position_element)
            && self.definition.is_some() == self.revision.is_some()
            && self
                .definition
                .as_deref()
                .is_none_or(|id| is_prefixed_ulid(id, DEFINITION_ID_PREFIX))
            && self.revision.is_none_or(|revision| revision >= 1)
            && digest_hex(&self.tree).is_some()
            && is_canonical_relative_path(&self.workflow)
            && self.subject.as_deref().is_none_or(is_subject)
            && !self.owner.is_empty()
            && OffsetDateTime::parse(&self.started_at, &Rfc3339).is_ok()
            && self.attempts >= 1
            && self.edges.iter().all(|(key, count)| {
                *count >= 1
                    && key
                        .split_once('.')
                        .is_some_and(|(step, outcome)| is_identifier(step) && !outcome.is_empty())
            })
    }
}

/// A position element (REQ-WFDEF-4): a step identifier, or `{"item": n}` naming the
/// one-based element of a fan-out.
pub(crate) fn is_position_element(element: &Value) -> bool {
    match element {
        Value::String(step) => is_identifier(step),
        Value::Object(element) => {
            element.len() == 1
                && element
                    .get("item")
                    .and_then(Value::as_u64)
                    .is_some_and(|item| item >= 1)
        }
        _ => false,
    }
}

/// The position element `{"item": n}` of fan-out element `item`.
pub(crate) fn item_position_element(item: u64) -> Value {
    serde_json::json!({ "item": item })
}

#[cfg(test)]
mod tests {
    use super::*;

    const TREE: &str = "sha256:0000000000000000000000000000000000000000000000000000000000000000";

    fn root() -> WorkflowRunRecord {
        let mut record = WorkflowRunRecord::new(
            Some(("def_01jzzzzzzzzzzzzzzzzzzzzzzz".to_owned(), 1)),
            TREE,
            "workflow.yaml",
            "operator",
        );
        record.subject = Some("AUTO-1".to_owned());
        record
    }

    // REQ-LOCALRUN-177
    #[test]
    fn a_child_carries_its_parents_subject() {
        let parent = root();
        let child = WorkflowRunRecord::child_of(&parent, "design", "design.yaml", None);
        assert_eq!(child.subject.as_deref(), Some("AUTO-1"));
        assert!(child.is_valid());
    }

    // REQ-LOCALRUN-175, REQ-WFDEF-28
    #[test]
    fn a_subject_outside_the_subject_form_is_invalid() {
        let mut record = root();
        assert!(record.is_valid());
        record.subject = Some("AUTO 1".to_owned());
        assert!(!record.is_valid());
        record.subject = None;
        assert!(record.is_valid());
    }
}
