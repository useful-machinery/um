//! Private, immutable output evidence for cloud continuation attempts. The
//! publication bundle is deliberately not used as the source of step inputs:
//! unexported outputs and attempts without a portable bundle must be usable.
use super::super::continuation;
use super::super::evidence::{InheritedDetail, InheritedPriorState};
use super::super::execution::WorkflowExecutionResult;
use super::super::runtime::InheritedDisposition;
use super::super::runtime::{ExecutionSeed, InheritedStepSeed, OutputProducer, StepState};
use super::*;
use serde::{Deserialize, Serialize};
use std::os::unix::fs::DirBuilderExt as _;

const DIRECTORY: &str = "cloud-retained-v1";
const STATE_FILE: &str = "state.json";
const MAX_STATE_BYTES: u64 = 8 * 1024 * 1024;

/// Bind an engine-owned context only after the retained claim and ready proof.
/// This is deliberately distinct from caller-supplied environment variables.
pub fn bind_cloud_continuation_context(
    admitted: AdmittedWorkflow,
    private: &Path,
    record: &super::super::publication::ContinuationRecordV1,
    prior: Option<(&Path, &str, u64)>,
) -> Result<AdmittedWorkflow, LocalRunDirectoryError> {
    let directory =
        open_directory_path(private).map_err(|_| LocalRunDirectoryError::StateInvalid)?;
    let mut inherited = BTreeMap::<String, BTreeMap<String, OutputProducer>>::new();
    if let Some((path, id, number)) = prior.filter(|_| record.inherited_step_ids().next().is_some())
    {
        let state = read_attempt(path)?;
        if state.attempt_id != id || state.attempt_number != number {
            return Err(LocalRunDirectoryError::StateInvalid);
        }
        for node in record.inherited_step_ids() {
            let step = state
                .steps
                .get(node)
                .ok_or(LocalRunDirectoryError::StateInvalid)?;
            inherited.insert(
                node.to_owned(),
                step.references
                    .iter()
                    .map(|reference| (reference.output.clone(), reference.clone()))
                    .collect(),
            );
        }
    }
    const NAME: &str = "continuation-context.json";
    let bytes = encode_json(&serde_json::json!({
        "continuation": record,
        "inheritedOutputs": inherited,
    }))?;
    write_new_immutable_file(&directory, NAME, &bytes)?;
    sync_directory(&directory)?;
    let path = private.join(NAME);
    super::super::recovery::verify_regular_path_binding(&path, &directory, NAME)
        .map_err(|()| LocalRunDirectoryError::StateInvalid)?;
    Ok(admitted.with_continuation_context(&path))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum StoredState {
    Succeeded,
    Skipped,
    InheritedSucceeded,
    InheritedSkipped,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StoredStep {
    state: StoredState,
    outputs: Vec<RetainedOutputV1>,
    // Descriptors survive even when this attempt did not consume the value.
    // The carrier remains in the original producer's private directory.
    #[serde(default)]
    references: Vec<OutputProducer>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StoredAttempt {
    attempt_id: String,
    attempt_number: u64,
    steps: BTreeMap<String, StoredStep>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct InheritedRequest {
    id: String,
    prior_state: InheritedPriorState,
    definition_changed: bool,
}

/// Called off the async executor, before terminal publication. A missing or
/// incomplete record is never evidence for a later attempt.
pub fn retain_cloud_workflow_evidence<Deadline>(
    private: &Path,
    attempt_id: &str,
    attempt_number: u64,
    result: &WorkflowExecutionResult<Deadline>,
    artifacts: &ArtifactStaging,
) -> Result<(), LocalRunDirectoryError> {
    retain_cloud_continuation_evidence(private, attempt_id, attempt_number, result, artifacts, None)
}

/// `prior` is the immediately preceding private record, not a portable set.
/// Copy its original-producer references regardless of which outputs this
/// attempt happened to consume.
pub fn retain_cloud_continuation_evidence<Deadline>(
    private: &Path,
    attempt_id: &str,
    attempt_number: u64,
    result: &WorkflowExecutionResult<Deadline>,
    artifacts: &ArtifactStaging,
    prior: Option<(&Path, &str, u64)>,
) -> Result<(), LocalRunDirectoryError> {
    let previous = prior
        .filter(|_| {
            result
                .steps
                .values()
                .any(|step| matches!(step, StepState::Inherited { .. }))
        })
        .map(|(path, id, number)| {
            let state = read_attempt(path)?;
            if state.attempt_id != id || state.attempt_number != number || number >= attempt_number
            {
                return Err(LocalRunDirectoryError::StateInvalid);
            }
            Ok(state)
        })
        .transpose()?;
    let (root, stored) = retain_steps(
        CloudAttempt {
            private,
            attempt_id,
            attempt_number,
        },
        &result.steps,
        &result.output_producers,
        artifacts,
        previous.as_ref(),
        false,
    )?;
    let bytes = serde_json::to_vec(&stored).map_err(|_| LocalRunDirectoryError::StateInvalid)?;
    if bytes.len() as u64 > MAX_STATE_BYTES {
        return Err(LocalRunDirectoryError::StateInvalid);
    }
    match statat(&root, STATE_FILE, AtFlags::SYMLINK_NOFOLLOW) {
        Err(rustix::io::Errno::NOENT) => {
            match unlinkat(&root, ".state.pending", AtFlags::empty()) {
                Ok(()) | Err(rustix::io::Errno::NOENT) => {}
                Err(_) => return Err(LocalRunDirectoryError::StateWriteUnavailable),
            }
            write_new_immutable_file(&root, ".state.pending", &bytes)?;
            renameat_with(
                &root,
                ".state.pending",
                &root,
                STATE_FILE,
                RenameFlags::NOREPLACE,
            )
            .map_err(|_| LocalRunDirectoryError::StateWriteUnavailable)?;
        }
        Ok(_) => write_or_verify_immutable_file(&root, STATE_FILE, &bytes)?,
        Err(_) => return Err(LocalRunDirectoryError::StateInvalid),
    }
    sync_directory(&root)?;
    let private_directory =
        open_directory_path(private).map_err(|_| LocalRunDirectoryError::StateWriteUnavailable)?;
    sync_directory(&private_directory)?;
    Ok(())
}

/// Commit a newly succeeded step before the engine exposes its success transition.
/// A commit marker is published only after its private carriers have been synced.
pub(crate) fn retain_cloud_completed_steps(
    private: &Path,
    attempt_id: &str,
    attempt_number: u64,
    steps: &BTreeMap<String, StepState<CapturedValue>>,
    producers: &BTreeMap<(String, String), OutputProducer>,
    artifacts: &ArtifactStaging,
    prior: Option<(&Path, &str, u64)>,
) -> Result<(), LocalRunDirectoryError> {
    let previous = prior
        .filter(|_| {
            steps
                .values()
                .any(|step| matches!(step, StepState::Inherited { .. }))
        })
        .map(|(path, id, number)| {
            let state = read_attempt(path)?;
            if state.attempt_id != id || state.attempt_number != number || number >= attempt_number
            {
                return Err(LocalRunDirectoryError::StateInvalid);
            }
            Ok(state)
        })
        .transpose()?;
    retain_steps(
        CloudAttempt {
            private,
            attempt_id,
            attempt_number,
        },
        steps,
        producers,
        artifacts,
        previous.as_ref(),
        true,
    )?;
    Ok(())
}

struct CloudAttempt<'a> {
    private: &'a Path,
    attempt_id: &'a str,
    attempt_number: u64,
}

fn retain_steps(
    attempt: CloudAttempt<'_>,
    states: &BTreeMap<String, StepState<CapturedValue>>,
    producers: &BTreeMap<(String, String), OutputProducer>,
    artifacts: &ArtifactStaging,
    previous: Option<&StoredAttempt>,
    incremental: bool,
) -> Result<(OwnedFd, StoredAttempt), LocalRunDirectoryError> {
    let CloudAttempt {
        private,
        attempt_id,
        attempt_number,
    } = attempt;
    let dir = private.join(DIRECTORY);
    match std::fs::DirBuilder::new().mode(0o700).create(&dir) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(_) => return Err(LocalRunDirectoryError::StagingUnavailable),
    }
    let root = open_directory_path(&dir).map_err(|_| LocalRunDirectoryError::StagingUnavailable)?;
    let values = create_or_open_directory(&root, VALUES_DIRECTORY)?;
    let steps = create_or_open_directory(&values, "steps")?;
    let commits = create_or_open_directory(&root, "commits")?;
    let mut stored = StoredAttempt {
        attempt_id: attempt_id.to_owned(),
        attempt_number,
        steps: BTreeMap::new(),
    };
    for (node, state) in states {
        if incremental
            && !matches!(
                state,
                StepState::Succeeded { .. }
                    | StepState::Skipped { .. }
                    | StepState::Inherited { .. }
            )
        {
            continue;
        }
        if !valid_file_component(node) {
            return Err(LocalRunDirectoryError::StateInvalid);
        }
        let marker = format!("{node}.json");
        if incremental {
            match statat(&commits, marker.as_str(), AtFlags::SYMLINK_NOFOLLOW) {
                Ok(_) => {
                    let file = File::from(
                        openat(
                            &commits,
                            marker.as_str(),
                            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK,
                            Mode::empty(),
                        )
                        .map_err(|_| LocalRunDirectoryError::StateInvalid)?,
                    );
                    let record = read_cloud_record(file)?;
                    if record.attempt_id != attempt_id
                        || record.attempt_number != attempt_number
                        || record.steps.len() != 1
                        || !record.steps.contains_key(node)
                    {
                        return Err(LocalRunDirectoryError::StateInvalid);
                    }
                    continue;
                }
                Err(rustix::io::Errno::NOENT) => {}
                Err(_) => return Err(LocalRunDirectoryError::StateInvalid),
            }
        }
        let (kind, outputs) = match state {
            StepState::Succeeded { outputs } => (StoredState::Succeeded, Some(outputs)),
            StepState::Skipped { .. } => (StoredState::Skipped, None),
            StepState::Inherited {
                disposition: InheritedDisposition::Succeeded,
                outputs,
                ..
            } => (StoredState::InheritedSucceeded, Some(outputs)),
            StepState::Inherited {
                disposition: InheritedDisposition::Skipped,
                ..
            } => (StoredState::InheritedSkipped, None),
            _ => continue,
        };
        let mut retained = Vec::new();
        let mut references = if matches!(kind, StoredState::InheritedSucceeded) {
            previous
                .as_ref()
                .and_then(|state| state.steps.get(node))
                .filter(|step| {
                    matches!(
                        step.state,
                        StoredState::Succeeded | StoredState::InheritedSucceeded
                    )
                })
                .ok_or(LocalRunDirectoryError::StateInvalid)?
                .references
                .clone()
        } else {
            Vec::new()
        };
        if let Some(outputs) = outputs {
            let node_directory = create_or_open_directory(&steps, node)?;
            for (name, value) in outputs {
                let producer = producers
                    .get(&(node.clone(), name.clone()))
                    .cloned()
                    .unwrap_or_else(|| OutputProducer {
                        attempt_id: attempt_id.to_owned(),
                        attempt_number,
                        node: node.clone(),
                        output: name.clone(),
                    });
                if !references.iter().any(|reference| reference.output == *name) {
                    references.push(producer.clone());
                }
                retained.push(retain_output_value_with_producer(
                    artifacts,
                    &node_directory,
                    AttemptNodeRoleV1::Step,
                    node,
                    name,
                    value,
                    Some(producer),
                )?);
            }
            sync_directory(&node_directory)?;
        }
        let step = StoredStep {
            state: kind,
            outputs: retained,
            references,
        };
        stored.steps.insert(node.clone(), step.clone());
        if incremental {
            sync_directory(&steps)?;
            sync_directory(&values)?;
            sync_directory(&root)?;
            let record = StoredAttempt {
                attempt_id: attempt_id.to_owned(),
                attempt_number,
                steps: BTreeMap::from([(node.clone(), step)]),
            };
            let bytes =
                serde_json::to_vec(&record).map_err(|_| LocalRunDirectoryError::StateInvalid)?;
            if bytes.len() as u64 > MAX_STATE_BYTES {
                return Err(LocalRunDirectoryError::StateInvalid);
            }
            // An abandoned pending file is never success evidence. Replace it only
            // while holding this attempt's sole engine commit port.
            let pending = format!(".{node}.pending");
            match unlinkat(&commits, pending.as_str(), AtFlags::empty()) {
                Ok(()) | Err(rustix::io::Errno::NOENT) => {}
                Err(_) => return Err(LocalRunDirectoryError::StateWriteUnavailable),
            }
            write_new_immutable_file(&commits, &pending, &bytes)?;
            renameat_with(
                &commits,
                pending.as_str(),
                &commits,
                marker.as_str(),
                RenameFlags::NOREPLACE,
            )
            .map_err(|_| LocalRunDirectoryError::StateWriteUnavailable)?;
            sync_directory(&commits)?;
            sync_directory(&root)?;
            let private_directory = open_directory_path(private)
                .map_err(|_| LocalRunDirectoryError::StateWriteUnavailable)?;
            sync_directory(&private_directory)?;
        }
    }
    sync_directory(&steps)?;
    sync_directory(&values)?;
    sync_directory(&root)?;
    Ok((root, stored))
}

fn open_retained_directory(path: &Path) -> Result<OwnedFd, LocalRunDirectoryError> {
    // Do not let a rebound ancestor redirect inherited private evidence into
    // another attempt (NOFOLLOW on the final component alone is insufficient).
    let mut components = path.components();
    if components.next() != Some(std::path::Component::RootDir) {
        return Err(LocalRunDirectoryError::StateInvalid);
    }
    let mut directory =
        open_directory_path(Path::new("/")).map_err(|_| LocalRunDirectoryError::StateInvalid)?;
    for part in components {
        let std::path::Component::Normal(name) = part else {
            return Err(LocalRunDirectoryError::StateInvalid);
        };
        directory = open_directory_at(&directory, name)
            .map_err(|_| LocalRunDirectoryError::StateInvalid)?;
    }
    Ok(directory)
}

fn read_attempt(private: &Path) -> Result<StoredAttempt, LocalRunDirectoryError> {
    let root = open_retained_directory(&private.join(DIRECTORY))?;
    let mut state: Option<StoredAttempt> = match rustix::fs::openat(
        &root,
        STATE_FILE,
        rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK,
        rustix::fs::Mode::empty(),
    ) {
        Ok(file) => Some(read_cloud_record(File::from(file))?),
        Err(rustix::io::Errno::NOENT) => None,
        Err(_) => return Err(LocalRunDirectoryError::StateInvalid),
    };
    let commits = match statat(&root, "commits", AtFlags::SYMLINK_NOFOLLOW) {
        Err(rustix::io::Errno::NOENT) => None,
        Ok(_) => Some(open_directory_at(&root, "commits")?),
        Err(_) => return Err(LocalRunDirectoryError::StateInvalid),
    };
    if let Some(commits) = commits {
        let names =
            directory_entry_names(&commits).map_err(|_| LocalRunDirectoryError::StateInvalid)?;
        for name in names {
            let name =
                std::str::from_utf8(&name).map_err(|_| LocalRunDirectoryError::StateInvalid)?;
            if name.starts_with('.') && name.ends_with(".pending") {
                continue;
            } // Interrupted, unpublished checkpoint.
            let node = name
                .strip_suffix(".json")
                .filter(|node| valid_file_component(node))
                .ok_or(LocalRunDirectoryError::StateInvalid)?;
            let file = File::from(
                rustix::fs::openat(
                    &commits,
                    name,
                    rustix::fs::OFlags::RDONLY
                        | rustix::fs::OFlags::NOFOLLOW
                        | rustix::fs::OFlags::NONBLOCK,
                    rustix::fs::Mode::empty(),
                )
                .map_err(|_| LocalRunDirectoryError::StateInvalid)?,
            );
            let record = read_cloud_record(file)?;
            if record.steps.len() != 1 || !record.steps.contains_key(node) {
                return Err(LocalRunDirectoryError::StateInvalid);
            }
            match &mut state {
                Some(state) => {
                    if state.attempt_id != record.attempt_id
                        || state.attempt_number != record.attempt_number
                    {
                        return Err(LocalRunDirectoryError::StateInvalid);
                    }
                    for (node, step) in record.steps {
                        if let Some(existing) = state.steps.get(&node) {
                            if existing.state != step.state
                                || existing.references != step.references
                                || existing.outputs != step.outputs
                            {
                                return Err(LocalRunDirectoryError::StateInvalid);
                            }
                        } else {
                            state.steps.insert(node, step);
                        }
                    }
                }
                None => state = Some(record),
            }
        }
    }
    state.ok_or(LocalRunDirectoryError::StateInvalid)
}

fn read_cloud_record(file: File) -> Result<StoredAttempt, LocalRunDirectoryError> {
    let mut bytes = Vec::new();
    file.take(MAX_STATE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| LocalRunDirectoryError::StateInvalid)?;
    if bytes.len() as u64 > MAX_STATE_BYTES {
        return Err(LocalRunDirectoryError::StateInvalid);
    }
    serde_json::from_slice(&bytes).map_err(|_| LocalRunDirectoryError::StateInvalid)
}

fn open_cloud_carrier(
    private: &Path,
    node: &str,
    retained: &RetainedOutputV1,
) -> Result<RetainedCarrierProducer, LocalRunDirectoryError> {
    let name = retained.name();
    if !valid_file_component(node) || !valid_file_component(name) {
        return Err(LocalRunDirectoryError::StateInvalid);
    }
    let relative = retained_value_relative_path(AttemptNodeRoleV1::Step, node, name);
    let carrier = match retained {
        RetainedOutputV1::Text { carrier, .. }
        | RetainedOutputV1::Json { carrier, .. }
        | RetainedOutputV1::File { carrier, .. } => Some(carrier),
        RetainedOutputV1::GitBranch { carrier, .. } => carrier.as_ref(),
    }
    .ok_or(LocalRunDirectoryError::StateInvalid)?;
    if carrier.relative_path != relative {
        return Err(LocalRunDirectoryError::StateInvalid);
    }
    let root = open_retained_directory(&private.join(DIRECTORY))?;
    let values = open_directory_at(&root, VALUES_DIRECTORY)?;
    let steps = open_directory_at(&values, "steps")?;
    let parent = open_directory_at(&steps, node)?;
    let source = rustix::fs::openat(
        &parent,
        name,
        rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK,
        rustix::fs::Mode::empty(),
    )
    .map_err(|_| LocalRunDirectoryError::StateInvalid)?;
    Ok(RetainedCarrierProducer {
        source: File::from(source),
        parent,
        name: name.into(),
    })
}

fn valid_file_component(value: &str) -> bool {
    !value.is_empty()
        && value != "."
        && value != ".."
        && !value.contains('/')
        && !value.contains('\\')
        && !value.contains('\0')
}

/// Only referenced outputs are read. The runner calls this after acceptance,
/// so a lost carrier is an ordinary consumer failure, never an admission veto.
pub fn load_cloud_continuation_seed(
    admitted: &AdmittedWorkflow,
    artifacts: &ArtifactStaging,
    private_by_attempt: &BTreeMap<String, PathBuf>,
    prior_attempt_id: &str,
    prior_attempt_number: u64,
    inherited: &[serde_json::Value],
    reexecuted: &[String],
) -> Result<ExecutionSeed<CapturedValue>, LocalRunDirectoryError> {
    if inherited.is_empty() {
        // A pre-engine predecessor has no result or output ledger. Its
        // continuation can still reexecute every node in the requested slice.
        return ExecutionSeed::new(admitted, BTreeMap::new())
            .map_err(|_| LocalRunDirectoryError::StateInvalid);
    }
    let prior_private = private_by_attempt
        .get(prior_attempt_id)
        .ok_or(LocalRunDirectoryError::StateInvalid)?;
    let prior = read_attempt(prior_private)?;
    if prior.attempt_id != prior_attempt_id || prior.attempt_number != prior_attempt_number {
        return Err(LocalRunDirectoryError::StateInvalid);
    }
    let referenced = continuation::referenced_outputs(&admitted.workflow().definition, reexecuted);
    let required: BTreeSet<_> = referenced
        .into_iter()
        .map(|source| (source.node.id, source.output))
        .collect();
    let mut seeds = BTreeMap::new();
    for raw in inherited {
        let request: InheritedRequest = serde_json::from_value(raw.clone())
            .map_err(|_| LocalRunDirectoryError::StateInvalid)?;
        let step = prior
            .steps
            .get(&request.id)
            .ok_or(LocalRunDirectoryError::StateInvalid)?;
        let disposition = match (request.prior_state, step.state) {
            (InheritedPriorState::Succeeded, StoredState::Succeeded)
            | (InheritedPriorState::Inherited, StoredState::InheritedSucceeded) => {
                InheritedDisposition::Succeeded
            }
            (InheritedPriorState::Skipped, StoredState::Skipped)
            | (InheritedPriorState::Inherited, StoredState::InheritedSkipped) => {
                InheritedDisposition::Skipped
            }
            _ => return Err(LocalRunDirectoryError::StateInvalid),
        };
        let mut outputs = BTreeMap::new();
        let mut producers = BTreeMap::new();
        for producer in &step.references {
            let name = producer.output.clone();
            if !required.contains(&(request.id.clone(), name.clone())) {
                continue;
            }
            if producer.node != request.id
                || producer.output != name
                || producer.attempt_number > prior_attempt_number
            {
                continue;
            }
            let Some(producer_private) = private_by_attempt.get(&producer.attempt_id) else {
                continue;
            };
            let Ok(origin) = read_attempt(producer_private) else {
                continue;
            };
            if origin.attempt_id != producer.attempt_id
                || origin.attempt_number != producer.attempt_number
            {
                continue;
            }
            let Some(source) = origin
                .steps
                .get(&request.id)
                .filter(|step| step.state == StoredState::Succeeded)
                .and_then(|step| {
                    step.outputs
                        .iter()
                        .find(|candidate| candidate.name() == name)
                })
                .filter(|candidate| candidate.producer() == Some(producer))
            else {
                // An unavailable needed value fails at the consumer.
                continue;
            };
            let Ok(value) = load_retained_value_from(
                admitted.workflow(),
                artifacts,
                &request.id,
                source,
                |retained| open_cloud_carrier(producer_private, &request.id, retained),
            ) else {
                // The engine represents absent output as an unavailable
                // consumer input. The previously committed node stays inherited.
                continue;
            };
            outputs.insert(name.clone(), value);
            producers.insert(name, producer.clone());
        }
        seeds.insert(
            request.id.clone(),
            InheritedStepSeed {
                detail: InheritedDetail {
                    prior_attempt_id: prior_attempt_id.to_owned(),
                    prior_attempt_number,
                    prior_state: request.prior_state,
                    definition_changed: request.definition_changed,
                },
                disposition,
                outputs,
                producers,
            },
        );
    }
    ExecutionSeed::new(admitted, seeds).map_err(|_| LocalRunDirectoryError::StateInvalid)
}

#[cfg(test)]
mod tests {
    use super::*;

    // INV-ENGINE-13
    #[test]
    fn inherited_private_reader_rejects_rebound_ancestor() {
        let root = tempfile::tempdir().expect("isolated root");
        let real = root
            .path()
            .canonicalize()
            .expect("canonical root")
            .join("original");
        std::fs::create_dir(&real).expect("original");
        let alias = root
            .path()
            .canonicalize()
            .expect("canonical root")
            .join("replacement");
        std::os::unix::fs::symlink(&real, &alias).expect("substituted ancestor");
        assert!(open_retained_directory(&real).is_ok());
        assert!(open_retained_directory(&alias).is_err());
    }
}
