use std::collections::{HashMap, HashSet};
use std::io::{self, Write};
use std::path::PathBuf;

use anyhow::{Context, anyhow};
use clap::Args;
use serde::Serialize;
use serde_json::json;

use super::super::super::{
    self as cli, CommonArgs, OrganizationArg, PrincipalAuthenticationArgs, ProjectArg, WorkflowJson,
};
use super::cloud::{resend_once, with_api};
use super::report::{display, write_invalid, write_json};
use crate::exit_code::ExitCode;
use useful_api::{
    WorkflowBindingValues, WorkflowDefinitionApi, WorkflowDefinitionFailure,
    WorkflowDefinitionRevision, WorkflowProblemDetail, WorkflowPublication, WorkflowTreeFile,
};
use useful_execution::{
    BindingOption, BindingValue, BindingsFile, Named, NamedBindings, PublishTarget, Published,
    ValidatedTree, WorkflowHome, check_definition_id, publish, record_publication, resolve_publish,
    validate_workflow_tree,
};
use useful_human_auth::Deployment;

pub(super) const ABOUT: &str = "Publish a local workflow tree as a definition revision";
const COMMAND_NAME: &str = "useful workflow definition publish";
const REFUSED: &str = "Publish refused.";
/// The problem slug of a reuse that matched several identical Cloud definitions.
const DEFINITION_REUSE_AMBIGUOUS: &str = "definition-reuse-ambiguous";
/// The problem slug of a Cloud definition the project does not hold, a deleted one included.
const NOT_FOUND: &str = "not-found";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[arg(
        value_name = "DIR",
        help = "Tree directory holding workflow.yaml, resolved from the initial working directory"
    )]
    directory: PathBuf,

    #[arg(
        long,
        value_name = "FILE",
        help = "JSON object naming the binding values to set, null for a binding's default, an integration's nested under its id, resolved from the initial working directory"
    )]
    bindings: Option<PathBuf>,

    #[arg(
        long,
        value_name = "KEY=VALUE",
        value_parser = parse_binding,
        help = "Set binding KEY, `<integration>.<name>` for an integration's, to VALUE; a members value is comma-separated ids (repeatable)"
    )]
    binding: Vec<(String, String)>,

    #[arg(
        long,
        value_name = "KEY",
        help = "Set binding KEY to its declared default (repeatable)"
    )]
    binding_default: Vec<String>,

    #[arg(
        long,
        value_name = "ID",
        conflicts_with = "new",
        help = "Publish to this local definition id, as `useful workflow definition list` prints it, whatever its name"
    )]
    to: Option<String>,

    #[arg(
        long,
        help = "Create a new local definition even when the name is taken"
    )]
    new: bool,

    #[arg(
        long,
        value_name = OrganizationArg::VALUE_NAME,
        requires = "project",
        help = "Publish to Cloud in this organization (ID or exact slug) and record the revision Cloud answered locally"
    )]
    cloud: Option<OrganizationArg>,

    #[arg(
        long,
        value_name = ProjectArg::VALUE_NAME,
        requires = "cloud",
        help = "Cloud project ID of the definition --cloud publishes"
    )]
    project: Option<ProjectArg>,

    #[arg(
        long,
        value_name = "CLOUD_ID",
        requires = "cloud",
        conflicts_with = "cloud_new",
        help = "Publish to this Cloud definition id, as `useful workflow definition cloud list` prints it, and record it for the local definition"
    )]
    cloud_to: Option<String>,

    #[arg(
        long,
        requires = "cloud",
        help = "Create another Cloud definition even when the local definition records one or the project holds an identical one"
    )]
    cloud_new: bool,

    #[command(flatten)]
    options: CommonArgs<WorkflowJson, PrincipalAuthenticationArgs>,
}

/// REQ-WFDEF-41: `KEY=VALUE`, split at the first `=`.
fn parse_binding(value: &str) -> Result<(String, String), String> {
    let (key, value) = value
        .split_once('=')
        .ok_or_else(|| "expected KEY=VALUE".to_owned())?;
    Ok((key.to_owned(), value.to_owned()))
}

/// REQ-WFDEF-41: the bindings `--binding` and then `--binding-default` name, in the order
/// given, each `KEY` named at most once across both.
fn binding_options(
    values: Vec<(String, String)>,
    defaults: Vec<String>,
) -> anyhow::Result<Vec<BindingOption>> {
    let options = values
        .into_iter()
        .map(|(key, value)| BindingOption {
            key,
            value: Some(value),
        })
        .chain(
            defaults
                .into_iter()
                .map(|key| BindingOption { key, value: None }),
        )
        .collect::<Vec<_>>();
    let mut keys = HashSet::new();
    if let Some(repeated) = options
        .iter()
        .find(|option| !keys.insert(option.key.as_str()))
    {
        return Err(anyhow!(
            "--binding and --binding-default name `{}` more than once",
            repeated.key
        ));
    }
    Ok(options)
}

/// Where `--cloud` publishes (REQ-CLOUDDEF-14).
struct CloudTarget {
    organization: String,
    project: String,
    definition: CloudDefinition,
}

/// How the Cloud definition `--cloud` publishes to is chosen.
enum CloudDefinition {
    /// The local record's `cloudId` or, when the record holds none, the project's identical
    /// definition if Cloud holds one and otherwise a new one (REQ-CLOUDDEF-22).
    Recorded,
    /// `--cloud-to CLOUD_ID`, whatever the record holds (REQ-CLOUDDEF-20).
    Named(String),
    /// `--cloud-new`: a new one, whatever the record or the project holds.
    New,
}

impl CloudTarget {
    /// The existing Cloud definition the publication is addressed to, given the record's
    /// `cloudId`; none when it sends a create, which creates a definition or reuses one.
    fn existing<'a>(&'a self, recorded: Option<&'a str>) -> Option<&'a str> {
        match &self.definition {
            CloudDefinition::Recorded => recorded,
            CloudDefinition::Named(id) => Some(id),
            CloudDefinition::New => None,
        }
    }

    /// Whether the create sent when no existing definition is addressed asks Cloud for the
    /// project's identical definition: only for a record with no `cloudId` and neither
    /// `--cloud-to` nor `--cloud-new`.
    const fn reuses_identical(&self, recorded: Option<&str>) -> bool {
        matches!(self.definition, CloudDefinition::Recorded) && recorded.is_none()
    }

    /// `--cloud-to`'s id, when it was given.
    fn named(&self) -> Option<&str> {
        match &self.definition {
            CloudDefinition::Named(id) => Some(id),
            CloudDefinition::Recorded | CloudDefinition::New => None,
        }
    }
}

/// The Cloud definition a publication reached and its current revision: the one it recorded,
/// or for a reused definition the one it already had, with that revision's snapshot; and
/// whether Cloud already held the tree sealed.
struct CloudPublished {
    definition: String,
    created: bool,
    reused: bool,
    revision: i32,
    tree_digest: String,
    tree_reused: bool,
    /// The revision's flat bindings and each integration's under its id, each as `{kind,
    /// value}`, which the local revision records (REQ-CLOUDDEF-14).
    bindings: BindingsFile,
}

impl Command {
    pub(super) fn execute(self) -> cli::CommandResult {
        let json = self.options.json;
        let options = binding_options(self.binding, self.binding_default)
            .map_err(|error| cli::CommandFailure::with_exit_code(error, ExitCode::UsageError))?;
        let tree = match validate_workflow_tree(&self.directory) {
            Ok(tree) => tree,
            Err(diagnostics) => {
                write_invalid(COMMAND_NAME, &self.directory, &diagnostics, json)
                    .context("write workflow definition publish result")?;
                return Ok(ExitCode::GeneralFailure);
            }
        };
        let target = match (self.to, self.new) {
            (Some(id), _) => PublishTarget::To(id),
            (None, true) => PublishTarget::New,
            (None, false) => PublishTarget::Resolve,
        };
        let cloud = match (self.cloud, self.project) {
            (Some(organization), Some(project)) => Some(CloudTarget {
                organization: organization.into_string(),
                project: project.to_string(),
                definition: match (self.cloud_to, self.cloud_new) {
                    (Some(id), _) => CloudDefinition::Named(id),
                    (None, true) => CloudDefinition::New,
                    (None, false) => CloudDefinition::Recorded,
                },
            }),
            _ => None,
        };
        // A named Cloud definition id is checked before anything is written or sent.
        if let Some(Err(error)) = cloud
            .as_ref()
            .and_then(CloudTarget::named)
            .map(check_definition_id)
        {
            return super::finish::<()>(COMMAND_NAME, REFUSED, Err(error), json, |_, _| Ok(()));
        }
        // The deployment is configuration, read before the home is touched.
        let deployment = cloud
            .as_ref()
            .map(|_| {
                Deployment::load()
                    .map_err(|error| anyhow!(error))
                    .context("configure Useful Machinery workflow definition publication")
            })
            .transpose()?;
        let judged = WorkflowHome::from_environment().and_then(|home| {
            let file = self
                .bindings
                .as_deref()
                .map(BindingsFile::read)
                .transpose()?;
            NamedBindings::judge(&tree, file.as_ref(), &options).map(|named| (home, named))
        });
        let (Some(cloud), Some(deployment)) = (cloud, deployment) else {
            let published = judged.and_then(|(home, named)| {
                publish(&home, &tree, target, &named, &super::super::login_name())
            });
            return super::finish(COMMAND_NAME, REFUSED, published, json, |published, json| {
                if json {
                    write_json(
                        &published_report(Some(published), None),
                        "workflow definition publish",
                    )
                } else {
                    write_human_published(published)
                }
            });
        };
        // The local record is resolved before anything is sent, refusing as without
        // `--cloud`, and written only once Cloud has answered (REQ-CLOUDDEF-14).
        let resolved = judged.and_then(|(home, named)| {
            resolve_publish(&home, &tree, target).map(|place| (home, named, place))
        });
        let (home, named, place) = match resolved {
            Ok(resolved) => resolved,
            Err(error) => {
                return super::finish::<()>(COMMAND_NAME, REFUSED, Err(error), json, |_, _| Ok(()));
            }
        };
        let recorded = place.record().and_then(|record| record.cloud_id.clone());
        let addressed = cloud.existing(recorded.as_deref());
        let reached = match publish_to_cloud(
            &deployment,
            &self.options,
            &cloud,
            &tree,
            &named,
            recorded.as_deref(),
        )? {
            Ok(reached) => reached,
            Err(failure) => {
                write_cloud_failed(&cloud, addressed, &failure, json)
                    .context("write workflow definition publish result")?;
                return Ok(cloud_failure(&failure).1);
            }
        };
        let published = match record_publication(
            &home,
            &tree,
            place,
            &reached.bindings,
            &reached.definition,
            &super::super::login_name(),
        ) {
            Ok(published) => published,
            Err(error) => {
                writeln!(
                    io::stderr().lock(),
                    "error: Cloud definition {} was reached at revision {} but no local revision was recorded",
                    display(&reached.definition),
                    reached.revision
                )
                .context("write workflow definition publish result")?;
                return super::finish::<()>(COMMAND_NAME, REFUSED, Err(error), json, |_, _| Ok(()));
            }
        };
        write_cloud_published(&published, &cloud, addressed, &reached, json)
            .context("write workflow definition publish result")?;
        Ok(ExitCode::Success)
    }
}

/// Uploads and seals the tree, then publishes to the Cloud definition `--cloud-to` names
/// or, without it, the one the local record names (`recorded`) unless `--cloud-new`, over
/// the current revision it reads first (REQ-CLOUDDEF-14, REQ-CLOUDDEF-20). Otherwise it
/// creates one: `--cloud-new` always, and a record with no `cloudId` asking Cloud for the
/// project's identical definition first (REQ-CLOUDDEF-22). A refused read or publication of
/// an existing definition never falls through to a create. It sends the bindings `named`
/// names and nothing else, `null` for one named for its default, so Cloud keeps every other
/// binding of the current revision or, for a create, gives it its default
/// (REQ-CLOUDDEF-41): the flat ones in `bindings`, a `members` value as an array, and each
/// integration's under its id in `integrationBindings`, each member absent when it would be
/// empty. It sends the tree's executable files too; one fresh key covers the seal and the
/// publication for the whole invocation.
fn publish_to_cloud(
    deployment: &Deployment,
    options: &CommonArgs<WorkflowJson, PrincipalAuthenticationArgs>,
    target: &CloudTarget,
    tree: &ValidatedTree,
    named: &NamedBindings,
    recorded: Option<&str>,
) -> anyhow::Result<Result<CloudPublished, WorkflowDefinitionFailure>> {
    let key = useful_support::generate_idempotency_key()
        .context("generate Cloud workflow definition request identity")?;
    let files = tree
        .files
        .iter()
        .map(|(path, file)| WorkflowTreeFile {
            path,
            bytes: &file.bytes,
            executable: file.executable,
        })
        .collect::<Vec<_>>();
    let bindings = named
        .bindings()
        .iter()
        .map(|(binding, named)| {
            let value = match named {
                Named::Value(value) => serde_json::to_value(value)?,
                Named::Default => serde_json::Value::Null,
            };
            Ok((binding.clone(), value))
        })
        .collect::<Result<WorkflowBindingValues, serde_json::Error>>()
        .context("encode the named bindings")?;
    let integration_bindings = named
        .integration_bindings()
        .iter()
        .map(|(provider, group)| {
            let values = group
                .iter()
                .map(|(name, named)| {
                    let value = match named {
                        Named::Value(BindingValue::Text(text)) => Some(text.clone()),
                        Named::Value(BindingValue::List(_)) => {
                            return Err(anyhow!(
                                "integration binding `{}.{name}` holds a list",
                                provider.as_str()
                            ));
                        }
                        Named::Default => None,
                    };
                    Ok((name.clone(), value))
                })
                .collect::<anyhow::Result<HashMap<_, _>>>()?;
            Ok((provider.as_str().to_owned(), values))
        })
        .collect::<anyhow::Result<HashMap<_, _>>>()
        .context("encode the named integration bindings")?;
    // Every executable file, `[]` when none is, so clearing the last bit is published too.
    let executable_paths = tree
        .files
        .iter()
        .filter(|(_, file)| file.executable)
        .map(|(path, _)| path.clone())
        .collect::<Vec<_>>();
    let digest = format!("{}:{}", tree.digest.algorithm.as_str(), tree.digest.value);
    let steps = CloudSteps {
        target,
        digest: &digest,
        files: &files,
        bindings: (!bindings.is_empty()).then_some(&bindings),
        integration_bindings: (!integration_bindings.is_empty()).then_some(&integration_bindings),
        executable_paths: &executable_paths,
        existing: target.existing(recorded),
        reuse_identical: target.reuses_identical(recorded),
        key: &key,
    };
    with_api(deployment, options, |api| steps.run(api))
}

/// One invocation's Cloud steps, rerun whole with the same key when a refreshed session
/// retries them.
struct CloudSteps<'a> {
    target: &'a CloudTarget,
    /// The tree digest, `sha256:<hex>`.
    digest: &'a str,
    files: &'a [WorkflowTreeFile<'a>],
    /// The flat bindings named, `Value::Null` for a default; `None` when none is named.
    bindings: Option<&'a WorkflowBindingValues>,
    /// Each integration's bindings named, by name under its id, `None` for a default; `None`
    /// when none is named.
    integration_bindings: Option<&'a HashMap<String, HashMap<String, Option<String>>>>,
    /// The tree's executable files, in bytewise order.
    executable_paths: &'a [String],
    /// The Cloud definition `--cloud-to` or the local record names, unless `--cloud-new`.
    existing: Option<&'a str>,
    /// Whether the create sent without `existing` asks for the project's identical
    /// definition: false for `--cloud-new`.
    reuse_identical: bool,
    key: &'a str,
}

impl CloudSteps<'_> {
    fn run(
        &self,
        api: &WorkflowDefinitionApi<'_>,
    ) -> Result<CloudPublished, WorkflowDefinitionFailure> {
        let (organization, project) = (
            self.target.organization.as_str(),
            self.target.project.as_str(),
        );
        let (_, tree_reused) = api.upload_tree(organization, self.digest, self.files, self.key)?;
        let publication = WorkflowPublication {
            tree_digest: self.digest,
            bindings: self.bindings,
            integration_bindings: self.integration_bindings,
            executable_paths: Some(self.executable_paths),
        };
        // The create or publication is sent again once, under the same key, when Cloud
        // does not answer it or answers with a transient problem (REQ-CLOUDDEF-25).
        let (publication, reused) = match self.existing {
            Some(id) => {
                let current = api.definition(organization, project, id)?.current_revision;
                let published = resend_once(|| {
                    api.publish_revision(organization, project, id, self.key, current, publication)
                })?;
                (published, false)
            }
            None => resend_once(|| {
                api.create_definition(
                    organization,
                    project,
                    self.key,
                    publication,
                    self.reuse_identical,
                )
            })?,
        };
        Ok(CloudPublished {
            bindings: answered_bindings(&publication.revision),
            definition: publication.definition.id,
            created: self.existing.is_none() && !reused,
            reused,
            revision: publication.revision.number,
            tree_digest: publication.revision.tree_digest,
            tree_reused,
        })
    }
}

/// The snapshot of the revision Cloud answered, as the file the local revision is judged and
/// recorded from: each flat binding and each integration's under its id, as `{kind, value}`, a
/// flat binding without a value as `null`, which the judge refuses (REQ-CLOUDDEF-14).
fn answered_bindings(revision: &WorkflowDefinitionRevision) -> BindingsFile {
    let mut members = revision
        .bindings
        .iter()
        .map(|(key, binding)| {
            (
                key.clone(),
                json!({"kind": binding.kind.to_string(), "value": binding.value}),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    for (integration, group) in revision.integration_bindings.iter().flatten() {
        let group = group
            .iter()
            .map(|(name, binding)| {
                (
                    name.clone(),
                    json!({"kind": binding.kind.to_string(), "value": binding.value}),
                )
            })
            .collect();
        members.insert(integration.clone(), serde_json::Value::Object(group));
    }
    BindingsFile::from_object(members)
}

/// The JSON `failure` of a Cloud failure and its exit status: 1 for a refusal.
pub(super) const fn cloud_failure(failure: &WorkflowDefinitionFailure) -> (&'static str, ExitCode) {
    match failure {
        WorkflowDefinitionFailure::Refused(_) => ("refused", ExitCode::GeneralFailure),
        WorkflowDefinitionFailure::UploadRejected { .. } => {
            ("upload_rejected", ExitCode::GeneralFailure)
        }
        WorkflowDefinitionFailure::Unauthenticated => {
            ("authentication_required", ExitCode::AuthenticationRequired)
        }
        WorkflowDefinitionFailure::Unreachable { .. }
        | WorkflowDefinitionFailure::RateLimited { .. } => ("unavailable", ExitCode::Unavailable),
        WorkflowDefinitionFailure::InvalidResponse { .. } => {
            ("invalid_response", ExitCode::GeneralFailure)
        }
    }
}

/// Where an invalid response was detected (REQ-CLOUDDEF-28): the JSON `diagnostic` of the
/// failure, or none for every other failure. It holds the slugs and the status alone, never
/// the error the failure retains.
pub(super) fn invalid_response_diagnostic(
    failure: &WorkflowDefinitionFailure,
) -> Option<InvalidResponseDiagnostic> {
    match failure {
        WorkflowDefinitionFailure::InvalidResponse(invalid) => Some(InvalidResponseDiagnostic {
            stage: invalid.stage().slug(),
            check: invalid.check().slug(),
            http_status: invalid.status(),
        }),
        _ => None,
    }
}

/// The stage and check an invalid response failed, with the status a check of the status
/// judged.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct InvalidResponseDiagnostic {
    stage: &'static str,
    check: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    http_status: Option<u16>,
}

impl InvalidResponseDiagnostic {
    /// Writes the stage, the check and the status as labelled lines under an error.
    pub(super) fn write(&self, out: &mut impl Write) -> io::Result<()> {
        writeln!(out, "stage: {}", self.stage)?;
        writeln!(out, "check: {}", self.check)?;
        if let Some(status) = self.http_status {
            writeln!(out, "http status: {status}")?;
        }
        Ok(())
    }
}

/// The `cloud` object of `--json`: what the publication reached, or its failure.
fn json_cloud<'a>(
    target: &'a CloudTarget,
    addressed: Option<&'a str>,
    reached: Result<&'a CloudPublished, &'a WorkflowDefinitionFailure>,
) -> JsonCloud<'a> {
    let refusal = match reached {
        Err(WorkflowDefinitionFailure::Refused(refusal)) => Some(refusal),
        _ => None,
    };
    let reached_ok = reached.ok();
    JsonCloud {
        organization_ref: &target.organization,
        project_id: &target.project,
        target: addressed,
        definition: reached_ok.map(|reached| reached.definition.as_str()),
        created: reached_ok.map(|reached| reached.created),
        reused: reached_ok.map(|reached| reached.reused),
        revision: reached_ok.map(|reached| reached.revision),
        tree_digest: reached_ok.map(|reached| reached.tree_digest.as_str()),
        tree_reused: reached_ok.map(|reached| reached.tree_reused),
        failure: reached.err().map(|failure| cloud_failure(failure).0),
        status: refusal.map(|refusal| refusal.status),
        problem: refusal.map(|refusal| refusal.problem.as_str()),
        workflow: refusal.and_then(|refusal| refusal.workflow.as_ref()),
        diagnostic: reached.err().and_then(invalid_response_diagnostic),
    }
}

/// Reports the local revision recorded and the Cloud definition and revision it records.
fn write_cloud_published(
    published: &Published,
    target: &CloudTarget,
    addressed: Option<&str>,
    reached: &CloudPublished,
    json: bool,
) -> anyhow::Result<()> {
    if json {
        return write_json(
            &published_report(
                Some(published),
                Some(json_cloud(target, addressed, Ok(reached))),
            ),
            "workflow definition publish",
        );
    }
    write_human_published(published)?;
    let mut stdout = io::stdout().lock();
    writeln!(stdout, "cloud definition: {}", display(&reached.definition))?;
    writeln!(stdout, "cloud created: {}", super::yes_no(reached.created))?;
    writeln!(
        stdout,
        "cloud definition reused: {}",
        super::yes_no(reached.reused)
    )?;
    writeln!(stdout, "cloud revision: {}", reached.revision)?;
    writeln!(stdout, "cloud tree: {}", display(&reached.tree_digest))?;
    writeln!(
        stdout,
        "cloud tree reused: {}",
        super::yes_no(reached.tree_reused)
    )?;
    Ok(())
}

/// Reports a Cloud refusal or failure, after which nothing was recorded locally
/// (REQ-CLOUDDEF-14).
fn write_cloud_failed(
    target: &CloudTarget,
    addressed: Option<&str>,
    failure: &WorkflowDefinitionFailure,
    json: bool,
) -> anyhow::Result<()> {
    if json {
        return write_json(
            &published_report(None, Some(json_cloud(target, addressed, Err(failure)))),
            "workflow definition publish",
        );
    }
    let refusal = match failure {
        WorkflowDefinitionFailure::Refused(refusal) => Some(refusal),
        _ => None,
    };
    let mut stderr = io::stderr().lock();
    let publication = addressed.map_or_else(
        || "Cloud publication".to_owned(),
        |id| format!("Cloud publication to {}", display(id)),
    );
    match refusal {
        Some(refusal) => {
            writeln!(
                stderr,
                "error: {publication} refused: {} (HTTP {})",
                refusal.problem, refusal.status
            )?;
            if let Some(current) = refusal
                .workflow
                .as_ref()
                .and_then(|detail| detail.current_revision)
            {
                writeln!(stderr, "cloud current revision: {current}")?;
            }
            if let Some(integration) = refusal
                .workflow
                .as_ref()
                .and_then(|detail| detail.integration.as_deref())
            {
                writeln!(stderr, "integration: {}", display(integration))?;
            }
            if let Some(key) = refusal
                .workflow
                .as_ref()
                .and_then(|detail| detail.binding_key.as_deref())
            {
                writeln!(stderr, "binding: {}", display(key))?;
            }
            // The loop the publication would close, in start order (REQ-CLOUDDEF-35).
            if let Some(ids) = refusal
                .workflow
                .as_ref()
                .and_then(|detail| detail.r#loop.as_deref())
            {
                let ids = ids.iter().map(|id| display(id)).collect::<Vec<_>>();
                writeln!(stderr, "loop: {}", ids.join(" -> "))?;
            }
            if let Some(ids) = refusal
                .workflow
                .as_ref()
                .and_then(|detail| detail.definition_ids.as_deref())
            {
                let ids = ids.iter().map(|id| display(id)).collect::<Vec<_>>();
                writeln!(stderr, "cloud definitions: {}", ids.join(", "))?;
            }
        }
        None => {
            writeln!(
                stderr,
                "error: {publication} failed: {}",
                cloud_failure(failure).0
            )?;
            if let Some(diagnostic) = invalid_response_diagnostic(failure) {
                diagnostic.write(&mut stderr)?;
            }
        }
    }
    let ambiguous = refusal.is_some_and(|refusal| refusal.problem == DEFINITION_REUSE_AMBIGUOUS);
    // The addressed definition, named or recorded, is not found in the project: the owner
    // creates another or names one, as the next attempt at the same target would be refused
    // again (REQ-CLOUDDEF-14).
    let addressed_not_found =
        addressed.is_some() && refusal.is_some_and(|refusal| refusal.problem == NOT_FOUND);
    match target.named() {
        _ if addressed_not_found => writeln!(
            stderr,
            "\nNothing was recorded locally. Cloud does not hold that definition in the project. Publish again to create another Cloud definition, or to one that you choose:\n  useful workflow definition publish <DIR> --cloud {} --project {} --cloud-new\n  useful workflow definition publish <DIR> --cloud {} --project {} --cloud-to <CLOUD_ID>",
            display(&target.organization),
            display(&target.project),
            display(&target.organization),
            display(&target.project)
        )?,
        // The record's Cloud definition was not changed, so the next attempt names the
        // target again.
        Some(id) => writeln!(
            stderr,
            "\nNothing was recorded locally. Resolve the error and publish again with the target:\n  useful workflow definition publish <DIR> --cloud {} --project {} --cloud-to {}",
            display(&target.organization),
            display(&target.project),
            display(id)
        )?,
        // Several identical definitions answered the reuse: the owner chooses one.
        None if ambiguous => writeln!(
            stderr,
            "\nNothing was recorded locally. Several Cloud definitions of the project are identical to the publication. Publish again to the one listed that you choose, or create another:\n  useful workflow definition publish <DIR> --cloud {} --project {} --cloud-to <CLOUD_ID>\n  useful workflow definition publish <DIR> --cloud {} --project {} --cloud-new",
            display(&target.organization),
            display(&target.project),
            display(&target.organization),
            display(&target.project)
        )?,
        None => writeln!(
            stderr,
            "\nNothing was recorded locally; resolve the error and publish again."
        )?,
    }
    Ok(())
}

fn write_human_published(published: &Published) -> anyhow::Result<()> {
    let record = &published.record;
    let revision = &published.revision;
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    writeln!(
        stdout,
        "✓ Published {} revision {}.",
        display(&record.name),
        revision.n
    )?;
    writeln!(stdout, "definition: {}", display(&record.id))?;
    writeln!(stdout, "name: {}", display(&record.name))?;
    writeln!(stdout, "created: {}", super::yes_no(published.created))?;
    writeln!(stdout, "revision: {}", revision.n)?;
    writeln!(stdout, "tree: {}", display(&revision.tree))?;
    writeln!(stdout, "bindings: {}", display(&revision.bindings))?;
    writeln!(stdout, "published at: {}", display(&revision.published_at))?;
    writeln!(stdout, "by: {}", display(&revision.by))?;
    Ok(())
}

/// The `--json` report: the local revision `published` recorded, none when a Cloud failure
/// recorded nothing, and the Cloud half of `--cloud`.
fn published_report<'a>(
    published: Option<&'a Published>,
    cloud: Option<JsonCloud<'a>>,
) -> JsonPublished<'a> {
    JsonPublished {
        schema_version: 1,
        command: COMMAND_NAME,
        outcome: if cloud.as_ref().is_some_and(|cloud| cloud.failure.is_some()) {
            "cloud_failed"
        } else {
            "published"
        },
        definition: published.map(|published| JsonDefinition {
            id: &published.record.id,
            name: &published.record.name,
            enabled: published.record.enabled,
            current: published.record.current,
            created: published.created,
            path: published.path.to_string_lossy().into_owned(),
        }),
        revision: published.map(|published| JsonRevision {
            n: published.revision.n,
            tree: &published.revision.tree,
            bindings: &published.revision.bindings,
            published_at: &published.revision.published_at,
            by: &published.revision.by,
        }),
        cloud,
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JsonPublished<'a> {
    schema_version: u8,
    command: &'static str,
    outcome: &'static str,
    /// The local definition and revision recorded; absent for `cloud_failed`, which records
    /// none.
    #[serde(skip_serializing_if = "Option::is_none")]
    definition: Option<JsonDefinition<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    revision: Option<JsonRevision<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cloud: Option<JsonCloud<'a>>,
}

/// The Cloud half of `--cloud`: the existing definition it was addressed to, the definition
/// and revision it reached with whether it was created, reused or published to and whether
/// the tree was reused (REQ-CLOUDDEF-22), or its failure with a refusal's status, problem
/// slug and `workflow` safe facts or an invalid response's diagnostic (REQ-CLOUDDEF-28).
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JsonCloud<'a> {
    organization_ref: &'a str,
    project_id: &'a str,
    /// The id from `--cloud-to` or the record's `cloudId`; absent when a create is sent,
    /// whether it creates a definition or reuses one.
    #[serde(skip_serializing_if = "Option::is_none")]
    target: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    definition: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    created: Option<bool>,
    /// Whether the create answered the project's identical definition; present exactly when
    /// `created` is.
    #[serde(skip_serializing_if = "Option::is_none")]
    reused: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    revision: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tree_digest: Option<&'a str>,
    /// Whether Cloud already held the tree sealed, so nothing was uploaded or sealed; present
    /// exactly when `created` is.
    #[serde(skip_serializing_if = "Option::is_none")]
    tree_reused: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    failure: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    problem: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    workflow: Option<&'a WorkflowProblemDetail>,
    #[serde(skip_serializing_if = "Option::is_none")]
    diagnostic: Option<InvalidResponseDiagnostic>,
}

#[derive(Serialize)]
struct JsonDefinition<'a> {
    id: &'a str,
    name: &'a str,
    enabled: bool,
    current: u64,
    created: bool,
    path: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JsonRevision<'a> {
    n: u64,
    tree: &'a str,
    bindings: &'a str,
    published_at: &'a str,
    by: &'a str,
}
