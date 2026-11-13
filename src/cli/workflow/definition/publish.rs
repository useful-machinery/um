use std::io::{self, Write};
use std::path::PathBuf;

use anyhow::{Context, anyhow};
use clap::Args;
use serde::Serialize;

use super::super::super::{
    self as cli, CommonArgs, OrganizationArg, PrincipalAuthenticationArgs, ProjectArg, WorkflowJson,
};
use super::cloud::{resend_once, with_api};
use super::report::{display, write_invalid, write_json};
use crate::exit_code::ExitCode;
use um_api::{
    WorkflowBindingValues, WorkflowDefinitionApi, WorkflowDefinitionFailure, WorkflowProblemDetail,
    WorkflowPublication, WorkflowTreeFile,
};
use um_execution::{
    BindingsFile, PublishTarget, Published, ValidatedTree, WorkflowHome, check_definition_id,
    publish, validate_workflow_tree,
};
use um_human_auth::Deployment;

pub(super) const ABOUT: &str = "Publish a local workflow tree as a definition revision";
const COMMAND_NAME: &str = "um workflow definition publish";
const REFUSED: &str = "Publish refused.";

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
        help = "JSON object of binding values, resolved from the initial working directory"
    )]
    bindings: Option<PathBuf>,

    #[arg(
        long,
        value_name = "ID",
        conflicts_with = "new",
        help = "Publish to this local definition id, as `um workflow definition list` prints it, whatever its name"
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
        help = "Also publish the revision to Cloud in this organization (ID or exact slug)"
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
        help = "Publish to this Cloud definition id, as `um workflow definition cloud list` prints it, and record it for the local definition"
    )]
    cloud_to: Option<String>,

    #[arg(
        long,
        requires = "cloud",
        help = "Create another Cloud definition even when the local definition records one"
    )]
    cloud_new: bool,

    #[command(flatten)]
    options: CommonArgs<WorkflowJson, PrincipalAuthenticationArgs>,
}

/// Where `--cloud` publishes (REQ-CLOUDDEF-14).
struct CloudTarget {
    organization: String,
    project: String,
    definition: CloudDefinition,
}

/// How the Cloud definition `--cloud` publishes to is chosen.
enum CloudDefinition {
    /// The local record's `cloudId`, or a new one when the record holds none.
    Recorded,
    /// `--cloud-to CLOUD_ID`, whatever the record holds (REQ-CLOUDDEF-20).
    Named(String),
    /// `--cloud-new`.
    New,
}

impl CloudTarget {
    /// The existing Cloud definition the publication is addressed to, given the record's
    /// `cloudId`; none when it creates one.
    fn existing<'a>(&'a self, recorded: Option<&'a str>) -> Option<&'a str> {
        match &self.definition {
            CloudDefinition::Recorded => recorded,
            CloudDefinition::Named(id) => Some(id),
            CloudDefinition::New => None,
        }
    }

    /// `--cloud-to`'s id, when it was given.
    fn named(&self) -> Option<&str> {
        match &self.definition {
            CloudDefinition::Named(id) => Some(id),
            CloudDefinition::Recorded | CloudDefinition::New => None,
        }
    }
}

/// The Cloud definition a publication reached and the revision it recorded.
struct CloudPublished {
    definition: String,
    created: bool,
    revision: i32,
    tree_digest: String,
}

impl Command {
    pub(super) fn execute(self) -> cli::CommandResult {
        let json = self.options.json;
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
        // The deployment is configuration, read before anything is written; the Cloud
        // steps follow the local publish, which runs as it does without `--cloud`.
        let deployment = cloud
            .as_ref()
            .map(|_| {
                Deployment::load()
                    .map_err(|error| anyhow!(error))
                    .context("configure Useful Machinery workflow definition publication")
            })
            .transpose()?;
        let published = WorkflowHome::from_environment().and_then(|home| {
            let bindings = self
                .bindings
                .as_deref()
                .map(BindingsFile::read)
                .transpose()?;
            publish(
                &home,
                &tree,
                target,
                bindings.as_ref(),
                &super::super::login_name(),
            )
            .map(|published| (home, published))
        });
        let (Some(cloud), Some(deployment)) = (cloud, deployment) else {
            return super::finish(
                COMMAND_NAME,
                REFUSED,
                published.map(|(_, published)| published),
                json,
                |published, json| {
                    if json {
                        write_json(
                            &published_report(published, None),
                            "workflow definition publish",
                        )
                    } else {
                        write_human_published(published)
                    }
                },
            );
        };
        let (home, published) = match published {
            Ok(published) => published,
            Err(error) => {
                return super::finish::<()>(COMMAND_NAME, REFUSED, Err(error), json, |_, _| Ok(()));
            }
        };
        let reached = publish_to_cloud(&deployment, &self.options, &cloud, &tree, &published)?;
        // The record names the Cloud definition its last successful publication reached.
        if let Ok(reached) = &reached
            && published.record.cloud_id.as_deref() != Some(reached.definition.as_str())
            && let Err(error) = home.set_cloud_id(&published.record.id, &reached.definition)
        {
            writeln!(
                io::stderr().lock(),
                "error: Cloud definition {} received revision {} but is not recorded as the cloudId of {}",
                display(&reached.definition),
                reached.revision,
                display(&published.record.id)
            )
            .context("write workflow definition publish result")?;
            return super::finish::<()>(COMMAND_NAME, REFUSED, Err(error), json, |_, _| Ok(()));
        }
        write_cloud_published(&published, &cloud, &reached, json)
            .context("write workflow definition publish result")?;
        Ok(match &reached {
            Ok(_) => ExitCode::Success,
            Err(failure) => cloud_failure(failure).1,
        })
    }
}

/// Uploads and seals the tree, then publishes to the Cloud definition `--cloud-to` names
/// or, without it, the one the local record names unless `--cloud-new`, over the current
/// revision it reads first, else creates one (REQ-CLOUDDEF-14, REQ-CLOUDDEF-20). A refused
/// read or publication of an existing definition never falls through to a create. The revision's bindings snapshot is sent whole, a
/// `members` value as an array, with the tree's executable files; one fresh key covers the
/// seal and the publication for the whole invocation.
fn publish_to_cloud(
    deployment: &Deployment,
    options: &CommonArgs<WorkflowJson, PrincipalAuthenticationArgs>,
    target: &CloudTarget,
    tree: &ValidatedTree,
    published: &Published,
) -> anyhow::Result<Result<CloudPublished, WorkflowDefinitionFailure>> {
    let key = um_support::generate_idempotency_key()
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
    let bindings = published
        .snapshot
        .bindings()
        .iter()
        .map(|(binding, bound)| Ok((binding.clone(), serde_json::to_value(&bound.value)?)))
        .collect::<Result<WorkflowBindingValues, serde_json::Error>>()
        .context("encode the bindings snapshot")?;
    // Every executable file, `[]` when none is, so clearing the last bit is published too.
    let executable_paths = tree
        .files
        .iter()
        .filter(|(_, file)| file.executable)
        .map(|(path, _)| path.clone())
        .collect::<Vec<_>>();
    let steps = CloudSteps {
        target,
        digest: &published.revision.tree,
        files: &files,
        bindings: &bindings,
        executable_paths: &executable_paths,
        existing: target.existing(published.record.cloud_id.as_deref()),
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
    bindings: &'a WorkflowBindingValues,
    /// The tree's executable files, in bytewise order.
    executable_paths: &'a [String],
    /// The Cloud definition `--cloud-to` or the local record names, unless `--cloud-new`.
    existing: Option<&'a str>,
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
        api.upload_tree(organization, self.digest, self.files, self.key)?;
        let publication = WorkflowPublication {
            tree_digest: self.digest,
            bindings: Some(self.bindings),
            executable_paths: Some(self.executable_paths),
        };
        // The create or publication is sent again once, under the same key, when Cloud
        // does not answer it.
        let publication = match self.existing {
            Some(id) => {
                let current = api.definition(organization, project, id)?.current_revision;
                resend_once(|| {
                    api.publish_revision(organization, project, id, self.key, current, publication)
                })?
            }
            None => {
                resend_once(|| api.create_definition(organization, project, self.key, publication))?
            }
        };
        Ok(CloudPublished {
            definition: publication.definition.id,
            created: self.existing.is_none(),
            revision: publication.revision.number,
            tree_digest: publication.revision.tree_digest,
        })
    }
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

fn write_cloud_published(
    published: &Published,
    target: &CloudTarget,
    reached: &Result<CloudPublished, WorkflowDefinitionFailure>,
    json: bool,
) -> anyhow::Result<()> {
    let refusal = match reached {
        Err(WorkflowDefinitionFailure::Refused(refusal)) => Some(refusal),
        _ => None,
    };
    let addressed = target.existing(published.record.cloud_id.as_deref());
    if json {
        let reached_ok = reached.as_ref().ok();
        let cloud = JsonCloud {
            organization_ref: &target.organization,
            project_id: &target.project,
            target: addressed,
            definition: reached_ok.map(|reached| reached.definition.as_str()),
            created: reached_ok.map(|reached| reached.created),
            revision: reached_ok.map(|reached| reached.revision),
            tree_digest: reached_ok.map(|reached| reached.tree_digest.as_str()),
            failure: reached
                .as_ref()
                .err()
                .map(|failure| cloud_failure(failure).0),
            status: refusal.map(|refusal| refusal.status),
            problem: refusal.map(|refusal| refusal.problem.as_str()),
            workflow: refusal.and_then(|refusal| refusal.workflow.as_ref()),
        };
        return write_json(
            &published_report(published, Some(cloud)),
            "workflow definition publish",
        );
    }
    write_human_published(published)?;
    match reached {
        Ok(reached) => {
            let mut stdout = io::stdout().lock();
            writeln!(stdout, "cloud definition: {}", display(&reached.definition))?;
            writeln!(stdout, "cloud created: {}", super::yes_no(reached.created))?;
            writeln!(stdout, "cloud revision: {}", reached.revision)?;
            writeln!(stdout, "cloud tree: {}", display(&reached.tree_digest))?;
        }
        Err(failure) => {
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
                    if let Some(key) = refusal
                        .workflow
                        .as_ref()
                        .and_then(|detail| detail.binding_key.as_deref())
                    {
                        writeln!(stderr, "binding: {}", display(key))?;
                    }
                }
                None => writeln!(
                    stderr,
                    "error: {publication} failed: {}",
                    cloud_failure(failure).0
                )?,
            }
            match target.named() {
                // The record's Cloud definition was not changed, so the next attempt names
                // the target again.
                Some(id) => writeln!(
                    stderr,
                    "\nRevision {} of {} stays recorded locally, without {} as its Cloud definition. Resolve the error and publish again with the target:\n  um workflow definition publish <DIR> --cloud {} --project {} --cloud-to {}",
                    published.revision.n,
                    display(&published.record.id),
                    display(id),
                    display(&target.organization),
                    display(&target.project),
                    display(id)
                )?,
                None => writeln!(
                    stderr,
                    "\nRevision {} of {} stays recorded locally; resolve the error and publish again.",
                    published.revision.n,
                    display(&published.record.id)
                )?,
            }
        }
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

fn published_report<'a>(
    published: &'a Published,
    cloud: Option<JsonCloud<'a>>,
) -> JsonPublished<'a> {
    let record = &published.record;
    let revision = &published.revision;
    JsonPublished {
        schema_version: 1,
        command: COMMAND_NAME,
        outcome: if cloud.as_ref().is_some_and(|cloud| cloud.failure.is_some()) {
            "cloud_failed"
        } else {
            "published"
        },
        definition: JsonDefinition {
            id: &record.id,
            name: &record.name,
            enabled: record.enabled,
            current: record.current,
            created: published.created,
            path: published.path.to_string_lossy().into_owned(),
        },
        revision: JsonRevision {
            n: revision.n,
            tree: &revision.tree,
            bindings: &revision.bindings,
            published_at: &revision.published_at,
            by: &revision.by,
        },
        cloud,
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JsonPublished<'a> {
    schema_version: u8,
    command: &'static str,
    outcome: &'static str,
    definition: JsonDefinition<'a>,
    revision: JsonRevision<'a>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cloud: Option<JsonCloud<'a>>,
}

/// The Cloud half of `--cloud`: the existing definition it was addressed to, the definition
/// and revision it reached, or its failure with a refusal's status, problem slug and
/// `workflow` safe facts.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JsonCloud<'a> {
    organization_ref: &'a str,
    project_id: &'a str,
    /// The id from `--cloud-to` or the record's `cloudId`; absent when a create is sent.
    #[serde(skip_serializing_if = "Option::is_none")]
    target: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    definition: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    created: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    revision: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tree_digest: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    failure: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    problem: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    workflow: Option<&'a WorkflowProblemDetail>,
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
