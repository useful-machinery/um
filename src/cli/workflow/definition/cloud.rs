//! `um workflow definition cloud` (REQ-CLOUDDEF-15): lists, shows and changes the Cloud
//! workflow definitions of a project through the public API (REQ-CLOUDDEF-1), and lists and
//! shows a definition's start evaluations, with the CLI's session handling and each
//! refusal's problem type.

mod evaluations;
mod report;

use anyhow::{Context, anyhow};
use clap::{Args, Subcommand};

use super::super::super::{
    self as cli, CommonArgs, OrganizationArg, PaginationArgs, PrincipalAuthenticationArgs,
    ProjectArg, WorkflowJson,
};
use um_api::{HttpClient, WorkflowDefinition, WorkflowDefinitionApi, WorkflowDefinitionFailure};
use um_human_auth::Deployment;

pub(super) const ABOUT: &str = "Manage Cloud workflow definitions";
const COMMAND_PATH: &[&str] = &["workflow", "definition", "cloud"];
/// The most revisions `show` reads, the API's page bound.
const SHOWN_REVISIONS: i32 = 100;

pub(super) type Options = CommonArgs<WorkflowJson, PrincipalAuthenticationArgs>;

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(subcommand)]
    command: Option<Leaf>,
}

#[derive(Debug, Subcommand)]
enum Leaf {
    #[command(about = "Archive a Cloud workflow definition and turn off its automatic starts")]
    Archive(Target),
    #[command(about = "Turn off automatic starts of a Cloud workflow definition")]
    Disable(Target),
    #[command(about = "Turn on automatic starts of a Cloud workflow definition")]
    Enable(Target),
    #[command(about = evaluations::ABOUT)]
    Evaluation(evaluations::Command),
    #[command(about = "List a project's Cloud workflow definitions")]
    List(List),
    #[command(about = "Set the principal a Cloud workflow definition's automatic starts run as")]
    SetPrincipal(SetPrincipal),
    #[command(about = "Show a Cloud workflow definition with its current revision and revisions")]
    Show(Target),
    #[command(about = "Restore an archived Cloud workflow definition")]
    Unarchive(Target),
}

#[derive(Debug, Args)]
struct List {
    #[arg(value_name = OrganizationArg::VALUE_NAME, help = OrganizationArg::HELP)]
    organization: OrganizationArg,
    #[arg(value_name = ProjectArg::VALUE_NAME, help = ProjectArg::HELP)]
    project: ProjectArg,
    #[command(flatten)]
    page: PaginationArgs<100>,
    #[command(flatten)]
    options: Options,
}

#[derive(Debug, Args)]
struct Target {
    #[arg(value_name = OrganizationArg::VALUE_NAME, help = OrganizationArg::HELP)]
    organization: OrganizationArg,
    #[arg(value_name = "DEFINITION", help = "Cloud workflow definition ID")]
    definition: String,
    #[arg(long, value_name = ProjectArg::VALUE_NAME, help = ProjectArg::HELP)]
    project_id: ProjectArg,
    #[command(flatten)]
    options: Options,
}

#[derive(Debug, Args)]
struct SetPrincipal {
    #[command(flatten)]
    target: Target,
    #[arg(
        value_name = "PRINCIPAL",
        help = "Service principal ID the definition's automatic starts run as"
    )]
    principal: String,
}

/// A keyed change one leaf makes to a definition's settings.
#[derive(Clone, Copy)]
enum Change<'a> {
    Enable,
    Disable,
    Archive,
    Unarchive,
    Principal(&'a str),
}

impl Change<'_> {
    const fn leaf(self) -> &'static str {
        match self {
            Self::Enable => "enable",
            Self::Disable => "disable",
            Self::Archive => "archive",
            Self::Unarchive => "unarchive",
            Self::Principal(_) => "set-principal",
        }
    }

    const fn outcome(self) -> &'static str {
        match self {
            Self::Enable => "enabled",
            Self::Disable => "disabled",
            Self::Archive => "archived",
            Self::Unarchive => "unarchived",
            Self::Principal(_) => "principal_set",
        }
    }

    fn apply(
        self,
        api: &WorkflowDefinitionApi<'_>,
        target: &Target,
        key: &str,
    ) -> Result<WorkflowDefinition, WorkflowDefinitionFailure> {
        let (organization, project, id) = (
            &*target.organization,
            &*target.project_id,
            target.definition.as_str(),
        );
        match self {
            Self::Enable => api.enable(organization, project, id, key),
            Self::Disable => api.disable(organization, project, id, key),
            Self::Archive => api.archive(organization, project, id, key),
            Self::Unarchive => api.unarchive(organization, project, id, key),
            Self::Principal(principal) => {
                api.set_execution_principal(organization, project, id, key, principal)
            }
        }
    }
}

impl Command {
    pub(super) fn execute(self) -> cli::CommandResult {
        // The `evaluation` group prints its own help before loading any configuration.
        if let Some(Leaf::Evaluation(evaluation)) = self.command {
            return evaluation.execute();
        }
        cli::execute_deployment_command(
            self.command,
            COMMAND_PATH,
            "configure Useful Machinery workflow definitions",
            |leaf, deployment| match leaf {
                Leaf::Evaluation(evaluation) => evaluation.execute(),
                Leaf::List(list) => list.run(deployment),
                Leaf::Show(target) => target.show(deployment),
                Leaf::Enable(target) => target.change(deployment, Change::Enable),
                Leaf::Disable(target) => target.change(deployment, Change::Disable),
                Leaf::Archive(target) => target.change(deployment, Change::Archive),
                Leaf::Unarchive(target) => target.change(deployment, Change::Unarchive),
                Leaf::SetPrincipal(command) => command
                    .target
                    .change(deployment, Change::Principal(&command.principal)),
            },
        )
    }
}

impl List {
    fn run(self, deployment: &Deployment) -> cli::CommandResult {
        let page = with_api(deployment, &self.options, |api| {
            api.definitions(
                &self.organization,
                &self.project,
                self.page.limit.map(i32::from),
                self.page.cursor.as_deref(),
            )
        })?;
        let scope = report::Scope {
            leaf: "list",
            organization: &self.organization,
            project: &self.project,
            json: self.options.json,
        };
        report::finish(&scope, page.as_ref(), |(definitions, cursor)| {
            report::listed(&scope, definitions, cursor.as_deref())
        })
    }
}

impl Target {
    fn scope(&self, leaf: &'static str) -> report::Scope<'_> {
        report::Scope {
            leaf,
            organization: &self.organization,
            project: &self.project_id,
            json: self.options.json,
        }
    }

    /// The definition, its current revision and the newest page of its revisions.
    fn show(self, deployment: &Deployment) -> cli::CommandResult {
        let (organization, project, id) = (
            &*self.organization,
            &*self.project_id,
            self.definition.as_str(),
        );
        let shown = with_api(deployment, &self.options, |api| {
            let definition = api.definition(organization, project, id)?;
            let current = api.revision(organization, project, id, definition.current_revision)?;
            let (revisions, more) =
                api.revisions(organization, project, id, Some(SHOWN_REVISIONS), None)?;
            Ok(report::Shown {
                definition,
                current,
                revisions,
                more: more.is_some(),
            })
        })?;
        let scope = self.scope("show");
        report::finish(&scope, shown.as_ref(), |shown| report::shown(&scope, shown))
    }

    /// Applies `change` under one fresh key, sent again once when the response is lost.
    fn change(&self, deployment: &Deployment, change: Change<'_>) -> cli::CommandResult {
        let key = um_support::generate_idempotency_key()
            .context("generate Cloud workflow definition request identity")?;
        let changed = with_api(deployment, &self.options, |api| {
            resend_once(|| change.apply(api, self, &key))
        })?;
        let scope = self.scope(change.leaf());
        report::finish(&scope, changed.as_ref(), |definition| {
            report::changed(&scope, change.outcome(), definition)
        })
    }
}

/// Sends a keyed request, and sends it once more when Cloud does not answer: the caller
/// keeps the key, so a request Cloud applied before its answer was lost is answered again,
/// not applied twice.
pub(super) fn resend_once<T>(
    mut send: impl FnMut() -> Result<T, WorkflowDefinitionFailure>,
) -> Result<T, WorkflowDefinitionFailure> {
    match send() {
        Err(WorkflowDefinitionFailure::Unreachable { .. }) => send(),
        sent => sent,
    }
}

/// Runs `operation` against the deployment under the leaf's session, again with a refreshed
/// session when the first is rejected.
pub(super) fn with_api<T>(
    deployment: &Deployment,
    options: &Options,
    mut operation: impl FnMut(&WorkflowDefinitionApi<'_>) -> Result<T, WorkflowDefinitionFailure>,
) -> anyhow::Result<Result<T, WorkflowDefinitionFailure>> {
    let client = HttpClient::new(options.http.transport_policy())
        .map_err(|error| anyhow!(error))
        .context("prepare workflow definition networking")?;
    cli::execute_selected_api_operation(
        cli::principal_api_context(
            &client,
            deployment,
            &options.authentication,
            "acquire human session",
        ),
        |token| {
            let api =
                WorkflowDefinitionApi::new(deployment.fingerprint().api_url(), token, &client)
                    .map_err(|error| anyhow!(error))
                    .context("prepare workflow definition API")?;
            Ok(operation(&api))
        },
        WorkflowDefinitionFailure::credential_rejected,
        || WorkflowDefinitionFailure::Unauthenticated,
        |category| WorkflowDefinitionFailure::Unreachable { category },
    )
}
