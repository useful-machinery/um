mod workflow;

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, anyhow};
use clap::{ArgGroup, Args};
use zeroize::Zeroizing;

use crate::cli::integration::{OrganizationSignIn, Reach};
use crate::cli::runner::doctor::roots::Roots;
use crate::cli::{CommandFailure, HttpOptions, NoAuthenticationArgs, OrganizationArg};
use crate::exit_code::ExitCode;
use useful_api::{
    MemberGrant, MemberGrantApi, MemberGrantFailure, MemberGrantKind, MemberGrantProvider,
    ServedGrantProvider, ServedTokenService,
};
use useful_human_auth::Deployment;
use useful_runner::{CredentialKind, WorkflowCredentials};

use super::super::secret_file::FileMode;
use super::consent::{Consent, ConsentProvider, Ended, Front, Options, write_refusal};
use super::organization::Organization;
use super::program::Stop;
use super::projection::{self, Provider};
use super::{
    browser, claude_setup_token, codex_login, material, organization, output, replacement, replayed,
};

pub(super) const ABOUT: &str = "Create a grant through the provider's own sign-in";

#[derive(Debug, Args)]
#[command(group(ArgGroup::new("source").multiple(false)))]
#[command(group(ArgGroup::new("replaces").args(["provider", "workflows"]).multiple(true)))]
pub(in crate::cli) struct Command {
    #[arg(
        value_name = "PROVIDER",
        value_parser = parse_provider,
        help = "Service to connect: github, gitlab.com, linear, claude_code, codex or the id of another OAuth provider or token service your deployment serves; without one, each provider you hold no grant of"
    )]
    provider: Option<ProviderName>,

    // Parsed only: `--all` says what no PROVIDER says.
    #[arg(
        id = "all",
        long = "all",
        conflicts_with = "provider",
        help = "Sign in to each provider you hold no grant of, as when no PROVIDER is named"
    )]
    _all: bool,

    #[arg(
        long = "workflow",
        value_name = "FILE",
        conflicts_with_all = ["provider", "all"],
        help = "Sign in to only the providers this workflow and the workflows it runs call for; repeats"
    )]
    workflows: Vec<PathBuf>,

    #[arg(
        long,
        value_name = "ROOT",
        requires = "workflows",
        help = "Directory boundary for the --workflow files"
    )]
    source_root: Option<PathBuf>,

    #[arg(long, value_name = OrganizationArg::VALUE_NAME, help = OrganizationArg::HELP)]
    organization: Option<OrganizationArg>,

    #[arg(
        long,
        group = "source",
        help = "Run Codex's sign-in, as when no other source is named"
    )]
    login: bool,

    #[arg(
        long,
        group = "source",
        help = "Sign in to Codex with a device code, which any browser can complete"
    )]
    device: bool,

    #[arg(
        long,
        value_name = "PATH",
        group = "source",
        requires = "provider",
        help = "Read a Codex auth.json or a token service's token from an owned mode-0600 file, or - for standard input"
    )]
    from_file: Option<PathBuf>,

    #[arg(
        long,
        value_name = "PATH",
        group = "source",
        requires = "provider",
        help = "Read the token claude setup-token printed from an owned mode-0600 file, or - for standard input"
    )]
    token_file: Option<PathBuf>,

    // `--all` conflicts with both of the group, and clap does not hold an argument required
    // when it conflicts with one given, so `--all` is refused here too.
    #[arg(
        long,
        requires = "replaces",
        conflicts_with = "all",
        help = "Replace the grant of the provider you already hold without asking; with --workflow, only a grant that needs reconnecting"
    )]
    replace: bool,

    #[command(flatten)]
    wait: super::super::WaitTimeoutArgs,

    #[arg(
        long,
        conflicts_with = "timeout",
        help = "Return after starting a consent instead of waiting for the callback; OAuth providers only"
    )]
    no_wait: bool,

    #[command(flatten)]
    options: Options,
}

/// What the flow of `useful integration connect` reads of the command that runs it, besides the
/// providers: `useful integration connect` builds it from its options, and `useful runner doctor
/// --workflow --connect` from its own, taking the defaults for the rest (INV-CLI-15).
#[derive(Default)]
pub(in crate::cli) struct Flow<'a> {
    /// Whether each line printed is a JSON event.
    pub(in crate::cli) json: bool,
    /// Whether the requests may use insecure HTTP.
    pub(in crate::cli) allow_insecure_http: bool,
    /// The organization a coding agent's grant is for, by ID or exact slug (REQ-GRANT-72).
    pub(in crate::cli) organization: Option<&'a str>,
    /// Whether each replacement is settled without asking (REQ-GRANT-73, REQ-GRANT-79).
    pub(in crate::cli) replace: bool,
    /// Whether Codex signs in by device code even where this machine has a browser.
    pub(in crate::cli) device: bool,
    /// Whether a consent returns once started instead of waiting for its callback.
    pub(in crate::cli) no_wait: bool,
    /// How long a consent is waited for, when not its default.
    pub(in crate::cli) timeout: Option<Duration>,
}

/// How the flow over a workflow tree ended.
#[derive(Clone, Copy)]
pub(in crate::cli) struct Ending {
    /// Whether it printed its summary; a failure reported before any sign-in ends it without.
    pub(in crate::cli) summarized: bool,
    /// Its exit status (REQ-GRANT-75).
    pub(in crate::cli) code: ExitCode,
}

impl Ending {
    /// Whether a signal stopped it, as its status says.
    pub(in crate::cli) const fn stopped(self) -> bool {
        matches!(self.code, ExitCode::Interrupted | ExitCode::Terminated)
    }
}

/// A provider `useful integration connect` has a built-in name for.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProviderArg {
    Github,
    GitlabCom,
    Linear,
    ClaudeCode,
    Codex,
}

/// The `PROVIDER` `useful integration connect` is given (REQ-GRANT-69): one of the five it has a
/// built-in name for, or any other provider id, taken for an OAuth provider the deployment serves,
/// which the command reads before it begins anything.
#[derive(Clone, Debug, Eq, PartialEq)]
enum ProviderName {
    BuiltIn(ProviderArg),
    Other(String),
}

/// Parses `PROVIDER`: one of the five by its wire name, or any other name of the provider id
/// shape; anything else is a usage error before any request.
fn parse_provider(value: &str) -> Result<ProviderName, String> {
    if let Some(provider) = ProviderArg::from_name(value) {
        return Ok(ProviderName::BuiltIn(provider));
    }
    if useful_api::valid_provider_id(value) {
        return Ok(ProviderName::Other(value.to_owned()));
    }
    Err(
        "provider must be github, gitlab.com, linear, claude_code or codex, or the id of another \
         OAuth provider your deployment serves, as useful integration list shows it"
            .to_owned(),
    )
}

/// An OAuth provider beyond `github`, `gitlab.com` and `linear` that the deployment serves
/// (REQ-GRANT-78): its id, and its title, the display name the deployment serves for it or else
/// its id (REQ-GRANT-96).
#[derive(Clone, Debug, Eq, PartialEq)]
struct NamedProvider {
    name: String,
    title: String,
}

impl NamedProvider {
    /// The provider `served` names, when it is an OAuth provider with no built-in name.
    fn of(served: &ServedGrantProvider) -> Option<Self> {
        (served.kind == MemberGrantKind::GrantKindOAuth
            && ProviderArg::from_name(&served.name).is_none())
        .then(|| Self {
            name: served.name.clone(),
            title: served
                .display_name
                .clone()
                .unwrap_or_else(|| served.name.clone()),
        })
    }

    /// The consent that makes its grant.
    fn consent(&self) -> ConsentProvider<'_> {
        ConsentProvider {
            name: &self.name,
            title: &self.title,
        }
    }
}

/// A provider in the order `useful integration connect` signs in to several in: one of the five it
/// has a built-in name for, or another OAuth provider the deployment serves (REQ-GRANT-75).
enum Ordered {
    BuiltIn(ProviderArg),
    Served(NamedProvider),
}

impl Ordered {
    /// The consent of an OAuth provider; a coding agent has none.
    fn consent(&self) -> Option<ConsentProvider<'_>> {
        match self {
            Self::BuiltIn(provider) => provider.consent(),
            Self::Served(provider) => Some(provider.consent()),
        }
    }

    /// The provider when it is one of the five.
    const fn built_in(&self) -> Option<ProviderArg> {
        match self {
            Self::BuiltIn(provider) => Some(*provider),
            Self::Served(_) => None,
        }
    }
}

/// The order `useful integration connect` signs in to several providers in (REQ-GRANT-75), that of
/// [`projection::providers`] for the providers `served` names: the OAuth providers first,
/// whose consents need only a browser anywhere, the deployment's other OAuth providers after
/// the built-in ones, then the coding agents, whose sign-ins run on this machine.
fn order(served: &[ServedGrantProvider]) -> Vec<Ordered> {
    projection::providers(served)
        .into_iter()
        .filter_map(|provider| {
            let name = provider.name();
            ProviderArg::from_name(name)
                .map(Ordered::BuiltIn)
                .or_else(|| {
                    served
                        .iter()
                        .find(|served| served.name == name)
                        .and_then(NamedProvider::of)
                        .map(Ordered::Served)
                })
        })
        .collect()
}

/// What a selection chooses among.
#[derive(Clone, Copy)]
enum Wanted<'a> {
    /// Every provider of the [`order`], the deployment's other OAuth providers among them
    /// (REQ-GRANT-75).
    Every,
    /// Only these of the five, as workflows call for them (REQ-GRANT-76).
    Only(&'a [ProviderArg]),
}

impl Wanted<'_> {
    fn wants(self, provider: &Ordered) -> bool {
        match (self, provider) {
            (Self::Every, _) => true,
            (Self::Only(wanted), Ordered::BuiltIn(provider)) => wanted.contains(provider),
            (Self::Only(_), Ordered::Served(_)) => false,
        }
    }
}

impl ProviderArg {
    const ALL: [Self; 5] = [
        Self::Github,
        Self::GitlabCom,
        Self::Linear,
        Self::ClaudeCode,
        Self::Codex,
    ];

    /// The provider whose wire name is `name`, when it is one of the five.
    fn from_name(name: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|provider| provider.grant().as_str() == name)
    }

    const fn grant(self) -> MemberGrantProvider {
        match self {
            Self::Github => MemberGrantProvider::GrantProviderGithub,
            Self::GitlabCom => MemberGrantProvider::GrantProviderGitlabCom,
            Self::Linear => MemberGrantProvider::GrantProviderLinear,
            Self::ClaudeCode => MemberGrantProvider::GrantProviderClaudeCode,
            Self::Codex => MemberGrantProvider::GrantProviderCodex,
        }
    }

    /// The consent of an OAuth provider; a coding agent has none.
    const fn consent(self) -> Option<ConsentProvider<'static>> {
        match self {
            Self::Github => Some(ConsentProvider::GITHUB),
            Self::GitlabCom => Some(ConsentProvider::GITLAB_COM),
            Self::Linear => Some(ConsentProvider::LINEAR),
            Self::ClaudeCode | Self::Codex => None,
        }
    }

    const fn native(self) -> bool {
        self.consent().is_none()
    }

    /// The provider whose grant a kind a workflow calls for names; `gitlab` is GitLab.com
    /// (REQ-GRANT-76). A token service has none (REQ-CREDREADY-29).
    const fn of_kind(kind: CredentialKind) -> Option<Self> {
        Some(match kind {
            CredentialKind::Github => Self::Github,
            CredentialKind::Gitlab => Self::GitlabCom,
            CredentialKind::Linear => Self::Linear,
            CredentialKind::ClaudeCode => Self::ClaudeCode,
            CredentialKind::Codex => Self::Codex,
            CredentialKind::Mergify | CredentialKind::Buildkite => return None,
        })
    }
}

/// How this invocation makes the grant.
enum SignIn<'a> {
    /// The consent of `github`, `gitlab.com` or `linear`.
    Consent(ConsentProvider<'static>),
    /// The consent of another OAuth provider, by its id, once the deployment is found to serve
    /// it (REQ-GRANT-69).
    Served(&'a str),
    /// A coding agent's login, imported for one organization.
    Native(ProviderArg, Login),
}

/// A coding agent's login: read from a file or stream the member names, before any network
/// use, or made by a sign-in `useful` runs in a directory it made once the command has settled
/// what the login replaces (INV-CLI-13).
enum Login {
    Read(Zeroizing<Vec<u8>>),
    /// `codex` with these arguments.
    Codex(&'static [&'static str]),
    /// `claude setup-token` on a terminal `useful` owns.
    ClaudeCode,
}

/// The providers one invocation signs in to, in [`order`]: the OAuth providers' consents, then
/// the coding agents', among them those the deployment does not serve, which are passed over,
/// and with `--workflow` the providers whose every held grant needs reconnecting.
struct Selection {
    /// The providers wanted, in [`order`].
    order: Vec<Ordered>,
    /// The OAuth providers consented to, by name.
    consents: Vec<String>,
    natives: Option<Natives>,
    /// The coding agents passed over without a sign-in because the deployment does not serve
    /// them (REQ-GRANT-75, REQ-GRANT-78).
    unavailable: Vec<ProviderArg>,
    /// The providers whose every held grant needs reconnecting, in [`order`] (REQ-GRANT-79).
    reconnects: Vec<Reconnect>,
}

/// The coding agents a [`Selection`] takes, in [`order`].
enum Natives {
    /// Signed in to for the one organization their grants are for.
    For(Organization, Vec<ProviderArg>),
    /// Passed over, since no organization was resolved: the refusal that says why and names
    /// `--organization` (REQ-GRANT-75).
    Unresolved(String, Vec<ProviderArg>),
}

impl Natives {
    fn providers(&self) -> &[ProviderArg] {
        match self {
            Self::For(_, providers) | Self::Unresolved(_, providers) => providers,
        }
    }
}

/// A provider every grant of which the member holds needs reconnecting, and whether the
/// member settled its replacement (REQ-GRANT-79).
struct Reconnect {
    provider: ProviderArg,
    held: Held,
    replace: bool,
}

/// What reconnecting a provider replaces.
enum Held {
    /// The grants of an OAuth provider, of which the account the member consents as decides
    /// which, if any, gets the next generation (REQ-GRANT-73, REQ-GRANT-2).
    Consent(ConsentProvider<'static>, Vec<MemberGrant>),
    /// The one grant of a coding agent, which the sign-in's next generation is stored for, in
    /// its own organization.
    Native(Box<MemberGrant>, Organization),
    /// Grants of a coding agent the command cannot choose between, several, or one that names
    /// no organization: passed over without asking.
    Unchosen,
}

impl Held {
    /// What reconnecting a coding agent replaces, of the grants `held` that need it: its one
    /// grant, in its own organization, which is `resolved` when they match and otherwise named
    /// by ID, which sends nothing.
    fn native(
        api: &MemberGrantApi,
        held: &[&MemberGrant],
        resolved: Option<&(String, Option<String>)>,
    ) -> Result<Self, MemberGrantFailure> {
        let [grant] = held else {
            return Ok(Self::Unchosen);
        };
        let Some(id) = grant.organization_id.as_deref() else {
            return Ok(Self::Unchosen);
        };
        let organization = match resolved {
            Some((resolved, slug)) if resolved == id => Organization {
                id: resolved.clone(),
                slug: slug.clone(),
            },
            _ => organization::named(api, id)?,
        };
        Ok(Self::Native(Box::new((*grant).clone()), organization))
    }
}

impl Reconnect {
    /// The grants a replacement asks about, or none when there is no choosing between them.
    fn asked(&self) -> Option<&[MemberGrant]> {
        match &self.held {
            Held::Consent(_, held) => Some(held),
            Held::Native(grant, _) => Some(std::slice::from_ref(grant.as_ref())),
            Held::Unchosen => None,
        }
    }
}

impl Selection {
    const NONE: Self = Self {
        order: Vec::new(),
        consents: Vec::new(),
        natives: None,
        unavailable: Vec::new(),
        reconnects: Vec::new(),
    };

    /// The step that signs in to or passes over `provider`, or none when it is held and not
    /// asked about.
    fn step<'a>(&'a self, provider: &'a Ordered) -> Option<Step<'a>> {
        let provider = match provider {
            Ordered::BuiltIn(provider) => *provider,
            Ordered::Served(provider) => {
                return self
                    .consents
                    .contains(&provider.name)
                    .then(|| Step::Consent(provider.consent()));
            }
        };
        if let Some(reconnect) = self
            .reconnects
            .iter()
            .find(|reconnect| reconnect.provider == provider)
        {
            return Some(match &reconnect.held {
                _ if !reconnect.replace => Step::Kept(provider),
                Held::Consent(consent, _) => Step::Reconsent(*consent),
                Held::Native(grant, organization) => Step::Renew(provider, grant, organization),
                Held::Unchosen => Step::Kept(provider),
            });
        }
        if let Some(consent) = provider.consent() {
            return self
                .consents
                .iter()
                .any(|selected| selected == consent.name)
                .then_some(Step::Consent(consent));
        }
        if self.unavailable.contains(&provider) {
            return Some(Step::Unavailable(provider));
        }
        let natives = self.natives.as_ref()?;
        natives
            .providers()
            .contains(&provider)
            .then(|| match natives {
                Natives::For(organization, _) => Step::Native(provider, organization),
                Natives::Unresolved(refusal, _) => Step::PassedOver(provider, refusal),
            })
    }

    /// What the plan says the run does about `provider` (REQ-GRANT-76, REQ-GRANT-79).
    fn planned(&self, provider: ProviderArg) -> workflow::State {
        match self.step(&Ordered::BuiltIn(provider)) {
            None | Some(Step::Kept(_)) => workflow::State::Held,
            Some(Step::Reconsent(_) | Step::Renew(..)) => workflow::State::Reconnect,
            Some(_) => workflow::State::SignIn,
        }
    }
}

/// One provider's sign-in in a [`Selection`].
#[derive(Clone, Copy)]
enum Step<'a> {
    Consent(ConsentProvider<'a>),
    Native(ProviderArg, &'a Organization),
    /// The consent that replaces a held OAuth grant that needs reconnecting (REQ-GRANT-79).
    Reconsent(ConsentProvider<'a>),
    /// A coding agent's sign-in stored as the next generation of the held grant that needs
    /// reconnecting, in that grant's organization (REQ-GRANT-79).
    Renew(ProviderArg, &'a MemberGrant, &'a Organization),
    /// A coding agent passed over for want of an organization, with the refusal that says so.
    PassedOver(ProviderArg, &'a str),
    /// A coding agent passed over because the deployment does not serve it.
    Unavailable(ProviderArg),
    /// A provider whose grants need reconnecting passed over, its replacement not settled.
    Kept(ProviderArg),
}

impl<'a> Step<'a> {
    const fn provider(self) -> &'a str {
        match self {
            Self::Consent(consent) | Self::Reconsent(consent) => consent.name,
            Self::Native(provider, _)
            | Self::Renew(provider, ..)
            | Self::PassedOver(provider, _)
            | Self::Unavailable(provider)
            | Self::Kept(provider) => provider.grant().as_str(),
        }
    }
}

/// How one provider's sign-in ended, as the summary names it.
#[derive(Clone, Copy)]
enum Outcome {
    Created,
    /// A grant that needed reconnecting replaced by the sign-in (REQ-GRANT-79).
    Replaced,
    /// A consent started under `--no-wait`.
    Started,
    /// The deployment does not serve the provider, which is passed over.
    Unavailable,
    /// A coding agent passed over because no organization was resolved for its grant.
    OrganizationRequired,
    /// A provider passed over because its grants need reconnecting and their replacement was
    /// not settled (REQ-GRANT-79).
    ReconnectRequired,
    Failed,
    /// A signal ended the command with this status.
    Stopped(ExitCode),
}

/// How an import ended, and the signal that stopped its sign-in when one did.
struct Imported {
    ended: Ended,
    stopped: Option<Stop>,
}

impl Outcome {
    /// How a sign-in that `waits` for its grant ended, one that `replaces` a held grant
    /// reported as `replaced` when it made one.
    fn of(ended: &Ended, waits: bool, replaces: bool) -> Self {
        match ended.code {
            ExitCode::Success if waits && replaces => Self::Replaced,
            ExitCode::Success if waits => Self::Created,
            ExitCode::Success => Self::Started,
            ExitCode::Interrupted | ExitCode::Terminated => Self::Stopped(ended.code),
            _ if ended.unavailable => Self::Unavailable,
            _ => Self::Failed,
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Replaced => "replaced",
            Self::Started => "started",
            Self::Unavailable => "provider_unavailable",
            Self::OrganizationRequired => output::ORGANIZATION_REQUIRED,
            Self::ReconnectRequired => output::RECONNECT_REQUIRED,
            Self::Failed => "failed",
            Self::Stopped(ExitCode::Terminated) => "terminated",
            Self::Stopped(_) => "interrupted",
        }
    }
}

/// The first of `options` that was given.
fn first_given<const N: usize>(options: [(bool, &'static str); N]) -> Option<&'static str> {
    options
        .into_iter()
        .find_map(|(given, option)| given.then_some(option))
}

/// A usage error, exit 2, saying `message`.
fn usage(message: String) -> CommandFailure {
    CommandFailure::with_exit_code(anyhow!(message), ExitCode::UsageError)
}

/// The usage error of `option` given for the service `name`, which does not take it, with
/// `remedy`.
fn unsuited(option: &str, name: &str, remedy: &str) -> CommandFailure {
    usage(format!("{option} does not apply to {name}\n\n{remedy}"))
}

/// The OAuth provider with no built-in name `name` names among those `served` lists
/// (REQ-GRANT-69), or the refusal of a name it does not list as one, exit 2, which names the
/// providers that can be given and, where `token_services` holds the token services served
/// because they are in reach, after them those (REQ-INTEGRATION-16).
fn served_oauth(
    name: &str,
    served: &[ServedGrantProvider],
    token_services: Option<&[ServedTokenService]>,
) -> Result<NamedProvider, CommandFailure> {
    if let Some(provider) = served
        .iter()
        .filter_map(NamedProvider::of)
        .find(|provider| provider.name == name)
    {
        return Ok(provider);
    }
    // The built-in OAuth providers, the served ones, the coding agents, then the token
    // services.
    let mut given: Vec<_> = projection::providers(served)
        .into_iter()
        .map(Provider::name)
        .chain(
            token_services
                .unwrap_or_default()
                .iter()
                .map(|service| service.name.as_str()),
        )
        .collect();
    let last = given.pop().unwrap_or_default();
    let served_as = match token_services {
        Some(_) => "an OAuth provider or a token service",
        None => "an OAuth provider",
    };
    Err(usage(format!(
        "this deployment does not serve {name} as {served_as}\n\n\
         Name {} or {last} instead:\n  useful integration connect PROVIDER",
        given.join(", ")
    )))
}

/// The exit status of several sign-ins (REQ-GRANT-75): a signal's when one stopped them, 1 when
/// any failed or when every one was passed over, as unavailable, for want of an organization or
/// as needing reconnecting (REQ-GRANT-79), and otherwise 0, as when there were none.
fn summary_code(outcomes: impl Iterator<Item = Outcome> + Clone) -> ExitCode {
    if let Some(code) = outcomes.clone().find_map(|outcome| match outcome {
        Outcome::Stopped(code) => Some(code),
        _ => None,
    }) {
        return code;
    }
    let mut made = false;
    let mut passed_over = false;
    for outcome in outcomes {
        match outcome {
            Outcome::Failed => return ExitCode::GeneralFailure,
            Outcome::Unavailable | Outcome::OrganizationRequired | Outcome::ReconnectRequired => {
                passed_over = true
            }
            _ => made = true,
        }
    }
    if passed_over && !made {
        ExitCode::GeneralFailure
    } else {
        ExitCode::Success
    }
}

impl Command {
    /// With `PROVIDER`, checks which options suit it before anything is run, read or sent
    /// (REQ-GRANT-70 to REQ-GRANT-72), then makes its grant: by the provider's consent, for a
    /// provider outside the five once the deployment is found to serve it (REQ-GRANT-69), or by
    /// importing a coding agent's login (REQ-GRANT-70, REQ-GRANT-71).
    /// Without it, makes a grant of each provider the member lacks (REQ-GRANT-75), or with
    /// `--workflow` of each the workflows call for (REQ-GRANT-76). Where `reach` puts token
    /// services in reach, under `useful integration`, a name outside the five is read for first,
    /// and may be a token service's (REQ-INTEGRATION-16); with `--org`, `github` and `linear`
    /// connect the organization (REQ-INTEGRATION-21) and the other three of the five refuse it,
    /// before anything is read or sent (REQ-INTEGRATION-17).
    pub(super) fn run(self, deployment: &Deployment, reach: &Reach) -> super::super::CommandResult {
        let flow = self.flow();
        let Some(provider) = &self.provider else {
            if !self.workflows.is_empty() {
                return self.run_workflows(deployment);
            }
            let scope = output::Scope::Every;
            return match flow.select(deployment, Wanted::Every, scope, false)? {
                Ok(selection) => flow.run_selection(deployment, selection, scope),
                Err(refused) => Ok(refused),
            };
        };
        match provider {
            ProviderName::BuiltIn(built_in) if reach.org().is_some() => {
                return self.connect_organization(deployment, *built_in, reach);
            }
            ProviderName::Other(name) if reach.tokens() => {
                return self.connect_served(deployment, provider, name, reach);
            }
            _ => {}
        }
        match self.sign_in(provider)? {
            SignIn::Consent(consent) => flow.replace_by_consent(deployment, consent),
            SignIn::Served(name) => flow.consent_served(deployment, name),
            SignIn::Native(provider, login) => {
                // A Claude Code sign-in the member could not answer is refused before the first
                // request (REQ-GRANT-71).
                if matches!(login, Login::ClaudeCode)
                    && let Some(failure) = claude_setup_token::unanswerable()
                {
                    return Ok(output::write_import_refusal(
                        deployment.fingerprint().api_url(),
                        &failure.report(),
                        self.options.json,
                    )?);
                }
                let signs_in = !matches!(login, Login::Read(_));
                let (organization, target) = match flow.settle(deployment, provider, signs_in)? {
                    Ok(settled) => settled,
                    Err(refused) => return Ok(refused),
                };
                flow.import(deployment, provider, login, &organization, target.as_ref())
                    .map(|imported| imported.ended.code)
            }
        }
    }

    /// What the flow reads of this invocation's options.
    fn flow(&self) -> Flow<'_> {
        Flow {
            json: self.options.json,
            allow_insecure_http: self.options.http.allow_insecure_http,
            organization: self.organization.as_deref(),
            replace: self.replace,
            device: self.device,
            no_wait: self.no_wait,
            timeout: self.wait.timeout,
        }
    }

    /// The sign-in the provider and options name; a file or stream is read here, so that an
    /// insecurely held or malformed one is refused with exit 2 before any network use, as is an
    /// option that does not suit the provider: a source of another provider, or for a coding
    /// agent, whose sign-in has no consent, `--no-wait` or `--timeout` (REQ-GRANT-71). A provider
    /// outside the five is an OAuth provider's, and takes the options `github` takes.
    fn sign_in<'a>(&self, provider: &'a ProviderName) -> Result<SignIn<'a>, CommandFailure> {
        let (name, provider) = match provider {
            ProviderName::BuiltIn(provider) => (provider.grant().as_str(), Some(*provider)),
            ProviderName::Other(name) => (name.as_str(), None),
        };
        let unsuited = |option: &str, remedy: &str| unsuited(option, name, remedy);
        if provider.is_some_and(ProviderArg::native)
            && let Some(option) = first_given([
                (self.no_wait, "--no-wait"),
                (self.wait.timeout.is_some(), "--timeout"),
            ])
        {
            return Err(unsuited(
                option,
                &format!(
                    "A {name} grant is made by the coding agent's own sign-in, which has no \
                     consent to wait for:\n  useful integration connect {name}"
                ),
            ));
        }
        let consent = match provider {
            Some(ProviderArg::Github) => SignIn::Consent(ConsentProvider::GITHUB),
            Some(ProviderArg::GitlabCom) => SignIn::Consent(ConsentProvider::GITLAB_COM),
            Some(ProviderArg::Linear) => SignIn::Consent(ConsentProvider::LINEAR),
            None => SignIn::Served(name),
            Some(ProviderArg::Codex) => {
                if self.token_file.is_some() {
                    return Err(unsuited(
                        "--token-file",
                        "Import a Codex auth.json with --from-file PATH, or sign in with --login or --device.",
                    ));
                }
                return Ok(SignIn::Native(
                    ProviderArg::Codex,
                    match &self.from_file {
                        Some(path) => Login::Read(
                            material::read_codex_login(path, FileMode::Private).map_err(
                                |error| CommandFailure::with_exit_code(error, ExitCode::UsageError),
                            )?,
                        ),
                        None => self.flow().codex_sign_in(),
                    },
                ));
            }
            Some(ProviderArg::ClaudeCode) => {
                if let Some(option) = first_given([
                    (self.from_file.is_some(), "--from-file"),
                    (self.login, "--login"),
                    (self.device, "--device"),
                ]) {
                    return Err(unsuited(
                        option,
                        "Run Claude Code's sign-in, or import the token claude setup-token \
                         printed with --token-file PATH:\n  useful integration connect claude_code",
                    ));
                }
                return Ok(SignIn::Native(
                    ProviderArg::ClaudeCode,
                    match &self.token_file {
                        Some(path) => Login::Read(material::read_claude_code_token(path).map_err(
                            |error| CommandFailure::with_exit_code(error, ExitCode::UsageError),
                        )?),
                        None => Login::ClaudeCode,
                    },
                ));
            }
        };
        if let Some(option) = first_given([
            (self.organization.is_some(), "--organization"),
            (self.login, "--login"),
            (self.device, "--device"),
            (self.from_file.is_some(), "--from-file"),
            (self.token_file.is_some(), "--token-file"),
        ]) {
            return Err(unsuited(
                option,
                &format!(
                    "A {name} grant is made by the provider's consent and is yours alone:\n  \
                     useful integration connect {name}"
                ),
            ));
        }
        Ok(consent)
    }

    /// Under `useful integration`, connects `provider`, the provider id `name` outside the five,
    /// once one read of what the deployment serves says what it is (REQ-INTEGRATION-16): a token
    /// service's token, once the options of a sign-in are refused, for the member or for the
    /// organization `reach` names; or an OAuth provider's consent, as under `useful grant`, but
    /// for `--org`, which it does not take (REQ-INTEGRATION-17). Any other name is refused as
    /// [`Flow::consent_served`] refuses it, naming the token services too, and before anything is
    /// read from a file, standard input or the terminal.
    fn connect_served(
        &self,
        deployment: &Deployment,
        provider: &ProviderName,
        name: &str,
        reach: &Reach,
    ) -> super::super::CommandResult {
        let flow = self.flow();
        let (served, token_services) =
            match flow.with_api(deployment, MemberGrantApi::served_integrations)? {
                Ok(served) => served
                    .map(|served| (served.providers, served.token_services))
                    .unwrap_or_default(),
                Err(failure) => return flow.write_read_refusal(deployment, &failure),
            };
        if let Some(service) = token_services.iter().find(|service| service.name == name) {
            if let Some(option) = first_given([
                (self.login, "--login"),
                (self.device, "--device"),
                (self.token_file.is_some(), "--token-file"),
                (self.no_wait, "--no-wait"),
                (self.wait.timeout.is_some(), "--timeout"),
                (self.organization.is_some(), "--organization"),
            ]) {
                return Err(unsuited(
                    option,
                    name,
                    &format!(
                        "A {name} token is read from --from-file PATH, from standard input or at \
                         a prompt, and is yours, or with --org an organization's:\n  \
                         useful integration connect {name}"
                    ),
                ));
            }
            return reach.connect_token(&flow, deployment, service, self.from_file.as_deref());
        }
        let oauth = served_oauth(name, &served, Some(&token_services))?;
        if reach.org().is_some() {
            return Err(reach.org_refused(
                name,
                false,
                &format!("useful integration connect {name}"),
            ));
        }
        self.sign_in(provider)?;
        flow.replace_by_consent(deployment, oauth.consent())
    }

    /// Under `useful integration` with `--org`, connects the organization `reach` names at
    /// `provider`, `github` or `linear`, through the `useful connection` leaf that does it
    /// (REQ-INTEGRATION-21), once the options of the member's sign-in are refused, before any
    /// file is read or anything is sent: each source, `--organization` and `--replace`, and with
    /// `github`, whose setup waits for nothing, `--no-wait` and `--timeout`. The other three of
    /// the five take no `--org` (REQ-INTEGRATION-17).
    fn connect_organization(
        &self,
        deployment: &Deployment,
        provider: ProviderArg,
        reach: &Reach,
    ) -> super::super::CommandResult {
        let name = provider.grant().as_str();
        let sign_in = match provider {
            ProviderArg::Github => OrganizationSignIn::GitHub,
            ProviderArg::Linear => OrganizationSignIn::Linear,
            ProviderArg::GitlabCom | ProviderArg::ClaudeCode | ProviderArg::Codex => {
                let example = if provider.native() {
                    format!("useful integration connect {name} --organization ORGANIZATION")
                } else {
                    format!("useful integration connect {name}")
                };
                return Err(reach.org_refused(name, provider.native(), &example));
            }
        };
        let with_org = format!("{name} --org");
        let connects = format!(
            "Connect the organization at {name} with your session alone:\n  \
             useful integration connect {name} --org ORGANIZATION"
        );
        if let Some(option) = first_given([
            (self.organization.is_some(), "--organization"),
            (self.login, "--login"),
            (self.device, "--device"),
            (self.from_file.is_some(), "--from-file"),
            (self.token_file.is_some(), "--token-file"),
            (self.replace, "--replace"),
        ]) {
            let remedy = match (option, sign_in) {
                ("--replace", OrganizationSignIn::Linear) => format!(
                    "{connects}\n\nReauthorize an existing connection with:\n  \
                     useful connection linear authorization create ORGANIZATION --connection-id \
                     CONNECTION"
                ),
                _ => connects,
            };
            return Err(unsuited(option, &with_org, &remedy));
        }
        if sign_in == OrganizationSignIn::GitHub
            && let Some(option) = first_given([
                (self.no_wait, "--no-wait"),
                (self.wait.timeout.is_some(), "--timeout"),
            ])
        {
            return Err(unsuited(
                option,
                &with_org,
                &format!(
                    "The GitHub setup waits for nothing; complete it with the command its report \
                     names.\n\n{connects}"
                ),
            ));
        }
        reach.connect_organization(&self.flow(), deployment, sign_in)
    }

    /// Signs in to what the `--workflow` files call for (REQ-GRANT-76) through the flow over
    /// their derivation. The files are derived before any request, so a file that does not
    /// resolve, or one in no Git repository without `--source-root`, ends the command before any
    /// sign-in.
    fn run_workflows(&self, deployment: &Deployment) -> super::super::CommandResult {
        let roots = Roots::find(
            &self.workflows,
            self.source_root.as_deref(),
            "useful integration connect --workflow FILE --source-root ROOT",
        )?;
        let Some(derived) = roots.derive(&roots.origin_remote(), self.options.json)? else {
            return Ok(ExitCode::GeneralFailure);
        };
        // The sign-ins select by the inference, not by a declared block (REQ-GRANT-76).
        self.flow()
            .run_workflows(deployment, &derived.inferred, output::Scope::Workflows)
            .map(|ending| ending.code)
    }
}

impl Flow<'_> {
    /// Codex's own sign-in: by device code with `--device` or where this machine has no
    /// browser, and otherwise in this machine's browser (REQ-GRANT-70).
    fn codex_sign_in(&self) -> Login {
        if self.device || !browser::available(|name| std::env::var_os(name)) {
            Login::Codex(codex_login::DEVICE)
        } else {
            Login::Codex(codex_login::BROWSER)
        }
    }

    /// The flow over a derivation of workflow files, which `useful integration connect --workflow`
    /// and `useful runner doctor --workflow --connect` both run (INV-CLI-15): signs in to the
    /// providers the files call for that the member lacks (REQ-GRANT-76), and to those whose every
    /// held grant needs reconnecting once their replacement is settled (REQ-GRANT-79). A tree that
    /// calls for no provider is not taken to Cloud. Each replacement is settled once the grants are
    /// read, and the plan is printed once the selection is settled, so no sign-in precedes a
    /// question; then the selection runs as without `--workflow`, its summary naming the command of
    /// `scope` to run again. Returns whether it reached its summary and its status.
    pub(in crate::cli) fn run_workflows(
        &self,
        deployment: &Deployment,
        credentials: &WorkflowCredentials,
        scope: output::Scope,
    ) -> Result<Ending, CommandFailure> {
        let wanted: Vec<_> = credentials
            .requirements
            .iter()
            .filter_map(|requirement| ProviderArg::of_kind(requirement.kind))
            .collect();
        let mut selection = if wanted.is_empty() {
            Selection::NONE
        } else {
            match self.select(deployment, Wanted::Only(&wanted), scope, true)? {
                Ok(selection) => selection,
                Err(code) => {
                    return Ok(Ending {
                        summarized: false,
                        code,
                    });
                }
            }
        };
        for reconnect in &mut selection.reconnects {
            if let Some(held) = reconnect.asked() {
                reconnect.replace =
                    replacement::reconnects(reconnect.provider.grant(), held, self.replace)?;
            }
        }
        let planned: Vec<_> = credentials
            .requirements
            .iter()
            .filter_map(|requirement| {
                let provider = ProviderArg::of_kind(requirement.kind)?;
                Some(workflow::Planned {
                    provider: provider.grant().as_str(),
                    state: selection.planned(provider),
                    requirement,
                })
            })
            .collect();
        workflow::write_plan(
            deployment.fingerprint().api_url(),
            credentials,
            &planned,
            self.json,
        )?;
        Ok(Ending {
            summarized: true,
            code: self.run_selection(deployment, selection, scope)?,
        })
    }

    /// Selects, among `wanted` and in [`order`], the providers the member lacks: those it holds
    /// no grant of that is `active` or `reconnect_required`, for a native provider in the
    /// organization a new grant would be for (REQ-GRANT-75). The member's grants of both kinds
    /// are read once. With every provider wanted, the providers the deployment serves are read
    /// next, every time, and each other OAuth provider among them is wanted after the built-in
    /// ones; with only some of the five wanted, they are read next only where a coding agent
    /// might be signed in to (REQ-GRANT-78). A coding agent the deployment does not serve is
    /// selected to be passed over without a sign-in; a deployment that predates that read is
    /// taken to serve each one and no other OAuth provider. The organization is resolved once, and only for the native providers it serves,
    /// when `--organization` names it or one of those in `wanted` is held in no organization
    /// (REQ-GRANT-72), so a member who holds every provider wanted is asked for nothing more.
    /// When the member is an active member of none or of several and names none, each native
    /// provider held in no organization is selected to be passed over with the refusal, which
    /// names the command of `scope` with an organization, and the consents are selected as
    /// ever. With `reconnect`, a provider held, a coding agent in that organization when one
    /// was resolved and in any otherwise, every grant of which so held needs reconnecting is
    /// selected to be settled (REQ-GRANT-79), a coding agent the deployment serves only, with
    /// the grants its replacement replaces. The inner error is the exit status of a reported
    /// failure.
    fn select(
        &self,
        deployment: &Deployment,
        wanted: Wanted<'_>,
        scope: output::Scope,
        reconnect: bool,
    ) -> Result<Result<Selection, ExitCode>, CommandFailure> {
        let natives: Vec<_> = order(&[])
            .iter()
            .filter(|provider| wanted.wants(provider))
            .filter_map(Ordered::built_in)
            .filter(|provider| provider.native())
            .collect();
        let read = self.with_api(deployment, |api| {
            let grants = super::every_grant(api)?;
            // With every provider wanted, what the deployment serves names the other OAuth
            // providers too, so it is read every time, right after the grants (REQ-GRANT-75).
            let every = match wanted {
                Wanted::Every => Some(api.served_providers()?),
                Wanted::Only(_) => None,
            };
            let held_by_name = |name: &str, organization_id: Option<&str>| {
                grants
                    .iter()
                    .filter(|grant| replacement::holds(grant, name, organization_id))
                    .collect::<Vec<_>>()
            };
            let held_of = |provider: ProviderArg, organization_id: Option<&str>| {
                held_by_name(provider.grant().as_str(), organization_id)
            };
            let held = |provider: ProviderArg, organization_id: Option<&str>| {
                !held_of(provider, organization_id).is_empty()
            };
            let reconnects = |provider: ProviderArg, organization_id: Option<&str>| {
                reconnect && {
                    let held = held_of(provider, organization_id);
                    !held.is_empty()
                        && held
                            .iter()
                            .all(|grant| replacement::needs_reconnecting(grant))
                }
            };
            let unheld: Vec<_> = natives
                .iter()
                .copied()
                .filter(|provider| !held(*provider, None))
                .collect();
            let signs_in = |natives: &[ProviderArg], unheld: &[ProviderArg]| {
                !natives.is_empty() && (self.organization.is_some() || !unheld.is_empty())
            };
            let served = match every {
                Some(served) => served,
                None if signs_in(&natives, &unheld)
                    || natives.iter().any(|provider| reconnects(*provider, None)) =>
                {
                    api.served_providers()?
                }
                None => None,
            };
            let ordered: Vec<_> = order(served.as_deref().unwrap_or_default())
                .into_iter()
                .filter(|provider| wanted.wants(provider))
                .collect();
            let serves = |provider: &ProviderArg| {
                served.as_ref().is_none_or(|served| {
                    served
                        .iter()
                        .any(|served| served.name == provider.grant().as_str())
                })
            };
            let (natives, unserved): (Vec<_>, Vec<_>) = natives.iter().copied().partition(serves);
            let unheld: Vec<_> = unheld.into_iter().filter(serves).collect();
            let mut resolved = None;
            let selected_natives = if signs_in(&natives, &unheld) {
                let names = unheld
                    .iter()
                    .map(|provider| provider.grant().as_str())
                    .collect::<Vec<_>>()
                    .join(" or ");
                match organization::resolve(api, self.organization, &names, scope.organization())? {
                    Ok(organization) => {
                        resolved = Some((organization.id.clone(), organization.slug.clone()));
                        let lacked = natives
                            .iter()
                            .copied()
                            .filter(|provider| !held(*provider, Some(&organization.id)))
                            .collect::<Vec<_>>();
                        (!lacked.is_empty()).then_some(Natives::For(organization, lacked))
                    }
                    Err(refusal) => Some(Natives::Unresolved(refusal.to_string(), unheld)),
                }
            } else {
                None
            };
            let organization_id = resolved.as_ref().map(|(id, _)| id.as_str());
            let unavailable = unserved
                .into_iter()
                .filter(|provider| !held(*provider, organization_id))
                .collect();
            let consents = ordered
                .iter()
                .filter_map(Ordered::consent)
                .filter(|consent| held_by_name(consent.name, None).is_empty())
                .map(|consent| consent.name.to_owned())
                .collect();
            let mut selected = Vec::new();
            for provider in ordered.iter().filter_map(Ordered::built_in) {
                let held = match provider.consent() {
                    Some(consent) if reconnects(provider, None) => Held::Consent(
                        consent,
                        held_of(provider, None).into_iter().cloned().collect(),
                    ),
                    None if natives.contains(&provider)
                        && reconnects(provider, organization_id) =>
                    {
                        Held::native(api, &held_of(provider, organization_id), resolved.as_ref())?
                    }
                    _ => continue,
                };
                selected.push(Reconnect {
                    provider,
                    held,
                    replace: false,
                });
            }
            Ok(Selection {
                order: ordered,
                consents,
                natives: selected_natives,
                unavailable,
                reconnects: selected,
            })
        })?;
        match read {
            Ok(selection) => Ok(Ok(selection)),
            Err(failure) => {
                let api_url = deployment.fingerprint().api_url();
                output::write_create_refusal(api_url, &output::report(api_url, &failure), self.json)
                    .map(Err)
                    .map_err(Into::into)
            }
        }
    }

    /// Runs the sign-in of each selected provider in turn, naming the provider on standard
    /// error before its prompts, then prints one outcome per provider (REQ-GRANT-75). A held
    /// provider is never selected, and one whose grants need reconnecting was settled before
    /// the plan: a settled one is signed in to again, its consent run or its coding agent's
    /// sign-in stored as its grant's next generation, and reported `replaced`, and one not
    /// settled is passed over as `reconnect_required` (REQ-GRANT-79). A sign-in that fails
    /// does not stop the ones after it; a signal does, whether `useful` received it or it ended a
    /// coding agent's sign-in on its own terminal. A coding agent the deployment does not serve,
    /// or one no organization was resolved for, is reported and passed over without a sign-in.
    /// Exits 0 when each provider ended with a grant or was passed over, as unavailable, for
    /// want of an organization or as needing reconnecting, and at least one grant was made or
    /// none was to be, 1 when any sign-in failed or nothing was made, and the signal's status
    /// when one stopped the command.
    fn run_selection(
        &self,
        deployment: &Deployment,
        selection: Selection,
        scope: output::Scope,
    ) -> super::super::CommandResult {
        let mut outcomes = Vec::new();
        for step in selection
            .order
            .iter()
            .filter_map(|provider| selection.step(provider))
        {
            let name = step.provider();
            if !matches!(step, Step::Kept(_)) {
                let after = outcomes
                    .iter()
                    .any(|(_, outcome)| !matches!(outcome, Outcome::ReconnectRequired));
                output::write_provider_heading(name, after, self.json)?;
            }
            let outcome = match step {
                Step::Consent(consent) | Step::Reconsent(consent) => Outcome::of(
                    &self.consent(deployment, consent)?,
                    !self.no_wait,
                    matches!(step, Step::Reconsent(_)),
                ),
                Step::Native(provider, organization) => {
                    self.sign_in_native(deployment, provider, organization, None)?
                }
                Step::Renew(provider, grant, organization) => {
                    self.sign_in_native(deployment, provider, organization, Some(grant))?
                }
                Step::PassedOver(_, refusal) => {
                    output::write_organization_required(
                        deployment.fingerprint().api_url(),
                        refusal,
                        self.json,
                    )?;
                    Outcome::OrganizationRequired
                }
                Step::Unavailable(_) => {
                    let api_url = deployment.fingerprint().api_url();
                    output::write_import_refusal(
                        api_url,
                        &output::report(api_url, &MemberGrantFailure::ProviderUnavailable),
                        self.json,
                    )?;
                    Outcome::Unavailable
                }
                Step::Kept(_) => Outcome::ReconnectRequired,
            };
            outcomes.push((name, outcome));
            if matches!(outcome, Outcome::Stopped(_)) {
                break;
            }
        }
        let code = summary_code(outcomes.iter().map(|(_, outcome)| *outcome));
        let named: Vec<_> = outcomes
            .iter()
            .map(|(provider, outcome)| (*provider, outcome.name()))
            .collect();
        output::write_create_summary(
            deployment.fingerprint().api_url(),
            &named,
            code,
            scope,
            self.json,
        )?;
        Ok(code)
    }

    /// Runs a coding agent's own sign-in in a selection and imports its login for
    /// `organization`, or stores it as the next generation of `target` (REQ-GRANT-79).
    fn sign_in_native(
        &self,
        deployment: &Deployment,
        provider: ProviderArg,
        organization: &Organization,
        target: Option<&MemberGrant>,
    ) -> Result<Outcome, CommandFailure> {
        let login = match provider {
            ProviderArg::Codex => self.codex_sign_in(),
            _ => Login::ClaudeCode,
        };
        let imported = self.import(deployment, provider, login, organization, target)?;
        Ok(match imported.stopped {
            Some(stop) => Outcome::Stopped(stop.exit_code()),
            None => Outcome::of(&imported.ended, true, target.is_some()),
        })
    }

    /// Runs the consent of `name`, an OAuth provider with no built-in name, once the providers
    /// the deployment serves, read first, list it as one (REQ-GRANT-69): it is named by its id
    /// and titled by its served display name (REQ-GRANT-96). A name they do not list as an OAuth
    /// provider, and any such name when the deployment predates the read, is refused with exit 2
    /// before anything is begun, naming the providers that can be given.
    fn consent_served(&self, deployment: &Deployment, name: &str) -> super::super::CommandResult {
        let served = match self.with_api(deployment, MemberGrantApi::served_providers)? {
            Ok(served) => served.unwrap_or_default(),
            Err(failure) => return self.write_read_refusal(deployment, &failure),
        };
        let provider = served_oauth(name, &served, None)?;
        self.replace_by_consent(deployment, provider.consent())
    }

    /// Settles the replacement in the member's session (REQ-GRANT-73), then runs the consent.
    fn replace_by_consent(
        &self,
        deployment: &Deployment,
        consent: ConsentProvider<'_>,
    ) -> super::super::CommandResult {
        let read = self.with_api(deployment, |api| {
            replacement::held(api, consent.name, MemberGrantKind::GrantKindOAuth, None)
        })?;
        let held = match read {
            Ok(held) => held,
            Err(failure) => return self.write_read_refusal(deployment, &failure),
        };
        replacement::settle(consent.name, &held, self.replace)?;
        self.consent(deployment, consent).map(|ended| ended.code)
    }

    /// Reports a read that failed before a consent began, as the consent's result.
    fn write_read_refusal(
        &self,
        deployment: &Deployment,
        failure: &MemberGrantFailure,
    ) -> super::super::CommandResult {
        let api_url = deployment.fingerprint().api_url();
        write_refusal(api_url, None, &output::report(api_url, failure), self.json)
            .map_err(Into::into)
    }

    /// Runs the consent with no identity named, opening its URL when this machine has a
    /// browser (REQ-GRANT-69).
    fn consent(
        &self,
        deployment: &Deployment,
        consent: ConsentProvider<'_>,
    ) -> Result<Ended, CommandFailure> {
        Consent {
            front: Front::GRANT,
            provider: consent,
            identity_id: None,
            open: browser::available(|name| std::env::var_os(name)),
            no_wait: self.no_wait,
            timeout: self.timeout,
            options: Options::new(
                self.json,
                NoAuthenticationArgs::default(),
                HttpOptions {
                    allow_insecure_http: self.allow_insecure_http,
                },
            ),
        }
        .run_to_end(deployment)
    }

    /// Imports a coding agent's login for `organization` (REQ-GRANT-70 to REQ-GRANT-72), or
    /// stores it as the next generation of the grant `target` held there (REQ-GRANT-73). The
    /// session, the organization and the replacement are settled before this is called, so a
    /// refusal leaves no provider session behind. A sign-in a signal ended is reported as one
    /// that failed, with the signal's stop besides.
    fn import(
        &self,
        deployment: &Deployment,
        provider: ProviderArg,
        login: Login,
        organization: &Organization,
        target: Option<&MemberGrant>,
    ) -> Result<Imported, CommandFailure> {
        let api_url = deployment.fingerprint().api_url();
        let json = self.json;
        let ended = |code| Imported {
            ended: Ended {
                code,
                unavailable: false,
            },
            stopped: None,
        };
        let signed_in = match login {
            Login::Read(material) => Ok(material),
            Login::Codex(arguments) => codex_login::sign_in(arguments, material::read_codex_login)?
                .map_err(|failure| (failure.report(), failure.stop())),
            Login::ClaudeCode => {
                claude_setup_token::sign_in()?.map_err(|failure| (failure.report(), failure.stop()))
            }
        };
        let material = match signed_in {
            Ok(material) => material,
            Err((report, stopped)) => {
                return Ok(Imported {
                    stopped,
                    ..ended(output::write_import_refusal(api_url, &report, json)?)
                });
            }
        };
        let provider = provider.grant();
        let key =
            useful_support::generate_idempotency_key().context("generate grant request key")?;
        let stored = self.with_api(deployment, |api| {
            replayed(|| match target {
                Some(grant) => api.replace(&grant.id, &material, &key),
                None => api.import(&organization.id, provider, &material, &key),
            })
        })?;
        match stored {
            Ok(grant) => Ok(ended(output::write_imported(
                api_url,
                &grant,
                organization.slug.as_deref(),
                target.is_some(),
                json,
            )?)),
            Err(failure) => Ok(Imported {
                ended: Ended {
                    code: output::write_import_refusal(
                        api_url,
                        &output::report(api_url, &failure),
                        json,
                    )?,
                    unavailable: matches!(failure, MemberGrantFailure::ProviderUnavailable),
                },
                stopped: None,
            }),
        }
    }

    /// Establishes the session, resolves the organization and settles which grant held there,
    /// if any, the login replaces, returning the exit status of a reported refusal instead.
    /// Before a sign-in, `signs_in`, the providers the deployment serves are read first, and one
    /// it does not serve is refused as unavailable before anything else is asked or run
    /// (REQ-GRANT-75, REQ-GRANT-78); a deployment that predates that read is taken to serve it.
    fn settle(
        &self,
        deployment: &Deployment,
        provider: ProviderArg,
        signs_in: bool,
    ) -> Result<Result<(Organization, Option<MemberGrant>), ExitCode>, CommandFailure> {
        let provider = provider.grant();
        let name = provider.as_str();
        let read = self.with_api(deployment, |api| {
            if signs_in
                && api
                    .served_providers()?
                    .is_some_and(|served| !served.iter().any(|served| served.name == name))
            {
                return Err(MemberGrantFailure::ProviderUnavailable);
            }
            let organization = match organization::resolve(
                api,
                self.organization,
                name,
                &format!("useful integration connect {name} --organization ORGANIZATION"),
            )? {
                Ok(organization) => organization,
                Err(refusal) => return Ok(Err(refusal)),
            };
            let held = replacement::held(api, name, provider.kind(), Some(&organization.id))?;
            Ok(Ok((organization, held)))
        })?;
        match read {
            Ok(Ok((organization, held))) => {
                let target = replacement::native_target(provider, held, self.replace)?;
                Ok(Ok((organization, target)))
            }
            Ok(Err(refusal)) => Err(CommandFailure::with_exit_code(
                refusal,
                ExitCode::UsageError,
            )),
            Err(failure) => {
                let api_url = deployment.fingerprint().api_url();
                let report = output::report(api_url, &failure);
                output::write_import_refusal(api_url, &report, self.json)
                    .map(Err)
                    .map_err(Into::into)
            }
        }
    }

    /// Runs `f` against the grants resource with the signed-in human session, refreshing a
    /// rejected session once. A token service's token is stored through it too.
    pub(in crate::cli) fn with_api<T>(
        &self,
        deployment: &Deployment,
        f: impl FnMut(&MemberGrantApi) -> Result<T, MemberGrantFailure>,
    ) -> anyhow::Result<Result<T, MemberGrantFailure>> {
        let http = HttpOptions {
            allow_insecure_http: self.allow_insecure_http,
        };
        super::super::with_human_session_api(deployment, http.transport_policy(), f)
    }
}
