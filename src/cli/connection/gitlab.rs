use std::fs::{self, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

use anyhow::{Context, anyhow, bail};
use clap::{Args, Subcommand, ValueEnum};
use serde_json::{Value, json};
use um_api::{
    GitLabAccountUse, GitLabApi, GitLabFailure, GitLabSubmission, GitLabTokenKind, HttpClient,
};
use um_human_auth::Deployment;
use zeroize::Zeroizing;

use super::NAME;
type Options =
    super::super::CommonArgs<super::super::GitlabJson, super::super::PrincipalAuthenticationArgs>;
use crate::exit_code::ExitCode;

const GUIDE: &str = "https://docs.usefulmachinery.com/agent/gitlab-setup.md";
const MAX_TOKEN: u64 = 16 * 1024;

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(subcommand)]
    command: Option<Leaf>,
}
#[derive(Debug, Subcommand)]
enum Leaf {
    #[command(about = "Create a GitLab.com connection", after_help = GUIDE)]
    Create(Create),
    #[command(about = "Manage GitLab credential candidates", after_help = GUIDE)]
    Credential(Credential),
    #[command(about = "List GitLab connections", after_help = GUIDE)]
    List(List),
    #[command(about = "Remove a GitLab connection", after_help = GUIDE)]
    Remove(Remove),
    #[command(about = "Show a GitLab connection", after_help = GUIDE)]
    Show(Target),
}
#[derive(Debug, Args)]
struct Credential {
    #[command(subcommand)]
    command: Option<CredentialLeaf>,
}
#[derive(Debug, Subcommand)]
enum CredentialLeaf {
    #[command(about = "Activate a GitLab credential candidate", after_help = GUIDE)]
    Activate(Activate),
    #[command(about = "Cancel a credential candidate", after_help = GUIDE)]
    Cancel(Cancel),
    #[command(about = "Create a GitLab credential candidate", after_help = GUIDE)]
    Create(Submit),
    #[command(about = "Show a GitLab credential candidate", after_help = GUIDE)]
    Show(CandidateTarget),
}
#[derive(Debug, Args)]
struct Base {
    #[arg(value_name = "ORGANIZATION", help = super::super::OrganizationArg::HELP)]
    organization: super::super::OrganizationArg,
    #[command(flatten)]
    options: Options,
}
#[derive(Debug, Args)]
struct Target {
    #[command(flatten)]
    base: Base,
    #[arg(value_name = "CONNECTION")]
    connection: String,
}
#[derive(Debug, Args)]
struct CandidateTarget {
    #[command(flatten)]
    target: Target,
    #[arg(value_name = "CANDIDATE")]
    candidate: String,
}
#[derive(Debug, Args)]
struct Key {
    #[arg(
        long,
        value_name = "KEY",
        help = "Request key for replaying an uncertain result (generated when omitted)"
    )]
    idempotency_key: Option<String>,
}
impl Key {
    fn value(&self) -> anyhow::Result<String> {
        let value = match &self.idempotency_key {
            Some(key) => key.clone(),
            None => um_support::generate_idempotency_key().context("generate request key")?,
        };
        if value.is_empty()
            || value.len() > 255
            || !value.bytes().all(|b| (b'!'..=b'~').contains(&b))
        {
            bail!("invalid idempotency key: use 1–255 visible ASCII characters without whitespace");
        }
        Ok(value)
    }
}
#[derive(Debug, Args)]
struct ExpectedVersion {
    #[arg(
        long,
        value_name = "VERSION",
        help = "Connection version shown by connection gitlab show (must be current)"
    )]
    expected_version: i64,
}
#[derive(Debug, Args)]
struct Attribution {
    #[arg(
        long,
        value_name = "PRINCIPAL",
        help = "Human delegator for service-action attribution (must be active)"
    )]
    delegator_principal_id: Option<String>,
}
#[derive(Debug, Args)]
struct Create {
    #[command(flatten)]
    base: Base,
    #[arg(
        value_name = "INSTANCE_URL",
        help = "GitLab.com URL (see the setup guide)"
    )]
    instance: String,
    #[command(flatten)]
    attribution: Attribution,
    #[command(flatten)]
    key: Key,
}
#[derive(Debug, Args)]
struct List {
    #[command(flatten)]
    base: Base,
    #[command(flatten)]
    page: super::super::PaginationArgs<100>,
}
#[derive(Debug, Args)]
struct Remove {
    #[command(flatten)]
    target: Target,
    #[command(flatten)]
    precondition: ExpectedVersion,
    #[command(flatten)]
    confirmation: super::super::ConfirmationArgs,
    #[command(flatten)]
    attribution: Attribution,
    #[command(flatten)]
    key: Key,
}
#[derive(Clone, Copy, Debug, ValueEnum)]
enum CredentialKind {
    GroupAccessToken,
    ServiceAccount,
    DedicatedUser,
}
#[derive(Debug, Args)]
struct Submit {
    #[command(flatten)]
    target: Target,
    #[command(flatten)]
    precondition: ExpectedVersion,
    #[arg(long, value_enum, help = "Credential source profile (see setup guide)")]
    kind: CredentialKind,
    #[arg(
        long,
        value_name = "PATH",
        help = "Read the token from an owned mode-0600 file, or - for standard input"
    )]
    token_file: PathBuf,
    #[command(flatten)]
    attribution: Attribution,
    #[command(flatten)]
    key: Key,
}
#[derive(Debug, Args)]
struct Activate {
    #[command(flatten)]
    target: CandidateTarget,
    #[arg(
        long,
        requires = "dedicated_automation",
        help = "Provider user ID reviewed for noninteractive activation"
    )]
    expected_provider_user_id: Option<String>,
    #[arg(
        long,
        requires = "expected_provider_user_id",
        help = "Attest this is a dedicated automation identity"
    )]
    dedicated_automation: bool,
    #[command(flatten)]
    key: Key,
}
#[derive(Debug, Args)]
struct Cancel {
    #[command(flatten)]
    target: CandidateTarget,
    #[command(flatten)]
    key: Key,
}

impl Command {
    pub(super) fn execute(self) -> super::super::CommandResult {
        let path = &[NAME, "gitlab"];
        match self.command {
            None => super::super::print_help(path),
            Some(Leaf::Credential(cmd)) => match cmd.command {
                None => super::super::print_help(&[NAME, "gitlab", "credential"]),
                Some(CredentialLeaf::Create(cmd)) => cmd.execute(),
                Some(CredentialLeaf::Show(cmd)) => cmd.read_candidate(),
                Some(CredentialLeaf::Activate(cmd)) => cmd.execute(),
                Some(CredentialLeaf::Cancel(cmd)) => cmd.execute(),
            },
            Some(Leaf::Create(cmd)) => cmd.execute(),
            Some(Leaf::List(cmd)) => cmd.execute(),
            Some(Leaf::Show(cmd)) => cmd.read_connection(),
            Some(Leaf::Remove(cmd)) => cmd.execute(),
        }
    }
}
fn deployment<T>(
    command: T,
    run: impl FnOnce(T, Deployment) -> super::super::CommandResult,
) -> super::super::CommandResult {
    super::super::execute_deployment_command(
        Some(command),
        &[NAME, "gitlab"],
        "configure GitLab access",
        |command, d| run(command, d.clone()),
    )
}
fn with_api<T>(
    d: &Deployment,
    base: &Base,
    mut call: impl FnMut(&GitLabApi) -> Result<T, GitLabFailure>,
) -> anyhow::Result<Result<T, GitLabFailure>> {
    let policy = base.options.http.transport_policy();
    let client = HttpClient::new(policy)
        .map_err(|e| anyhow!(e))
        .context("prepare human session networking")?;
    super::super::execute_selected_api_operation(
        super::super::principal_api_context(
            &client,
            d,
            &base.options.authentication,
            "acquire human session",
        ),
        |token| {
            let api =
                GitLabApi::new(d.fingerprint().api_url(), token, policy).map_err(|e| anyhow!(e))?;
            Ok(call(&api))
        },
        GitLabFailure::credential_rejected,
        || GitLabFailure::Unauthenticated,
        GitLabFailure::Unreachable,
    )
}
fn usage(error: anyhow::Error) -> super::super::CommandFailure {
    super::super::CommandFailure::with_exit_code(error, ExitCode::UsageError)
}
fn version(value: i64) -> anyhow::Result<i64> {
    if !(1..=9_007_199_254_740_991).contains(&value) {
        bail!(
            "invalid expected version: use the positive safe JSON integer from connection gitlab show"
        );
    }
    Ok(value)
}
fn result(
    d: &Deployment,
    base: &Base,
    outcome: &str,
    value: Result<Value, GitLabFailure>,
    key: Option<&str>,
) -> super::super::CommandResult {
    match value {
        Ok(value) => {
            let write = || -> anyhow::Result<()> {
                if base.options.json {
                    serde_json::to_writer(
                        io::stdout().lock(),
                        &json!({"schemaVersion":1,"deployment":d.fingerprint().api_url(),"organizationRef":base.organization.to_string(),"outcome":outcome,"result":value,"idempotencyKey":key}),
                    )?;
                    println!();
                } else {
                    let mut out = io::stdout().lock();
                    let kind = if value.get("connectionId").is_some() {
                        "GitLab credential candidate"
                    } else {
                        "GitLab connection"
                    };
                    writeln!(out, "{kind}: {outcome}")?;
                    if let Some(items) = value.get("items").and_then(Value::as_array) {
                        for item in items {
                            writeln!(
                                out,
                                "{}  {}  {}  {}",
                                item["id"].as_str().unwrap_or("?"),
                                item["state"].as_str().unwrap_or("?"),
                                item["instanceUrl"].as_str().unwrap_or("?"),
                                item["credential"]["expiryNotice"].as_str().unwrap_or("-")
                            )?;
                        }
                        if let Some(cursor) = value["nextCursor"].as_str() {
                            writeln!(out, "next cursor: {cursor}")?;
                        }
                    } else {
                        for (field, label) in [
                            ("id", "id"),
                            ("connectionId", "connection"),
                            ("instanceUrl", "instance"),
                            ("state", "state"),
                            ("version", "version"),
                            ("providerUserId", "provider user id"),
                            ("providerUsername", "provider username"),
                            ("previousProviderUserId", "previous provider user id"),
                            ("previousProviderUsername", "previous provider username"),
                            ("kind", "kind"),
                            ("accountUse", "account use"),
                            ("permissionProfile", "permission profile"),
                            ("tokenExpiresAt", "token expires"),
                            ("failureCode", "failure code"),
                            ("expiresAt", "review expires"),
                        ] {
                            if let Some(v) = value.get(field).filter(|v| !v.is_null()) {
                                writeln!(
                                    out,
                                    "{}: {}",
                                    label,
                                    v.as_str()
                                        .map(str::to_owned)
                                        .unwrap_or_else(|| v.to_string())
                                )?;
                            }
                        }
                    }
                    if let Some(user) = value.get("providerUser").filter(|v| !v.is_null()) {
                        if let Some(id) = user["id"].as_str() {
                            writeln!(out, "provider user id: {id}")?;
                        }
                        if let Some(username) = user["username"].as_str() {
                            writeln!(out, "provider username: {username}")?;
                        }
                    }
                    if let Some(credential) = value.get("credential").filter(|v| !v.is_null()) {
                        for (field, label) in [
                            ("kind", "kind"),
                            ("accountUse", "account use"),
                            ("permissionProfile", "permission profile"),
                            ("expiresAt", "expires"),
                            ("expiryNotice", "expiry notice"),
                        ] {
                            if let Some(v) = credential.get(field).filter(|v| !v.is_null()) {
                                writeln!(
                                    out,
                                    "credential {label}: {}",
                                    v.as_str().unwrap_or("unknown")
                                )?;
                            }
                        }
                    }
                    writeln!(out, "deployment: {}", d.fingerprint().api_url())?;
                    if let Some(key) = key {
                        writeln!(out, "request key: {key}")?;
                    }
                }
                Ok(())
            };
            write()?;
            Ok(ExitCode::Success)
        }
        Err(failure) => {
            let (name, remedy, exit) = match failure {
                GitLabFailure::Unauthenticated => (
                    "authentication required",
                    "Sign in again or supply a current service API key.",
                    ExitCode::AuthenticationRequired,
                ),
                GitLabFailure::Forbidden => (
                    "access denied",
                    "Ask an organization owner for access.",
                    ExitCode::GeneralFailure,
                ),
                GitLabFailure::Conflict => (
                    "connection conflict",
                    "Show the current connection or candidate before retrying with a new request key.",
                    ExitCode::Conflict,
                ),
                GitLabFailure::CandidateUnavailable => (
                    "candidate unavailable",
                    "Show the candidate after validation completes; expired or failed candidates cannot be activated.",
                    ExitCode::CandidateUnavailable,
                ),
                GitLabFailure::IdentityMismatch => (
                    "identity mismatch",
                    "Review the verified provider user and provide its exact ID.",
                    ExitCode::IdentityMismatch,
                ),
                GitLabFailure::Unreachable(_) => (
                    "gitlab connection API unavailable",
                    "Show the resource before replaying a mutation with the same request key.",
                    ExitCode::Unavailable,
                ),
                GitLabFailure::InvalidInput => (
                    "invalid gitlab connection request",
                    "Review the setup guide and correct the request.",
                    ExitCode::UsageError,
                ),
                GitLabFailure::NotFound => (
                    "gitlab connection or candidate not found",
                    "Check the organization and resource IDs.",
                    ExitCode::GeneralFailure,
                ),
                GitLabFailure::Protocol => (
                    "invalid gitlab connection response",
                    "Check the deployment and try again later.",
                    ExitCode::GeneralFailure,
                ),
            };
            if let Some(key) = key {
                writeln!(
                    io::stderr().lock(),
                    "error: {name}\nrequest key: {key}\n\n{remedy}\n{GUIDE}"
                )
                .map_err(anyhow::Error::from)?;
            } else {
                writeln!(io::stderr().lock(), "error: {name}\n\n{remedy}\n{GUIDE}")
                    .map_err(anyhow::Error::from)?;
            }
            Ok(exit)
        }
    }
}
fn mutation(
    d: Deployment,
    base: Base,
    key: String,
    outcome: &'static str,
    mut call: impl FnMut(&GitLabApi, &str) -> Result<Value, GitLabFailure> + Send + 'static,
) -> super::super::CommandResult {
    let stopped_key = key.clone();
    super::super::execute_mutation_with_signals(
        "GitLab connection mutation",
        (),
        move |control| {
            if !control.begin_dispatch() {
                return Ok(ExitCode::Interrupted);
            }
            let value = with_api(&d, &base, |api| {
                let first = call(api, &key);
                if matches!(first, Err(GitLabFailure::Unreachable(_))) {
                    call(api, &key)
                } else {
                    first
                }
            })?;
            super::super::complete_operation(control, || {
                result(&d, &base, outcome, value, Some(&key))
            })
        },
        move |signal, snapshot| {
            if snapshot.dispatched {
                writeln!(io::stderr().lock(),
                    "error: gitlab request interrupted after dispatch\nrequest key: {stopped_key}\n\nShow the resource before replaying this request with the same key."
                ).map_err(anyhow::Error::from)?;
            }
            Ok(signal)
        },
    )
}
impl Create {
    fn execute(self) -> super::super::CommandResult {
        let key = self.key.value().map_err(usage)?;
        deployment(self, move |cmd, d| {
            let org = cmd.base.organization.to_string();
            let instance = cmd.instance;
            let delegator = cmd.attribution.delegator_principal_id;
            mutation(d, cmd.base, key, "created", move |api, key| {
                api.create(&org, key, &instance, delegator.as_deref())
            })
        })
    }
}
impl List {
    fn execute(self) -> super::super::CommandResult {
        deployment(self, |cmd, d| {
            let org = cmd.base.organization.to_string();
            let value = with_api(&d, &cmd.base, |api| {
                api.list(
                    &org,
                    cmd.page.limit.map(i32::from),
                    cmd.page.cursor.as_deref(),
                )
            })?;
            result(&d, &cmd.base, "listed", value, None)
        })
    }
}
impl Target {
    fn read_connection(self) -> super::super::CommandResult {
        deployment(self, |cmd, d| {
            let value = with_api(&d, &cmd.base, |api| {
                api.show(&cmd.base.organization, &cmd.connection)
            })?;
            result(&d, &cmd.base, "shown", value, None)
        })
    }
}
impl Remove {
    fn execute(self) -> super::super::CommandResult {
        let v = version(self.precondition.expected_version).map_err(usage)?;
        let key = self.key.value().map_err(usage)?;
        deployment(self, move |cmd, d| {
            let org = cmd.target.base.organization.to_string();
            let id = cmd.target.connection;
            let delegator = cmd.attribution.delegator_principal_id;
            mutation(d, cmd.target.base, key, "disconnected", move |api, key| {
                api.remove(&org, &id, key, v, delegator.as_deref())
            })
        })
    }
}
impl Submit {
    fn execute(self) -> super::super::CommandResult {
        let v = version(self.precondition.expected_version).map_err(usage)?;
        let key = self.key.value().map_err(usage)?;
        if self.token_file == Path::new("-") && self.target.base.options.authentication.uses_stdin()
        {
            return Err(usage(anyhow!(
                "standard input cannot contain both a service API key and a GitLab token"
            )));
        }
        let token = read_token(&self.token_file).map_err(usage)?;
        deployment(self, move |cmd, d| {
            let org = cmd.target.base.organization.to_string();
            let id = cmd.target.connection;
            let delegator = cmd.attribution.delegator_principal_id;
            let (kind, account) = match cmd.kind {
                CredentialKind::GroupAccessToken => (GitLabTokenKind::GroupAccessToken, None),
                CredentialKind::ServiceAccount => (
                    GitLabTokenKind::PersonalAccessToken,
                    Some(GitLabAccountUse::ServiceAccount),
                ),
                CredentialKind::DedicatedUser => (
                    GitLabTokenKind::PersonalAccessToken,
                    Some(GitLabAccountUse::DedicatedUser),
                ),
            };
            mutation(d, cmd.target.base, key, "submitted", move |api, key| {
                api.submit(
                    &org,
                    &id,
                    key,
                    GitLabSubmission {
                        version: v,
                        kind,
                        account_use: account,
                        token: &token,
                        delegator: delegator.as_deref(),
                    },
                )
            })
        })
    }
}
impl CandidateTarget {
    fn read_candidate(self) -> super::super::CommandResult {
        deployment(self, |cmd, d| {
            let value = with_api(&d, &cmd.target.base, |api| {
                api.candidate(
                    &cmd.target.base.organization,
                    &cmd.target.connection,
                    &cmd.candidate,
                )
            })?;
            result(&d, &cmd.target.base, "shown", value, None)
        })
    }
}
impl Activate {
    fn execute(self) -> super::super::CommandResult {
        let key = self.key.value().map_err(usage)?;
        if self.target.target.base.options.json && self.expected_provider_user_id.is_none() {
            return Err(usage(anyhow!(
                "JSON activation requires --expected-provider-user-id and --dedicated-automation"
            )));
        }
        deployment(self, move |cmd, d| {
            let org = cmd.target.target.base.organization.to_string();
            let id = cmd.target.target.connection;
            let candidate = cmd.target.candidate;
            // Only an explicitly keyed, fully attested replay can proceed when a completed
            // activation's candidate metadata is terminal or has been cleaned up. The
            // server resolves the exact key/body against its retained activation receipt.
            let explicit_replay = cmd.key.idempotency_key.is_some()
                && cmd
                    .expected_provider_user_id
                    .as_ref()
                    .is_some_and(|id| !id.is_empty())
                && cmd.dedicated_automation;
            let shown = with_api(&d, &cmd.target.target.base, |api| {
                api.candidate(&org, &id, &candidate)
            })?;
            let Some(identity) = (match shown {
                Ok(shown) if shown["state"] == "verified" => {
                    let Some(user) = shown["providerUserId"].as_str().filter(|s| !s.is_empty())
                    else {
                        return result(
                            &d,
                            &cmd.target.target.base,
                            "reviewed",
                            Err(GitLabFailure::CandidateUnavailable),
                            None,
                        );
                    };
                    // New activations always review the verified identity; the flags
                    // attest a separate, explicitly reviewed automation identity.
                    let identity = if let Some(expected) = cmd.expected_provider_user_id {
                        expected
                    } else {
                        review_identity(&shown).map_err(usage)?
                    };
                    if identity != user {
                        return result(
                            &d,
                            &cmd.target.target.base,
                            "reviewed",
                            Err(GitLabFailure::IdentityMismatch),
                            None,
                        );
                    }
                    Some(identity)
                }
                Ok(shown) if shown["state"] == "activated" && explicit_replay => {
                    cmd.expected_provider_user_id
                }
                Err(GitLabFailure::NotFound) if explicit_replay => cmd.expected_provider_user_id,
                Ok(_) | Err(GitLabFailure::NotFound) => {
                    return result(
                        &d,
                        &cmd.target.target.base,
                        "reviewed",
                        Err(GitLabFailure::CandidateUnavailable),
                        None,
                    );
                }
                Err(failure) => {
                    return result(&d, &cmd.target.target.base, "reviewed", Err(failure), None);
                }
            }) else {
                return result(
                    &d,
                    &cmd.target.target.base,
                    "reviewed",
                    Err(GitLabFailure::CandidateUnavailable),
                    None,
                );
            };
            mutation(
                d,
                cmd.target.target.base,
                key,
                "activated",
                move |api, key| {
                    api.activate(&org, &id, &candidate, key, &identity)
                        .map_err(|failure| {
                            if failure == GitLabFailure::NotFound {
                                GitLabFailure::CandidateUnavailable
                            } else {
                                failure
                            }
                        })
                },
            )
        })
    }
}
impl Cancel {
    fn execute(self) -> super::super::CommandResult {
        let key = self.key.value().map_err(usage)?;
        deployment(self, move |cmd, d| {
            let org = cmd.target.target.base.organization.to_string();
            let id = cmd.target.target.connection;
            let candidate = cmd.target.candidate;
            mutation(
                d,
                cmd.target.target.base,
                key,
                "cancellation acknowledged",
                move |api, key| api.cancel(&org, &id, &candidate, key),
            )
        })
    }
}

fn review_identity(shown: &Value) -> anyhow::Result<String> {
    let user = shown["providerUserId"]
        .as_str()
        .ok_or_else(|| anyhow!("verified provider identity is unavailable"))?;
    let mut tty = OpenOptions::new().read(true).write(true).open("/dev/tty").context("interactive identity review requires a terminal; otherwise supply --expected-provider-user-id and --dedicated-automation")?;
    writeln!(
        tty,
        "GitLab identity: {} ({user})",
        shown["providerUsername"].as_str().unwrap_or("unknown")
    )?;
    writeln!(
        tty,
        "Credential: {} / {}",
        shown["kind"].as_str().unwrap_or("unknown"),
        shown["accountUse"].as_str().unwrap_or("not applicable")
    )?;
    writeln!(
        tty,
        "Permission profile: {}; expires: {}",
        shown["permissionProfile"].as_str().unwrap_or("unknown"),
        shown["tokenExpiresAt"].as_str().unwrap_or("unknown")
    )?;
    writeln!(
        tty,
        "Previous identity: {} ({})",
        shown["previousProviderUsername"].as_str().unwrap_or("none"),
        shown["previousProviderUserId"].as_str().unwrap_or("none")
    )?;
    write!(tty, "Enter the verified provider user ID: ")?;
    tty.flush()?;
    let mut reader = BufReader::new(tty.try_clone()?.take(512));
    let mut input = String::new();
    reader.read_line(&mut input)?;
    if input.trim_end_matches(['\r', '\n']) != user {
        return Ok(input.trim_end_matches(['\r', '\n']).to_owned());
    }
    write!(
        tty,
        "Type 'dedicated automation' to attest this identity is reserved for automation: "
    )?;
    tty.flush()?;
    let mut attestation = String::new();
    reader.read_line(&mut attestation)?;
    if attestation.trim_end_matches(['\r', '\n']) != "dedicated automation" {
        bail!("activation requires an explicit dedicated automation attestation");
    }
    Ok(user.to_owned())
}

fn validate_token_file(metadata: &fs::Metadata, uid: u32) -> anyhow::Result<()> {
    if !metadata.file_type().is_file() || metadata.uid() != uid || metadata.mode() & 0o7777 != 0o600
    {
        bail!("GitLab token file must be a regular non-symlink file owned by you with mode 0600");
    }
    if metadata.len() > MAX_TOKEN + 2 {
        bail!("GitLab token file exceeds 16 KiB");
    }
    Ok(())
}

fn read_token(path: &Path) -> anyhow::Result<Zeroizing<String>> {
    let mut bytes = Zeroizing::new(Vec::new());
    if path == Path::new("-") {
        io::stdin()
            .lock()
            .take(MAX_TOKEN + 3)
            .read_to_end(&mut bytes)
            .context("read GitLab token from standard input")?;
    } else {
        let uid = rustix::process::geteuid().as_raw();
        validate_token_file(
            &fs::symlink_metadata(path).context("inspect GitLab token file")?,
            uid,
        )?;
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(path)
            .context("open GitLab token file")?;
        validate_token_file(
            &file
                .metadata()
                .context("inspect opened GitLab token file")?,
            uid,
        )?;
        file.take(MAX_TOKEN + 3)
            .read_to_end(&mut bytes)
            .context("read GitLab token file")?;
    }
    if bytes.len() as u64 > MAX_TOKEN + 2 {
        bail!("GitLab token input exceeds 16 KiB");
    }
    if bytes.last() == Some(&b'\n') {
        bytes.pop();
        if bytes.last() == Some(&b'\r') {
            bytes.pop();
        }
    }
    if bytes.is_empty() || bytes.len() as u64 > MAX_TOKEN {
        bail!("GitLab token must contain 1–16384 UTF-8 bytes");
    }
    let token = std::str::from_utf8(&bytes).context("GitLab token must be UTF-8")?;
    if token.chars().any(char::is_control) {
        bail!("GitLab token cannot contain control characters");
    }
    Ok(Zeroizing::new(token.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn private_token_file_rejects_a_different_owner() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("token");
        fs::write(&path, b"fixture")?;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
        let metadata = fs::symlink_metadata(path)?;
        assert!(validate_token_file(&metadata, metadata.uid()).is_ok());
        assert!(validate_token_file(&metadata, metadata.uid().wrapping_add(1)).is_err());
        Ok(())
    }
}
