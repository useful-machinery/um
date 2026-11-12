//! The Cloud reads of `um runner doctor --readiness` (REQ-CREDREADY-13 to REQ-CREDREADY-17,
//! REQ-CREDREADY-21). They live in the command because `um-runner` reaches neither the API nor
//! the human session. One `Session` serves every read of an invocation; a connection family
//! supplies its API, its failure as a `FamilyFailure` and a pure function from what it lists to
//! the requirement's code, and `read_for` names the read of each kind. A named project's
//! repository is read first, since it names a kind, and `Repository::code` combines it with
//! that kind's read.

use std::collections::BTreeSet;
use std::fmt::{Debug, Display};

use anyhow::{Context, anyhow};
use serde_json::Value;
use um_api::{
    GitHubApi, GitHubFailure, GitHubInstallationState, GitLabApi, GitLabFailure, HttpClient,
    HttpTransportPolicy, LinearApi, LinearConnection, LinearFailure, ProjectApi, ProjectFailure,
    UnreachableCategory, linear_connection,
};
use um_human_auth::Deployment;
use um_runner::{CredentialKind, ReadinessCode};

use crate::exit_code::{ExitCode, OutcomeClass};

use super::super::super::{
    PrincipalAuthenticationArgs, PrincipalAuthenticationKind, execute_selected_api_operation,
    principal_api_context, unreachable_outcome_class, write_api_failure,
};

/// The largest page a connection list returns.
const PAGE_LIMIT: i32 = 100;

/// What `--readiness` asks: the organization, the project whose repository names the
/// repository's provider when one is named, and how to reach and authenticate to Cloud.
pub(super) struct Readiness<'a> {
    pub(super) organization: &'a str,
    pub(super) project: Option<&'a str>,
    pub(super) authentication: &'a PrincipalAuthenticationArgs,
    pub(super) policy: HttpTransportPolicy,
}

/// A read of one kind: the code Cloud's answer gives its requirement, or the failure that ends
/// the command.
pub(super) type Read = fn(&Session<'_>) -> anyhow::Result<Result<ReadinessCode, Ending>>;

/// The read `--readiness` makes for `kind`, if any; the deployment is loaded only when some
/// requirement has one.
pub(super) const fn read_for(kind: CredentialKind) -> Option<Read> {
    match kind {
        CredentialKind::Github => Some(read_github),
        CredentialKind::Gitlab => Some(read_gitlab),
        CredentialKind::Linear => Some(read_linear),
        CredentialKind::ClaudeCode | CredentialKind::Codex => None,
    }
}

/// One deployment's client and the caller's credential, shared by every read: the service API
/// key when one is given, else the stored human session, never a prompt.
pub(super) struct Session<'a> {
    client: HttpClient,
    deployment: &'a Deployment,
    readiness: &'a Readiness<'a>,
}

impl<'a> Session<'a> {
    pub(super) fn new(
        deployment: &'a Deployment,
        readiness: &'a Readiness<'a>,
    ) -> anyhow::Result<Self> {
        let client = HttpClient::new(readiness.policy)
            .map_err(|error| anyhow!(error))
            .context("prepare human session networking")?;
        Ok(Self {
            client,
            deployment,
            readiness,
        })
    }

    /// Asks the organization with `operation` through the family's API, which `api` builds and
    /// `networking` names when it cannot be built, and returns Cloud's answer as it is.
    fn call<A, N, T, E: FamilyFailure>(
        &self,
        api: impl Fn(&str, &str, HttpTransportPolicy) -> Result<A, N>,
        networking: &'static str,
        mut operation: impl FnMut(&A, &str) -> Result<T, E>,
    ) -> anyhow::Result<Result<T, E>>
    where
        N: Display + Debug + Send + Sync + 'static,
    {
        let api_url = self.deployment.fingerprint().api_url();
        let organization = self.readiness.organization;
        execute_selected_api_operation(
            principal_api_context(
                &self.client,
                self.deployment,
                self.readiness.authentication,
                "acquire human session",
            ),
            |token| {
                let api = api(api_url, token, self.readiness.policy)
                    .map_err(|error| anyhow!(error))
                    .context(networking)?;
                Ok(operation(&api, organization))
            },
            E::credential_rejected,
            E::unauthenticated,
            E::unreachable,
        )
    }

    /// Lists with `list` as `call` does and maps the answer with `code`. A read Cloud refuses
    /// the caller gives `not_permitted` (REQ-CREDREADY-14); any other failure ends the command.
    fn read<A, N, T, E: FamilyFailure>(
        &self,
        api: impl Fn(&str, &str, HttpTransportPolicy) -> Result<A, N>,
        networking: &'static str,
        list: impl FnMut(&A, &str) -> Result<T, E>,
        code: impl FnOnce(T) -> ReadinessCode,
    ) -> anyhow::Result<Result<ReadinessCode, Ending>>
    where
        N: Display + Debug + Send + Sync + 'static,
    {
        Ok(match self.call(api, networking, list)? {
            Ok(listed) => Ok(code(listed)),
            Err(failure) => failure
                .ending()
                .map_or(Ok(ReadinessCode::NotPermitted), Err),
        })
    }

    /// The repository of `project`, or `None` when it has none (REQ-CREDREADY-21). Without
    /// the project the repository's provider is not known, so every failure ends the command,
    /// a refused read included.
    pub(super) fn read_project(
        &self,
        project: &str,
    ) -> anyhow::Result<Result<Option<Repository>, Ending>> {
        let read = self.call(
            ProjectApi::new,
            "prepare project management networking",
            |api, organization| api.get(organization, project),
        )?;
        let repository = match read {
            Ok(read) => read.repository,
            // Every project failure has an ending; a refused read is the project's.
            Err(failure) => return Ok(Err(failure.ending().unwrap_or(Ending::ProjectForbidden))),
        };
        let Some(repository) = repository else {
            return Ok(Ok(None));
        };
        let document =
            serde_json::to_value(&*repository).context("read the project's repository fields")?;
        Ok(Repository::from_document(&document).map(Some))
    }
}

/// What a named project shows of its repository (REQ-CREDREADY-21).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Repository {
    pub(super) kind: CredentialKind,
    availability: Availability,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Availability {
    Active,
    Unavailable,
    /// Unavailable because the deployment's GitLab admission is closed (REQ-PROJECT-70).
    AdmissionClosed,
}

impl Repository {
    /// The repository as a project document carries it: `providerKind`, `availability` and
    /// `admissionDiagnostic`.
    fn from_document(document: &Value) -> Result<Self, Ending> {
        let kind = match document["providerKind"].as_str() {
            Some("github") => CredentialKind::Github,
            Some("gitlab") => CredentialKind::Gitlab,
            _ => return Err(Ending::InvalidResponse),
        };
        let availability = match document["availability"].as_str() {
            Some("active") => Availability::Active,
            Some("unavailable") if document["admissionDiagnostic"].is_string() => {
                Availability::AdmissionClosed
            }
            Some("unavailable") => Availability::Unavailable,
            _ => return Err(Ending::InvalidResponse),
        };
        Ok(Self { kind, availability })
    }

    /// The code of the repository's kind, given the code its connection read gave: a
    /// connection Cloud shows missing, inactive or lacking a scope stands; else an unavailable
    /// repository needs reconnecting, unless the deployment closed the provider's admission,
    /// which no reconnection fixes; else the read's code.
    pub(super) fn code(self, read: ReadinessCode) -> ReadinessCode {
        if read.fails() {
            return read;
        }
        match self.availability {
            Availability::Active => read,
            Availability::Unavailable => ReadinessCode::RepositoryUnavailable,
            Availability::AdmissionClosed => ReadinessCode::ProviderUnavailable,
        }
    }
}

/// A connection family's failure as the reads see it.
trait FamilyFailure: Sized {
    fn unauthenticated() -> Self;
    fn unreachable(category: UnreachableCategory) -> Self;
    /// A response outside the public contract, such as a cursor that does not advance.
    fn invalid_response() -> Self;
    fn credential_rejected(&self) -> bool;
    /// How the failure ends the command, or `None` when Cloud refused the caller the read.
    fn ending(&self) -> Option<Ending>;
}

/// A failure that ends the command with no report (REQ-CREDREADY-14).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Ending {
    Unauthenticated,
    /// Cloud does not know the organization.
    NotFound,
    /// Cloud does not know the named project, or not in the organization.
    ProjectNotFound,
    /// Cloud refused the caller the named project.
    ProjectForbidden,
    InvalidInput,
    Unreachable(UnreachableCategory),
    RateLimited {
        retry_after: Option<u64>,
    },
    InvalidResponse,
}

impl Ending {
    /// Writes the failure as `ApiFailureResult` or as an error and its remedy.
    pub(super) fn write(
        self,
        deployment: &str,
        authentication: PrincipalAuthenticationKind,
        json: bool,
    ) -> anyhow::Result<ExitCode> {
        let (outcome, category, retry_after, human, class) = match self {
            Self::Unauthenticated => (
                "unauthenticated",
                None,
                None,
                authentication
                    .rejected_error(
                        "error: the readiness check requires sign-in\n\nSign in first:\n  um auth login",
                    )
                    .to_owned(),
                OutcomeClass::Unauthenticated,
            ),
            Self::NotFound => (
                "not_found",
                None,
                None,
                "error: organization not found or unavailable\n\nCheck the organization, then try again."
                    .to_owned(),
                OutcomeClass::GeneralFailure,
            ),
            // As `um project show` reports it.
            Self::ProjectNotFound => (
                "not_found",
                None,
                None,
                "error: Useful Machinery resource not found or unavailable\n\nCheck the organization and resource identifier, then try again."
                    .to_owned(),
                OutcomeClass::GeneralFailure,
            ),
            Self::ProjectForbidden => (
                "forbidden",
                None,
                None,
                "error: this account may not read the project\n\nAsk an organization owner to run this check."
                    .to_owned(),
                OutcomeClass::Forbidden,
            ),
            Self::InvalidInput => (
                "invalid_input",
                None,
                None,
                format!(
                    "error: organization rejected by {deployment}\n\nCheck the organization, then try again."
                ),
                OutcomeClass::GeneralFailure,
            ),
            Self::Unreachable(category) => (
                "unreachable",
                Some(category.as_str()),
                None,
                format!(
                    "error: contact {deployment}: {}\n\nCheck network access to the deployment and try again.",
                    category.as_str()
                ),
                unreachable_outcome_class(category),
            ),
            Self::RateLimited { retry_after } => (
                "rate_limited",
                None,
                retry_after,
                format!("error: {deployment} rate limited the readiness check\n\nWait, then try again."),
                OutcomeClass::RateLimited,
            ),
            Self::InvalidResponse => (
                "invalid_response",
                None,
                None,
                "error: Useful Machinery API response does not match the public contract\n\nUse a CLI version supported by this deployment."
                    .to_owned(),
                OutcomeClass::Protocol,
            ),
        };
        write_api_failure(
            deployment,
            outcome,
            category,
            retry_after,
            &human,
            class,
            json,
        )
    }
}

/// Every item of a paged list, following `nextCursor` until a page has none; `page` lists
/// the page at a cursor as its items and the next cursor.
fn every_page<I, E: FamilyFailure>(
    mut page: impl FnMut(Option<&str>) -> Result<(Vec<I>, Option<String>), E>,
) -> Result<Vec<I>, E> {
    let mut items = Vec::new();
    let mut seen = BTreeSet::new();
    let mut cursor: Option<String> = None;
    loop {
        let (listed, next) = page(cursor.as_deref())?;
        items.extend(listed);
        let Some(next) = next else {
            return Ok(items);
        };
        // A cursor seen before would page forever.
        if !seen.insert(next.clone()) {
            return Err(E::invalid_response());
        }
        cursor = Some(next);
    }
}

impl FamilyFailure for LinearFailure {
    fn unauthenticated() -> Self {
        Self::Unauthenticated
    }

    fn unreachable(category: UnreachableCategory) -> Self {
        Self::Unreachable { category }
    }

    fn invalid_response() -> Self {
        Self::InvalidResponse {
            credential_rejected: false,
        }
    }

    fn credential_rejected(&self) -> bool {
        Self::credential_rejected(self)
    }

    fn ending(&self) -> Option<Ending> {
        Some(match self {
            Self::Forbidden => return None,
            Self::Unauthenticated => Ending::Unauthenticated,
            Self::NotFound => Ending::NotFound,
            Self::InvalidInput { .. } => Ending::InvalidInput,
            Self::Unreachable { category } => Ending::Unreachable(*category),
            Self::RateLimited { retry_after } => Ending::RateLimited {
                retry_after: *retry_after,
            },
            // A list answers no conflict.
            Self::Conflict | Self::InvalidResponse { .. } => Ending::InvalidResponse,
        })
    }
}

impl FamilyFailure for GitLabFailure {
    fn unauthenticated() -> Self {
        Self::Unauthenticated
    }

    fn unreachable(category: UnreachableCategory) -> Self {
        Self::Unreachable(category)
    }

    fn invalid_response() -> Self {
        Self::Protocol
    }

    fn credential_rejected(&self) -> bool {
        Self::credential_rejected(self)
    }

    fn ending(&self) -> Option<Ending> {
        Some(match self {
            Self::Forbidden => return None,
            Self::Unauthenticated => Ending::Unauthenticated,
            Self::NotFound => Ending::NotFound,
            Self::InvalidInput => Ending::InvalidInput,
            Self::Unreachable(category) => Ending::Unreachable(*category),
            // The deployment cannot serve GitLab connections now, as the family's exit 4 says.
            Self::ProviderUnavailable | Self::ServiceUnavailable => {
                Ending::Unreachable(UnreachableCategory::Server)
            }
            // A list answers no conflict, candidate or identity.
            Self::Conflict
            | Self::CandidateUnavailable
            | Self::IdentityMismatch
            | Self::Protocol => Ending::InvalidResponse,
        })
    }
}

impl FamilyFailure for GitHubFailure {
    fn unauthenticated() -> Self {
        Self::Unauthenticated
    }

    fn unreachable(category: UnreachableCategory) -> Self {
        Self::Unreachable(category)
    }

    fn invalid_response() -> Self {
        Self::Protocol {
            credential_rejected: false,
        }
    }

    fn credential_rejected(&self) -> bool {
        Self::credential_rejected(self)
    }

    /// Only an owner may list installations (REQ-CONNECTION-12), so `Forbidden` is an ordinary
    /// member's answer and leaves the requirement `not_permitted`.
    fn ending(&self) -> Option<Ending> {
        Some(match self {
            Self::Forbidden => return None,
            Self::Unauthenticated => Ending::Unauthenticated,
            Self::NotFound => Ending::NotFound,
            Self::InvalidInput => Ending::InvalidInput,
            Self::Unreachable(category) => Ending::Unreachable(*category),
            // A list answers no conflict.
            Self::SourceConnectionConflict | Self::Protocol { .. } => Ending::InvalidResponse,
        })
    }
}

impl FamilyFailure for ProjectFailure {
    fn unauthenticated() -> Self {
        Self::Unauthenticated
    }

    fn unreachable(category: UnreachableCategory) -> Self {
        Self::Unreachable(category)
    }

    fn invalid_response() -> Self {
        Self::Protocol {
            credential_rejected: false,
        }
    }

    fn credential_rejected(&self) -> bool {
        Self::credential_rejected(self)
    }

    /// Every failure ends the command, `Forbidden` included: without the project the
    /// repository's provider is not known.
    fn ending(&self) -> Option<Ending> {
        Some(match *self {
            Self::Unauthenticated => Ending::Unauthenticated,
            Self::Forbidden => Ending::ProjectForbidden,
            Self::NotFound => Ending::ProjectNotFound,
            Self::InvalidInput => Ending::InvalidInput,
            Self::Unreachable(category) => Ending::Unreachable(category),
            Self::RateLimited { retry_after } => Ending::RateLimited {
                retry_after: Some(retry_after),
            },
            // GitHub or GitLab cannot answer now, as the project family's exit 4 says.
            Self::ProviderUnavailable { .. } => Ending::Unreachable(UnreachableCategory::Server),
            // A read answers no binding, name, quantity, idempotency or source conflict.
            Self::RepositoryNotBound
            | Self::NameUnavailable
            | Self::QuantityLimitReached
            | Self::IdempotencyConflict
            | Self::SourceConflict
            | Self::RepositoryUnavailable
            | Self::Conflict
            | Self::Protocol { .. } => Ending::InvalidResponse,
        })
    }
}

fn read_github(session: &Session<'_>) -> anyhow::Result<Result<ReadinessCode, Ending>> {
    session.read(
        GitHubApi::new,
        "prepare GitHub connection networking",
        |api, organization| api.list_installations(organization),
        |installations| github_code(installations.iter().map(|installation| installation.state)),
    )
}

/// The code the organization's GitHub App installations give the `github` requirement
/// (REQ-CREDREADY-20, REQ-CREDREADY-16): an active installation is held ready, else a
/// disconnected or revoked one is inactive, else none is connected. A suspension changes no
/// installation's state (REQ-CONNECTION-10), so it does not show here.
fn github_code(states: impl IntoIterator<Item = GitHubInstallationState>) -> ReadinessCode {
    let mut code = ReadinessCode::NotConnected;
    for state in states {
        match state {
            GitHubInstallationState::Active => return ReadinessCode::RunnerUnverified,
            GitHubInstallationState::Disconnected | GitHubInstallationState::Revoked => {
                code = ReadinessCode::ConnectionInactive;
            }
        }
    }
    code
}

fn read_gitlab(session: &Session<'_>) -> anyhow::Result<Result<ReadinessCode, Ending>> {
    session.read(
        GitLabApi::new,
        "prepare GitLab connection networking",
        |api, organization| {
            every_page(|cursor| {
                let mut page = api.list(organization, Some(PAGE_LIMIT), cursor)?;
                let next = page["nextCursor"].as_str().map(str::to_owned);
                match page.get_mut("items").map(Value::take) {
                    Some(Value::Array(items)) => Ok((items, next)),
                    _ => Err(GitLabFailure::Protocol),
                }
            })
        },
        |connections| gitlab_code(&connections),
    )
}

/// The code the organization's GitLab connections, as listed documents, give the `gitlab`
/// requirement (REQ-CREDREADY-19, REQ-CREDREADY-16), by the first that holds: an active
/// connection is held ready; a credential that expired needs replacing; a credential GitLab
/// rejected, or a disconnected connection, is inactive; otherwise, with none or only ones never
/// given a credential (`not-configured`), none is connected.
fn gitlab_code(connections: &[Value]) -> ReadinessCode {
    let any = |state: &str, reasons: &[&str]| {
        connections.iter().any(|connection| {
            connection["state"] == state
                && (reasons.is_empty()
                    || reasons
                        .iter()
                        .any(|reason| connection["credentialReason"] == *reason))
        })
    };
    if any("active", &[]) {
        ReadinessCode::RunnerUnverified
    } else if any("credential_required", &["expired"]) {
        ReadinessCode::CredentialExpired
    } else if any("disconnected", &[])
        || any(
            "credential_required",
            &["credentials-rejected", "identity-rejected"],
        )
    {
        ReadinessCode::ConnectionInactive
    } else {
        ReadinessCode::NotConnected
    }
}

fn read_linear(session: &Session<'_>) -> anyhow::Result<Result<ReadinessCode, Ending>> {
    session.read(
        LinearApi::new,
        "prepare Linear connection networking",
        |api, organization| {
            every_page(|cursor| {
                api.list(organization, Some(PAGE_LIMIT), cursor)
                    .map(|page| (page.items, page.next_cursor))
            })
        },
        |connections| linear_code(&connections),
    )
}

/// The code the organization's Linear connections give the `linear` requirement
/// (REQ-CREDREADY-15, REQ-CREDREADY-16), by the first that holds: an active connection
/// granted every scope it requires is held ready; an active one lacks a scope; a
/// `recovery_required`, `disconnected` or `revoked` one is inactive; otherwise, with none or
/// only deleted ones, none is connected. `stateUpdatesConsented` is retained consent, not
/// current authority (REQ-CONNECTION-85), and is not read.
fn linear_code(connections: &[LinearConnection]) -> ReadinessCode {
    use linear_connection::Status;

    let active = || {
        connections
            .iter()
            .filter(|connection| connection.status == Status::Active)
    };
    if active().any(has_required_scopes) {
        ReadinessCode::RunnerUnverified
    } else if active().next().is_some() {
        ReadinessCode::ScopeMissing
    } else if connections.iter().any(|connection| {
        matches!(
            connection.status,
            Status::RecoveryRequired | Status::Disconnected | Status::Revoked
        )
    }) {
        ReadinessCode::ConnectionInactive
    } else {
        ReadinessCode::NotConnected
    }
}

fn has_required_scopes(connection: &LinearConnection) -> bool {
    connection.required_scopes.iter().all(|scope| {
        let scope = match scope {
            linear_connection::RequiredScopes::Read => "read",
        };
        connection
            .granted_scopes
            .iter()
            .any(|granted| granted == scope)
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use serde_json::{Value, json};
    use um_api::linear_connection::{RequiredScopes, Status};
    use um_api::{GitHubInstallationState, LinearConnection};
    use um_runner::ReadinessCode;

    use super::{Availability, Repository, github_code, gitlab_code, linear_code};

    fn connection(status: Status, granted_scopes: &[&str]) -> LinearConnection {
        LinearConnection {
            status,
            required_scopes: HashSet::from([RequiredScopes::Read]),
            granted_scopes: granted_scopes
                .iter()
                .map(|scope| (*scope).to_owned())
                .collect(),
            ..LinearConnection::default()
        }
    }

    // REQ-CREDREADY-15
    #[test]
    fn linear_connections_give_the_code_of_the_best_connection() {
        let ready = || connection(Status::Active, &["read"]);
        let lacking = || connection(Status::Active, &["write"]);
        let inactive = [
            Status::RecoveryRequired,
            Status::Disconnected,
            Status::Revoked,
        ]
        .map(|status| connection(status, &["read"]));
        let deleted = || connection(Status::Deleted, &["read"]);

        for (connections, expected) in [
            (
                vec![deleted(), lacking(), inactive[0].clone(), ready()],
                ReadinessCode::RunnerUnverified,
            ),
            (
                vec![inactive[1].clone(), lacking(), deleted()],
                ReadinessCode::ScopeMissing,
            ),
            (
                vec![deleted(), inactive[2].clone()],
                ReadinessCode::ConnectionInactive,
            ),
            (inactive.to_vec(), ReadinessCode::ConnectionInactive),
            (vec![deleted(), deleted()], ReadinessCode::NotConnected),
            (Vec::new(), ReadinessCode::NotConnected),
        ] {
            assert_eq!(linear_code(&connections), expected, "{connections:?}");
        }
    }

    fn gitlab(state: &str, credential_reason: Option<&str>) -> Value {
        json!({"state": state, "credentialReason": credential_reason})
    }

    // REQ-CREDREADY-19
    #[test]
    fn gitlab_connections_give_the_code_of_the_best_connection() {
        let active = || gitlab("active", None);
        let expired = || gitlab("credential_required", Some("expired"));
        let rejected = || gitlab("credential_required", Some("credentials-rejected"));
        let identity_rejected = || gitlab("credential_required", Some("identity-rejected"));
        let disconnected = || gitlab("disconnected", None);
        let unconfigured = || gitlab("credential_required", Some("not-configured"));

        for (connections, expected) in [
            (
                vec![expired(), disconnected(), unconfigured(), active()],
                ReadinessCode::RunnerUnverified,
            ),
            (
                vec![rejected(), disconnected(), expired(), unconfigured()],
                ReadinessCode::CredentialExpired,
            ),
            (
                vec![unconfigured(), rejected()],
                ReadinessCode::ConnectionInactive,
            ),
            (vec![identity_rejected()], ReadinessCode::ConnectionInactive),
            (
                vec![unconfigured(), disconnected()],
                ReadinessCode::ConnectionInactive,
            ),
            (
                vec![unconfigured(), unconfigured()],
                ReadinessCode::NotConnected,
            ),
            (Vec::new(), ReadinessCode::NotConnected),
        ] {
            assert_eq!(gitlab_code(&connections), expected, "{connections:?}");
        }
    }

    // REQ-CREDREADY-20
    #[test]
    fn github_installations_give_the_code_of_the_best_installation() {
        use GitHubInstallationState::{Active, Disconnected, Revoked};

        for (states, expected) in [
            (
                vec![Revoked, Disconnected, Active],
                ReadinessCode::RunnerUnverified,
            ),
            (
                vec![Revoked, Disconnected],
                ReadinessCode::ConnectionInactive,
            ),
            (vec![Disconnected], ReadinessCode::ConnectionInactive),
            (Vec::new(), ReadinessCode::NotConnected),
        ] {
            assert_eq!(github_code(states.clone()), expected, "{states:?}");
        }
    }

    // REQ-CREDREADY-21
    #[test]
    fn a_project_repository_changes_only_a_code_its_connection_read_does_not_fail() {
        let repository = |availability| Repository {
            kind: um_runner::CredentialKind::Github,
            availability,
        };
        let failing = [
            ReadinessCode::NotConnected,
            ReadinessCode::ConnectionInactive,
            ReadinessCode::CredentialExpired,
            ReadinessCode::ScopeMissing,
        ];
        let passing = [ReadinessCode::RunnerUnverified, ReadinessCode::NotPermitted];
        for read in failing {
            for availability in [
                Availability::Active,
                Availability::Unavailable,
                Availability::AdmissionClosed,
            ] {
                assert_eq!(repository(availability).code(read), read, "{read:?}");
            }
        }
        for read in passing {
            assert_eq!(repository(Availability::Active).code(read), read);
            assert_eq!(
                repository(Availability::Unavailable).code(read),
                ReadinessCode::RepositoryUnavailable
            );
            assert_eq!(
                repository(Availability::AdmissionClosed).code(read),
                ReadinessCode::ProviderUnavailable
            );
        }
    }
}
