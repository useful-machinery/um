//! The grant tokens of each assignment the serving runner holds
//! (REQ-GRANT-86): at each admission the runner asks Cloud for the
//! assignment's providers and, at the same time, its token services
//! (REQ-INTEGRATION-12), then for the token of the run's grant of each
//! provider listed, and of each token service, that the host does not provide
//! (REQ-GRANT-87), keeps each token it is issued in a private file of its own
//! in the assignment's own directory of its agent-login directory, records
//! the outcome of each of GitHub, GitLab.com and Linear for the run in the
//! readiness document (REQ-GRANT-88), and writes a `runner.grant_token`
//! record for each changed state and each failure (REQ-TELEMETRY-46). A
//! token file is only ever the token of the assignment whose directory holds
//! it (INV-RUNNER-28): every setup, an admission's included, removes what no
//! assignment the runner holds owns, an admission clears its own
//! assignment's directory before it uses any answer, and the hold an
//! admission returns removes that directory when the assignment ends. The
//! admission offers each token it holds, and its file, to the steps of its
//! workflow that pass their names or, for the runner's own three, declare
//! the provider (REQ-WFEXEC-78): the runner's own names for its three, and
//! the names of a listed provider's entry once it has checked them
//! (REQ-GRANT-87, INV-RUNNER-32). While the assignment runs, a task the hold
//! owns keeps each file current (REQ-GRANT-89): it asks again when the wait
//! Cloud named has passed and replaces the file in one step, retries a
//! request Cloud did not answer, removes the file at a refusal or once the
//! held token has expired, and stops when the hold is dropped. Dropping the
//! hold then marks the entries ended, unless a later admission has recorded
//! its own since (INV-RUNNER-30); a refresh records an entry only while its
//! admission is the last that recorded them.

use std::collections::BTreeSet;
use std::ffi::{OsStr, OsString};
use std::fs;
use std::io;
use std::os::unix::ffi::OsStringExt as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::{Duration, Instant};

use opentelemetry::KeyValue;
use time::OffsetDateTime;
use useful_execution::{
    CLAUDE_CODE_LOGIN_VARIABLE, CODEX_LOGIN_VARIABLE, EnvironmentSnapshot,
    is_declared_only_environment_name,
};

use super::cloud::{
    AgentLoginCloud, CloudFailure, GrantAccessToken, GrantProvider, GrantProvidersFailure,
    GrantProvidersRequest, GrantTokenFailure, GrantTokenRefusal, GrantTokenRequest, IssuedToken,
    ListedProvider, ReportedTokenService, TokenServiceSource,
};
use super::directory::{self, LoginDirectory};
use super::readiness::{GrantReadiness, IssuedGrant, ReadinessDocument, brokered_providers};
use super::setup::AgentLoginSetup;
use super::{FailureCause, ServiceEnvironment, credential_source};
use crate::control_protocol::{AgentLoginProvider, AgentLoginState};
use crate::service::Sleeper;
use crate::telemetry::{self, Outcome, Recorder, attribute};

/// The record of a changed grant-token state or a failure.
pub(crate) const GRANT_TOKEN_EVENT: &str = "runner.grant_token";

/// The variables each of the runner's three providers' grant token is
/// offered as, whatever Cloud's list names them: the token, and the path of
/// its file (REQ-GRANT-87). The host provides a provider while the service
/// environment carries either. They are the names the engine delivers for a
/// declared integration of the provider (INV-ENGINE-43).
pub(super) const OFFERED_VARIABLES: [(AgentLoginProvider, &str, &str); 3] = [
    (AgentLoginProvider::Github, "GH_TOKEN", "GH_TOKEN_FILE"),
    (
        AgentLoginProvider::Gitlab,
        "GITLAB_TOKEN",
        "GITLAB_TOKEN_FILE",
    ),
    (
        AgentLoginProvider::Linear,
        "USEFUL_LINEAR_ACCESS_TOKEN",
        "USEFUL_LINEAR_ACCESS_TOKEN_FILE",
    ),
];

/// The least a refresh waits before asking again after an answer
/// (REQ-GRANT-89), and the wait after the first failure in a row.
const MINIMUM_WAIT: Duration = Duration::from_secs(5);

/// The longest wait after a failure: each failure in a row doubles the wait
/// from `MINIMUM_WAIT` up to this (REQ-GRANT-89).
const MAXIMUM_RETRY: Duration = Duration::from_secs(5 * 60);

/// The status Cloud refuses `assignment_not_current` with (REQ-GRANT-84,
/// REQ-GRANT-97), which a record of that failed request carries
/// (REQ-TELEMETRY-46).
const ASSIGNMENT_NOT_CURRENT_STATUS: u16 = 409;

/// What an admission's two lists answered: the OAuth providers' and the
/// token services'.
type Lists = (
    Result<Vec<ListedProvider>, GrantProvidersFailure>,
    Result<Vec<ListedProvider>, GrantProvidersFailure>,
);

/// Which of an assignment's requests a record is of (REQ-TELEMETRY-46).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    /// The request its admission makes.
    Admission,
    /// A request made to keep a held token current while it runs.
    Refresh,
}

impl Phase {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Admission => "admission",
            Self::Refresh => "refresh",
        }
    }
}

/// Where a grant token's setup failed, as its record names it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Stage {
    /// Asking Cloud for the assignment's providers or its token services.
    List,
    /// Asking Cloud for the token.
    Issue,
    /// Keeping it in its file.
    Write,
    /// Removing the provider's file, or what was at its assignment's
    /// directory.
    Remove,
}

impl Stage {
    const fn as_str(self) -> &'static str {
        match self {
            Self::List => "list",
            Self::Issue => "issue",
            Self::Write => "write",
            Self::Remove => "remove",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Failure {
    stage: Stage,
    cause: FailureCause,
}

impl Failure {
    fn io(stage: Stage, error: &io::Error) -> Self {
        Self {
            stage,
            cause: FailureCause::Io(error.kind()),
        }
    }
}

/// The provider as a `runner.grant_token` record names it
/// (`um.grant_token.provider`, REQ-TELEMETRY-46): `github`, `gitlab` or
/// `linear` for the runner's three, as they have always been recorded, and
/// Cloud's id for any other provider.
fn record_name(provider: &GrantProvider) -> &str {
    match provider.agent_login_provider() {
        Some(provider) => provider.as_str(),
        None => provider.cloud_id(),
    }
}

/// A provider whose grant token the runner asks Cloud for, with the two
/// names `offer` offers the token and the path of its file under
/// (REQ-GRANT-87): the runner's own for its three, and for any other
/// provider the names of its entry in Cloud's list, once checked
/// (INV-RUNNER-32).
#[derive(Clone, Debug, Eq, PartialEq)]
struct Offering {
    provider: GrantProvider,
    variable: String,
    file_variable: String,
}

impl Offering {
    /// One of the runner's three under its own names; `None` for an
    /// installable provider, which has no grant token.
    fn runner(provider: AgentLoginProvider) -> Option<Self> {
        let (_, variable, file_variable) = OFFERED_VARIABLES
            .into_iter()
            .find(|(offered, _, _)| *offered == provider)?;
        Some(Self {
            provider: GrantProvider::try_from(provider).ok()?,
            variable: variable.to_owned(),
            file_variable: file_variable.to_owned(),
        })
    }

    /// Whether the service environment carries either name, the host's
    /// providing the provider (REQ-GRANT-87).
    fn provided_by(&self, service: &ServiceEnvironment) -> bool {
        service.carries(&self.variable) || service.carries(&self.file_variable)
    }
}

/// Whether the entry of a provider other than the runner's three may offer
/// its token under `variable` and `file_variable` (REQ-GRANT-87,
/// INV-RUNNER-32): the two differ, and each is a name a step receives only
/// by declaring it, is neither name the runner sets for an agent login, is
/// no name of the runner's three, and is no name of a configured entry
/// before it in the list, `earlier`, whether or not that entry was used.
fn usable_names(variable: &str, file_variable: &str, earlier: &[&str]) -> bool {
    variable != file_variable
        && [variable, file_variable].into_iter().all(|name| {
            is_declared_only_environment_name(name)
                && name != CODEX_LOGIN_VARIABLE
                && name != CLAUDE_CODE_LOGIN_VARIABLE
                && !OFFERED_VARIABLES
                    .iter()
                    .any(|&(_, own, own_file)| name == own || name == own_file)
                && !earlier.contains(&name)
        })
}

/// The runner's grant token files and the assignments it holds, whose
/// directories hold them. Every write and removal of a token file is made
/// under its lock, so the end of one assignment never removes the files of
/// another (INV-RUNNER-28).
pub(crate) struct TokenFiles {
    directory: LoginDirectory,
    recorder: Arc<Recorder>,
    /// Each assignment the runner holds, from its admission's registration,
    /// made before it asks Cloud, to its hold's end.
    held: Mutex<BTreeSet<String>>,
}

impl TokenFiles {
    pub(crate) fn new(directory: LoginDirectory, recorder: Arc<Recorder>) -> Self {
        Self {
            directory,
            recorder,
            held: Mutex::new(BTreeSet::new()),
        }
    }

    fn held(&self) -> std::sync::MutexGuard<'_, BTreeSet<String>> {
        self.held.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Whether the runner holds `assignment_id`.
    fn holds(&self, assignment_id: &str) -> bool {
        self.held().contains(assignment_id)
    }

    /// Whether the runner holds any assignment: while it does, no setup
    /// marks an entry ended (INV-RUNNER-30).
    pub(crate) fn holds_any(&self) -> bool {
        !self.held().is_empty()
    }

    /// The ID of each assignment the runner holds.
    pub(crate) fn held_assignments(&self) -> Vec<String> {
        self.held().iter().cloned().collect()
    }

    /// Removes whatever is at `path` in the runner's directory. A directory
    /// that is not a real directory holds no file of the runner's, so there
    /// is nothing to remove from it.
    fn remove_at(&self, path: &Path) -> io::Result<()> {
        match self.directory.remove_tree(path) {
            Err(error) if error.kind() == io::ErrorKind::NotADirectory => Ok(()),
            removed => removed,
        }
    }

    /// Removes what no assignment in `held` owns: the token files and
    /// `grant-tokens` an earlier release kept at fixed names, and every
    /// entry of `assignments`, whatever it is, whose name is not a held
    /// assignment's, or `assignments` itself when it is not a real
    /// directory. Each it cannot remove is recorded with no provider.
    fn remove_unowned(&self, held: &BTreeSet<String>) {
        for path in self.directory.earlier_grant_tokens() {
            if let Err(error) = self.remove_at(&path) {
                self.record_removal_failure(None, None, &error);
            }
        }
        let assignments = self.directory.assignments();
        if !directory::real_directory(&assignments) {
            if let Err(error) = self.remove_at(&assignments) {
                self.record_removal_failure(None, None, &error);
            }
            return;
        }
        let entries = match fs::read_dir(&assignments) {
            Ok(entries) => entries,
            Err(error) => {
                self.record_removal_failure(None, None, &error);
                return;
            }
        };
        for entry in entries {
            let removed = entry.and_then(|entry| {
                let owned = entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| held.contains(name));
                if owned {
                    Ok(())
                } else {
                    self.remove_at(&entry.path())
                }
            });
            if let Err(error) = removed {
                self.record_removal_failure(None, None, &error);
            }
        }
    }

    /// Removes what no assignment the runner holds owns, at a setup
    /// (INV-RUNNER-28); nothing of a held assignment is touched.
    pub(crate) fn remove_leftovers(&self) {
        let held = self.held();
        self.remove_unowned(&held);
    }

    /// Registers the assignment `assignment_id` an admission is admitting,
    /// removes what no held assignment owns and clears whatever is at the
    /// assignment's own directory; answers the failure to clear it.
    fn admit(&self, assignment_id: &str) -> Option<Failure> {
        let mut held = self.held();
        held.insert(assignment_id.to_owned());
        self.remove_unowned(&held);
        // Anything but an assignment ID has no directory, and each write
        // for it fails.
        let own = self.directory.assignment(assignment_id)?;
        self.remove_at(&own)
            .err()
            .map(|error| Failure::io(Stage::Remove, &error))
    }

    /// Removes the directory of the assignment `assignment_id`, with every
    /// token file in it, and no longer counts it held; a directory it cannot
    /// remove is recorded with the assignment and no provider, and is
    /// removed by the next setup.
    fn release(&self, assignment_id: &str) {
        let mut held = self.held();
        if let Some(own) = self.directory.assignment(assignment_id)
            && let Err(error) = self.remove_at(&own)
        {
            self.record_removal_failure(None, Some(assignment_id), &error);
        }
        held.remove(assignment_id);
    }

    /// Records a token file or directory that could not be removed, with
    /// the provider as its record names it when one is known.
    fn record_removal_failure(
        &self,
        provider: Option<&str>,
        assignment_id: Option<&str>,
        error: &io::Error,
    ) {
        let mut attributes: Vec<KeyValue> = provider
            .map(|provider| KeyValue::new(attribute::GRANT_TOKEN_PROVIDER, provider.to_owned()))
            .into_iter()
            .collect();
        attributes.extend(failure_attributes(Failure::io(Stage::Remove, error)));
        attributes.extend(assignment_id.map(|assignment_id| {
            KeyValue::new(attribute::ASSIGNMENT_ID, assignment_id.to_owned())
        }));
        self.recorder
            .start(GRANT_TOKEN_EVENT, attributes)
            .finish(Outcome::Failure);
    }
}

/// A token the runner kept in its file, the provider and the two names it
/// is offered under, the grant Cloud issued it from, and the wait Cloud
/// named with it.
#[derive(Clone, Debug)]
struct HeldToken {
    offering: Offering,
    /// The `pgr_` ID of the grant, or for a token service the `itk_` ID of
    /// its integration token, as issued at the admission, which the
    /// admission reports (REQ-GRANT-93, REQ-INTEGRATION-12).
    grant_id: String,
    file: PathBuf,
    generation: u64,
    expires_at: OffsetDateTime,
    refresh_after: Duration,
}

/// What an admission holds of its assignment's grant tokens, which the manager
/// holds with the assignment until it ends, whether or not it kept a token. It is
/// made before the admission registers the assignment in `TokenFiles`, so
/// whatever ends the admission ends the registration with it: dropping it
/// stops its refresh task, then removes the assignment's directory, with
/// every token file in it, no longer counts the assignment held and marks
/// the entries its admission recorded ended (INV-RUNNER-28, INV-RUNNER-30),
/// on a blocking thread when there is a runtime to run one on.
pub(crate) struct AssignmentGrantTokens {
    files: Arc<TokenFiles>,
    /// The setup each refresh is made and recorded through, and the end
    /// marked through.
    setup: Weak<AgentLoginSetup>,
    boot_id: String,
    run_id: String,
    assignment_id: String,
    /// Each token the admission kept.
    held: Vec<HeldToken>,
    /// Each token service Cloud's list named whose names the runner
    /// checked, in the list's order, whether or not a token of it was kept:
    /// those whose source the admission reports (REQ-INTEGRATION-12). None
    /// where Cloud did not answer the list.
    services: Vec<Offering>,
    /// The task keeping the files current, once started.
    refresh: Option<GrantTokenRefresh>,
    /// Where each readiness document a refresh or the end records is
    /// published, once given.
    publish: Option<PublishReadiness>,
}

/// What a refresh, and the end of the assignment, publishes each readiness
/// document it records to: the live status, so that `useful runner status`
/// shows it without waiting for a pass.
pub(crate) type PublishReadiness = Arc<dyn Fn(&ReadinessDocument) + Send + Sync>;

impl AssignmentGrantTokens {
    /// The token of one of the runner's three the admission kept, if it
    /// kept one.
    fn held(&self, provider: AgentLoginProvider) -> Option<&HeldToken> {
        self.held
            .iter()
            .find(|held| held.offering.provider.agent_login_provider() == Some(provider))
    }

    /// The file the admission kept the provider's token in, if it kept one.
    fn file(&self, provider: AgentLoginProvider) -> Option<&Path> {
        self.held(provider).map(|held| held.file.as_path())
    }

    /// The provider's token as its file holds it now: what `offer` offers;
    /// none when the admission kept no token of it or the file no longer
    /// holds one of its form.
    fn read_back(&self, provider: AgentLoginProvider) -> Option<GrantAccessToken> {
        read_token(self.file(provider)?)
    }

    /// Whether the admission holds a token of the provider it can read
    /// back, which `offer` offers (REQ-GRANT-87, REQ-GRANT-92).
    pub(crate) fn holds(&self, provider: AgentLoginProvider) -> bool {
        self.read_back(provider).is_some()
    }

    /// The ID of the grant the admission's token of the provider was issued
    /// from, if it kept one (REQ-GRANT-93).
    pub(crate) fn grant_id(&self, provider: AgentLoginProvider) -> Option<&str> {
        self.held(provider).map(|held| held.grant_id.as_str())
    }

    /// Publishes with `publish` each readiness document a refresh of this
    /// hold records, and the one its end records.
    pub(crate) fn publish_with(&mut self, publish: PublishReadiness) {
        self.publish = Some(publish);
    }

    /// Starts the task that keeps each token file this hold has current
    /// while it is held, of the runner's three and of any other provider
    /// alike (REQ-GRANT-89): for each provider, it waits on `sleeper` the
    /// time Cloud named with its token, from now, then asks again, and
    /// publishes each readiness document a refresh records as
    /// `publish_with` named. Nothing is started without a token, without a
    /// runtime to run on, or a second time.
    pub(crate) fn keep_current(&mut self, sleeper: Arc<dyn Sleeper>) {
        if self.refresh.is_some() || self.held.is_empty() {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let started = sleeper.now();
        let due = self
            .held
            .iter()
            .map(|held| Due {
                offering: held.offering.clone(),
                file: held.file.clone(),
                grant_id: held.grant_id.clone(),
                generation: held.generation,
                expires_at: Some(held.expires_at),
                recorded: Recorded {
                    state: AgentLoginState::Ready,
                    grant_id: Some(held.grant_id.clone()),
                    generation: Some(held.generation),
                },
                at: after(started, held.refresh_after),
                retry: MINIMUM_WAIT,
                ended: false,
            })
            .collect();
        let refresh = Refresh {
            setup: Weak::clone(&self.setup),
            boot_id: self.boot_id.clone(),
            run_id: self.run_id.clone(),
            assignment_id: self.assignment_id.clone(),
        };
        self.refresh = Some(GrantTokenRefresh {
            task: runtime.spawn(refresh.keep_current(due, sleeper, self.publish.clone())),
        });
    }
}

/// The task that keeps an assignment's token files current; dropping it
/// stops it (REQ-GRANT-89).
struct GrantTokenRefresh {
    task: tokio::task::JoinHandle<()>,
}

impl Drop for GrantTokenRefresh {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// `start` and the wait an answer named, never under `MINIMUM_WAIT`.
fn after(start: Instant, wait: Duration) -> Instant {
    start + wait.max(MINIMUM_WAIT)
}

/// When the refresh task next asks for one provider's token, and what it
/// holds of it.
struct Due {
    offering: Offering,
    /// The file the admission kept the token in.
    file: PathBuf,
    /// The ID of the grant or integration token the token the runner last
    /// kept was issued from.
    grant_id: String,
    /// The generation of the token the runner last kept.
    generation: u64,
    /// The expiry of the token in the file, as the last answer for it named
    /// it; `None` once it is removed.
    expires_at: Option<OffsetDateTime>,
    /// The state, grant and generation the assignment last recorded of the
    /// provider: `ready` with the kept token when the task starts, then what
    /// each refresh recorded.
    recorded: Recorded,
    at: Instant,
    /// The wait after the next failure.
    retry: Duration,
    /// Whether the task asks no more for the provider.
    ended: bool,
}

/// One refresh of a provider's token, as the setup makes it.
pub(crate) struct RefreshStep {
    offering: Offering,
    /// The file the admission kept the token in.
    file: PathBuf,
    /// The ID of the grant or integration token the token the runner last
    /// kept was issued from.
    grant_id: String,
    /// The generation of the token the runner last kept.
    generation: u64,
    /// Whether its file still holds that token.
    held: bool,
    /// Whether the token's expiry has passed, on the task's clock.
    expired: bool,
    /// The state, grant and generation the task carries for the provider,
    /// which the refresh of a provider with no readiness entry compares
    /// against.
    recorded: Recorded,
}

/// The state, grant and generation last recorded of a provider, which a
/// refresh compares its outcome against to tell whether to write a record
/// (REQ-TELEMETRY-46) and, for one of the runner's three, its readiness
/// entry: the three's from the readiness document, and any other
/// provider's as the refresh task carries it for the assignment. A token
/// issued from another grant or integration token is a change, whatever
/// its generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Recorded {
    state: AgentLoginState,
    grant_id: Option<String>,
    generation: Option<u64>,
}

impl Recorded {
    /// What `grant` records; a provider never recorded is `not_delivered`.
    fn of(grant: Option<&GrantReadiness>) -> Self {
        let issued = grant.and_then(|grant| grant.issued.as_ref());
        Self {
            state: grant.map_or(AgentLoginState::NotDelivered, |grant| grant.state),
            grant_id: issued.map(|issued| issued.grant_id.clone()),
            generation: issued.map(|issued| issued.generation),
        }
    }

    /// What `outcome` records.
    fn outcome(outcome: &Settled) -> Self {
        Self {
            state: outcome.state,
            grant_id: outcome
                .issued
                .as_ref()
                .map(|issued| issued.grant_id.clone()),
            generation: outcome.issued.as_ref().map(|issued| issued.generation),
        }
    }
}

/// What the refresh task does after a refresh.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Next {
    /// A token of a newer generation, or issued from another grant or
    /// integration token, is in the file: ask again after `wait`.
    Kept {
        grant_id: String,
        generation: u64,
        expires_at: OffsetDateTime,
        wait: Duration,
    },
    /// The token in the file is the current one, which expires at
    /// `expires_at` as the answer names it: ask again after `wait`.
    Unchanged {
        expires_at: OffsetDateTime,
        wait: Duration,
    },
    /// Ask again after the next retry; `removed` when the file was removed,
    /// since its token had expired.
    Retry { removed: bool },
    /// Ask no more for the provider: Cloud refused it, or the files are no
    /// longer the assignment's.
    End,
}

/// What the refresh task asks through.
struct Refresh {
    setup: Weak<AgentLoginSetup>,
    boot_id: String,
    run_id: String,
    assignment_id: String,
}

impl Refresh {
    /// Asks for each provider in `due` when it falls due, until none is
    /// left to ask for or the setup is gone. Each request and file write
    /// blocks, so each refresh runs on the blocking pool.
    async fn keep_current(
        self,
        mut due: Vec<Due>,
        sleeper: Arc<dyn Sleeper>,
        publish: Option<PublishReadiness>,
    ) {
        let refresh = Arc::new(self);
        while let Some(next) = due.iter().map(|due| due.at).min() {
            if let Some(wait) = next
                .checked_duration_since(sleeper.now())
                .filter(|wait| !wait.is_zero())
            {
                sleeper.sleep(wait).await;
            }
            let now = sleeper.now();
            for due in due.iter_mut().filter(|due| due.at <= now) {
                let step = RefreshStep {
                    offering: due.offering.clone(),
                    file: due.file.clone(),
                    grant_id: due.grant_id.clone(),
                    generation: due.generation,
                    held: due.expires_at.is_some(),
                    expired: due
                        .expires_at
                        .is_some_and(|expires_at| sleeper.utc_now() >= expires_at),
                    recorded: due.recorded.clone(),
                };
                let asking = Arc::clone(&refresh);
                let answered = tokio::task::spawn_blocking(move || {
                    let setup = asking.setup.upgrade()?;
                    Some(setup.refresh_grant_token(
                        &asking.boot_id,
                        &asking.run_id,
                        &asking.assignment_id,
                        &step,
                    ))
                })
                .await;
                let next = match answered {
                    Ok(Some((next, recorded, document))) => {
                        due.recorded = recorded;
                        if let (Some(document), Some(publish)) = (document, &publish) {
                            publish(&document);
                        }
                        next
                    }
                    Ok(None) => return,
                    // A refresh that panicked is retried.
                    Err(_) => Next::Retry { removed: false },
                };
                let answered_at = sleeper.now();
                match next {
                    Next::Kept {
                        grant_id,
                        generation,
                        expires_at,
                        wait,
                    } => {
                        due.grant_id = grant_id;
                        due.generation = generation;
                        due.expires_at = Some(expires_at);
                        due.at = after(answered_at, wait);
                        due.retry = MINIMUM_WAIT;
                    }
                    // Each answer for the held token names the expiry the
                    // file stands until (REQ-INTEGRATION-12).
                    Next::Unchanged { expires_at, wait } => {
                        due.expires_at = Some(expires_at);
                        due.at = after(answered_at, wait);
                        due.retry = MINIMUM_WAIT;
                    }
                    Next::Retry { removed } => {
                        if removed {
                            due.expires_at = None;
                        }
                        due.at = answered_at + due.retry;
                        due.retry = due.retry.saturating_mul(2).min(MAXIMUM_RETRY);
                    }
                    Next::End => due.ended = true,
                }
            }
            due.retain(|due| !due.ended);
        }
    }
}

impl std::fmt::Debug for AssignmentGrantTokens {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AssignmentGrantTokens")
            .field("run_id", &self.run_id)
            .field("assignment_id", &self.assignment_id)
            .field("held", &self.held)
            .field("services", &self.services)
            .field("refreshing", &self.refresh.is_some())
            .finish_non_exhaustive()
    }
}

impl Drop for AssignmentGrantTokens {
    fn drop(&mut self) {
        // The task asks no more once the hold is gone; a refresh already
        // on the blocking pool writes nothing once the files are released.
        drop(self.refresh.take());
        let files = Arc::clone(&self.files);
        let setup = Weak::clone(&self.setup);
        let publish = self.publish.take();
        let assignment_id = std::mem::take(&mut self.assignment_id);
        // The files are released first, so a refresh still waiting for the
        // pass lock writes nothing after the end is marked.
        let release = move || {
            files.release(&assignment_id);
            let ended = setup
                .upgrade()
                .and_then(|setup| setup.mark_grants_ended(&assignment_id));
            if let (Some(document), Some(publish)) = (ended, publish) {
                publish(&document);
            }
        };
        // Removing a file, syncing its directory and writing the readiness
        // document block, so on a runtime they run on the runtime's
        // blocking pool (`cli/AGENTS.md`).
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => drop(runtime.spawn_blocking(release)),
            Err(_) => release(),
        }
    }
}

/// `environment` with the grant tokens of `tokens` offered in it
/// (REQ-GRANT-87): for each token the admission kept and can read back, the
/// token as its file holds it under the first of the two names the hold
/// carries for it and the file's path under the second, the runner's own
/// for its three and the checked names of Cloud's list for any other
/// provider (INV-RUNNER-32). The admission asks for no token of a provider
/// the host provides, and a token either of whose names `environment`
/// carries is skipped here too, so a token never replaces a value the
/// service carries, whichever reading of the service's environment the
/// admission asked from (INV-RUNNER-17). Every other name stays as
/// `environment` has it: nothing is removed or withheld, and a provider the
/// host does not provide and the admission holds no token of has its names
/// unset as the service has them.
pub(crate) fn offer(
    environment: &EnvironmentSnapshot,
    tokens: Option<&AssignmentGrantTokens>,
) -> EnvironmentSnapshot {
    let mut variables = environment.variables().clone();
    for held in tokens.map_or(&[][..], |tokens| tokens.held.as_slice()) {
        // The host provides a provider either of whose names `environment`
        // carries, whatever the admission held (INV-RUNNER-17).
        if carried(environment, &held.offering) {
            continue;
        }
        let Some(token) = read_token(&held.file) else {
            continue;
        };
        let Offering {
            variable,
            file_variable,
            ..
        } = &held.offering;
        variables.insert(
            OsString::from(variable),
            OsString::from_vec(token.as_bytes().to_vec()),
        );
        variables.insert(
            OsString::from(file_variable),
            held.file.as_os_str().to_owned(),
        );
    }
    EnvironmentSnapshot::new(variables)
}

/// Whether `environment` carries either name `offering` offers its token
/// under: the host's providing it, whatever the admission held
/// (INV-RUNNER-17).
fn carried(environment: &EnvironmentSnapshot, offering: &Offering) -> bool {
    [&offering.variable, &offering.file_variable]
        .into_iter()
        .any(|name| environment.variable(OsStr::new(name)).is_some())
}

/// The source of each token service Cloud listed for the assignment of
/// `tokens` whose names the runner checked, in the list's order, settled
/// from what `offer` reads of `environment` and `tokens` (REQ-INTEGRATION-12,
/// INV-RUNNER-31): `host` where `environment` carries either of its names,
/// else `token`, with the `itk_` ID it was issued from, where the
/// admission kept its token and the file reads back, else none, and no
/// entry. None where Cloud did not answer the list, or there is no hold.
pub(crate) fn token_service_sources(
    environment: &EnvironmentSnapshot,
    tokens: Option<&AssignmentGrantTokens>,
) -> Vec<ReportedTokenService> {
    let Some(tokens) = tokens else {
        return Vec::new();
    };
    tokens
        .services
        .iter()
        .filter_map(|offering| {
            let source = if carried(environment, offering) {
                TokenServiceSource::Host
            } else {
                let held = tokens
                    .held
                    .iter()
                    .find(|held| held.offering.provider == offering.provider)
                    .filter(|held| read_token(&held.file).is_some())?;
                TokenServiceSource::Token {
                    integration_token_id: held.grant_id.clone(),
                }
            };
            Some(ReportedTokenService {
                provider: offering.provider.cloud_id().to_owned(),
                source,
            })
        })
        .collect()
}

/// The token in the file at `path` (`directory::read_token`); `None` when
/// there is none, or the file is not a private one holding a single token of
/// its form.
fn read_token(path: &Path) -> Option<GrantAccessToken> {
    directory::read_token(path).and_then(|bytes| GrantAccessToken::new(&bytes))
}

/// What an admission or a refresh asks Cloud with and keeps its tokens in.
pub(crate) struct Asking<'a> {
    pub(crate) cloud: &'a dyn AgentLoginCloud,
    pub(crate) files: &'a Arc<TokenFiles>,
    pub(crate) boot_id: &'a str,
    /// The run of the assignment, which each entry records (REQ-GRANT-88).
    pub(crate) run_id: &'a str,
    pub(crate) assignment_id: &'a str,
    pub(crate) now: OffsetDateTime,
}

/// Which providers an admission asks Cloud for the token of, by Cloud's
/// list of the assignment's providers (REQ-GRANT-86).
enum Listed {
    /// Cloud has no list: each of the runner's three is asked for, as
    /// before it, and no other provider.
    NoRoute,
    Named {
        /// The runner's three the list names by Cloud's id; the others are
        /// not authorized.
        runner: Vec<AgentLoginProvider>,
        /// Each other provider the list names, in its order, as the runner
        /// checked the names its entry gives.
        configured: Vec<Configured>,
    },
}

impl Listed {
    fn names(&self, provider: AgentLoginProvider) -> bool {
        match self {
            Self::NoRoute => true,
            Self::Named { runner, .. } => runner.contains(&provider),
        }
    }

    fn configured(&self) -> &[Configured] {
        match self {
            Self::NoRoute => &[],
            Self::Named { configured, .. } => configured,
        }
    }
}

/// A provider other than the runner's three that Cloud lists, a token
/// service included, as the runner checked the two names its entry gives
/// (`usable_names`).
enum Configured {
    /// Its token may be offered under both names (INV-RUNNER-32).
    Checked(Offering),
    /// A name may not carry its token, or a token service's id is one the
    /// OAuth list also names: the entry is not used, and its provider is
    /// asked for nothing (REQ-GRANT-87, REQ-INTEGRATION-12).
    Refused(GrantProvider),
}

impl Configured {
    /// The entry of `listed`, as `provider`, checked against the names of
    /// each configured entry before it, `earlier`, which it then joins; an
    /// entry `refused` already is refused whatever its names.
    fn check<'l>(
        listed: &'l ListedProvider,
        provider: GrantProvider,
        refused: bool,
        earlier: &mut Vec<&'l str>,
    ) -> Self {
        let usable = !refused && usable_names(&listed.token, &listed.token_file, earlier);
        earlier.extend([listed.token.as_str(), listed.token_file.as_str()]);
        if usable {
            Self::Checked(Offering {
                provider,
                variable: listed.token.clone(),
                file_variable: listed.token_file.clone(),
            })
        } else {
            Self::Refused(provider)
        }
    }

    fn provider(&self) -> &GrantProvider {
        match self {
            Self::Checked(offering) => &offering.provider,
            Self::Refused(provider) => provider,
        }
    }
}

/// What a list answered that the admission uses: its entries, none for a
/// Cloud without the route, or the failure of a list Cloud did not answer
/// usefully, `assignment_not_current` as Cloud's 409 as the token route's
/// is.
fn listed_entries(
    answer: &Result<Vec<ListedProvider>, GrantProvidersFailure>,
) -> Result<Option<&[ListedProvider]>, CloudFailure> {
    match answer {
        Ok(providers) => Ok(Some(providers)),
        Err(GrantProvidersFailure::NoRoute) => Ok(None),
        Err(GrantProvidersFailure::AssignmentNotCurrent) => {
            Err(CloudFailure::Status(ASSIGNMENT_NOT_CURRENT_STATUS))
        }
        Err(GrantProvidersFailure::Cloud(failure)) => Err(*failure),
    }
}

/// The outcome of a request for one provider, before it is recorded: the
/// state and the token issued that the readiness entry of one of the
/// runner's three records, and the record of any provider names.
struct Settled {
    provider: GrantProvider,
    state: AgentLoginState,
    issued: Option<IssuedGrant>,
    failure: Option<Failure>,
    /// The token kept in its file.
    kept: Option<HeldToken>,
}

impl Asking<'_> {
    /// Registers the assignment as held, with the hold it answers, removes
    /// what no held assignment owns and clears the assignment's own
    /// directory (INV-RUNNER-28); then asks Cloud once for the assignment's
    /// providers (REQ-GRANT-97) and, at the same time, once for its token
    /// services (REQ-INTEGRATION-12), then for the token of the grant of
    /// each provider the first list names, and of each token service the
    /// second names, that the host, whose environment is `service`, does not
    /// provide (REQ-GRANT-86, REQ-GRANT-87), the token requests at once so
    /// that a Cloud that does not answer them costs one bounded request; the
    /// two lists are independent. A token service is asked for, kept,
    /// offered and recorded as a configured provider is; a Cloud without its
    /// list lists none and records nothing, and a list Cloud did not answer
    /// usefully lists none and is recorded once, with no provider
    /// (REQ-TELEMETRY-46). A Cloud without the OAuth list is asked for each
    /// of the runner's three as before it, one of the three a list does not
    /// name is `not_authorized`, and a list Cloud did not answer usefully
    /// asks for none and leaves each of the three `cloud_unavailable`
    /// (REQ-GRANT-88). A listed provider other than the three is asked for
    /// only when the names its entry gives may carry its token
    /// (`usable_names`, INV-RUNNER-32), and an entry whose names may not is
    /// recorded as a failure of stage `list` (REQ-TELEMETRY-46). A failure to
    /// clear its own directory asks for no token and leaves each provider it
    /// would have asked for `install_failed` with stage `remove`; a leftover
    /// it cannot remove is recorded and withholds nothing. Keeps each
    /// issued token in its file; records the entry of each of the three in
    /// `document`, as `previous` recorded it before, a provider the host
    /// provides as `not_delivered` for the run, each with its source as
    /// `credential_source` settles it from what the hold keeps
    /// (REQ-GRANT-92), and writes one record per changed state or failure,
    /// a provider that has no entry counting as `not_delivered`. It never
    /// fails: an answer it cannot use is the provider's state. Its hold
    /// refreshes through `setup` once started.
    pub(crate) fn admit(
        &self,
        setup: Weak<AgentLoginSetup>,
        service: &ServiceEnvironment,
        previous: Option<&ReadinessDocument>,
        document: &mut ReadinessDocument,
    ) -> AssignmentGrantTokens {
        let recorded = |provider| previous.and_then(|document| document.grant(provider));
        // The hold is made before the assignment is registered, so whatever
        // stops the admission, a panic included, releases it.
        let mut tokens = AssignmentGrantTokens {
            files: Arc::clone(self.files),
            setup,
            boot_id: self.boot_id.to_owned(),
            run_id: self.run_id.to_owned(),
            assignment_id: self.assignment_id.to_owned(),
            held: Vec::new(),
            services: Vec::new(),
            refresh: None,
            publish: None,
        };
        let uncleared = self.files.admit(self.assignment_id);
        let (providers, services) = self.lists();
        // The names of each configured entry so far, used or not, of the
        // OAuth list and then of the token-service list.
        let mut earlier: Vec<&str> = Vec::new();
        let listed = Self::list(&providers, &mut earlier);
        let oauth = listed.as_ref().map_or(&[][..], Listed::configured);
        let services = Self::token_services(&services, oauth, &mut earlier);
        let service_entries = services.as_ref().map_or(&[][..], Vec::as_slice);
        // The token services whose source the report names: each the list
        // named whose names the runner checked, kept or not.
        let checked_services: Vec<Offering> = service_entries
            .iter()
            .filter_map(|entry| match entry {
                Configured::Checked(offering) => Some(offering.clone()),
                Configured::Refused(_) => None,
            })
            .collect();
        let configured: Vec<&Configured> = oauth.iter().chain(service_entries).collect();
        let asked: Vec<GrantProvider> = brokered_providers()
            .filter(|&provider| {
                !service.provides(provider)
                    && listed.as_ref().is_ok_and(|listed| listed.names(provider))
            })
            .filter_map(|provider| GrantProvider::try_from(provider).ok())
            .chain(configured.iter().copied().filter_map(|entry| match entry {
                Configured::Checked(offering) if !offering.provided_by(service) => {
                    Some(offering.provider.clone())
                }
                Configured::Checked(_) | Configured::Refused(_) => None,
            }))
            .filter(|_| uncleared.is_none())
            .collect();
        let answers = self.ask(&asked);
        // What its directory held could not be cleared: each provider it
        // would have asked for fails to be kept.
        let answer = |offering: &Offering| match (
            uncleared,
            answers
                .iter()
                .find(|(asked, _)| **asked == offering.provider),
        ) {
            (Some(failure), _) => self.failed_write(&offering.provider, failure),
            (None, Some((_, answer))) => self.settle(offering, answer),
            (None, None) => {
                self.unavailable(&offering.provider, Stage::Issue, CloudFailure::Transport)
            }
        };
        let mut settled: Vec<Settled> = Vec::new();
        {
            let _held = self.files.held();
            for offering in brokered_providers().filter_map(Offering::runner) {
                let provider = &offering.provider;
                let outcome = match provider.agent_login_provider() {
                    Some(own) if service.provides(own) => {
                        self.settled(provider, AgentLoginState::NotDelivered)
                    }
                    Some(own) => match &listed {
                        Err(failure) => self.unavailable(provider, Stage::List, *failure),
                        Ok(listed) if !listed.names(own) => {
                            self.settled(provider, AgentLoginState::NotAuthorized)
                        }
                        Ok(_) => answer(&offering),
                    },
                    None => continue,
                };
                settled.push(outcome);
            }
            for &entry in &configured {
                let outcome = match entry {
                    // Its entry names its token after a name the runner
                    // offers no token under, or a token service's id is
                    // one the OAuth list names too: an answer it cannot
                    // use.
                    Configured::Refused(provider) => {
                        self.unavailable(provider, Stage::List, CloudFailure::InvalidResponse)
                    }
                    // The host provides it, so it is `not_delivered`, which
                    // it already counts as: nothing is recorded.
                    Configured::Checked(offering) if offering.provided_by(service) => continue,
                    Configured::Checked(offering) => answer(offering),
                };
                settled.push(outcome);
            }
        }
        tokens.held = settled
            .iter_mut()
            .filter_map(|outcome| outcome.kept.take())
            .collect();
        tokens.services = checked_services;
        for outcome in &settled {
            match outcome.provider.agent_login_provider() {
                Some(provider) => {
                    let mut entry = self.entry(provider, outcome);
                    entry.source = credential_source(provider, service, None, Some(&tokens));
                    self.record(Phase::Admission, &Recorded::of(recorded(provider)), outcome);
                    document.record_grant(entry);
                }
                // A provider other than the three has no entry in the
                // document and no source: what it was issued is its record
                // alone, its state compared against `not_delivered` at each
                // admission.
                None => self.record(Phase::Admission, &Recorded::of(None), outcome),
            }
        }
        if let Err(failure) = services {
            self.record_list_failure(failure);
        }
        tokens
    }

    /// Asks Cloud again for the token `step` names, for the assignment
    /// whose files these are (REQ-GRANT-89): keeps a token of a newer
    /// generation, or one issued from another grant or integration token
    /// whatever its generation, in place of the held one, at the same path
    /// in one step; answers the expiry an answer for the held token names,
    /// which the task holds for the file from then on; leaves the file as it
    /// is when Cloud did not answer usefully, unless the held token has
    /// expired, when it removes it, and removes it at a refusal. Writes one
    /// record per changed state, grant, generation or failure, against the
    /// provider's entry as `previous` recorded it for one of the runner's
    /// three, and against what the task carries in `step` for any other
    /// provider. Records the entry of one of the three in `document` when
    /// its state, grant or generation changed; any other provider has no
    /// entry, so its refresh changes no document. Answers what the task does
    /// next, the state, grant and generation now recorded for it to carry
    /// on, and whether it changed `document`. `entries` is `None` while the
    /// entries are another admission's (INV-RUNNER-30): the refresh then
    /// compares against what the task carries, as any other provider's does,
    /// and records no entry. It asks nothing, and changes nothing, once the
    /// runner no longer holds the assignment.
    pub(crate) fn refresh(
        &self,
        step: &RefreshStep,
        entries: Option<(Option<&ReadinessDocument>, &mut ReadinessDocument)>,
    ) -> (Next, Recorded, bool) {
        let offering = &step.offering;
        let provider = &offering.provider;
        if !self.files.holds(self.assignment_id) {
            return (Next::End, step.recorded.clone(), false);
        }
        let answer = self.grant_token(provider);
        let held = self.files.held();
        if !held.contains(self.assignment_id) {
            return (Next::End, step.recorded.clone(), false);
        }
        let (outcome, next) = match &answer {
            // The token in the file is still the current one: issued from
            // the grant or integration token it was, at no newer generation.
            Ok(issued)
                if step.held
                    && issued.grant_id == step.grant_id
                    && issued.generation <= step.generation =>
            {
                (
                    self.ready(provider, issued),
                    Next::Unchanged {
                        expires_at: issued.expires_at,
                        wait: issued.refresh_after,
                    },
                )
            }
            Ok(issued) => {
                let outcome = self.keep(offering, issued);
                let next = match &outcome.kept {
                    Some(kept) => Next::Kept {
                        grant_id: kept.grant_id.clone(),
                        generation: kept.generation,
                        expires_at: kept.expires_at,
                        wait: kept.refresh_after,
                    },
                    // A failed write leaves the earlier file in place.
                    None => Next::Retry { removed: false },
                };
                (outcome, next)
            }
            Err(
                GrantTokenFailure::Cloud(_)
                | GrantTokenFailure::Refused(GrantTokenRefusal::AssignmentNotCurrent),
            ) => {
                let outcome = self.settle(offering, &answer);
                let removed = step.held && step.expired && self.remove(provider, &step.file);
                (outcome, Next::Retry { removed })
            }
            Err(_) => {
                let outcome = self.settle(offering, &answer);
                self.remove(provider, &step.file);
                (outcome, Next::End)
            }
        };
        drop(held);
        let entry = match (provider.agent_login_provider(), entries) {
            (Some(own), Some((previous, document))) => Some((
                own,
                previous.and_then(|document| document.grant(own)),
                document,
            )),
            (Some(_) | None, _) => None,
        };
        let recorded = entry.as_ref().map_or_else(
            || step.recorded.clone(),
            |(_, grant, _)| Recorded::of(*grant),
        );
        self.record(Phase::Refresh, &recorded, &outcome);
        let now = Recorded::outcome(&outcome);
        let changed = match entry {
            Some((own, grant, document)) if now != recorded => {
                // The source is the one the admission settled for the run
                // (INV-RUNNER-31): a refresh changes the file, never the
                // offer.
                let mut entry = self.entry(own, &outcome);
                entry.source = grant
                    .filter(|grant| grant.run_id.as_deref() == Some(self.run_id))
                    .and_then(|grant| grant.source);
                document.record_grant(entry);
                true
            }
            Some(_) | None => false,
        };
        (next, now, changed)
    }

    /// Removes the provider's token file at `file`, which the caller holds
    /// the files' lock for, recording a failure; answers whether it is
    /// gone.
    fn remove(&self, provider: &GrantProvider, file: &Path) -> bool {
        match self.files.remove_at(file) {
            Ok(()) => true,
            Err(error) => {
                self.files.record_removal_failure(
                    Some(record_name(provider)),
                    Some(self.assignment_id),
                    &error,
                );
                false
            }
        }
    }

    /// Cloud's answers for the assignment's providers and its token
    /// services, each asked once with the same request, the second on a
    /// thread of its own so that a Cloud that answers neither costs one
    /// bounded request, or after the first when no thread can be started.
    fn lists(&self) -> Lists {
        let request = GrantProvidersRequest {
            boot_id: self.boot_id.to_owned(),
            assignment_id: self.assignment_id.to_owned(),
        };
        let request = &request;
        std::thread::scope(|scope| {
            let services = std::thread::Builder::new()
                .name("useful-token-services".to_owned())
                .spawn_scoped(scope, move || self.cloud.token_services(request));
            let providers = self.cloud.grant_providers(request);
            let services = match services {
                // A request whose thread panicked got no answer.
                Ok(thread) => thread
                    .join()
                    .unwrap_or(Err(GrantProvidersFailure::Cloud(CloudFailure::Transport))),
                Err(_) => self.cloud.token_services(request),
            };
            (providers, services)
        })
    }

    /// Cloud's list of the assignment's providers (REQ-GRANT-97) as it
    /// answered it: the runner's three it names, and each other provider it
    /// names with the names its entry gives, checked in the list's order
    /// against `earlier`; a list Cloud did not answer usefully is its
    /// failure.
    fn list<'l>(
        answer: &'l Result<Vec<ListedProvider>, GrantProvidersFailure>,
        earlier: &mut Vec<&'l str>,
    ) -> Result<Listed, CloudFailure> {
        let Some(providers) = listed_entries(answer)? else {
            return Ok(Listed::NoRoute);
        };
        let mut runner = Vec::new();
        let mut configured = Vec::new();
        for listed in providers {
            let Some(provider) = GrantProvider::listed(&listed.provider) else {
                continue;
            };
            if let Some(own) = provider.agent_login_provider() {
                runner.push(own);
                continue;
            }
            configured.push(Configured::check(listed, provider, false, earlier));
        }
        Ok(Listed::Named { runner, configured })
    }

    /// Cloud's list of the assignment's token services
    /// (REQ-INTEGRATION-12) as it answered it: each entry checked in the
    /// list's order against `earlier`, which holds the names of the OAuth
    /// list's configured entries `oauth` first, and refused when one of
    /// them has its id; none for a Cloud without the list, and a list Cloud
    /// did not answer usefully is its failure.
    fn token_services<'l>(
        answer: &'l Result<Vec<ListedProvider>, GrantProvidersFailure>,
        oauth: &[Configured],
        earlier: &mut Vec<&'l str>,
    ) -> Result<Vec<Configured>, CloudFailure> {
        let Some(services) = listed_entries(answer)? else {
            return Ok(Vec::new());
        };
        Ok(services
            .iter()
            .filter_map(|listed| {
                let provider = GrantProvider::token_service(&listed.provider)?;
                let shared = oauth
                    .iter()
                    .any(|entry| entry.provider().cloud_id() == provider.cloud_id());
                Some(Configured::check(listed, provider, shared, earlier))
            })
            .collect())
    }

    /// Cloud's answer for the provider's token.
    fn grant_token(&self, provider: &GrantProvider) -> Result<IssuedToken, GrantTokenFailure> {
        self.cloud.grant_token(&GrantTokenRequest {
            boot_id: self.boot_id.to_owned(),
            assignment_id: self.assignment_id.to_owned(),
            provider: provider.clone(),
        })
    }

    /// Cloud's answer for each of `providers`, each request on a thread of
    /// its own, or on this one when no thread can be started.
    fn ask<'p>(
        &self,
        providers: &'p [GrantProvider],
    ) -> Vec<(&'p GrantProvider, Result<IssuedToken, GrantTokenFailure>)> {
        std::thread::scope(|scope| {
            let started: Vec<_> = providers
                .iter()
                .map(|provider| {
                    let spawned = std::thread::Builder::new()
                        .name("useful-grant-token".to_owned())
                        .spawn_scoped(scope, move || self.grant_token(provider));
                    (provider, spawned)
                })
                .collect();
            started
                .into_iter()
                .map(|(provider, spawned)| {
                    let answer = match spawned {
                        // A request whose thread panicked got no answer.
                        Ok(thread) => thread
                            .join()
                            .unwrap_or(Err(GrantTokenFailure::Cloud(CloudFailure::Transport))),
                        Err(_) => self.grant_token(provider),
                    };
                    (provider, answer)
                })
                .collect()
        })
    }

    /// The provider's outcome from Cloud's answer; the caller holds the
    /// files' lock.
    fn settle(
        &self,
        offering: &Offering,
        answer: &Result<IssuedToken, GrantTokenFailure>,
    ) -> Settled {
        let provider = &offering.provider;
        let state = match answer {
            Ok(issued) => return self.keep(offering, issued),
            Err(GrantTokenFailure::Cloud(failure)) => {
                return self.unavailable(provider, Stage::Issue, *failure);
            }
            // Cloud answered, but not for an assignment it counts as this
            // runner's: nothing about the grant has changed, and the request
            // failed with Cloud's 409 as any other status fails it
            // (REQ-TELEMETRY-46).
            Err(GrantTokenFailure::Refused(GrantTokenRefusal::AssignmentNotCurrent)) => {
                return self.unavailable(
                    provider,
                    Stage::Issue,
                    CloudFailure::Status(ASSIGNMENT_NOT_CURRENT_STATUS),
                );
            }
            Err(
                GrantTokenFailure::NoRoute
                | GrantTokenFailure::Refused(GrantTokenRefusal::Unauthorized),
            ) => AgentLoginState::NotAuthorized,
            Err(GrantTokenFailure::Refused(GrantTokenRefusal::Revoked)) => AgentLoginState::Revoked,
            Err(GrantTokenFailure::Refused(GrantTokenRefusal::ReconnectRequired)) => {
                AgentLoginState::ReconnectRequired
            }
            Err(GrantTokenFailure::Refused(GrantTokenRefusal::ProviderUnavailable)) => {
                AgentLoginState::UnsupportedTarget
            }
        };
        self.settled(provider, state)
    }

    /// The provider `ready` with `issued`, which its file already holds.
    fn ready(&self, provider: &GrantProvider, issued: &IssuedToken) -> Settled {
        Settled {
            issued: Some(IssuedGrant {
                grant_id: issued.grant_id.clone(),
                generation: issued.generation,
                expires_at: issued.expires_at,
            }),
            ..self.settled(provider, AgentLoginState::Ready)
        }
    }

    /// Keeps `issued` in the provider's file in the assignment's directory,
    /// mode `0600`, in one step: the runner's own file for its three, and a
    /// file of its own in `grant-tokens` for any other provider. Each
    /// directory on the way, the agent-login directory, `assignments`, the
    /// assignment's and its `grant-tokens`, is made a private one first.
    fn keep(&self, offering: &Offering, issued: &IssuedToken) -> Settled {
        let provider = &offering.provider;
        let directory = &self.files.directory;
        let file = match provider.agent_login_provider() {
            Some(own) => directory.grant_token(self.assignment_id, own),
            None => directory.configured_grant_token(self.assignment_id, provider.cloud_id()),
        };
        let Some((path, temporary_prefix)) = file else {
            return self.failed_write(
                provider,
                Failure {
                    stage: Stage::Write,
                    cause: FailureCause::Io(io::ErrorKind::InvalidInput),
                },
            );
        };
        let mut directories: Vec<&Path> = path
            .ancestors()
            .skip(1)
            .take_while(|ancestor| ancestor.starts_with(directory.path()))
            .collect();
        directories.reverse();
        let written = directories
            .into_iter()
            .try_for_each(directory::ensure_private_directory)
            .and_then(|()| {
                directory::write_file(&path, issued.access_token.as_bytes(), temporary_prefix)
            });
        match written {
            Ok(()) => Settled {
                kept: Some(HeldToken {
                    offering: offering.clone(),
                    grant_id: issued.grant_id.clone(),
                    file: path,
                    generation: issued.generation,
                    expires_at: issued.expires_at,
                    refresh_after: issued.refresh_after,
                }),
                ..self.ready(provider, issued)
            },
            Err(error) => self.failed_write(provider, Failure::io(Stage::Write, &error)),
        }
    }

    /// A provider whose token the runner could not keep, or for which what
    /// was at its assignment's directory could not be removed.
    fn failed_write(&self, provider: &GrantProvider, failure: Failure) -> Settled {
        Settled {
            failure: Some(failure),
            ..self.settled(provider, AgentLoginState::InstallFailed)
        }
    }

    /// A request of `stage` Cloud did not answer usefully.
    fn unavailable(
        &self,
        provider: &GrantProvider,
        stage: Stage,
        failure: CloudFailure,
    ) -> Settled {
        Settled {
            failure: Some(Failure {
                stage,
                cause: failure.cause(),
            }),
            ..self.settled(provider, AgentLoginState::CloudUnavailable)
        }
    }

    /// The provider in `state` with no token.
    fn settled(&self, provider: &GrantProvider, state: AgentLoginState) -> Settled {
        Settled {
            provider: provider.clone(),
            state,
            issued: None,
            failure: None,
            kept: None,
        }
    }

    /// The readiness entry of `provider`, one of the runner's three, that
    /// `outcome` records: checked now, for the run and not ended.
    fn entry(&self, provider: AgentLoginProvider, outcome: &Settled) -> GrantReadiness {
        GrantReadiness {
            provider,
            state: outcome.state,
            checked_at: Some(self.now),
            issued: outcome.issued.clone(),
            run_id: Some(self.run_id.to_owned()),
            ended_at: None,
            source: None,
        }
    }

    /// Writes the provider's record of `phase` when its state, grant or
    /// generation changed from `previous`, or a step failed; none otherwise.
    /// The record names the provider as `record_name` does, never a name it
    /// is offered under, a path or a token.
    fn record(&self, phase: Phase, previous: &Recorded, outcome: &Settled) {
        if *previous == Recorded::outcome(outcome) && outcome.failure.is_none() {
            return;
        }
        let mut attributes = vec![
            KeyValue::new(
                attribute::GRANT_TOKEN_PROVIDER,
                record_name(&outcome.provider).to_owned(),
            ),
            KeyValue::new(attribute::GRANT_TOKEN_PHASE, phase.as_str()),
            KeyValue::new(attribute::GRANT_TOKEN_STATE, outcome.state.as_str()),
            KeyValue::new(attribute::ASSIGNMENT_ID, self.assignment_id.to_owned()),
        ];
        attributes.extend(Recorded::outcome(outcome).generation.map(|generation| {
            KeyValue::new(
                attribute::GRANT_TOKEN_GENERATION,
                telemetry::integer(generation),
            )
        }));
        attributes.extend(outcome.failure.into_iter().flat_map(failure_attributes));
        self.files
            .recorder
            .start(GRANT_TOKEN_EVENT, attributes)
            .finish(if outcome.failure.is_some() {
                Outcome::Failure
            } else {
                Outcome::Success
            });
    }

    /// Records a token-service list Cloud did not answer usefully
    /// (REQ-INTEGRATION-12, REQ-TELEMETRY-46): a failure of the admission's
    /// stage `list` with its cause, which names no provider and no state,
    /// since the list named none.
    fn record_list_failure(&self, failure: CloudFailure) {
        let mut attributes = vec![
            KeyValue::new(attribute::GRANT_TOKEN_PHASE, Phase::Admission.as_str()),
            KeyValue::new(attribute::ASSIGNMENT_ID, self.assignment_id.to_owned()),
        ];
        attributes.extend(failure_attributes(Failure {
            stage: Stage::List,
            cause: failure.cause(),
        }));
        self.files
            .recorder
            .start(GRANT_TOKEN_EVENT, attributes)
            .finish(Outcome::Failure);
    }
}

/// A failure's stage, closed cause and Cloud's HTTP status when the cause
/// is one; never a path, a message or a token.
fn failure_attributes(failure: Failure) -> Vec<KeyValue> {
    let mut attributes = vec![
        KeyValue::new(attribute::GRANT_TOKEN_FAILURE_STAGE, failure.stage.as_str()),
        KeyValue::new(attribute::GRANT_TOKEN_FAILURE_CAUSE, failure.cause.as_str()),
    ];
    if let FailureCause::HttpStatus(status) = failure.cause {
        attributes.push(KeyValue::new(
            attribute::GRANT_TOKEN_HTTP_STATUS,
            i64::from(status),
        ));
    }
    attributes
}

#[cfg(test)]
mod tests;
