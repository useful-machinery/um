//! The grant tokens of the assignment the serving runner holds
//! (REQ-GRANT-86): at each admission the runner asks Cloud for the
//! assignment's providers, then for the token of the run's grant of each
//! provider listed that the host does not provide (REQ-GRANT-87), keeps each
//! token it is issued in a private file of its own in its agent-login
//! directory, records the outcome of each of GitHub, GitLab.com and Linear
//! for the run in the readiness document (REQ-GRANT-88), and writes a
//! `runner.grant_token` record for each changed state and each failure
//! (REQ-TELEMETRY-46). A token file is only ever the token of the assignment
//! the runner holds (INV-RUNNER-28): the admission removes every token file
//! before it uses any answer, an idle setup removes what an interrupted
//! runner left, and the hold an admission returns removes them when the
//! assignment ends. The admission offers each token it holds, and its file,
//! to the steps of its workflow that pass their names or, for the runner's
//! own three, declare the provider (REQ-WFEXEC-78): the runner's own names
//! for its three, and the names of a listed provider's entry once it has
//! checked them (REQ-GRANT-87, INV-RUNNER-32). While
//! the assignment runs, a task the hold owns keeps each file current
//! (REQ-GRANT-89): it asks again when the wait Cloud named has passed and
//! replaces the file in one step, retries a request Cloud did not answer,
//! removes the file at a refusal or once the held token has expired, and
//! stops when the hold is dropped. Dropping the hold then marks the entries
//! ended, unless a later admission has recorded its own since
//! (INV-RUNNER-30).

use std::ffi::{OsStr, OsString};
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
    /// Asking Cloud for the assignment's providers.
    List,
    /// Asking Cloud for the token.
    Issue,
    /// Keeping it in its file.
    Write,
    /// Removing the provider's file.
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

/// The token files `TokenFiles::remove_all` could not remove.
struct RemovalFailures {
    /// Each of the runner's three whose file stayed.
    runner: Vec<(AgentLoginProvider, io::Error)>,
    /// The failure to remove `grant-tokens`, the directory of every other
    /// provider's file.
    configured: Option<io::Error>,
}

impl RemovalFailures {
    /// The failure to remove the file of one of the runner's three.
    fn of(&self, provider: AgentLoginProvider) -> Option<Failure> {
        self.runner
            .iter()
            .find(|(failed, _)| *failed == provider)
            .map(|(_, error)| Failure::io(Stage::Remove, error))
    }
}

/// The runner's grant token files and the assignment whose tokens they
/// hold. Every write and removal of a token file is made under its lock, so
/// the end of one assignment never removes the files of the next.
pub(crate) struct TokenFiles {
    directory: LoginDirectory,
    recorder: Arc<Recorder>,
    /// The assignment the files were written for, while they may hold its
    /// tokens.
    holder: Mutex<Option<String>>,
}

impl TokenFiles {
    pub(crate) fn new(directory: LoginDirectory, recorder: Arc<Recorder>) -> Self {
        Self {
            directory,
            recorder,
            holder: Mutex::new(None),
        }
    }

    fn holder(&self) -> std::sync::MutexGuard<'_, Option<String>> {
        self.holder.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Whether the files are still those of `assignment_id`.
    fn held_for(&self, assignment_id: &str) -> bool {
        self.holder().as_deref() == Some(assignment_id)
    }

    /// Removes the token file of one of the runner's three, and anything
    /// else at its location.
    fn remove(&self, provider: AgentLoginProvider) -> io::Result<()> {
        let Some((path, _)) = self.directory.grant_token(provider) else {
            return Ok(());
        };
        self.remove_at(&path)
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

    /// Removes every token file: those of the runner's three, then
    /// `grant-tokens` whole, with any temporary file a write left in it.
    fn remove_all(&self) -> RemovalFailures {
        RemovalFailures {
            runner: brokered_providers()
                .filter_map(|provider| self.remove(provider).err().map(|error| (provider, error)))
                .collect(),
            configured: self.remove_at(&self.directory.grant_tokens()).err(),
        }
    }

    /// Removes every token file an earlier assignment or an interrupted
    /// runner left, for a setup while the runner holds no assignment; each
    /// file of the runner's three it cannot remove is recorded with its
    /// provider, and `grant-tokens`, which no provider is known for, with
    /// none.
    pub(crate) fn remove_leftovers(&self) {
        let mut holder = self.holder();
        let failures = self.remove_all();
        for (provider, error) in &failures.runner {
            self.record_removal_failure(Some(provider.as_str()), None, error);
        }
        if let Some(error) = &failures.configured {
            self.record_removal_failure(None, None, error);
        }
        *holder = None;
    }

    /// Removes the token files of `held`, each a provider as its record
    /// names it with the file kept for `assignment_id`, one by one, when
    /// they are still its own; each file it cannot remove is recorded. The
    /// emptied `grant-tokens` is left for the next setup to remove.
    fn release(&self, assignment_id: &str, held: &[(String, PathBuf)]) {
        let mut holder = self.holder();
        if holder.as_deref() != Some(assignment_id) {
            return;
        }
        for (provider, file) in held {
            if let Err(error) = self.remove_at(file) {
                self.record_removal_failure(Some(provider), Some(assignment_id), &error);
            }
        }
        *holder = None;
    }

    /// Records a token file that could not be removed, with the provider
    /// as its record names it when one is known.
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
    /// The `pgr_` ID of the grant, which the admission reports
    /// (REQ-GRANT-93).
    grant_id: String,
    file: PathBuf,
    generation: u64,
    expires_at: OffsetDateTime,
    refresh_after: Duration,
}

/// What an admission holds of its assignment's grant tokens, which the slot
/// carries until the assignment ends, whether or not it kept a token:
/// dropping it stops its refresh task, then removes the token files it
/// holds and marks the entries its admission recorded ended
/// (INV-RUNNER-30), on a blocking thread when there is a runtime to run one
/// on.
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
                generation: held.generation,
                expires_at: Some(held.expires_at),
                recorded: Recorded {
                    state: AgentLoginState::Ready,
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
    /// The generation of the token the runner last kept.
    generation: u64,
    /// The expiry of the token in the file; `None` once it is removed.
    expires_at: Option<OffsetDateTime>,
    /// The state and generation the assignment last recorded of the
    /// provider: `ready` with the kept generation when the task starts,
    /// then what each refresh recorded.
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
    /// The generation of the token the runner last kept.
    generation: u64,
    /// Whether its file still holds that token.
    held: bool,
    /// Whether the token's expiry has passed, on the task's clock.
    expired: bool,
    /// The state and generation the task carries for the provider, which
    /// the refresh of a provider with no readiness entry compares against.
    recorded: Recorded,
}

/// The state and generation last recorded of a provider, which a refresh
/// compares its outcome against to tell whether to write a record
/// (REQ-TELEMETRY-46) and, for one of the runner's three, its readiness
/// entry: the three's from the readiness document, and any other
/// provider's as the refresh task carries it for the assignment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Recorded {
    state: AgentLoginState,
    generation: Option<u64>,
}

impl Recorded {
    /// What `grant` records; a provider never recorded is `not_delivered`.
    fn of(grant: Option<&GrantReadiness>) -> Self {
        Self {
            state: grant.map_or(AgentLoginState::NotDelivered, |grant| grant.state),
            generation: grant
                .and_then(|grant| grant.issued.as_ref())
                .map(|issued| issued.generation),
        }
    }

    /// What `outcome` records.
    fn outcome(outcome: &Settled) -> Self {
        Self {
            state: outcome.state,
            generation: outcome.issued.as_ref().map(|issued| issued.generation),
        }
    }
}

/// What the refresh task does after a refresh.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Next {
    /// A token of a newer generation is in the file: ask again after `wait`.
    Kept {
        generation: u64,
        expires_at: OffsetDateTime,
        wait: Duration,
    },
    /// The token in the file is the current one: ask again after `wait`.
    Unchanged { wait: Duration },
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
                    generation: due.generation,
                    held: due.expires_at.is_some(),
                    expired: due
                        .expires_at
                        .is_some_and(|expires_at| sleeper.utc_now() >= expires_at),
                    recorded: due.recorded,
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
                        generation,
                        expires_at,
                        wait,
                    } => {
                        due.generation = generation;
                        due.expires_at = Some(expires_at);
                        due.at = after(answered_at, wait);
                        due.retry = MINIMUM_WAIT;
                    }
                    Next::Unchanged { wait } => {
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
        let held: Vec<(String, PathBuf)> = self
            .held
            .iter()
            .map(|held| {
                (
                    record_name(&held.offering.provider).to_owned(),
                    held.file.clone(),
                )
            })
            .collect();
        // The files are released first, so a refresh still waiting for the
        // pass lock writes nothing after the end is marked.
        let release = move || {
            files.release(&assignment_id, &held);
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
        let Offering {
            variable,
            file_variable,
            ..
        } = &held.offering;
        // The host provides a provider either of whose names `environment`
        // carries, whatever the admission held (INV-RUNNER-17).
        if [variable, file_variable]
            .into_iter()
            .any(|name| environment.variable(OsStr::new(name)).is_some())
        {
            continue;
        }
        let Some(token) = read_token(&held.file) else {
            continue;
        };
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

/// A provider other than the runner's three that Cloud lists, as the runner
/// checked the two names its entry gives (`usable_names`).
enum Configured {
    /// Its token may be offered under both names (INV-RUNNER-32).
    Checked(Offering),
    /// A name may not carry its token: the entry is not used, and its
    /// provider is asked for nothing (REQ-GRANT-87).
    Refused(GrantProvider),
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
    /// Once every token file is gone, asks Cloud once for the assignment's
    /// providers (REQ-GRANT-97), then for the token of the grant of each
    /// provider the list names and the host, whose environment is
    /// `service`, does not provide (REQ-GRANT-86, REQ-GRANT-87), the token
    /// requests at once so that a Cloud that does not answer them costs one
    /// bounded request; a Cloud without the list is asked for each of the
    /// runner's three as before it, one of the three a list does not name is
    /// `not_authorized`, and a list Cloud did not answer usefully asks for
    /// none and leaves each of the three `cloud_unavailable`
    /// (REQ-GRANT-88). A listed provider other than the three is asked for
    /// only when the names its entry gives may carry its token
    /// (`usable_names`, INV-RUNNER-32), and an entry whose names may not is
    /// recorded as a failure of stage `list` (REQ-TELEMETRY-46). Keeps each
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
        let removal = {
            let mut holder = self.files.holder();
            *holder = None;
            self.files.remove_all()
        };
        let listed = self.list();
        let configured = listed.as_ref().map_or(&[][..], Listed::configured);
        let asked: Vec<GrantProvider> = brokered_providers()
            .filter(|&provider| {
                !service.provides(provider)
                    && removal.of(provider).is_none()
                    && listed.as_ref().is_ok_and(|listed| listed.names(provider))
            })
            .filter_map(|provider| GrantProvider::try_from(provider).ok())
            .chain(configured.iter().filter_map(|entry| match entry {
                Configured::Checked(offering)
                    if !offering.provided_by(service) && removal.configured.is_none() =>
                {
                    Some(offering.provider.clone())
                }
                Configured::Checked(_) | Configured::Refused(_) => None,
            }))
            .collect();
        let answers = self.ask(&asked);
        let answer = |offering: &Offering| match answers
            .iter()
            .find(|(asked, _)| **asked == offering.provider)
        {
            Some((_, answer)) => self.settle(offering, answer),
            None => self.unavailable(&offering.provider, Stage::Issue, CloudFailure::Transport),
        };
        let mut settled: Vec<Settled> = Vec::new();
        {
            let mut holder = self.files.holder();
            for offering in brokered_providers().filter_map(Offering::runner) {
                let provider = &offering.provider;
                let outcome = match provider.agent_login_provider() {
                    Some(own) => match removal.of(own) {
                        Some(failure) => self.failed_write(provider, failure),
                        None if service.provides(own) => {
                            self.settled(provider, AgentLoginState::NotDelivered)
                        }
                        None => match &listed {
                            Err(failure) => self.unavailable(provider, Stage::List, *failure),
                            Ok(listed) if !listed.names(own) => {
                                self.settled(provider, AgentLoginState::NotAuthorized)
                            }
                            Ok(_) => answer(&offering),
                        },
                    },
                    None => continue,
                };
                settled.push(outcome);
            }
            for entry in configured {
                let outcome = match entry {
                    // Its entry names its token after a name the runner
                    // offers no token under: an answer it cannot use.
                    Configured::Refused(provider) => {
                        self.unavailable(provider, Stage::List, CloudFailure::InvalidResponse)
                    }
                    // The host provides it, so it is `not_delivered`, which
                    // it already counts as: nothing is recorded.
                    Configured::Checked(offering) if offering.provided_by(service) => continue,
                    Configured::Checked(offering) => match &removal.configured {
                        Some(error) => {
                            self.failed_write(&offering.provider, Failure::io(Stage::Remove, error))
                        }
                        None => answer(offering),
                    },
                };
                settled.push(outcome);
            }
            if settled.iter().any(|outcome| outcome.kept.is_some()) {
                *holder = Some(self.assignment_id.to_owned());
            }
        }
        let tokens = AssignmentGrantTokens {
            files: Arc::clone(self.files),
            setup,
            boot_id: self.boot_id.to_owned(),
            run_id: self.run_id.to_owned(),
            assignment_id: self.assignment_id.to_owned(),
            held: settled
                .iter_mut()
                .filter_map(|outcome| outcome.kept.take())
                .collect(),
            refresh: None,
            publish: None,
        };
        for outcome in &settled {
            match outcome.provider.agent_login_provider() {
                Some(provider) => {
                    let mut entry = self.entry(provider, outcome);
                    entry.source = credential_source(provider, service, None, Some(&tokens));
                    self.record(Phase::Admission, Recorded::of(recorded(provider)), outcome);
                    document.record_grant(entry);
                }
                // A provider other than the three has no entry in the
                // document and no source: what it was issued is its record
                // alone, its state compared against `not_delivered` at each
                // admission.
                None => self.record(Phase::Admission, Recorded::of(None), outcome),
            }
        }
        tokens
    }

    /// Asks Cloud again for the token `step` names, for the assignment
    /// whose files these are (REQ-GRANT-89): keeps a token of a newer
    /// generation in place of the held one, at the same path in one step,
    /// leaves the file as it is when Cloud did not answer usefully, unless
    /// the held token has expired, when it removes it, and removes it at a
    /// refusal. Writes one record per changed state or failure, against the
    /// provider's entry as `previous` recorded it for one of the runner's
    /// three, and against the state the task carries in `step` for any
    /// other provider. Records the entry of one of the three in `document`
    /// when its state or generation changed; any other provider has no
    /// entry, so its refresh changes no document. Answers what the task does
    /// next, the state and generation now recorded for it to carry on, and
    /// whether it changed `document`. It asks nothing, and changes nothing,
    /// once the files are no longer the assignment's.
    pub(crate) fn refresh(
        &self,
        step: &RefreshStep,
        previous: Option<&ReadinessDocument>,
        document: &mut ReadinessDocument,
    ) -> (Next, Recorded, bool) {
        let offering = &step.offering;
        let provider = &offering.provider;
        if !self.files.held_for(self.assignment_id) {
            return (Next::End, step.recorded, false);
        }
        let answer = self.grant_token(provider);
        let holder = self.files.holder();
        if holder.as_deref() != Some(self.assignment_id) {
            return (Next::End, step.recorded, false);
        }
        let (outcome, next) = match &answer {
            // The token in the file is still the current one.
            Ok(issued) if step.held && issued.generation <= step.generation => (
                self.ready(provider, issued),
                Next::Unchanged {
                    wait: issued.refresh_after,
                },
            ),
            Ok(issued) => {
                let outcome = self.keep(offering, issued);
                let next = match &outcome.kept {
                    Some(kept) => Next::Kept {
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
        drop(holder);
        let entry = provider
            .agent_login_provider()
            .map(|own| (own, previous.and_then(|document| document.grant(own))));
        let recorded = entry.map_or(step.recorded, |(_, grant)| Recorded::of(grant));
        self.record(Phase::Refresh, recorded, &outcome);
        let now = Recorded::outcome(&outcome);
        let changed = match entry {
            Some((own, grant)) if now != recorded => {
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

    /// Cloud's list of the assignment's providers (REQ-GRANT-97): the
    /// runner's three it names, and each other provider it names with the
    /// names its entry gives, checked in the list's order; a list Cloud did
    /// not answer usefully is its failure, and `assignment_not_current` is
    /// Cloud's 409 as the token route's is.
    fn list(&self) -> Result<Listed, CloudFailure> {
        let request = GrantProvidersRequest {
            boot_id: self.boot_id.to_owned(),
            assignment_id: self.assignment_id.to_owned(),
        };
        let providers = match self.cloud.grant_providers(&request) {
            Ok(providers) => providers,
            Err(GrantProvidersFailure::NoRoute) => return Ok(Listed::NoRoute),
            Err(GrantProvidersFailure::AssignmentNotCurrent) => {
                return Err(CloudFailure::Status(ASSIGNMENT_NOT_CURRENT_STATUS));
            }
            Err(GrantProvidersFailure::Cloud(failure)) => return Err(failure),
        };
        let mut runner = Vec::new();
        let mut configured = Vec::new();
        // The names of each configured entry so far, used or not.
        let mut earlier: Vec<&str> = Vec::new();
        for listed in &providers {
            let Some(provider) = GrantProvider::listed(&listed.provider) else {
                continue;
            };
            if let Some(own) = provider.agent_login_provider() {
                runner.push(own);
                continue;
            }
            let usable = usable_names(&listed.token, &listed.token_file, &earlier);
            earlier.extend([listed.token.as_str(), listed.token_file.as_str()]);
            configured.push(if usable {
                Configured::Checked(Offering {
                    provider,
                    variable: listed.token.clone(),
                    file_variable: listed.token_file.clone(),
                })
            } else {
                Configured::Refused(provider)
            });
        }
        Ok(Listed::Named { runner, configured })
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

    /// Keeps `issued` in the provider's file, mode `0600`, in one step: the
    /// runner's own file for its three, and a file of its own in
    /// `grant-tokens` for any other provider.
    fn keep(&self, offering: &Offering, issued: &IssuedToken) -> Settled {
        let provider = &offering.provider;
        let directory = &self.files.directory;
        let (file, directories) = match provider.agent_login_provider() {
            Some(own) => (
                directory.grant_token(own),
                vec![directory.path().to_owned()],
            ),
            None => (
                Some(directory.configured_grant_token(provider.cloud_id())),
                vec![directory.path().to_owned(), directory.grant_tokens()],
            ),
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
        let written = directories
            .iter()
            .try_for_each(|directory| directory::ensure_private_directory(directory))
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

    /// A provider whose token the runner could not keep, or whose earlier
    /// file it could not remove.
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

    /// Writes the provider's record of `phase` when its state or generation
    /// changed from `previous`, or a step failed; none otherwise. The record
    /// names the provider as `record_name` does, never a name it is offered
    /// under, a path or a token.
    fn record(&self, phase: Phase, previous: Recorded, outcome: &Settled) {
        if previous == Recorded::outcome(outcome) && outcome.failure.is_none() {
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
