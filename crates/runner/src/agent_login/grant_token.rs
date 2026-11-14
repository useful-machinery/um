//! The grant tokens of the assignment the serving runner holds
//! (REQ-GRANT-86): at each admission the runner asks Cloud for the token of
//! the run's grant of each OAuth provider, keeps each token it is issued in a
//! private file of its own in its agent-login directory, records each
//! provider's outcome for the run in the readiness document (REQ-GRANT-88),
//! and writes a `runner.grant_token` record for each changed state and each
//! failure (REQ-TELEMETRY-46). A token file is only ever the token of the assignment
//! the runner holds (INV-RUNNER-28): the admission removes every token file
//! before it uses any answer, an idle setup removes what an interrupted
//! runner left, and the hold an admission returns removes them when the
//! assignment ends. The admission offers each token it holds, and its file,
//! to the steps of its workflow that pass their names (REQ-GRANT-87). While
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
use um_execution::EnvironmentSnapshot;

use super::FailureCause;
use super::cloud::{
    AgentLoginCloud, CloudFailure, GrantAccessToken, GrantTokenFailure, GrantTokenRefusal,
    GrantTokenRequest, IssuedToken,
};
use super::directory::{self, LoginDirectory};
use super::readiness::{GrantReadiness, IssuedGrant, ReadinessDocument, brokered_providers};
use super::setup::AgentLoginSetup;
use crate::control_protocol::{AgentLoginProvider, AgentLoginState};
use crate::service::Sleeper;
use crate::telemetry::{self, Outcome, Recorder, attribute};

/// The record of a changed grant-token state or a failure.
pub(crate) const GRANT_TOKEN_EVENT: &str = "runner.grant_token";

/// The variables each provider's grant token is offered as: the token, and
/// the path of its file (REQ-GRANT-87).
const OFFERED_VARIABLES: [(AgentLoginProvider, &str, &str); 3] = [
    (AgentLoginProvider::Github, "GH_TOKEN", "GH_TOKEN_FILE"),
    (
        AgentLoginProvider::Gitlab,
        "GITLAB_TOKEN",
        "GITLAB_TOKEN_FILE",
    ),
    (
        AgentLoginProvider::Linear,
        "UM_LINEAR_ACCESS_TOKEN",
        "UM_LINEAR_ACCESS_TOKEN_FILE",
    ),
];

/// The least a refresh waits before asking again after an answer
/// (REQ-GRANT-89), and the wait after the first failure in a row.
const MINIMUM_WAIT: Duration = Duration::from_secs(5);

/// The longest wait after a failure: each failure in a row doubles the wait
/// from `MINIMUM_WAIT` up to this (REQ-GRANT-89).
const MAXIMUM_RETRY: Duration = Duration::from_secs(5 * 60);

/// The status Cloud refuses `assignment_not_current` with (REQ-GRANT-84),
/// which a record of that failed request carries (REQ-TELEMETRY-46).
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

    /// Removes the provider's token file, and anything else at its
    /// location. A directory that is not a real directory holds no file of
    /// the runner's, so there is nothing to remove from it.
    fn remove(&self, provider: AgentLoginProvider) -> io::Result<()> {
        let Some((path, _)) = self.directory.grant_token(provider) else {
            return Ok(());
        };
        match self.directory.remove_tree(&path) {
            Err(error) if error.kind() == io::ErrorKind::NotADirectory => Ok(()),
            removed => removed,
        }
    }

    /// Removes every token file, with the failure of each provider whose
    /// file could not be removed.
    fn remove_all(&self) -> Vec<(AgentLoginProvider, io::Error)> {
        brokered_providers()
            .filter_map(|provider| self.remove(provider).err().map(|error| (provider, error)))
            .collect()
    }

    /// Removes every token file an earlier assignment or an interrupted
    /// runner left, for a setup while the runner holds no assignment; each
    /// file it cannot remove is recorded.
    pub(crate) fn remove_leftovers(&self) {
        let mut holder = self.holder();
        for (provider, error) in self.remove_all() {
            self.record_removal_failure(provider, None, &error);
        }
        *holder = None;
    }

    /// Removes the token files of `held`, kept for `assignment_id`, when
    /// they are still its own; each file it cannot remove is recorded.
    fn release(&self, assignment_id: &str, held: &[AgentLoginProvider]) {
        let mut holder = self.holder();
        if holder.as_deref() != Some(assignment_id) {
            return;
        }
        for &provider in held {
            if let Err(error) = self.remove(provider) {
                self.record_removal_failure(provider, Some(assignment_id), &error);
            }
        }
        *holder = None;
    }

    fn record_removal_failure(
        &self,
        provider: AgentLoginProvider,
        assignment_id: Option<&str>,
        error: &io::Error,
    ) {
        let mut attributes = vec![KeyValue::new(
            attribute::GRANT_TOKEN_PROVIDER,
            provider.as_str(),
        )];
        attributes.extend(failure_attributes(Failure::io(Stage::Remove, error)));
        attributes.extend(assignment_id.map(|assignment_id| {
            KeyValue::new(attribute::ASSIGNMENT_ID, assignment_id.to_owned())
        }));
        self.recorder
            .start(GRANT_TOKEN_EVENT, attributes)
            .finish(Outcome::Failure);
    }
}

/// A token the runner kept in its file, and the wait Cloud named with it.
#[derive(Clone, Debug)]
struct HeldToken {
    provider: AgentLoginProvider,
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
/// document it records to: the live status, so that `um runner status`
/// shows it without waiting for a pass.
pub(crate) type PublishReadiness = Arc<dyn Fn(&ReadinessDocument) + Send + Sync>;

impl AssignmentGrantTokens {
    /// The file the admission kept the provider's token in, if it kept one.
    fn file(&self, provider: AgentLoginProvider) -> Option<&Path> {
        self.held
            .iter()
            .find(|held| held.provider == provider)
            .map(|held| held.file.as_path())
    }

    /// Publishes with `publish` each readiness document a refresh of this
    /// hold records, and the one its end records.
    pub(crate) fn publish_with(&mut self, publish: PublishReadiness) {
        self.publish = Some(publish);
    }

    /// Starts the task that keeps each token file this hold has current
    /// while it is held (REQ-GRANT-89): for each provider, it waits on
    /// `sleeper` the time Cloud named with its token, from now, then asks
    /// again, and publishes each readiness document a refresh records as
    /// `publish_with` named. Nothing is started without a token, without a
    /// runtime to run on, or a second time.
    pub(crate) fn keep_current(&mut self, sleeper: Arc<dyn Sleeper>) {
        if self.held.is_empty() || self.refresh.is_some() {
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
                provider: held.provider,
                generation: held.generation,
                expires_at: Some(held.expires_at),
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
    provider: AgentLoginProvider,
    /// The generation of the token the runner last kept.
    generation: u64,
    /// The expiry of the token in the file; `None` once it is removed.
    expires_at: Option<OffsetDateTime>,
    at: Instant,
    /// The wait after the next failure.
    retry: Duration,
    /// Whether the task asks no more for the provider.
    ended: bool,
}

/// One refresh of a provider's token, as the setup makes it.
pub(crate) struct RefreshStep {
    provider: AgentLoginProvider,
    /// The generation of the token the runner last kept.
    generation: u64,
    /// Whether its file still holds that token.
    held: bool,
    /// Whether the token's expiry has passed, on the task's clock.
    expired: bool,
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
                    provider: due.provider,
                    generation: due.generation,
                    held: due.expires_at.is_some(),
                    expired: due
                        .expires_at
                        .is_some_and(|expires_at| sleeper.utc_now() >= expires_at),
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
                    Ok(Some((next, recorded))) => {
                        if let (Some(document), Some(publish)) = (recorded, &publish) {
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
        let held: Vec<AgentLoginProvider> = self.held.iter().map(|held| held.provider).collect();
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
/// (REQ-GRANT-87): for each provider whose token the admission kept, the
/// token as its file holds it and the file's path, in place of any value
/// `environment` carries of their names; for a provider whose token it kept
/// but cannot read back, or that `document` records as selected, neither name
/// at all, so that no step acts on the service's own identity
/// (REQ-GRANT-10). Every other provider's names stay as `environment` has
/// them. A name withheld here is only absent: it never refuses an agent step,
/// as a withheld agent login does (REQ-GRANT-51).
pub(crate) fn offer(
    environment: &EnvironmentSnapshot,
    tokens: Option<&AssignmentGrantTokens>,
    document: Option<&ReadinessDocument>,
) -> EnvironmentSnapshot {
    let mut variables = environment.variables().clone();
    for (provider, variable, file_variable) in OFFERED_VARIABLES {
        let held = tokens.and_then(|tokens| tokens.file(provider));
        match held.and_then(|file| read_token(file).map(|token| (file, token))) {
            Some((file, token)) => {
                variables.insert(
                    OsString::from(variable),
                    OsString::from_vec(token.as_bytes().to_vec()),
                );
                variables.insert(OsString::from(file_variable), file.as_os_str().to_owned());
            }
            None if held.is_some()
                || document
                    .and_then(|document| document.grant(provider))
                    .is_some_and(|grant| grant.selected) =>
            {
                variables.remove(OsStr::new(variable));
                variables.remove(OsStr::new(file_variable));
            }
            None => {}
        }
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

/// The outcome of a request for one provider, before it is recorded.
struct Settled {
    entry: GrantReadiness,
    failure: Option<Failure>,
    /// The token kept in its file.
    kept: Option<HeldToken>,
}

impl Asking<'_> {
    /// Asks Cloud for the token of each provider's grant, the three requests
    /// at once so that a Cloud that does not answer costs one bounded
    /// request, once every token file is gone; keeps each issued token in its
    /// file; records each provider's entry in `document`, as `previous`
    /// recorded it before, and writes one record per changed state or
    /// failure. It never fails: an answer it cannot use is the provider's
    /// state. Its hold refreshes through `setup` once started.
    pub(crate) fn admit(
        &self,
        setup: Weak<AgentLoginSetup>,
        previous: Option<&ReadinessDocument>,
        document: &mut ReadinessDocument,
    ) -> AssignmentGrantTokens {
        let recorded = |provider| previous.and_then(|document| document.grant(provider));
        let removal_failures = {
            let mut holder = self.files.holder();
            *holder = None;
            self.files.remove_all()
        };
        let asked: Vec<AgentLoginProvider> = brokered_providers()
            .filter(|provider| {
                removal_failures
                    .iter()
                    .all(|(failed, _)| failed != provider)
            })
            .collect();
        let answers = self.ask(&asked);
        let mut settled: Vec<Settled> = Vec::new();
        {
            let mut holder = self.files.holder();
            for provider in brokered_providers() {
                let was_selected = recorded(provider).is_some_and(|entry| entry.selected);
                let outcome = match removal_failures
                    .iter()
                    .find(|(failed, _)| *failed == provider)
                {
                    Some((_, error)) => {
                        self.failed_write(provider, Failure::io(Stage::Remove, error))
                    }
                    None => match answers.iter().find(|(asked, _)| *asked == provider) {
                        Some((_, answer)) => self.settle(provider, answer, was_selected),
                        None => self.unavailable(provider, CloudFailure::Transport, was_selected),
                    },
                };
                settled.push(outcome);
            }
            if settled.iter().any(|outcome| outcome.kept.is_some()) {
                *holder = Some(self.assignment_id.to_owned());
            }
        }
        for outcome in &settled {
            self.record(Phase::Admission, recorded(outcome.entry.provider), outcome);
            document.record_grant(outcome.entry.clone());
        }
        AssignmentGrantTokens {
            files: Arc::clone(self.files),
            setup,
            boot_id: self.boot_id.to_owned(),
            run_id: self.run_id.to_owned(),
            assignment_id: self.assignment_id.to_owned(),
            held: settled
                .into_iter()
                .filter_map(|outcome| outcome.kept)
                .collect(),
            refresh: None,
            publish: None,
        }
    }

    /// Asks Cloud again for the token `step` names, for the assignment
    /// whose files these are (REQ-GRANT-89): keeps a token of a newer
    /// generation in place of the held one, at the same path in one step,
    /// leaves the file as it is when Cloud did not answer usefully, unless
    /// the held token has expired, when it removes it, and removes it at a
    /// refusal. Records the provider's entry in `document`, as `previous`
    /// recorded it before, when its state or generation changed, and writes
    /// one record per changed state or failure. Answers what the task does
    /// next, and whether it changed `document`. It asks nothing, and changes
    /// nothing, once the files are no longer the assignment's.
    pub(crate) fn refresh(
        &self,
        step: &RefreshStep,
        previous: Option<&ReadinessDocument>,
        document: &mut ReadinessDocument,
    ) -> (Next, bool) {
        let provider = step.provider;
        if !self.files.held_for(self.assignment_id) {
            return (Next::End, false);
        }
        let answer = self.cloud.grant_token(&self.request(provider));
        let holder = self.files.holder();
        if holder.as_deref() != Some(self.assignment_id) {
            return (Next::End, false);
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
                let outcome = self.keep(provider, issued);
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
                let outcome = self.settle(provider, &answer, true);
                let removed = step.held && step.expired && self.remove(provider);
                (outcome, Next::Retry { removed })
            }
            Err(_) => {
                let outcome = self.settle(provider, &answer, true);
                self.remove(provider);
                (outcome, Next::End)
            }
        };
        drop(holder);
        let recorded = previous.and_then(|document| document.grant(provider));
        self.record(Phase::Refresh, recorded, &outcome);
        let changed = !unchanged(recorded, &outcome.entry);
        if changed {
            document.record_grant(outcome.entry);
        }
        (next, changed)
    }

    /// Removes the provider's file, which the caller holds the files' lock
    /// for, recording a failure; answers whether it is gone.
    fn remove(&self, provider: AgentLoginProvider) -> bool {
        match self.files.remove(provider) {
            Ok(()) => true,
            Err(error) => {
                self.files
                    .record_removal_failure(provider, Some(self.assignment_id), &error);
                false
            }
        }
    }

    fn request(&self, provider: AgentLoginProvider) -> GrantTokenRequest {
        GrantTokenRequest {
            boot_id: self.boot_id.to_owned(),
            assignment_id: self.assignment_id.to_owned(),
            provider,
        }
    }

    /// Cloud's answer for each of `providers`, each request on a thread of
    /// its own, or on this one when no thread can be started.
    fn ask(
        &self,
        providers: &[AgentLoginProvider],
    ) -> Vec<(AgentLoginProvider, Result<IssuedToken, GrantTokenFailure>)> {
        std::thread::scope(|scope| {
            let started: Vec<_> = providers
                .iter()
                .map(|&provider| {
                    let request = self.request(provider);
                    let spawned = std::thread::Builder::new()
                        .name("um-grant-token".to_owned())
                        .spawn_scoped(scope, move || self.cloud.grant_token(&request));
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
                        Err(_) => self.cloud.grant_token(&self.request(provider)),
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
        provider: AgentLoginProvider,
        answer: &Result<IssuedToken, GrantTokenFailure>,
        was_selected: bool,
    ) -> Settled {
        let state = match answer {
            Ok(issued) => return self.keep(provider, issued),
            Err(GrantTokenFailure::Cloud(failure)) => {
                return self.unavailable(provider, *failure, was_selected);
            }
            // Cloud answered, but not for an assignment it counts as this
            // runner's: nothing about the grant has changed, and the request
            // failed with Cloud's 409 as any other status fails it
            // (REQ-TELEMETRY-46).
            Err(GrantTokenFailure::Refused(GrantTokenRefusal::AssignmentNotCurrent)) => {
                return self.unavailable(
                    provider,
                    CloudFailure::Status(ASSIGNMENT_NOT_CURRENT_STATUS),
                    was_selected,
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
        self.settled(provider, state, state == AgentLoginState::ReconnectRequired)
    }

    /// The provider `ready` with `issued`, which its file already holds,
    /// recorded for the run and not ended.
    fn ready(&self, provider: AgentLoginProvider, issued: &IssuedToken) -> Settled {
        Settled {
            entry: GrantReadiness {
                provider,
                state: AgentLoginState::Ready,
                checked_at: Some(self.now),
                issued: Some(IssuedGrant {
                    grant_id: issued.grant_id.clone(),
                    generation: issued.generation,
                    expires_at: issued.expires_at,
                }),
                selected: true,
                run_id: Some(self.run_id.to_owned()),
                ended_at: None,
            },
            failure: None,
            kept: None,
        }
    }

    /// Keeps `issued` in the provider's file, mode `0600`, in one step.
    fn keep(&self, provider: AgentLoginProvider, issued: &IssuedToken) -> Settled {
        let Some((path, temporary_prefix)) = self.files.directory.grant_token(provider) else {
            return self.failed_write(
                provider,
                Failure {
                    stage: Stage::Write,
                    cause: FailureCause::Io(io::ErrorKind::InvalidInput),
                },
            );
        };
        let written =
            directory::ensure_private_directory(self.files.directory.path()).and_then(|()| {
                directory::write_file(&path, issued.access_token.as_bytes(), temporary_prefix)
            });
        match written {
            Ok(()) => Settled {
                kept: Some(HeldToken {
                    provider,
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
    /// file it could not remove: selected, since Cloud may hold a grant in
    /// force for it that the assignment cannot be offered.
    fn failed_write(&self, provider: AgentLoginProvider, failure: Failure) -> Settled {
        Settled {
            failure: Some(failure),
            ..self.settled(provider, AgentLoginState::InstallFailed, true)
        }
    }

    /// A request Cloud did not answer usefully, which leaves the provider
    /// selected as it was.
    fn unavailable(
        &self,
        provider: AgentLoginProvider,
        failure: CloudFailure,
        was_selected: bool,
    ) -> Settled {
        Settled {
            failure: Some(Failure {
                stage: Stage::Issue,
                cause: failure.cause(),
            }),
            ..self.settled(provider, AgentLoginState::CloudUnavailable, was_selected)
        }
    }

    /// The provider in `state` with no token, recorded for the run and not
    /// ended.
    fn settled(
        &self,
        provider: AgentLoginProvider,
        state: AgentLoginState,
        selected: bool,
    ) -> Settled {
        Settled {
            entry: GrantReadiness {
                provider,
                state,
                checked_at: Some(self.now),
                issued: None,
                selected,
                run_id: Some(self.run_id.to_owned()),
                ended_at: None,
            },
            failure: None,
            kept: None,
        }
    }

    /// Writes the provider's record of `phase` when its state or generation
    /// changed from `previous`, or a step failed; none otherwise.
    fn record(&self, phase: Phase, previous: Option<&GrantReadiness>, outcome: &Settled) {
        let entry = &outcome.entry;
        if unchanged(previous, entry) && outcome.failure.is_none() {
            return;
        }
        let mut attributes = vec![
            KeyValue::new(attribute::GRANT_TOKEN_PROVIDER, entry.provider.as_str()),
            KeyValue::new(attribute::GRANT_TOKEN_PHASE, phase.as_str()),
            KeyValue::new(attribute::GRANT_TOKEN_STATE, entry.state.as_str()),
            KeyValue::new(attribute::ASSIGNMENT_ID, self.assignment_id.to_owned()),
        ];
        attributes.extend(generation(entry).map(|generation| {
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

/// The generation of the token `grant` records as issued.
fn generation(grant: &GrantReadiness) -> Option<u64> {
    grant.issued.as_ref().map(|issued| issued.generation)
}

/// Whether `entry` has the state and generation `previous` recorded, a
/// provider never recorded being `not_delivered`.
fn unchanged(previous: Option<&GrantReadiness>, entry: &GrantReadiness) -> bool {
    previous.map_or(AgentLoginState::NotDelivered, |previous| previous.state) == entry.state
        && previous.and_then(generation) == generation(entry)
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
