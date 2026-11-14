//! One agent-login setup pass of the serving runner (REQ-GRANT-39,
//! REQ-GRANT-40): ask Cloud which agent logins the runner is authorized
//! for, install what is new, remove what has ended, and record each
//! provider's readiness. No agent login is installed or removed once an
//! assignment has been offered its logins (INV-RUNNER-15), and a pass that
//! changes a provider's readiness, or cannot record it, writes one
//! `runner.agent_login_setup` record (REQ-TELEMETRY-45). Each admission makes
//! a pass of its own before the assignment is offered anything
//! (REQ-GRANT-49), then asks Cloud for its assignment's grant tokens
//! (REQ-GRANT-86), whose files each refresh while it runs replaces or removes
//! under the same lock as a pass (REQ-GRANT-89). How the provider
//! answered each use of an offered login is noted in memory while the
//! assignment runs and folded into the readiness by the next pass
//! (REQ-GRANT-52). A pass made while the runner holds no assignment removes
//! the grant token files an earlier assignment or an interrupted runner left
//! (INV-RUNNER-28).

use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use opentelemetry::KeyValue;
use time::OffsetDateTime;
use um_execution::{HarnessLoginObserver, HarnessLoginOutcome};
use url::Url;

use super::claude_code;
use super::cloud::{
    AgentLoginCloud, Delivery, DeliveryFailure, DeliveryRefusal, DeliveryRequest,
    HttpAgentLoginCloud, InstalledLogin, LoginStatus, ProviderLogin, ProviderReport, StatusRequest,
};
use super::codex;
use super::directory::{self, LoginDirectory};
use super::grant_token::{self, AssignmentGrantTokens, TokenFiles};
use super::readiness::{
    self, Installation, LastUse, LoginReadiness, ReadinessDocument, ReadinessError, RecordedLogin,
    UseOutcome,
};
use super::{
    FailureCause, FailureStage, InstallRefusal, OfferedLogin, OperatingSystem, ServiceEnvironment,
    StepFailure,
};
use crate::control_protocol::{AgentLoginProvider, AgentLoginState};
use crate::credential::Credential;
use crate::enrollment::remove_leftover_temporary_files;
use crate::runner_http;
use crate::telemetry::{self, Outcome, Recorder, attribute};

/// The record a pass writes when it changes a provider's readiness or fails.
pub(crate) const SETUP_EVENT: &str = "runner.agent_login_setup";

/// How long `sync_agent_logins` waits for its pass (REQ-GRANT-41). A
/// pass makes at most a status request, one delivery per installable
/// provider and a closing status request, each bounded by the request
/// timeout, and a sync may first wait for one pass already running. A pass
/// whose closing answer ends a login it installed reports once more
/// (REQ-GRANT-50), a request this bound does not count.
pub(crate) const SYNC_BOUND: Duration = Duration::from_secs(
    runner_http::REQUEST_TIMEOUT.as_secs() * (2 + AgentLoginProvider::INSTALLABLE.len() as u64) * 2,
);

/// Where a pass failed apart from any one provider's setup, as its record's
/// `error.type`.
#[derive(Clone, Copy, Debug)]
enum PassStage {
    /// Reading the readiness document.
    Load,
    /// Replacing it.
    Write,
    /// Starting the pass's thread.
    Spawn,
}

impl PassStage {
    const fn error_type(self) -> &'static str {
        match self {
            Self::Load => "agent_login_readiness_load_failed",
            Self::Write => "agent_login_readiness_write_failed",
            Self::Spawn => "agent_login_setup_spawn_failed",
        }
    }
}

/// Whether the runner holds an assignment, as the serving process knows it.
pub(crate) trait AssignmentGate: Send + Sync {
    /// Runs `change` only while the runner holds no assignment, keeping one
    /// from starting until `change` returns; answers false, without running
    /// it, while one is held.
    fn while_idle(&self, change: &mut dyn FnMut()) -> bool;
}

/// The gate of the pass an admission makes for itself: the assignment it is
/// admitting holds the runner's one slot and has been offered nothing yet,
/// so each install and removal runs at once, without consulting the
/// assignment manager (REQ-GRANT-49, INV-RUNNER-15).
struct Admitting;

impl AssignmentGate for Admitting {
    fn while_idle(&self, change: &mut dyn FnMut()) -> bool {
        change();
        true
    }
}

/// What a pass did.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SetupOutcome {
    /// The runner holds an assignment: the pass installed and removed
    /// nothing more. What it installed or removed before the assignment
    /// started is recorded: the readiness it wrote then, or `None` when it
    /// changed nothing.
    Busy(Option<ReadinessDocument>),
    /// The pass ran to the end; the readiness it recorded.
    Recorded(ReadinessDocument),
    /// The readiness document could be neither read nor removed, or could
    /// not be replaced, or the pass could not start.
    Failed,
}

/// Everything a pass needs besides the assignment gate and the clock.
pub(crate) struct SetupDependencies {
    cloud: Arc<dyn AgentLoginCloud>,
    environment: ServiceEnvironment,
    readiness_path: PathBuf,
    directory: LoginDirectory,
    os: OperatingSystem,
    recorder: Arc<Recorder>,
}

impl SetupDependencies {
    /// The serving process's setup: Cloud at the runner's endpoint with its
    /// enrolled credential, the process's own environment, and the readiness
    /// document and agent-login directory beside the protected state at
    /// `runner_state_path`. `None` for an endpoint that is not `ws(s)`.
    pub(crate) fn enrolled(
        endpoint: &Url,
        credential: &Credential,
        runner_state_path: &Path,
        recorder: Arc<Recorder>,
    ) -> Option<Self> {
        let cloud = HttpAgentLoginCloud::new(endpoint, credential)?;
        Some(Self::new(
            Arc::new(cloud),
            ServiceEnvironment::from_lookup(|name| std::env::var_os(name)),
            runner_state_path,
            recorder,
        ))
    }

    /// Setup that asks `cloud`, stands back where `environment` has logins
    /// of its own, and keeps its readiness document and agent-login
    /// directory beside the protected state at `runner_state_path`.
    pub(crate) fn new(
        cloud: Arc<dyn AgentLoginCloud>,
        environment: ServiceEnvironment,
        runner_state_path: &Path,
        recorder: Arc<Recorder>,
    ) -> Self {
        Self {
            cloud,
            environment,
            readiness_path: readiness::readiness_path(runner_state_path),
            directory: LoginDirectory::beside(runner_state_path),
            os: OperatingSystem::current(),
            recorder,
        }
    }
}

/// The setup's clock, shared with the observers it builds.
type Clock = Arc<dyn Fn() -> OffsetDateTime + Send + Sync>;

/// Agent-login setup bound to one serving process. Passes never overlap: a
/// pass asked for while another runs waits for it and then runs.
pub(crate) struct AgentLoginSetup {
    dependencies: SetupDependencies,
    /// The gate of the periodic and `sync` passes.
    gate: Arc<dyn AssignmentGate>,
    clock: Clock,
    pass: Mutex<()>,
    /// The uses noted since a pass last folded them in.
    uses: Arc<NotedUses>,
    /// The grant token files of the assignment the runner holds.
    grant_files: Arc<TokenFiles>,
}

/// What the setup an admission makes for itself did: the agent-login pass,
/// when it made one, the readiness the grant-token requests recorded, when
/// the document could be read and written, and what it holds of the
/// assignment's grant tokens.
#[derive(Debug)]
pub(crate) struct AdmissionSetup {
    pub(crate) agent_logins: Option<SetupOutcome>,
    pub(crate) grants: Option<ReadinessDocument>,
    pub(crate) tokens: AssignmentGrantTokens,
}

impl AgentLoginSetup {
    pub(crate) fn new(
        dependencies: SetupDependencies,
        gate: Arc<dyn AssignmentGate>,
        clock: impl Fn() -> OffsetDateTime + Send + Sync + 'static,
    ) -> Self {
        let grant_files = Arc::new(TokenFiles::new(
            dependencies.directory.clone(),
            Arc::clone(&dependencies.recorder),
        ));
        Self {
            dependencies,
            gate,
            clock: Arc::new(clock),
            pass: Mutex::new(()),
            uses: Arc::default(),
            grant_files,
        }
    }

    /// The observer an admission gives its execution context for the
    /// logins `offered` added to its environment: each use the engine tells
    /// it of is noted for the agent login and generation offered under that
    /// variable, and one for any other variable is ignored
    /// (REQ-GRANT-52). `None` when the admission was offered none. It
    /// writes nothing: the next pass folds what it noted in.
    pub(crate) fn observer(
        &self,
        offered: Vec<OfferedLogin>,
    ) -> Option<Arc<dyn HarnessLoginObserver>> {
        (!offered.is_empty()).then(|| {
            Arc::new(UseObserver {
                offered,
                uses: Arc::clone(&self.uses),
                clock: Arc::clone(&self.clock),
            }) as Arc<dyn HarnessLoginObserver>
        })
    }

    /// Makes later passes ask Cloud with the credential the runner now
    /// serves with.
    pub(crate) fn follow_credential(&self, endpoint: &Url, credential: &Credential) {
        self.dependencies
            .cloud
            .follow_credential(endpoint, credential);
    }

    /// Runs one pass on a thread of its own, since its requests and file
    /// writes block, and answers what it did: `Busy` at once while the
    /// runner holds an assignment.
    pub(crate) async fn run_pass(self: &Arc<Self>) -> SetupOutcome {
        self.run_pass_on(|pass| {
            std::thread::Builder::new()
                .name("um-agent-login-setup".to_owned())
                .spawn(pass)
                .map(|_detached| ())
        })
        .await
    }

    /// `run_pass`, with `spawn` starting the pass on a thread of its own.
    async fn run_pass_on(
        self: &Arc<Self>,
        spawn: impl FnOnce(Box<dyn FnOnce() + Send>) -> io::Result<()>,
    ) -> SetupOutcome {
        let setup = Arc::clone(self);
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let started = spawn(Box::new(move || {
            let _ = sender.send(setup.run_pass_blocking());
        }));
        if let Err(error) = started {
            self.record_failure(PassStage::Spawn, error.kind());
            return SetupOutcome::Failed;
        }
        receiver.await.unwrap_or(SetupOutcome::Failed)
    }

    fn run_pass_blocking(&self) -> SetupOutcome {
        let _pass = self.pass.lock().unwrap_or_else(PoisonError::into_inner);
        self.pass_under(self.gate.as_ref())
    }

    /// Sets agent logins up for an assignment the runner is admitting, on
    /// the admission's own blocking task and before the assignment is offered
    /// them (REQ-GRANT-49): waits for a pass already running, then makes
    /// one whose installs and removals run at once, for the assignment holds
    /// the slot and has been offered nothing. It makes none, and asks Cloud
    /// nothing, unless the readiness document names an installable provider's
    /// login as installed, as authorized without being ready, or as selected:
    /// a selected provider that holds none is withheld from the assignment
    /// (REQ-GRANT-51), so the admission asks whether Cloud authorizes it
    /// again. A document it cannot read names none. Its `agent_logins` is
    /// `None` when it made no pass; a pass never answers `Busy`. Then,
    /// whatever the pass did, it asks Cloud for the grant tokens of the
    /// assignment `assignment_id` of the boot `boot_id` (REQ-GRANT-86),
    /// under the same hold of the pass lock; what it holds of them refreshes
    /// through this setup (REQ-GRANT-89).
    pub(crate) fn set_up_admission(
        self: &Arc<Self>,
        boot_id: &str,
        assignment_id: &str,
    ) -> AdmissionSetup {
        let _pass = self.pass.lock().unwrap_or_else(PoisonError::into_inner);
        let agent_logins = self.admission_pass();
        let (grants, tokens) = self.admit_grant_tokens(boot_id, assignment_id);
        AdmissionSetup {
            agent_logins,
            grants,
            tokens,
        }
    }

    /// Asks Cloud for the assignment's grant tokens and records each
    /// provider's entry in the readiness document (REQ-GRANT-88); the caller
    /// holds the pass lock. A document that cannot be read is neither
    /// replaced nor written, its failure recorded as a pass's is, and the
    /// tokens are kept all the same.
    fn admit_grant_tokens(
        self: &Arc<Self>,
        boot_id: &str,
        assignment_id: &str,
    ) -> (Option<ReadinessDocument>, AssignmentGrantTokens) {
        let now = (self.clock)();
        let (previous, mut document) = self.grant_document(now);
        let tokens = self.asking(boot_id, assignment_id, now).admit(
            Arc::downgrade(self),
            previous.as_ref().ok().and_then(Option::as_ref),
            &mut document,
        );
        let recorded = previous.is_ok() && self.write(&document);
        (recorded.then_some(document), tokens)
    }

    /// Asks Cloud again for the token of the provider `step` names, for the
    /// assignment `assignment_id` of the boot `boot_id` whose token files
    /// the runner holds, keeps or removes its file and records its entry
    /// (REQ-GRANT-89), under the pass lock, so that a refresh never
    /// interleaves with a pass's or an admission's writes to the directory
    /// or the readiness document. Answers what the refresh task does next,
    /// and the readiness it recorded when it changed the entry and could
    /// read and write the document.
    pub(crate) fn refresh_grant_token(
        &self,
        boot_id: &str,
        assignment_id: &str,
        step: &grant_token::RefreshStep,
    ) -> (grant_token::Next, Option<ReadinessDocument>) {
        let _pass = self.pass.lock().unwrap_or_else(PoisonError::into_inner);
        let now = (self.clock)();
        let (previous, mut document) = self.grant_document(now);
        let (next, changed) = self.asking(boot_id, assignment_id, now).refresh(
            step,
            previous.as_ref().ok().and_then(Option::as_ref),
            &mut document,
        );
        let recorded = changed && previous.is_ok() && self.write(&document);
        (next, recorded.then_some(document))
    }

    /// What a grant-token request of the assignment `assignment_id` of the
    /// boot `boot_id`, made at `now`, asks Cloud with and keeps its tokens
    /// in.
    fn asking<'a>(
        &'a self,
        boot_id: &'a str,
        assignment_id: &'a str,
        now: OffsetDateTime,
    ) -> grant_token::Asking<'a> {
        grant_token::Asking {
            cloud: self.dependencies.cloud.as_ref(),
            files: &self.grant_files,
            boot_id,
            assignment_id,
            now,
        }
    }

    /// The readiness document as recorded, its failure to load recorded as
    /// a pass's is, and the document a grant-token request at `now` records
    /// into: the recorded one, or an empty one when there is none or it
    /// cannot be read.
    fn grant_document(
        &self,
        now: OffsetDateTime,
    ) -> (
        Result<Option<ReadinessDocument>, ReadinessError>,
        ReadinessDocument,
    ) {
        let previous = ReadinessDocument::load(&self.dependencies.readiness_path)
            .inspect_err(|error| self.record_failure(PassStage::Load, error.kind()));
        let mut document = match &previous {
            Ok(Some(previous)) => previous.clone(),
            Ok(None) | Err(_) => ReadinessDocument {
                updated_at: now,
                logins: Vec::new(),
                unknown: Vec::new(),
            },
        };
        document.updated_at = now;
        (previous, document)
    }

    /// The admission's agent-login pass, under the pass lock its caller
    /// holds.
    fn admission_pass(&self) -> Option<SetupOutcome> {
        let named = ReadinessDocument::load(&self.dependencies.readiness_path)
            .ok()
            .flatten()
            .is_some_and(|document| {
                AgentLoginProvider::INSTALLABLE
                    .into_iter()
                    .filter_map(|provider| document.login(provider))
                    .any(|entry| authorized_or_installed(entry) || entry.selected)
            });
        named.then(|| self.pass_under(&Admitting))
    }

    /// One pass, each install and removal under `gate`; the caller holds the
    /// `pass` lock.
    fn pass_under(&self, gate: &dyn AssignmentGate) -> SetupOutcome {
        // While the runner holds no assignment no grant token file is any
        // assignment's (INV-RUNNER-28).
        if !gate.while_idle(&mut || {
            self.remove_leftover_temporary_files();
            self.grant_files.remove_leftovers();
        }) {
            return SetupOutcome::Busy(None);
        }
        let now = (self.clock)();
        let path = &self.dependencies.readiness_path;
        let previous = match ReadinessDocument::load(path) {
            Ok(previous) => previous,
            // A record that is not the runner's document vouches for nothing.
            // The runner's directory is its own, so the pass goes on as if
            // nothing were recorded, replacing or removing what is there,
            // once the record is gone: one this pass cannot remove fails
            // every pass. A record an I/O failure kept the pass from reading
            // may still be the only record of an install, so it stays and
            // the pass fails.
            Err(error) => {
                self.record_failure(PassStage::Load, error.kind());
                if !error.replaceable() || directory::remove_file(path).is_err() {
                    return SetupOutcome::Failed;
                }
                None
            }
        };
        let previous = previous.as_ref();
        let recorded =
            |provider: AgentLoginProvider| previous.and_then(|document| document.login(provider));
        // The uses noted so far, which this pass folds into the entry of
        // the install each was noted for (REQ-GRANT-52).
        let noted = self.uses.noted();
        let with_last_use = |mut login: LoginReadiness| {
            login.last_use = last_use(&login, recorded(login.provider), &noted);
            login
        };
        let cloud = self.dependencies.cloud.as_ref();
        let answer = cloud.status(&self.status_request(previous));
        // The providers settled so far, in order, then what was last
        // recorded for the rest.
        let document = |settled: &[LoginReadiness]| ReadinessDocument {
            updated_at: now,
            logins: settled
                .iter()
                .cloned()
                .chain(
                    AgentLoginProvider::INSTALLABLE
                        .into_iter()
                        .filter(|provider| settled.iter().all(|login| login.provider != *provider))
                        .filter_map(|provider| recorded(provider).cloned().map(with_last_use)),
                )
                .collect(),
            unknown: previous.map_or_else(Vec::new, |document| document.unknown.clone()),
        };
        let mut settled = Vec::with_capacity(AgentLoginProvider::INSTALLABLE.len());
        let mut failures = Vec::new();
        let mut interrupted = false;
        let mut recorded_change = false;
        for provider in AgentLoginProvider::INSTALLABLE {
            let entry = recorded(provider);
            // Unable to ask, the runner keeps what it last knew of a provider
            // Cloud did not authorize and the runner holds nothing of, or
            // that it has never recorded (REQ-GRANT-38).
            if answer.is_err() && !entry.is_some_and(authorized_or_installed) {
                settled.extend(entry.cloned());
                continue;
            }
            let mut installed = entry.and_then(LoginReadiness::installation);
            let named = answer
                .as_ref()
                .ok()
                .and_then(|answer| answer.iter().find(|login| login.provider == provider));
            // Whether the provider is selected once it is in `state`: as
            // Cloud answered, or as last recorded when Cloud did not answer.
            let selected_in = |state| match &answer {
                Ok(_) => selected(named, state),
                Err(_) => entry.is_some_and(|entry| entry.selected),
            };
            // What the pass recorded for the provider on the way, if anything.
            let mut ended = None;
            let outcome = match &answer {
                Err(failure) => Some(Settled::failed(
                    AgentLoginState::CloudUnavailable,
                    entry.and_then(|entry| entry.login.clone()),
                    StepFailure {
                        stage: FailureStage::Status,
                        cause: failure.cause(),
                    },
                )),
                Ok(_) => {
                    match installed.filter(|installation| self.superseded(installation, named)) {
                        None => self.settle(gate, cloud, provider, installed, named, now),
                        // Cloud names another agent login, or this build
                        // keeps the provider's login elsewhere: the install
                        // the record names ends, and is recorded as ended,
                        // before anything else is asked for
                        // (REQ-GRANT-40, INV-RUNNER-15).
                        Some(installation) => match self.end(
                            gate,
                            provider,
                            Some(installation),
                            AgentLoginState::NotAuthorized,
                            Some(RecordedLogin::Named(installation.agent_login_id.clone())),
                        ) {
                            Some(Settled {
                                state,
                                login,
                                failure: None,
                            }) => {
                                let entry = LoginReadiness {
                                    provider,
                                    state,
                                    checked_at: Some(now),
                                    login,
                                    selected: selected_in(state),
                                    last_use: None,
                                };
                                let mut recorded = settled.clone();
                                recorded.push(entry.clone());
                                if !self.write(&document(&recorded)) {
                                    return SetupOutcome::Failed;
                                }
                                recorded_change = true;
                                installed = None;
                                ended = Some(entry);
                                self.settle(gate, cloud, provider, None, named, now)
                            }
                            failed => failed,
                        },
                    }
                }
            };
            let Some(Settled {
                state,
                login,
                failure,
            }) = outcome
            else {
                // An assignment started before an install or removal this
                // provider needed: the rest waits for the next pass.
                settled.extend(ended);
                interrupted = true;
                break;
            };
            let login = with_last_use(LoginReadiness {
                provider,
                state,
                checked_at: Some(now),
                login,
                selected: selected_in(state),
                last_use: None,
            });
            let changed = login.installation() != installed;
            settled.push(login);
            failures.extend(failure.map(|failure| (provider, failure)));
            // An install or removal is recorded before the pass goes on, so
            // the record vouches for the file whatever stops the pass next
            // (INV-RUNNER-15).
            if changed {
                if !self.write(&document(&settled)) {
                    return SetupOutcome::Failed;
                }
                recorded_change = true;
            }
        }
        // A pass an assignment interrupted before it installed or removed
        // anything leaves the record as it was; one interrupted after records
        // what it settled and answers busy all the same.
        if interrupted && !recorded_change {
            return SetupOutcome::Busy(None);
        }
        let mut document = document(&settled);
        if !self.write(&document) {
            return SetupOutcome::Failed;
        }
        // What was noted is recorded, or was noted for an install that is
        // no longer the one recorded.
        self.uses.forget(&noted);
        // The status request carried what the previous pass recorded, so a
        // pass that changed it tells Cloud in the same pass, rather than
        // the next (REQ-GRANT-27); not once an assignment has started,
        // nor when Cloud did not answer, which leaves nothing new to report.
        // A failure changes nothing recorded: it is named only beside each
        // changed provider's state in the pass's record, unless the
        // provider's setup already failed a step, which `record_changes`
        // names first.
        let report = self.status_request(Some(&document));
        if !interrupted && answer.is_ok() && report != self.status_request(previous) {
            match cloud.status(&report) {
                Err(failure) => failures.extend(status_failures(failure.cause())),
                // The answer ends what this pass installed and Cloud no
                // longer authorizes, once, and the removal is recorded and
                // reported; the answer to that report, and everything else
                // in this one, is the next pass's to act on
                // (REQ-GRANT-50).
                Ok(closing) => {
                    match self.end_unauthorized(
                        gate,
                        previous,
                        &mut document,
                        &closing,
                        &mut failures,
                    ) {
                        Some(false) => {}
                        ended => {
                            if !self.write(&document) {
                                return SetupOutcome::Failed;
                            }
                            // A pass an assignment interrupted reports
                            // nothing more.
                            if ended.is_none() {
                                interrupted = true;
                            } else if let Err(failure) =
                                cloud.status(&self.status_request(Some(&document)))
                            {
                                failures.extend(status_failures(failure.cause()));
                            }
                        }
                    }
                }
            }
        }
        self.record_changes(previous, &document, &failures);
        if interrupted {
            SetupOutcome::Busy(Some(document))
        } else {
            SetupOutcome::Recorded(document)
        }
    }

    /// Removes the temporary files a write of the pass's own left when its
    /// process was killed before the rename, which may hold login material:
    /// the readiness document's beside the state, and the token files' and
    /// the Codex home's in the runner's directory while it is a real
    /// directory. Only the serving runner writes with these prefixes, one
    /// pass at a time, so every such file is a leftover; enrollment's own
    /// temporary files beside the state are not touched.
    fn remove_leftover_temporary_files(&self) {
        let dependencies = &self.dependencies;
        if let Some(state_directory) = dependencies.readiness_path.parent() {
            remove_leftover_temporary_files(state_directory, &[readiness::TEMPORARY_PREFIX]);
        }
        let login_directory = dependencies.directory.path();
        if directory::real_directory(login_directory) {
            let [github, gitlab, linear] = directory::GRANT_TOKEN_TEMPORARY_PREFIXES;
            remove_leftover_temporary_files(
                login_directory,
                &[claude_code::TEMPORARY_PREFIX, github, gitlab, linear],
            );
            let codex_home = dependencies.directory.codex_home();
            if directory::real_directory(&codex_home) {
                remove_leftover_temporary_files(&codex_home, &codex::TEMPORARY_PREFIXES);
            }
        }
    }

    /// Ends each login this pass installed, as `document` records it against
    /// `previous`, that Cloud's closing answer `closing` no longer authorizes:
    /// Cloud answers `revoked` or `unauthorized`, names another agent login
    /// or no longer names the provider (REQ-GRANT-50). It changes the
    /// provider's entry in `document` and asks for no delivery. Answers
    /// whether it ended one, or `None` when an assignment started before a
    /// removal it needed.
    fn end_unauthorized(
        &self,
        gate: &dyn AssignmentGate,
        previous: Option<&ReadinessDocument>,
        document: &mut ReadinessDocument,
        closing: &[ProviderLogin],
        failures: &mut Vec<(AgentLoginProvider, StepFailure)>,
    ) -> Option<bool> {
        let mut ended = false;
        for entry in &mut document.logins {
            let provider = entry.provider;
            let Some(installation) = entry.installation().cloned() else {
                continue;
            };
            let earlier = previous
                .and_then(|previous| previous.login(provider))
                .and_then(LoginReadiness::installation);
            if earlier == Some(&installation) {
                continue;
            }
            let named = closing.iter().find(|login| login.provider == provider);
            let state = match named.map(|named| named.status) {
                None => AgentLoginState::NotAuthorized,
                Some(LoginStatus::Revoked) => AgentLoginState::Revoked,
                Some(LoginStatus::Unauthorized) => AgentLoginState::NotAuthorized,
                Some(_) if self.superseded(&installation, named) => AgentLoginState::NotAuthorized,
                Some(_) => continue,
            };
            let login = named.map(|named| RecordedLogin::Named(named.agent_login_id.clone()));
            let Settled {
                state,
                login,
                failure,
            } = self.end(gate, provider, Some(&installation), state, login)?;
            entry.state = state;
            entry.login = login;
            entry.selected = selected(named, state);
            entry.last_use = None;
            failures.extend(failure.map(|failure| (provider, failure)));
            ended = true;
        }
        Some(ended)
    }

    /// Replaces the readiness document with `document`; on failure records
    /// why and answers false.
    fn write(&self, document: &ReadinessDocument) -> bool {
        match document.write(&self.dependencies.readiness_path) {
            Ok(()) => true,
            Err(error) => {
                self.record_failure(PassStage::Write, error.kind());
                false
            }
        }
    }

    /// Whether the install the record names has ended now that Cloud names
    /// `named` for its provider: Cloud names another agent login, or this
    /// build keeps the provider's login at another location.
    fn superseded(&self, installation: &Installation, named: Option<&ProviderLogin>) -> bool {
        named.is_some_and(|named| {
            named.agent_login_id != installation.agent_login_id
                || self.location(named.provider).as_ref() != Some(&installation.installed_path)
        })
    }

    /// What the runner tells Cloud it can install, has installed and last
    /// recorded for each installable provider, with the last use of a
    /// `ready` install (REQ-GRANT-52).
    fn status_request(&self, previous: Option<&ReadinessDocument>) -> StatusRequest {
        StatusRequest {
            os: self.dependencies.os,
            providers: AgentLoginProvider::INSTALLABLE
                .into_iter()
                .map(|provider| {
                    let entry = previous.and_then(|document| document.login(provider));
                    ProviderReport::new(
                        provider,
                        entry
                            .and_then(LoginReadiness::installation)
                            .map(|installation| InstalledLogin {
                                agent_login_id: installation.agent_login_id.clone(),
                                generation: installation.generation,
                            }),
                        entry.map(|entry| entry.state),
                        entry
                            .and_then(|entry| entry.last_use)
                            .map(|last_use| last_use.outcome),
                    )
                })
                .collect(),
        }
    }

    /// One provider's readiness from Cloud's answer about it, each install
    /// and removal under `gate`; `None` when an assignment started before an
    /// install or removal it needed.
    fn settle(
        &self,
        gate: &dyn AssignmentGate,
        cloud: &dyn AgentLoginCloud,
        provider: AgentLoginProvider,
        installed: Option<&Installation>,
        answer: Option<&ProviderLogin>,
        now: OffsetDateTime,
    ) -> Option<Settled> {
        let Some(answer) = answer else {
            // Cloud no longer names the provider: nothing is authorized.
            return self.end(
                gate,
                provider,
                installed,
                AgentLoginState::NotAuthorized,
                None,
            );
        };
        let named = || RecordedLogin::Named(answer.agent_login_id.clone());
        let kept = || {
            installed
                .cloned()
                .map_or_else(named, RecordedLogin::Installed)
        };
        let generation = match answer.status {
            LoginStatus::Revoked => {
                return self.end(
                    gate,
                    provider,
                    installed,
                    AgentLoginState::Revoked,
                    Some(named()),
                );
            }
            LoginStatus::Unauthorized => {
                return self.end(
                    gate,
                    provider,
                    installed,
                    AgentLoginState::NotAuthorized,
                    Some(named()),
                );
            }
            LoginStatus::UnsupportedTarget => {
                return Some(Settled::new(
                    AgentLoginState::UnsupportedTarget,
                    Some(kept()),
                ));
            }
            LoginStatus::Authorized {
                record_schema_version,
                ..
            } if !provider
                .record_schema_versions()
                .contains(&record_schema_version) =>
            {
                return Some(Settled::new(
                    AgentLoginState::UnsupportedTarget,
                    Some(kept()),
                ));
            }
            LoginStatus::Authorized { generation, .. } => generation,
        };
        // While the service has a login of its own the runner keeps and
        // asks for none: a kept token would outrank the service's Claude
        // Code login (REQ-GRANT-45), and the service's `CODEX_HOME` is
        // never replaced (REQ-GRANT-37).
        if self.foreign_login(provider) {
            return self.end(
                gate,
                provider,
                installed,
                AgentLoginState::ForeignLogin,
                Some(named()),
            );
        }
        // The install the record names is still held only while its file
        // is there and is what admission would offer: one deleted under the
        // runner (`codex logout` with the runner's Codex home), or a token
        // file admission reads no token from, is fetched and installed again.
        if let Some(installation) = installed
            && installation.agent_login_id == answer.agent_login_id
            && installation.generation == generation
            && self.location(provider).as_ref() == Some(&installation.installed_path)
            && Self::held(provider, &installation.installed_path)
        {
            return Some(Settled::new(AgentLoginState::Ready, Some(kept())));
        }
        // No request starts once an assignment is held: Cloud would record
        // and audit a delivery the runner then could not install.
        while_idle(gate, || ())?;
        let request = DeliveryRequest {
            os: self.dependencies.os,
            provider,
            agent_login_id: answer.agent_login_id.clone(),
            generation,
        };
        match cloud.delivery(&request) {
            Ok(delivery) => self.install(gate, installed, &delivery, now, kept()),
            Err(DeliveryFailure::Refused(DeliveryRefusal::Revoked)) => self.end(
                gate,
                provider,
                installed,
                AgentLoginState::Revoked,
                Some(named()),
            ),
            Err(DeliveryFailure::Refused(DeliveryRefusal::Unauthorized)) => self.end(
                gate,
                provider,
                installed,
                AgentLoginState::NotAuthorized,
                Some(named()),
            ),
            Err(DeliveryFailure::Refused(DeliveryRefusal::StaleGeneration)) => {
                Some(Settled::new(AgentLoginState::StaleGeneration, Some(kept())))
            }
            Err(DeliveryFailure::Refused(DeliveryRefusal::UnsupportedTarget)) => Some(
                Settled::new(AgentLoginState::UnsupportedTarget, Some(kept())),
            ),
            Err(DeliveryFailure::Cloud(failure)) => Some(Settled::failed(
                AgentLoginState::CloudUnavailable,
                Some(kept()),
                StepFailure {
                    stage: FailureStage::Delivery,
                    cause: failure.cause(),
                },
            )),
        }
    }

    fn install(
        &self,
        gate: &dyn AssignmentGate,
        installed: Option<&Installation>,
        delivery: &Delivery,
        now: OffsetDateTime,
        kept: RecordedLogin,
    ) -> Option<Settled> {
        if !delivery
            .provider
            .record_schema_versions()
            .contains(&delivery.record_schema_version)
        {
            return Some(Settled::new(AgentLoginState::UnsupportedTarget, Some(kept)));
        }
        let install: Installer = match delivery.provider {
            AgentLoginProvider::Codex => codex::install_codex,
            AgentLoginProvider::ClaudeCode => claude_code::install_claude_code,
            AgentLoginProvider::Github
            | AgentLoginProvider::Gitlab
            | AgentLoginProvider::Linear => {
                return Some(Settled::new(AgentLoginState::UnsupportedTarget, Some(kept)));
            }
        };
        let directory = &self.dependencies.directory;
        let result = while_idle(gate, || install(directory, installed, delivery, now))?;
        Some(match result {
            Ok(installation) => Settled::new(
                AgentLoginState::Ready,
                Some(RecordedLogin::Installed(installation)),
            ),
            Err(refusal) => Settled {
                state: refusal.state(),
                login: Some(kept),
                failure: refusal.failure(),
            },
        })
    }

    /// Ends the provider's login on this runner: removes what is at the
    /// provider's location in the runner's directory, recorded or not (an
    /// install the runner did not get to record is still its own, and left
    /// there it would be offered to every later assignment), and the
    /// recorded install's file when it is elsewhere in that directory, and
    /// records `state`. For Codex that location is the runner's whole Codex
    /// home, what Codex wrote there included (REQ-GRANT-40); an install
    /// creates it again. A recorded install outside the directory, an older
    /// build's, is forgotten, never removed. What could not be removed is
    /// recorded as `install_failed`, so a later pass removes what is left.
    /// Each removal runs under `gate`.
    fn end(
        &self,
        gate: &dyn AssignmentGate,
        provider: AgentLoginProvider,
        installed: Option<&Installation>,
        state: AgentLoginState,
        login: Option<RecordedLogin>,
    ) -> Option<Settled> {
        let directory = &self.dependencies.directory;
        let home = (provider == AgentLoginProvider::Codex).then(|| directory.codex_home());
        let recorded = installed
            .map(|installation| installation.installed_path.clone())
            .filter(|path| directory.contains(path))
            // The home's removal takes a file recorded in it.
            .filter(|path| home.as_ref().is_none_or(|home| !path.starts_with(home)));
        let mut present: Vec<PathBuf> = recorded
            .into_iter()
            .chain(home.clone().or_else(|| self.location(provider)))
            // Nothing is at a path that does not exist or lies under
            // something that is not a directory.
            .filter(|path| {
                !path.symlink_metadata().is_err_and(|error| {
                    matches!(
                        error.kind(),
                        io::ErrorKind::NotFound | io::ErrorKind::NotADirectory
                    )
                })
            })
            .collect();
        present.dedup();
        if present.is_empty() {
            return Some(Settled::new(state, login));
        }
        Some(
            match while_idle(gate, || {
                present.iter().try_for_each(|path| {
                    if home.as_ref() == Some(path) {
                        // The login itself goes first, so a home whose removal
                        // cannot finish holds no login to offer the next
                        // assignment. Its own failure is not the answer: what
                        // is left fails the home's removal, which reports it.
                        let _ = directory.remove_tree(&codex::auth_path(directory));
                    }
                    directory.remove_tree(path)
                })
            })? {
                Ok(()) => Settled::new(state, login),
                Err(error) => Settled::failed(
                    AgentLoginState::InstallFailed,
                    installed.cloned().map(RecordedLogin::Installed).or(login),
                    StepFailure {
                        stage: FailureStage::Remove,
                        cause: FailureCause::Io(error.kind()),
                    },
                ),
            },
        )
    }

    /// Whether the service has a login of the provider's own, which the
    /// runner never outranks or replaces.
    fn foreign_login(&self, provider: AgentLoginProvider) -> bool {
        let environment = &self.dependencies.environment;
        match provider {
            AgentLoginProvider::Codex => codex::foreign_login(environment),
            AgentLoginProvider::ClaudeCode => claude_code::foreign_login(environment),
            AgentLoginProvider::Github
            | AgentLoginProvider::Gitlab
            | AgentLoginProvider::Linear => false,
        }
    }

    /// Where this build keeps the provider's login; `None` for a brokered
    /// provider, whose login it never keeps.
    /// Whether the install recorded at `path` is still what the runner
    /// offers an assignment (`agent_login::offer`): a Codex `auth.json` that
    /// is there, which Codex itself rewrites, or a token file the runner
    /// reads a token from.
    fn held(provider: AgentLoginProvider, path: &Path) -> bool {
        match provider {
            AgentLoginProvider::ClaudeCode => claude_code::read_token(path).is_some(),
            AgentLoginProvider::Codex
            | AgentLoginProvider::Github
            | AgentLoginProvider::Gitlab
            | AgentLoginProvider::Linear => directory::holds_file(path),
        }
    }

    fn location(&self, provider: AgentLoginProvider) -> Option<PathBuf> {
        let directory = &self.dependencies.directory;
        match provider {
            AgentLoginProvider::Codex => Some(codex::auth_path(directory)),
            AgentLoginProvider::ClaudeCode => Some(directory.claude_code_token()),
            AgentLoginProvider::Github
            | AgentLoginProvider::Gitlab
            | AgentLoginProvider::Linear => None,
        }
    }

    /// Writes one record naming each provider whose state, agent login or
    /// generation this pass changed, with its new state and generation and
    /// the step that failed on the way, if one did; none when nothing
    /// changed, so an idle runner adds no record per interval.
    fn record_changes(
        &self,
        previous: Option<&ReadinessDocument>,
        document: &ReadinessDocument,
        failures: &[(AgentLoginProvider, StepFailure)],
    ) {
        let mut attributes = Vec::new();
        for login in &document.logins {
            let generation = login
                .installation()
                .map(|installation| installation.generation);
            let unchanged = previous
                .and_then(|previous| previous.login(login.provider))
                .is_some_and(|before| {
                    before.state == login.state
                        && agent_login_id(before) == agent_login_id(login)
                        && before
                            .installation()
                            .map(|installation| installation.generation)
                            == generation
                });
            if unchanged {
                continue;
            }
            let Some(keys) = ProviderKeys::of(login.provider) else {
                continue;
            };
            attributes.push(KeyValue::new(keys.state, login.state.as_str()));
            if let Some(generation) = generation {
                attributes.push(KeyValue::new(
                    keys.generation,
                    telemetry::integer(generation),
                ));
            }
            let failure = failures
                .iter()
                .find(|(provider, _)| *provider == login.provider);
            if let Some((_, failure)) = failure {
                attributes.push(KeyValue::new(keys.failure_stage, failure.stage.as_str()));
                attributes.push(KeyValue::new(keys.failure_cause, failure.cause.as_str()));
                if let FailureCause::HttpStatus(status) = failure.cause {
                    attributes.push(KeyValue::new(keys.http_status, i64::from(status)));
                }
            }
        }
        if !attributes.is_empty() {
            self.dependencies
                .recorder
                .start(SETUP_EVENT, attributes)
                .finish(Outcome::Success);
        }
    }

    /// Writes the record of a pass that failed at `stage` with an I/O
    /// failure of `kind`. A load failure of a document that is not the
    /// runner's is followed by the rest of the pass; a write failure after an
    /// install or removal leaves that change for the next pass to record.
    fn record_failure(&self, stage: PassStage, kind: io::ErrorKind) {
        self.dependencies
            .recorder
            .start(
                SETUP_EVENT,
                [
                    KeyValue::new(attribute::ERROR_TYPE, stage.error_type()),
                    KeyValue::new(
                        attribute::FAILURE_CAUSE_TYPE,
                        telemetry::io_error_class(kind),
                    ),
                ],
            )
            .finish(Outcome::Failure);
    }
}

/// The last use `login` records: the one noted for its install, else the one
/// last recorded beside it, while that install is the one last recorded; an
/// install, or an ending, records none (REQ-GRANT-52).
fn last_use(
    login: &LoginReadiness,
    previous: Option<&LoginReadiness>,
    noted: &[NotedUse],
) -> Option<LastUse> {
    let installation = login.installation()?;
    let previous = previous.filter(|previous| previous.installation() == Some(installation))?;
    noted
        .iter()
        .find(|noted| {
            noted.provider == login.provider
                && noted.agent_login_id == installation.agent_login_id
                && noted.generation == installation.generation
        })
        .map(|noted| noted.last_use)
        .or(previous.last_use)
}

/// A use of an offered login, noted while its assignment ran.
#[derive(Clone, Debug, Eq, PartialEq)]
struct NotedUse {
    provider: AgentLoginProvider,
    agent_login_id: String,
    generation: u64,
    last_use: LastUse,
}

/// The uses noted since a pass last folded them in, at most one per
/// provider, the latest. An observer notes into it while the assignment
/// runs, when no pass may write (INV-RUNNER-15), and a pass takes the lock
/// only to read and to forget what it folded in. A process that stops
/// before a pass loses them.
#[derive(Debug, Default)]
struct NotedUses(Mutex<Vec<NotedUse>>);

impl NotedUses {
    fn note(&self, noted: NotedUse) {
        let mut uses = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        uses.retain(|kept| kept.provider != noted.provider);
        uses.push(noted);
    }

    fn noted(&self) -> Vec<NotedUse> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Forgets each of `folded`, keeping a use noted since.
    fn forget(&self, folded: &[NotedUse]) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retain(|kept| !folded.contains(kept));
    }
}

/// The observer of one admission (REQ-GRANT-52).
struct UseObserver {
    offered: Vec<OfferedLogin>,
    uses: Arc<NotedUses>,
    clock: Clock,
}

impl HarnessLoginObserver for UseObserver {
    fn observe(&self, variable: &str, outcome: HarnessLoginOutcome) {
        // A variable the runner did not add, the service's own, says
        // nothing of a login the runner holds.
        let Some(offered) = self
            .offered
            .iter()
            .find(|offered| offered.variable == variable)
        else {
            return;
        };
        self.uses.note(NotedUse {
            provider: offered.provider,
            agent_login_id: offered.agent_login_id.clone(),
            generation: offered.generation,
            last_use: LastUse {
                outcome: match outcome {
                    HarnessLoginOutcome::Accepted => UseOutcome::Accepted,
                    HarnessLoginOutcome::Refused => UseOutcome::Refused,
                },
                at: (self.clock)(),
            },
        });
    }
}

/// Whether the runner last knew the provider's login as installed, or as
/// authorized by Cloud without being ready: the entries a pass that cannot
/// reach Cloud records as `cloud_unavailable`, so that state only ever marks a
/// provider Cloud did authorize (REQ-GRANT-38, REQ-GRANT-41).
fn authorized_or_installed(entry: &LoginReadiness) -> bool {
    entry.installation().is_some() || entry.state.fails_readiness()
}

/// Whether a provider Cloud answered `named` for, and that a pass left in
/// `state`, is selected: Cloud named an authorization in force for it,
/// `authorized`, `unauthorized` or `unsupported_target`, and no delivery
/// refused as `revoked` since (REQ-GRANT-38).
fn selected(named: Option<&ProviderLogin>, state: AgentLoginState) -> bool {
    named.is_some_and(|named| named.status != LoginStatus::Revoked)
        && state != AgentLoginState::Revoked
}

/// Runs `change` under `gate`; `None` while an assignment is held.
fn while_idle<T>(gate: &dyn AssignmentGate, change: impl FnOnce() -> T) -> Option<T> {
    let mut change = Some(change);
    let mut result = None;
    gate.while_idle(&mut || {
        if let Some(change) = change.take() {
            result = Some(change());
        }
    });
    result
}

/// A status request's failure with `cause`, named beside each installable
/// provider's state in the pass's record.
fn status_failures(
    cause: FailureCause,
) -> [(AgentLoginProvider, StepFailure); AgentLoginProvider::INSTALLABLE.len()] {
    AgentLoginProvider::INSTALLABLE.map(|provider| {
        (
            provider,
            StepFailure {
                stage: FailureStage::Status,
                cause,
            },
        )
    })
}

/// A provider's state after a pass, the agent login its entry records, and
/// the step that failed on the way there, if one did.
struct Settled {
    state: AgentLoginState,
    login: Option<RecordedLogin>,
    failure: Option<StepFailure>,
}

impl Settled {
    const fn new(state: AgentLoginState, login: Option<RecordedLogin>) -> Self {
        Self {
            state,
            login,
            failure: None,
        }
    }

    const fn failed(
        state: AgentLoginState,
        login: Option<RecordedLogin>,
        failure: StepFailure,
    ) -> Self {
        Self {
            state,
            login,
            failure: Some(failure),
        }
    }
}

/// How this build installs a provider's login.
type Installer = fn(
    &LoginDirectory,
    Option<&Installation>,
    &Delivery,
    OffsetDateTime,
) -> Result<Installation, InstallRefusal>;

/// The keys a provider's attributes have in the record.
struct ProviderKeys {
    state: &'static str,
    generation: &'static str,
    failure_stage: &'static str,
    failure_cause: &'static str,
    http_status: &'static str,
}

impl ProviderKeys {
    /// The keys of an installable provider; a brokered provider has none.
    const fn of(provider: AgentLoginProvider) -> Option<Self> {
        Some(match provider {
            AgentLoginProvider::Codex => Self {
                state: attribute::AGENT_LOGIN_CODEX_STATE,
                generation: attribute::AGENT_LOGIN_CODEX_GENERATION,
                failure_stage: attribute::AGENT_LOGIN_CODEX_FAILURE_STAGE,
                failure_cause: attribute::AGENT_LOGIN_CODEX_FAILURE_CAUSE,
                http_status: attribute::AGENT_LOGIN_CODEX_HTTP_STATUS,
            },
            AgentLoginProvider::ClaudeCode => Self {
                state: attribute::AGENT_LOGIN_CLAUDE_CODE_STATE,
                generation: attribute::AGENT_LOGIN_CLAUDE_CODE_GENERATION,
                failure_stage: attribute::AGENT_LOGIN_CLAUDE_CODE_FAILURE_STAGE,
                failure_cause: attribute::AGENT_LOGIN_CLAUDE_CODE_FAILURE_CAUSE,
                http_status: attribute::AGENT_LOGIN_CLAUDE_CODE_HTTP_STATUS,
            },
            AgentLoginProvider::Github
            | AgentLoginProvider::Gitlab
            | AgentLoginProvider::Linear => {
                return None;
            }
        })
    }
}

fn agent_login_id(login: &LoginReadiness) -> Option<&str> {
    login.login.as_ref().map(|recorded| match recorded {
        RecordedLogin::Installed(installation) => installation.agent_login_id.as_str(),
        RecordedLogin::Named(agent_login_id) => agent_login_id.as_str(),
    })
}

#[cfg(test)]
mod tests;
