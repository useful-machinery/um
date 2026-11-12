//! One agent-login setup pass of the serving runner (REQ-AGENTLOGIN-29,
//! REQ-AGENTLOGIN-30): ask Cloud which agent logins the runner is authorized
//! for, install what is new, remove what has ended, and record each
//! provider's readiness. Nothing is installed or removed while the runner
//! holds an assignment (INV-RUNNER-15), and a pass that changes a provider's
//! readiness, or cannot record it, writes one `runner.agent_login_setup`
//! record (REQ-TELEMETRY-45).

use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use opentelemetry::KeyValue;
use time::OffsetDateTime;
use url::Url;

use super::claude_code;
use super::cloud::{
    AgentLoginCloud, Delivery, DeliveryFailure, DeliveryRefusal, DeliveryRequest,
    HttpAgentLoginCloud, InstalledLogin, LoginStatus, ProviderLogin, ProviderReport, StatusRequest,
};
use super::codex;
use super::directory::{self, LoginDirectory};
use super::readiness::{self, Installation, LoginReadiness, ReadinessDocument, RecordedLogin};
use super::{
    FailureCause, FailureStage, InstallRefusal, OperatingSystem, ServiceEnvironment, StepFailure,
};
use crate::control_protocol::{AgentLoginProvider, AgentLoginState};
use crate::credential::Credential;
use crate::enrollment::remove_leftover_temporary_files;
use crate::runner_http;
use crate::telemetry::{self, Outcome, Recorder, attribute};

/// The record a pass writes when it changes a provider's readiness or fails.
pub(crate) const SETUP_EVENT: &str = "runner.agent_login_setup";

/// How long `sync_agent_logins` waits for its pass (REQ-AGENTLOGIN-31). A
/// pass makes at most a status request, one delivery per installable
/// provider and a closing status request, each bounded by the request
/// timeout, and a sync may first wait for one pass already running.
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

/// Agent-login setup bound to one serving process. Passes never overlap: a
/// pass asked for while another runs waits for it and then runs.
pub(crate) struct AgentLoginSetup {
    dependencies: SetupDependencies,
    gate: Arc<dyn AssignmentGate>,
    clock: Box<dyn Fn() -> OffsetDateTime + Send + Sync>,
    pass: Mutex<()>,
}

impl AgentLoginSetup {
    pub(crate) fn new(
        dependencies: SetupDependencies,
        gate: Arc<dyn AssignmentGate>,
        clock: impl Fn() -> OffsetDateTime + Send + Sync + 'static,
    ) -> Self {
        Self {
            dependencies,
            gate,
            clock: Box::new(clock),
            pass: Mutex::new(()),
        }
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
        if !self
            .gate
            .while_idle(&mut || self.remove_leftover_temporary_files())
        {
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
                        .filter_map(|provider| recorded(provider).cloned()),
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
            // that it has never recorded (REQ-AGENTLOGIN-28).
            if answer.is_err() && !entry.is_some_and(authorized_or_installed) {
                settled.extend(entry.cloned());
                continue;
            }
            let mut installed = entry.and_then(LoginReadiness::installation);
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
                Ok(answer) => {
                    let named = answer.iter().find(|login| login.provider == provider);
                    match installed.filter(|installation| self.superseded(installation, named)) {
                        None => self.settle(cloud, provider, installed, named, now),
                        // Cloud names another agent login, or this build
                        // keeps the provider's login elsewhere: the install
                        // the record names ends, and is recorded as ended,
                        // before anything else is asked for
                        // (REQ-AGENTLOGIN-30, INV-RUNNER-15).
                        Some(installation) => match self.end(
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
                                };
                                let mut recorded = settled.clone();
                                recorded.push(entry.clone());
                                if !self.write(&document(&recorded)) {
                                    return SetupOutcome::Failed;
                                }
                                recorded_change = true;
                                installed = None;
                                ended = Some(entry);
                                self.settle(cloud, provider, None, named, now)
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
            let login = LoginReadiness {
                provider,
                state,
                checked_at: Some(now),
                login,
            };
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
        let document = document(&settled);
        if !self.write(&document) {
            return SetupOutcome::Failed;
        }
        // The status request carried what the previous pass recorded, so a
        // pass that changed it tells Cloud in the same pass, rather than
        // the next (REQ-AGENTLOGIN-17); not once an assignment has started,
        // nor when Cloud did not answer, which leaves nothing new to report.
        // The answer is the next pass's to act on, and a failure changes
        // nothing recorded: it is named only beside each changed provider's
        // state in the pass's record, unless the provider's setup already
        // failed a step, which `record_changes` names first.
        let report = self.status_request(Some(&document));
        if !interrupted
            && answer.is_ok()
            && report != self.status_request(previous)
            && let Err(failure) = cloud.status(&report)
        {
            failures.extend(AgentLoginProvider::INSTALLABLE.map(|provider| {
                (
                    provider,
                    StepFailure {
                        stage: FailureStage::Status,
                        cause: failure.cause(),
                    },
                )
            }));
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
    /// the readiness document's beside the state, and the token file's and
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
            remove_leftover_temporary_files(login_directory, &[claude_code::TEMPORARY_PREFIX]);
            let codex_home = dependencies.directory.codex_home();
            if directory::real_directory(&codex_home) {
                remove_leftover_temporary_files(&codex_home, &codex::TEMPORARY_PREFIXES);
            }
        }
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
    /// recorded for each installable provider.
    fn status_request(&self, previous: Option<&ReadinessDocument>) -> StatusRequest {
        StatusRequest {
            os: self.dependencies.os,
            providers: AgentLoginProvider::INSTALLABLE
                .into_iter()
                .map(|provider| {
                    let entry = previous.and_then(|document| document.login(provider));
                    ProviderReport {
                        provider,
                        installed: entry.and_then(LoginReadiness::installation).map(
                            |installation| InstalledLogin {
                                agent_login_id: installation.agent_login_id.clone(),
                                generation: installation.generation,
                            },
                        ),
                        state: entry.map(|entry| entry.state),
                    }
                })
                .collect(),
        }
    }

    /// One provider's readiness from Cloud's answer about it; `None` when an
    /// assignment started before an install or removal it needed.
    fn settle(
        &self,
        cloud: &dyn AgentLoginCloud,
        provider: AgentLoginProvider,
        installed: Option<&Installation>,
        answer: Option<&ProviderLogin>,
        now: OffsetDateTime,
    ) -> Option<Settled> {
        let Some(answer) = answer else {
            // Cloud no longer names the provider: nothing is authorized.
            return self.end(provider, installed, AgentLoginState::NotAuthorized, None);
        };
        let named = || RecordedLogin::Named(answer.agent_login_id.clone());
        let kept = || {
            installed
                .cloned()
                .map_or_else(named, RecordedLogin::Installed)
        };
        let generation = match answer.status {
            LoginStatus::Revoked => {
                return self.end(provider, installed, AgentLoginState::Revoked, Some(named()));
            }
            LoginStatus::Unauthorized => {
                return self.end(
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
        // Code login (REQ-AGENTLOGIN-35), and the service's `CODEX_HOME` is
        // never replaced (REQ-AGENTLOGIN-27).
        if self.foreign_login(provider) {
            return self.end(
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
        self.while_idle(|| ())?;
        let request = DeliveryRequest {
            os: self.dependencies.os,
            provider,
            agent_login_id: answer.agent_login_id.clone(),
            generation,
        };
        match cloud.delivery(&request) {
            Ok(delivery) => self.install(installed, &delivery, now, kept()),
            Err(DeliveryFailure::Refused(DeliveryRefusal::Revoked)) => {
                self.end(provider, installed, AgentLoginState::Revoked, Some(named()))
            }
            Err(DeliveryFailure::Refused(DeliveryRefusal::Unauthorized)) => self.end(
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
        let result = self.while_idle(|| install(directory, installed, delivery, now))?;
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
    /// records `state`. A recorded install outside the directory, an older
    /// build's, is forgotten, never removed. A file that could not be
    /// removed is recorded as `install_failed`, so a later pass removes it.
    fn end(
        &self,
        provider: AgentLoginProvider,
        installed: Option<&Installation>,
        state: AgentLoginState,
        login: Option<RecordedLogin>,
    ) -> Option<Settled> {
        let recorded = installed
            .map(|installation| installation.installed_path.clone())
            .filter(|path| self.dependencies.directory.contains(path));
        let mut present: Vec<PathBuf> = recorded
            .into_iter()
            .chain(self.location(provider))
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
            match self.while_idle(|| {
                present
                    .iter()
                    .try_for_each(|path| directory::remove_file(path))
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

    /// Runs `change` under the assignment gate; `None` while an assignment is
    /// held.
    fn while_idle<T>(&self, change: impl FnOnce() -> T) -> Option<T> {
        let mut change = Some(change);
        let mut result = None;
        self.gate.while_idle(&mut || {
            if let Some(change) = change.take() {
                result = Some(change());
            }
        });
        result
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

/// Whether the runner last knew the provider's login as installed, or as
/// authorized by Cloud without being ready: the entries a pass that cannot
/// reach Cloud records as `cloud_unavailable`, so that state only ever marks a
/// provider Cloud did authorize (REQ-AGENTLOGIN-28, REQ-AGENTLOGIN-31).
fn authorized_or_installed(entry: &LoginReadiness) -> bool {
    entry.installation().is_some() || entry.state.fails_readiness()
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
