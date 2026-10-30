use std::io::{self, Write};
use std::path::PathBuf;

use anyhow::{Context, anyhow};
use clap::Args;
use um_api::{
    HttpTransportPolicy, LifecycleAction, ScheduleApi, ScheduleOccurrence,
    ScheduleOccurrenceFilters, ScheduleRead,
};
use um_human_auth::Deployment;

use super::super::super::{self as cli, OrganizationArg, ProjectArg};
use super::super::resource::{self, Run, Vocabulary};
use super::{Options, config};
use crate::exit_code::ExitCode;

const SCHEDULES: Vocabulary = Vocabulary {
    path: &["project", "schedule"],
    purpose: "manage schedules",
    noun: "schedule",
    nouns: &[("occurrence", "occurrence")],
    mutation: "schedule mutation",
    networking: "prepare schedule networking",
};

fn schedules(
    api_url: &str,
    token: &str,
    policy: HttpTransportPolicy,
) -> anyhow::Result<ScheduleApi> {
    ScheduleApi::new(api_url, token, policy)
        .map_err(|error| anyhow!(error))
        .context("prepare schedule API")
}

pub(super) fn execute<T: Run<Management>>(cmd: T, action: Management) -> cli::CommandResult {
    resource::execute(&SCHEDULES, cmd, action)
}

#[derive(Clone, Copy)]
pub(super) enum Management {
    Create,
    List,
    Show,
    Update,
    Enable,
    Disable,
    Delete,
    OccurrenceList,
    OccurrenceShow,
}

#[derive(Debug, Args)]
pub(super) struct Project {
    #[arg(value_name = OrganizationArg::VALUE_NAME, help = OrganizationArg::HELP)]
    organization: OrganizationArg,
    #[arg(value_name = ProjectArg::VALUE_NAME, help = ProjectArg::HELP)]
    project: ProjectArg,
}
#[derive(Debug, Args)]
pub(super) struct Create {
    #[command(flatten)]
    project: Project,
    #[arg(
        long,
        value_name = "PATH",
        help = "Read complete schedule configuration from a JSON file or - for stdin"
    )]
    config_file: PathBuf,
    #[command(flatten)]
    options: Options,
}
#[derive(Debug, Args)]
pub(super) struct List {
    #[command(flatten)]
    project: Project,
    #[command(flatten)]
    page: cli::PaginationArgs<100>,
    #[command(flatten)]
    options: Options,
}
#[derive(Debug, Args)]
pub(super) struct Target {
    #[command(flatten)]
    project: Project,
    #[arg(value_name = "SCHEDULE", help = "Schedule ID")]
    schedule: String,
    #[command(flatten)]
    options: Options,
}
pub(super) type Delete = resource::Delete<Target>;
pub(super) type Update = resource::Update<Target>;
#[derive(Debug, Args)]
pub(super) struct OccurrenceList {
    #[command(flatten)]
    target: Target,
    #[command(flatten)]
    page: cli::PaginationArgs<100>,
    #[arg(long, value_enum, help = "Filter occurrences by exact state")]
    state: Option<OccurrenceState>,
}
#[derive(Debug, Clone, Copy, clap::ValueEnum)]
#[value(rename_all = "snake_case")]
enum OccurrenceState {
    Pending,
    Admitting,
    RetryWaiting,
    RunCreated,
    SkippedActive,
    Stopped,
    Failed,
}
impl OccurrenceState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Admitting => "admitting",
            Self::RetryWaiting => "retry_waiting",
            Self::RunCreated => "run_created",
            Self::SkippedActive => "skipped_active",
            Self::Stopped => "stopped",
            Self::Failed => "failed",
        }
    }
}
#[derive(Debug, Args)]
pub(super) struct OccurrenceTarget {
    #[command(flatten)]
    target: Target,
    #[arg(value_name = "OCCURRENCE", help = "Occurrence ID")]
    occurrence: String,
}

impl Run<Management> for Create {
    fn run(self, d: Deployment, _: Management) -> cli::CommandResult {
        let config = config::read(
            &self.config_file,
            None,
            self.options.authentication.uses_stdin(),
        )
        .and_then(config::create);
        let config = match config {
            Ok(config) => config,
            Err(error) => {
                return resource::configuration_error(&SCHEDULES, error, self.options.json);
            }
        };
        let org = self.project.organization.to_string();
        let project = self.project.project.to_string();
        resource::mutate(
            &SCHEDULES,
            d,
            org.clone(),
            self.options,
            "created",
            "schedule",
            schedules,
            move |api, key| api.create(&org, &project, key, config.clone()),
        )
    }
}
impl Run<Management> for Update {
    fn run(self, d: Deployment, _: Management) -> cli::CommandResult {
        let config = config::read(
            &self.config_file,
            Some(self.expected_version),
            self.target.options.authentication.uses_stdin(),
        )
        .and_then(config::update);
        let config = match config {
            Ok(config) => config,
            Err(error) => {
                return resource::configuration_error(&SCHEDULES, error, self.target.options.json);
            }
        };
        let org = self.target.project.organization.to_string();
        let project = self.target.project.project.to_string();
        let id = self.target.schedule;
        resource::mutate(
            &SCHEDULES,
            d,
            org.clone(),
            self.target.options,
            "updated",
            "schedule",
            schedules,
            move |api, key| api.update(&org, &project, &id, key, config.clone()),
        )
    }
}
impl Run<Management> for Target {
    fn run(self, d: Deployment, action: Management) -> cli::CommandResult {
        let org = self.project.organization.to_string();
        let project = self.project.project.to_string();
        let id = self.schedule;
        let json = self.options.json;
        if matches!(action, Management::Show) {
            let read = resource::with_api(&SCHEDULES, &d, &self.options, schedules, |api| {
                api.schedule(&org, &project, &id)
            })?;
            return match read {
                Ok(read) if !json => report(|out| show(out, &read)),
                read => resource::result(
                    &SCHEDULES,
                    &d,
                    &org,
                    "shown",
                    "schedule",
                    read,
                    |_| None,
                    json,
                ),
            };
        }
        let (kind, label) = match action {
            Management::Enable => (LifecycleAction::Enable, "enabled"),
            Management::Disable => (LifecycleAction::Disable, "disabled"),
            Management::Delete => (LifecycleAction::Delete, "deleted"),
            _ => return Err(anyhow!("invalid schedule action").into()),
        };
        resource::mutate(
            &SCHEDULES,
            d,
            org.clone(),
            self.options,
            label,
            "schedule",
            schedules,
            move |api, key| api.lifecycle(&org, &project, &id, key, kind),
        )
    }
}
impl Run<Management> for List {
    fn run(self, d: Deployment, _: Management) -> cli::CommandResult {
        let org = self.project.organization.to_string();
        let project = self.project.project.to_string();
        let page = resource::with_api(&SCHEDULES, &d, &self.options, schedules, |api| {
            api.schedules(
                &org,
                &project,
                self.page.limit.map(i32::from),
                self.page.cursor.as_deref(),
            )
        })?;
        resource::list_output(
            &SCHEDULES,
            &d,
            &org,
            page.map(|page| (page.items, page.next_cursor)),
            self.options.json,
        )
    }
}
impl Run<Management> for OccurrenceList {
    fn run(self, d: Deployment, _: Management) -> cli::CommandResult {
        let org = self.target.project.organization.to_string();
        let project = self.target.project.project.to_string();
        let json = self.target.options.json;
        let page = resource::with_api(&SCHEDULES, &d, &self.target.options, schedules, |api| {
            api.occurrences(
                &org,
                &project,
                &self.target.schedule,
                ScheduleOccurrenceFilters {
                    limit: self.page.limit.map(i32::from),
                    cursor: self.page.cursor.as_deref(),
                    state: self.state.map(OccurrenceState::as_str),
                },
            )
        })?;
        match page {
            Ok(page) if !json => report(|out| {
                writeln!(out, "schedule: listed")?;
                for item in &page.items {
                    occurrence_line(out, item)?;
                }
                if let Some(cursor) = &page.next_cursor {
                    writeln!(out, "next cursor: {cursor}")?;
                }
                Ok(())
            }),
            page => resource::list_output(
                &SCHEDULES,
                &d,
                &org,
                page.map(|page| (page.items, page.next_cursor)),
                json,
            ),
        }
    }
}
impl Run<Management> for OccurrenceTarget {
    fn run(self, d: Deployment, _: Management) -> cli::CommandResult {
        let org = self.target.project.organization.to_string();
        let project = self.target.project.project.to_string();
        let value = resource::with_api(&SCHEDULES, &d, &self.target.options, schedules, |api| {
            api.occurrence(&org, &project, &self.target.schedule, &self.occurrence)
        })?;
        resource::result(
            &SCHEDULES,
            &d,
            &org,
            "shown",
            "occurrence",
            value,
            |_| None,
            self.target.options.json,
        )
    }
}

/// Writes a successful plain report (`cli/docs/output-style.md` "Reports").
fn report(write: impl FnOnce(&mut dyn Write) -> io::Result<()>) -> cli::CommandResult {
    write(&mut io::stdout().lock()).map_err(|error| {
        cli::CommandFailure::from(anyhow!(error).context("write schedule report"))
    })?;
    Ok(ExitCode::Success)
}

fn show(out: &mut dyn Write, read: &ScheduleRead) -> io::Result<()> {
    writeln!(out, "schedule: shown")?;
    let schedule = match read {
        ScheduleRead::Deleted(tombstone) => {
            writeln!(out, "id: {}", tombstone.id)?;
            writeln!(out, "deleted: true")?;
            return writeln!(out, "deleted at: {}", tombstone.deleted_at);
        }
        ScheduleRead::Active(schedule) => schedule,
    };
    writeln!(out, "id: {}", schedule.id)?;
    writeln!(out, "name: {}", schedule.name)?;
    writeln!(out, "enabled: {}", schedule.enabled)?;
    writeln!(out, "version: {}", schedule.version)?;
    writeln!(
        out,
        "timing: {} {}",
        schedule.timing.cron, schedule.timing.time_zone
    )?;
    writeln!(
        out,
        "next due: {}",
        schedule.next_due_at.as_deref().unwrap_or("none")
    )?;
    if schedule.upcoming.is_empty() {
        writeln!(out, "upcoming: none")?;
    } else {
        writeln!(out, "upcoming: {}", schedule.upcoming.join(" · "))?;
    }
    writeln!(
        out,
        "target: {} · {}",
        schedule.target.workflow_path, schedule.target.execution_principal_id
    )?;
    let Some(last) = &schedule.last_occurrence else {
        return writeln!(out, "last occurrence: none");
    };
    write!(
        out,
        "last occurrence: {} · {} · {}",
        last.id,
        last.due_at,
        um_support::wire_name(last.state).unwrap_or_default()
    )?;
    if let Some(reason) = &last.reason_code {
        write!(out, " · {reason}")?;
    }
    if let Some(run) = &last.run_id {
        write!(out, " · run {run}")?;
    }
    writeln!(out)
}

fn occurrence_line(out: &mut dyn Write, item: &ScheduleOccurrence) -> io::Result<()> {
    write!(
        out,
        "{} · {} · {}",
        item.id,
        item.due_at,
        um_support::wire_name(item.state).unwrap_or_default()
    )?;
    if let Some(reason) = &item.reason_code {
        write!(out, " · {reason}")?;
    }
    if item.missed_count > 0 {
        write!(out, " · missed {}", item.missed_count)?;
    }
    if let Some(run) = &item.run_id {
        write!(out, " · run {run}")?;
    }
    if let Some(run) = &item.blocking_run_id {
        write!(out, " · blocked by {run}")?;
    }
    writeln!(out)
}
