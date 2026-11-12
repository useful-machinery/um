use std::io::{self, Write};

use anyhow::Context;
use clap::error::{ContextKind, ContextValue, ErrorKind};
use clap::{ArgAction, ArgMatches, Args, FromArgMatches};

use crate::exit_code::ExitCode;
use um_api::AgentLoginFailure;
use um_human_auth::Deployment;

use super::{Options, RunnerTarget, output, run_leaf};

pub(super) const ABOUT: &str = "Authorize a runner to receive one of your coding agent logins";

/// Who can use the agent login once a runner holds it (REQ-AGENTLOGIN-25). The command states
/// it before it authorizes, and in place of acting when `--yes` is missing.
const EXPOSURE: [&str; 2] = [
    "every run the runner serves can use the agent login, whichever member caused the run",
    "every step on the runner can read the agent login",
];

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(flatten)]
    target: RunnerTarget,

    #[command(flatten)]
    confirmation: ExposureConfirmation,

    #[command(flatten)]
    options: Options,
}

impl Command {
    pub(super) fn run(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let RunnerTarget {
            agent_login_id,
            runner_id,
        } = &self.target;
        let mut stderr = io::stderr().lock();
        writeln!(
            stderr,
            "Authorizing runner {runner_id} for agent login {agent_login_id}:"
        )?;
        for exposure in EXPOSURE {
            writeln!(stderr, "  {exposure}")?;
        }
        drop(stderr);
        let key = um_support::generate_idempotency_key()
            .context("generate agent login authorization request key")?;
        run_leaf(
            deployment,
            &self.options,
            |api| {
                // A lost response may have authorized the runner. Replay only this key, once.
                match api.authorize(agent_login_id, runner_id, &key) {
                    Err(AgentLoginFailure::Unreachable { .. }) => {
                        api.authorize(agent_login_id, runner_id, &key)
                    }
                    result => result,
                }
            },
            |deployment, authorization, json| {
                output::write_authorize(deployment, agent_login_id, authorization, json)
            },
        )
    }
}

/// `--yes`, required like `ConfirmationArgs`, whose absence is refused with the exposure
/// statement: clap's own refusal of a missing required argument cannot carry it.
#[derive(Debug)]
struct ExposureConfirmation;

const YES: &str = "yes";

impl Args for ExposureConfirmation {
    fn augment_args(command: clap::Command) -> clap::Command {
        command.arg(
            clap::Arg::new(YES)
                .long(YES)
                .action(ArgAction::SetTrue)
                .help("Confirm this action"),
        )
    }

    fn augment_args_for_update(command: clap::Command) -> clap::Command {
        Self::augment_args(command)
    }
}

impl FromArgMatches for ExposureConfirmation {
    fn from_arg_matches(matches: &ArgMatches) -> Result<Self, clap::Error> {
        if matches.get_flag(YES) {
            return Ok(Self);
        }
        let mut error = clap::Error::new(ErrorKind::MissingRequiredArgument);
        error.insert(
            ContextKind::InvalidArg,
            ContextValue::Strings(vec![format!("--{YES}")]),
        );
        error.insert(
            ContextKind::Suggested,
            ContextValue::StyledStrs(
                EXPOSURE
                    .iter()
                    .copied()
                    .chain(["to authorize the runner, run the command again with --yes"])
                    .map(Into::into)
                    .collect(),
            ),
        );
        let mut command = <Command as Args>::augment_args(clap::Command::new("authorize"))
            .bin_name("um auth agent-login authorize");
        error.insert(
            ContextKind::Usage,
            ContextValue::StyledStr(command.render_usage()),
        );
        Err(error)
    }

    fn update_from_arg_matches(&mut self, matches: &ArgMatches) -> Result<(), clap::Error> {
        *self = Self::from_arg_matches(matches)?;
        Ok(())
    }
}
