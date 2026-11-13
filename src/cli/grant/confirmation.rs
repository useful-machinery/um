//! The statement a leaf makes before it acts on a grant, and its `--yes`, which refuses a
//! missing confirmation with that statement. `um grant` and the earlier commands it replaces
//! share them.

use std::fmt;
use std::io::{self, Write};
use std::marker::PhantomData;

use clap::error::{ContextKind, ContextValue, ErrorKind};
use clap::{ArgAction, ArgMatches, Args, FromArgMatches};

use um_api::MemberGrantKind;

/// Who can use a native grant once a runner holds it (REQ-GRANT-35, REQ-GRANT-66).
pub(in crate::cli) const NATIVE_AUTHORIZE: &[&str] = &[
    "every run the runner serves can use the agent login, whichever member caused the run",
    "every step on the runner can read the agent login",
];

/// The stop Cloud requests of the runner's run when a native grant's authorization ends
/// (REQ-GRANT-48).
pub(in crate::cli) const NATIVE_DEAUTHORIZE: &[&str] =
    &["the run the runner is serving will be asked to stop, whichever member started it"];

/// The stop Cloud requests of each runner's run when a native grant is revoked (REQ-GRANT-48).
pub(in crate::cli) const NATIVE_REVOKE: &[&str] = &[
    "the run each of the agent login's runners is serving will be asked to stop, whichever \
     member started it",
];

/// What acting does: the same for every subject, or, for a leaf of `um grant`, by the kind of
/// the grant it acts on, which the leaf knows only once it has read the grant.
pub(in crate::cli) enum Statement {
    Fixed(&'static [&'static str]),
    ByKind {
        oauth: &'static [&'static str],
        native: &'static [&'static str],
    },
}

impl Statement {
    /// The lines that state what acting on a grant of `kind` does.
    pub(in crate::cli) const fn of(&self, kind: MemberGrantKind) -> &'static [&'static str] {
        match (self, kind) {
            (Self::Fixed(lines), _)
            | (Self::ByKind { oauth: lines, .. }, MemberGrantKind::GrantKindOAuth)
            | (Self::ByKind { native: lines, .. }, MemberGrantKind::GrantKindNative) => lines,
        }
    }

    /// Every line, each attributed to its kind where the statement depends on it: a refusal
    /// made before anything is read states both.
    pub(super) fn every_line(&self) -> Vec<String> {
        match self {
            Self::Fixed(lines) => lines.iter().map(|&line| line.to_owned()).collect(),
            Self::ByKind { oauth, native } => oauth
                .iter()
                .map(|line| format!("for an OAuth grant, {line}"))
                .chain(
                    native
                        .iter()
                        .map(|line| format!("for a native grant, {line}")),
                )
                .collect(),
        }
    }
}

/// A leaf that states what acting does before it acts, and in place of acting when `--yes` is
/// missing: `authorize` who can use the grant (REQ-GRANT-35, REQ-GRANT-66), `revoke` and
/// `deauthorize` the runs that will be asked to stop (REQ-GRANT-30, REQ-GRANT-35,
/// REQ-GRANT-67, REQ-GRANT-68).
pub(in crate::cli) trait Stated: Args {
    /// The command path the refusal's usage names: the leaf's, or for an earlier leaf the
    /// `um grant` leaf that replaces it (REQ-GRANT-77).
    const COMMAND: &'static str;
    /// The arguments of the leaf at `COMMAND`, which the usage shows.
    type Usage: Args;
    /// The statement.
    const STATEMENT: Statement;
    /// What the leaf does, as the refusal's remedy names it.
    const ACTION: &'static str;
}

/// States `lines`, on standard error under `heading`: what a leaf is about to do.
pub(in crate::cli) fn state(
    heading: fmt::Arguments<'_>,
    lines: &[impl AsRef<str>],
) -> io::Result<()> {
    let mut stderr = io::stderr().lock();
    writeln!(stderr, "{heading}")?;
    for line in lines {
        writeln!(stderr, "  {}", line.as_ref())?;
    }
    Ok(())
}

/// The `--yes` of a `Stated` leaf `C`, required like `ConfirmationArgs`, whose absence is
/// refused with the leaf's statement: clap's own refusal of a missing required argument cannot
/// carry it.
pub(in crate::cli) struct Confirmation<C>(PhantomData<fn() -> C>);

impl<C> fmt::Debug for Confirmation<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Confirmation")
    }
}

const YES: &str = "yes";

impl<C: Stated> Args for Confirmation<C> {
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

impl<C: Stated> FromArgMatches for Confirmation<C> {
    fn from_arg_matches(matches: &ArgMatches) -> Result<Self, clap::Error> {
        if matches.get_flag(YES) {
            return Ok(Self(PhantomData));
        }
        let mut error = clap::Error::new(ErrorKind::MissingRequiredArgument);
        error.insert(
            ContextKind::InvalidArg,
            ContextValue::Strings(vec![format!("--{YES}")]),
        );
        let remedy = format!("{}, run the command again with --{YES}", C::ACTION);
        error.insert(
            ContextKind::Suggested,
            ContextValue::StyledStrs(
                C::STATEMENT
                    .every_line()
                    .into_iter()
                    .chain([remedy])
                    .map(Into::into)
                    .collect(),
            ),
        );
        let name = C::COMMAND.rsplit(' ').next().unwrap_or(C::COMMAND);
        let mut command = C::Usage::augment_args(clap::Command::new(name)).bin_name(C::COMMAND);
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
