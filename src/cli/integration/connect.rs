//! Connecting a token service's token, the member's or an organization's
//! (REQ-INTEGRATION-16 to REQ-INTEGRATION-18). The served read and the refusal of the options of
//! a sign-in are the connect leaf's; this reads the owner's tokens and settles a replacement
//! before the token is read, then reads it from a file, standard input or a prompt that does not
//! echo, checks its form and stores it under one request key. The token stays in zeroizing
//! storage from the read to the request body and is dropped when the command returns.

use std::io::{self, IsTerminal};
use std::path::Path;

use anyhow::Context;
use zeroize::Zeroizing;

use super::{output, tokens};
use crate::cli::grant::replacement::{self, Replacing};
use crate::cli::grant::{self, Flow, material};
use crate::cli::secret_file::{SecretInput, prompt_without_echo};
use crate::cli::{CommandFailure, CommandResult};
use crate::exit_code::ExitCode;
use useful_api::{IntegrationToken, IntegrationTokenOwner, MemberGrantFailure, ServedTokenService};
use useful_human_auth::Deployment;

/// A token read from a file or standard input: REQ-INTEGRATION-1's bound, and one trailing LF or
/// CRLF, which is trimmed after reading.
const TOKEN_INPUT: SecretInput = SecretInput {
    name: "integration token",
    max_bytes: 4096 + 2,
    max_text: "4 KiB",
};

/// Connects the token of `service` for the member, or for the organization `org` names: reads
/// the owner's tokens, settles replacing the one it holds of the service, reads the token, then
/// stores it once, again under the same key when the answer was lost. A failure is reported with
/// its remedy and status; a refusal of the replacement or of the token read is exit 2 with
/// nothing sent.
pub(super) fn run(
    flow: &Flow<'_>,
    deployment: &Deployment,
    service: &ServedTokenService,
    from_file: Option<&Path>,
    org: Option<&str>,
) -> CommandResult {
    let api_url = deployment.fingerprint().api_url();
    let owner = org.map_or(
        IntegrationTokenOwner::Member,
        IntegrationTokenOwner::Organization,
    );
    let refused = |failure: &MemberGrantFailure| {
        output::write_refusal(
            api_url,
            &output::report(api_url, failure, &service.name, org),
            flow.json,
        )
    };
    let held = match flow.with_api(deployment, |api| tokens::held(api, owner, &service.name))? {
        Ok(held) => held,
        Err(failure) => return Ok(refused(&failure)?),
    };
    if let Some(token) = &held {
        replacement::settle_replacing(&replacing(token, org), flow.replace)?;
    }
    let token = read_token(service, from_file)?;
    let replace = held.is_some();
    let key = useful_support::generate_idempotency_key()
        .context("generate integration token request key")?;
    let stored = flow.with_api(deployment, |api| {
        grant::replayed(|| api.store_integration_token(owner, &service.name, &token, replace, &key))
    })?;
    drop(token);
    match stored {
        Ok(record) => Ok(output::write_connected(
            api_url, &record, org, replace, flow.json,
        )?),
        Err(failure) => Ok(refused(&failure)?),
    }
}

/// What replacing `token` settles, the member's or the organization `org` names.
fn replacing(token: &IntegrationToken, org: Option<&str>) -> Replacing<'static> {
    let (provider, id) = (&token.provider, &token.id);
    let (held, kept) = match org {
        None => (
            format!("you already hold a {provider} token: {id}"),
            format!("you kept your {provider} token {id}"),
        ),
        Some(org) => (
            format!("the organization {org} already holds a {provider} token: {id}"),
            format!("you kept the {provider} token {id} of the organization {org}"),
        ),
    };
    Replacing {
        held,
        kept,
        by: "a new token",
    }
}

/// Reads the token of `service`: from `from_file`, a private file or `-` for standard input;
/// without it from standard input when that is no terminal, and otherwise at a prompt on the
/// terminal that does not echo. One trailing line ending is trimmed, and a read that fails or a
/// token outside REQ-INTEGRATION-1's form is exit 2, whose message never quotes the input.
fn read_token(
    service: &ServedTokenService,
    from_file: Option<&Path>,
) -> Result<Zeroizing<Vec<u8>>, CommandFailure> {
    let title = output::title(service);
    let read = match from_file {
        Some(path) => TOKEN_INPUT.read(path),
        None if !io::stdin().is_terminal() => TOKEN_INPUT.read(Path::new("-")),
        None => prompt_without_echo(&format!("Paste the {title} token (input is hidden): ")),
    };
    read.and_then(|bytes| material::one_line_token(&format!("{title} token"), bytes))
        .map_err(|error| CommandFailure::with_exit_code(error, ExitCode::UsageError))
}
