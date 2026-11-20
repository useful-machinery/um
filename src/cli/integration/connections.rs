//! What `useful integration list` prints of the organizations' side beside the member's grants:
//! their tokens and then their connections at GitHub, Linear and GitLab, which the connection
//! family reads and this family shows, each as side `org` (REQ-INTEGRATION-19,
//! REQ-INTEGRATION-22), with the runners' authorizations in force for it (REQ-INTEGRATION-34).

use std::io::{self, Write};

use serde::{Serialize, Serializer};

use super::authorizations;
use super::output::ConnectionView;
use super::tokens::{self, Tokens};
use crate::cli::connection::organization::{self, OrganizationConnection};
use crate::exit_code::ExitCode;
use useful_api::{ConnectionAuthorization, MemberGrantApi, MemberGrantFailure};
use useful_human_auth::Deployment;

/// An organization whose connections the list reads: by the reference it is read by, with the
/// ID and slug the command knows of it, and whether it is read as its owner's, which decides
/// whether its installations and its connection authorizations are read, which Cloud serves an
/// owner alone.
struct Organization {
    reference: String,
    id: Option<String>,
    slug: Option<String>,
    as_owner: bool,
}

/// A connection as the list prints it: its named fields, its organization's ID and slug, the
/// membership read's, or with `--org` the reference as given and the ID the record states, and
/// the authorizations in force that name it, where the organization's were read and answered.
struct ListedConnection {
    connection: OrganizationConnection,
    organization_id: Option<String>,
    slug: Option<String>,
    authorizations: Option<Vec<ConnectionAuthorization>>,
}

impl ListedConnection {
    fn view(&self) -> ConnectionView<'_> {
        ConnectionView::new(
            &self.connection,
            self.organization_id.as_deref(),
            self.slug.as_deref(),
            self.authorizations.as_deref(),
        )
    }
}

/// The connections the list read, in the order it prints them.
#[derive(Default)]
pub(in crate::cli) struct Connections(Vec<ListedConnection>);

impl Connections {
    /// Writes each connection's section of a human report.
    fn write_human(&self, output: &mut impl Write) -> io::Result<()> {
        for connection in &self.0 {
            writeln!(output, "\n── connection ──")?;
            connection.view().write_human(output)?;
        }
        Ok(())
    }
}

impl Serialize for Connections {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.0.iter().map(ListedConnection::view))
    }
}

/// What `useful integration list` prints of the organizations' side: the tokens it read, and the
/// connections of the organizations it read them for, once they are read.
#[derive(Default)]
pub(in crate::cli) struct Listing {
    tokens: Tokens,
    connections: Connections,
    organizations: Vec<Organization>,
}

impl Listing {
    /// Every token of the member's and their organizations', with the organizations of the
    /// member's active memberships, whatever the deployment serves; the tokens are read only
    /// where `served` says the served read named a token service.
    pub(super) fn every_side(
        api: &MemberGrantApi,
        served: bool,
    ) -> Result<Self, MemberGrantFailure> {
        let (tokens, memberships) = tokens::every_side(api, served)?;
        let organizations = memberships
            .into_iter()
            .map(|membership| Organization {
                as_owner: membership.owner,
                reference: membership.organization_id.clone(),
                id: Some(membership.organization_id),
                slug: Some(membership.slug),
            })
            .collect();
        Ok(Self {
            tokens,
            connections: Connections::default(),
            organizations,
        })
    }

    /// The tokens of the organization `reference` names, an ID or a slug, which `list --org`
    /// prints alone, with that organization, read as its owner's whatever the member's role,
    /// since no membership is read.
    pub(super) fn organization_side(
        api: &MemberGrantApi,
        reference: &str,
    ) -> Result<Self, MemberGrantFailure> {
        let id = useful_support::valid_typed_id(reference, "org_");
        Ok(Self {
            tokens: tokens::organization_side(api, reference)?,
            connections: Connections::default(),
            organizations: vec![Organization {
                reference: reference.to_owned(),
                id: id.then(|| reference.to_owned()),
                slug: (!id).then(|| reference.to_owned()),
                as_owner: true,
            }],
        })
    }

    /// Reads the connections of each of the listing's organizations, in its order, through the
    /// connection family, each read under a session of its own (REQ-INTEGRATION-22), and then,
    /// where it read one and the organization is read as its owner's, the organization's
    /// connection authorizations in force, each attached to the connection it names in the order
    /// Cloud answers them; one that names a connection not listed is dropped
    /// (REQ-INTEGRATION-34). A read the connection family passes over leaves the organization's
    /// other reads to run, and an authorizations read passed over leaves its connections without
    /// any; any other failure is the inner error.
    pub(super) fn with_connections(
        mut self,
        deployment: &Deployment,
        allow_insecure_http: bool,
    ) -> anyhow::Result<Result<Self, MemberGrantFailure>> {
        for organization in &self.organizations {
            let read = organization::of_organization(
                deployment,
                &organization.reference,
                organization.as_owner,
                allow_insecure_http,
            )?;
            let connections = match read {
                Ok(connections) => connections,
                Err(failure) => return Ok(Err(failure)),
            };
            let authorizations = if organization.as_owner && !connections.is_empty() {
                match authorizations::of_organization(
                    deployment,
                    &organization.reference,
                    allow_insecure_http,
                )? {
                    Ok(authorizations) => authorizations,
                    Err(failure) => return Ok(Err(failure)),
                }
            } else {
                None
            };
            self.connections
                .0
                .extend(connections.into_iter().map(|connection| {
                    ListedConnection {
                        organization_id: organization
                            .id
                            .clone()
                            .or_else(|| connection.organization_id.clone()),
                        slug: organization.slug.clone(),
                        authorizations: authorizations.as_ref().map(|authorizations| {
                            authorizations
                                .iter()
                                .filter(|authorization| {
                                    authorization.connection_id == connection.id
                                })
                                .cloned()
                                .collect()
                        }),
                        connection,
                    }
                }));
        }
        Ok(Ok(self))
    }

    pub(in crate::cli) const fn tokens(&self) -> &Tokens {
        &self.tokens
    }

    pub(in crate::cli) const fn connections(&self) -> &Connections {
        &self.connections
    }

    /// Writes the token sections and then the connection sections of a human report; nothing
    /// where there are neither.
    pub(in crate::cli) fn write_human(&self, output: &mut impl Write) -> io::Result<()> {
        self.tokens.write_human(output)?;
        self.connections.write_human(output)
    }

    /// Reports one organization's tokens and connections, which `list --org` prints alone, as
    /// the one document or a report (REQ-INTEGRATION-19, REQ-INTEGRATION-22).
    pub(in crate::cli) fn write_alone(
        &self,
        deployment: &str,
        json: bool,
    ) -> anyhow::Result<ExitCode> {
        super::output::write_organization_side(deployment, self, json)
    }
}
