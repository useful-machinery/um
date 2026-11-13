//! The published tree's files (REQ-RUNNERENROLL-91): download capabilities from the Gateway for
//! the spec's declared files, then each file's bytes, verified against the spec's
//! SHA-256 and size and written with the spec's executable bit.

use std::collections::BTreeMap;
use std::fs::{self, DirBuilder, File, OpenOptions};
use std::io::Write as _;
use std::os::unix::fs::{DirBuilderExt as _, OpenOptionsExt as _, PermissionsExt as _};
use std::path::{Component, Path};
use std::sync::Arc;

use reqwest::StatusCode;
use reqwest::header::CONTENT_TYPE;
use ring::digest::{Context as DigestContext, SHA256};
use serde::Deserialize;
use url::Url;

use crate::credential::Credential;
use crate::service::run_inputs::{
    BrokerFailure, PreparationDeadline, broker_operation_runtime, validate_capability_url,
};
use um_execution::CaptureCancellation;
use um_runner_protocol::{WorkflowTreeFileV1RunnerProjection, WorkflowTreeV1RunnerProjection};

const CAPABILITIES_PATH: &str = "/v1/runner/workflow-trees/download-capabilities";
/// The Gateway's request limit: at most 256 distinct paths per capability request.
const MAXIMUM_PATHS_PER_REQUEST: usize = 256;
const CAPABILITY_RESPONSE_LIMIT: usize = 2 * 1024 * 1024;

/// Why the tree's files could not be delivered.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::service) enum TreeDownloadFailure {
    /// The Gateway or the object store did not answer usefully.
    Unavailable,
    /// The preparation was cancelled, its deadline passed or Cloud refused the boot.
    Fenced,
    /// A capability answer that does not match the request.
    InvalidResponse,
    /// A file whose bytes disagree with the spec's SHA-256 or size.
    ContentMismatch,
    /// The local tree directory could not be written.
    Environment,
}

impl TreeDownloadFailure {
    pub(in crate::service) const fn cause(self) -> &'static str {
        match self {
            Self::Unavailable => "workflow_tree_service_unavailable",
            Self::Fenced => "assignment_fenced",
            Self::InvalidResponse => "workflow_tree_capability_invalid",
            Self::ContentMismatch => "workflow_tree_file_mismatch",
            Self::Environment => "execution_root_unavailable",
        }
    }
}

/// The tree's reading of a failure the run-input broker's shared helpers report.
impl From<BrokerFailure> for TreeDownloadFailure {
    fn from(failure: BrokerFailure) -> Self {
        match failure {
            BrokerFailure::Unavailable | BrokerFailure::ContentUnavailable => Self::Unavailable,
            BrokerFailure::Fenced => Self::Fenced,
            BrokerFailure::InvalidResponse => Self::InvalidResponse,
            BrokerFailure::ContentMismatch => Self::ContentMismatch,
            BrokerFailure::Environment => Self::Environment,
        }
    }
}

/// One file's signed download, as the capability answer names it.
#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(in crate::service) struct FileCapability {
    pub(in crate::service) path: String,
    pub(in crate::service) sha256: String,
    pub(in crate::service) size: u64,
    pub(in crate::service) executable: bool,
    pub(in crate::service) url: String,
}

impl std::fmt::Debug for FileCapability {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FileCapability")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

/// The assignment a capability request is made for.
#[derive(Clone, Copy)]
pub(in crate::service) struct TreeRequest<'a> {
    pub(in crate::service) assignment_id: &'a str,
    pub(in crate::service) execution_spec_id: &'a str,
    pub(in crate::service) digest: &'a str,
}

pub(in crate::service) trait TreeBroker: Send + Sync {
    fn capabilities(
        &self,
        request: TreeRequest<'_>,
        paths: &[&str],
        cancellation: &CaptureCancellation,
        deadline: PreparationDeadline,
    ) -> Result<Vec<FileCapability>, TreeDownloadFailure>;

    // The object-safe broker declaration mirrors its HTTP implementation.
    fn download(
        &self,
        url: &str,
        cancellation: &CaptureCancellation,
        deadline: PreparationDeadline,
        consume: &mut dyn FnMut(&[u8]) -> Result<(), TreeDownloadFailure>,
    ) -> Result<(), TreeDownloadFailure>;
}

/// The Gateway's private tree broker on the runner endpoint's host.
pub(in crate::service) struct HttpTreeBroker {
    capability_endpoint: Url,
    runner_credential: Credential,
    boot_id: Arc<str>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityAnswer {
    schema_version: u64,
    digest: String,
    #[serde(rename = "capabilityExpiresAt")]
    _capability_expires_at: String,
    files: Vec<FileCapability>,
}

impl HttpTreeBroker {
    pub(in crate::service) fn new(
        endpoint: &Url,
        runner_credential: &Credential,
        boot_id: &str,
    ) -> Result<Self, ()> {
        Ok(Self {
            capability_endpoint: crate::service::source::private_runner_http_endpoint(
                endpoint,
                CAPABILITIES_PATH,
            )?,
            runner_credential: runner_credential.clone(),
            boot_id: Arc::from(boot_id),
        })
    }
}

/// Runs one broker operation on the run-input broker's client and runtime, bounded by the
/// preparation deadline and abandoned when the preparation is cancelled.
fn block_on_operation<T>(
    deadline: PreparationDeadline,
    cancellation: &CaptureCancellation,
    operation: impl AsyncFnOnce(reqwest::Client) -> Result<T, TreeDownloadFailure>,
) -> Result<T, TreeDownloadFailure> {
    if cancellation.is_cancelled() {
        return Err(TreeDownloadFailure::Fenced);
    }
    let (client, runtime) = broker_operation_runtime(deadline)?;
    runtime.block_on(async {
        tokio::select! {
            result = operation(client) => result,
            () = crate::service::source::wait_for_cancellation(cancellation) => {
                Err(TreeDownloadFailure::Fenced)
            }
        }
    })
}

impl TreeBroker for HttpTreeBroker {
    fn capabilities(
        &self,
        request: TreeRequest<'_>,
        paths: &[&str],
        cancellation: &CaptureCancellation,
        deadline: PreparationDeadline,
    ) -> Result<Vec<FileCapability>, TreeDownloadFailure> {
        let body = serde_json::json!({
            "schemaVersion": 1,
            "bootId": self.boot_id.as_ref(),
            "assignmentId": request.assignment_id,
            "executionSpecId": request.execution_spec_id,
            "digest": request.digest,
            "files": paths,
        });
        let encoded = block_on_operation(deadline, cancellation, async |client| {
            let mut response = client
                .post(self.capability_endpoint.clone())
                .bearer_auth(self.runner_credential.bearer_value())
                .json(&body)
                .send()
                .await
                .map_err(|_| TreeDownloadFailure::Unavailable)?;
            match response.status() {
                StatusCode::OK => {}
                StatusCode::CONFLICT => return Err(TreeDownloadFailure::Fenced),
                StatusCode::BAD_REQUEST => return Err(TreeDownloadFailure::InvalidResponse),
                _ => return Err(TreeDownloadFailure::Unavailable),
            }
            if response
                .headers()
                .get(CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                != Some("application/json")
            {
                return Err(TreeDownloadFailure::InvalidResponse);
            }
            let mut encoded = Vec::new();
            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(|_| TreeDownloadFailure::Unavailable)?
            {
                if encoded.len() + chunk.len() > CAPABILITY_RESPONSE_LIMIT {
                    return Err(TreeDownloadFailure::InvalidResponse);
                }
                encoded.extend_from_slice(&chunk);
            }
            Ok(encoded)
        })?;
        let answer: CapabilityAnswer = um_support::strict_json_from_slice(&encoded)
            .and_then(serde_json::from_value)
            .map_err(|_| TreeDownloadFailure::InvalidResponse)?;
        if answer.schema_version != 1 || answer.digest != request.digest {
            return Err(TreeDownloadFailure::InvalidResponse);
        }
        Ok(answer.files)
    }

    fn download(
        &self,
        url: &str,
        cancellation: &CaptureCancellation,
        deadline: PreparationDeadline,
        consume: &mut dyn FnMut(&[u8]) -> Result<(), TreeDownloadFailure>,
    ) -> Result<(), TreeDownloadFailure> {
        let url = validate_capability_url(url)?;
        block_on_operation(deadline, cancellation, async |client| {
            let mut response = client
                .get(url)
                .header("Accept-Encoding", "identity")
                .send()
                .await
                .map_err(|_| TreeDownloadFailure::Unavailable)?;
            if response.status() != StatusCode::OK {
                return Err(TreeDownloadFailure::Unavailable);
            }
            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(|_| TreeDownloadFailure::Unavailable)?
            {
                consume(&chunk)?;
            }
            Ok(())
        })
    }
}

/// A tree-relative path the runner may create under the tree directory: normal
/// components only, so a declared path never leaves it.
pub(in crate::service) fn is_safe_tree_path(path: &str) -> bool {
    !path.is_empty()
        && !path.contains('\0')
        && Path::new(path)
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}

/// Downloads every declared file of `tree` into the empty directory `destination`
/// it creates, each verified against the spec before it counts as delivered.
pub(in crate::service) fn download_tree(
    broker: &dyn TreeBroker,
    request: TreeRequest<'_>,
    tree: &WorkflowTreeV1RunnerProjection,
    destination: &Path,
    cancellation: &CaptureCancellation,
    deadline: PreparationDeadline,
) -> Result<(), TreeDownloadFailure> {
    DirBuilder::new()
        .mode(0o700)
        .create(destination)
        .map_err(|_| TreeDownloadFailure::Environment)?;
    for files in tree.files.chunks(MAXIMUM_PATHS_PER_REQUEST) {
        let declared = files
            .iter()
            .map(|file| (file.path.as_str(), file))
            .collect::<BTreeMap<_, _>>();
        let paths = declared.keys().copied().collect::<Vec<_>>();
        let capabilities = broker.capabilities(request, &paths, cancellation, deadline)?;
        if capabilities.len() != declared.len() {
            return Err(TreeDownloadFailure::InvalidResponse);
        }
        for capability in &capabilities {
            let file = declared
                .get(capability.path.as_str())
                .ok_or(TreeDownloadFailure::InvalidResponse)?;
            if capability.sha256 != file.sha256
                || capability.size != file.size
                || capability.executable != file.executable
            {
                return Err(TreeDownloadFailure::InvalidResponse);
            }
            download_file(
                broker,
                capability,
                file,
                destination,
                cancellation,
                deadline,
            )?;
        }
    }
    Ok(())
}

fn download_file(
    broker: &dyn TreeBroker,
    capability: &FileCapability,
    file: &WorkflowTreeFileV1RunnerProjection,
    destination: &Path,
    cancellation: &CaptureCancellation,
    deadline: PreparationDeadline,
) -> Result<(), TreeDownloadFailure> {
    if !is_safe_tree_path(&file.path) {
        return Err(TreeDownloadFailure::InvalidResponse);
    }
    let target = destination.join(&file.path);
    if let Some(parent) = target.parent() {
        DirBuilder::new()
            .mode(0o700)
            .recursive(true)
            .create(parent)
            .map_err(|_| TreeDownloadFailure::Environment)?;
    }
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&target)
        .map_err(|_| TreeDownloadFailure::Environment)?;
    let mut digest = DigestContext::new(&SHA256);
    let mut written = 0_u64;
    broker.download(&capability.url, cancellation, deadline, &mut |chunk| {
        written = written
            .checked_add(chunk.len() as u64)
            .filter(|written| *written <= file.size)
            .ok_or(TreeDownloadFailure::ContentMismatch)?;
        digest.update(chunk);
        output
            .write_all(chunk)
            .map_err(|_| TreeDownloadFailure::Environment)
    })?;
    let observed = digest
        .finish()
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    if written != file.size || observed != file.sha256 {
        return Err(TreeDownloadFailure::ContentMismatch);
    }
    output
        .sync_all()
        .map_err(|_| TreeDownloadFailure::Environment)?;
    drop(output);
    let mode = if file.executable { 0o755 } else { 0o644 };
    fs::set_permissions(&target, fs::Permissions::from_mode(mode))
        .map_err(|_| TreeDownloadFailure::Environment)?;
    File::open(destination)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| TreeDownloadFailure::Environment)
}
