use super::*;
use crate::service::source::ProviderSecret;

struct LocalGitLabGrantBroker {
    read_url: Arc<str>,
    revoked_preparation: AtomicUsize,
    issued_preparation: AtomicUsize,
}

impl LocalGitLabGrantBroker {
    fn grant(&self) -> ProviderCredential {
        ProviderCredential {
            repository_url: Arc::clone(&self.read_url),
            canonical_repository_url: Some(Arc::from("https://gitlab.com/group/sub/project.git")),
            token: ProviderSecret(b"grg_fixture-grant".to_vec()),
            expires_at: um_support::utc_now() + time::Duration::hours(1),
        }
    }
}

impl SourceCredentialBroker for LocalGitLabGrantBroker {
    fn issue(
        &self,
        _: &str,
        cancellation: &CaptureCancellation,
    ) -> Result<ProviderCredential, CredentialBrokerFailure> {
        if cancellation.is_cancelled() {
            return Err(CredentialBrokerFailure::Fenced);
        }
        let first = self.issued_preparation.fetch_add(1, Ordering::SeqCst) == 0;
        let mut credential = self.grant();
        if first {
            credential.expires_at = um_support::utc_now() + time::Duration::seconds(5);
        }
        Ok(credential)
    }

    fn revoke_preparation(&self, _: &str, _: &[u8]) -> Result<(), CredentialBrokerFailure> {
        self.revoked_preparation.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn commit_availability(
        &self,
        _: &str,
        _: &CaptureCancellation,
    ) -> Result<CommitAvailability, CredentialBrokerFailure> {
        panic!("GitLab cannot prove a pinned commit is absent")
    }

    fn issue_workflow_git(
        &self,
        _: &str,
        _: &CaptureCancellation,
    ) -> Result<ProviderCredential, CredentialBrokerFailure> {
        Ok(self.grant())
    }

    fn revoke_workflow_git(
        &self,
        _: &str,
        _: &[u8],
    ) -> Result<WorkflowGitRevocation, CredentialBrokerFailure> {
        Ok(WorkflowGitRevocation::GitLabRevoked)
    }
}

// This assignment fixture exercises the preparation and execution lifecycle.
// The Gateway tests separately use an audited Cloud grant and real HTTPS Git.
fn gitlab_assignment_fixture(
    workflow: &str,
) -> (
    tempfile::TempDir,
    AssignmentManager,
    AssignmentOffer,
    Arc<LocalGitLabGrantBroker>,
    Arc<str>,
) {
    let (temporary, mut manager) = manager_fixture(workflow);
    let source = source_fixture_path(&manager);
    let read_url: Arc<str> = Arc::from(url::Url::from_file_path(&source).unwrap().to_string());
    let broker = Arc::new(LocalGitLabGrantBroker {
        read_url: Arc::clone(&read_url),
        revoked_preparation: AtomicUsize::new(0),
        issued_preparation: AtomicUsize::new(0),
    });
    manager.source_broker = Some(broker.clone());
    let mut frame: Value = serde_json::from_slice(include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/fixtures/runner-protocol/v1/valid/cloud-assignment-offer-source-display.json"
    )))
    .unwrap();
    frame["payload"]["executionSpec"]["primaryWorkspaceSource"]["providerKind"] =
        serde_json::json!("gitlab");
    frame["payload"]["executionSpec"]["sourceDisplaySnapshot"]["repository"] = serde_json::json!({
        "providerKind": "gitlab", "instanceUrl": "https://gitlab.com", "fullName": "group/sub/project"
    });
    let mut offered = decoded_offer_from_frame(&serde_json::to_vec(&frame).unwrap());
    align_offer_with_source_fixture(&manager, &mut offered);
    (temporary, manager, offered, broker, read_url)
}

// REQ-WORKSPACE-32, REQ-WORKSPACE-34: preparation renews, executes, and
// removes its relay wiring before terminal settlement.
#[tokio::test]
async fn gitlab_assignment_fetches_executes_and_restores_the_relay_origin() {
    let (_temporary, mut manager, offered, broker, read_url) = gitlab_assignment_fixture(
        "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command: {argv: [\"true\"]}\n",
    );
    manager.handle_offer(offered.clone()).unwrap();
    prepare_current(&mut manager, &offered).await;
    assert_eq!(broker.issued_preparation.load(Ordering::SeqCst), 2);
    assert_eq!(broker.revoked_preparation.load(Ordering::SeqCst), 2);
    let workspace = match &manager.slot {
        Some(LocalSlot::Accepted(accepted)) => accepted.root.workspace.path().to_owned(),
        _ => panic!("GitLab source must prepare for execution"),
    };
    assert_eq!(
        run_fixture_git(&workspace, &["remote", "get-url", "origin"]),
        read_url.as_ref()
    );
    let reports = execute_to_terminal(&mut manager, &offered).await;
    assert_succeeded(&reports);
    wait_for_execution_finalization(&mut manager).await;
    assert_eq!(
        run_fixture_git(&workspace, &["remote", "get-url", "origin"]),
        "https://gitlab.com/group/sub/project.git"
    );
    acknowledge_terminal_and_settle(&mut manager).await;
}

// REQ-WORKSPACE-26, REQ-WORKSPACE-34: failed GitLab work retains the exact
// workspace bytes but never retains the runner's grant, helper or relay origin.
#[tokio::test]
async fn failed_gitlab_assignment_retains_edits_without_relay_wiring() {
    let (_temporary, mut manager, offered, broker, _) = gitlab_assignment_fixture(
        "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command: {argv: [\"sh\", \"-c\", \"printf retained > edit.txt; exit 23\"]}\n",
    );
    manager.handle_offer(offered.clone()).unwrap();
    prepare_current(&mut manager, &offered).await;
    let (workspace, private) = match &manager.slot {
        Some(LocalSlot::Accepted(accepted)) => (
            accepted.root.workspace.path().to_owned(),
            accepted.root.private.path().to_owned(),
        ),
        _ => panic!("GitLab source must prepare for execution"),
    };
    let reports = execute_to_terminal(&mut manager, &offered).await;
    assert!(
        matches!(reports.last(), Some(ExecutionReport::Finished { outcome, .. })
        if outcome["outcome"] == "failed")
    );
    wait_for_execution_finalization(&mut manager).await;
    assert_eq!(fs::read(workspace.join("edit.txt")).unwrap(), b"retained");
    assert_eq!(
        run_fixture_git(&workspace, &["remote", "get-url", "origin"]),
        "https://gitlab.com/group/sub/project.git"
    );
    assert!(!private.join("workflow-git-credential").exists());
    assert!(fs::read_dir(&private).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with("source-askpass-")
    }));
    let credential_config = um_test_support::fixture_git_command("git")
        .current_dir(&workspace)
        .args(["config", "--local", "--get-regexp", "^credential\\."])
        .output()
        .unwrap();
    assert!(!credential_config.status.success());
    assert!(broker.revoked_preparation.load(Ordering::SeqCst) >= 1);
    acknowledge_terminal_and_settle(&mut manager).await;
    assert!(workspace.exists());
}
