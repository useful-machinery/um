use std::fs::File;
use std::sync::Arc;

use super::{RecoveryError, RecoveryHost};

// Model one host, filesystem and OS boot without consulting build-host identity.
// Directory identity, ownership, path validation and durable I/O remain real.
struct FixtureRecoveryHost;

impl RecoveryHost for FixtureRecoveryHost {
    fn host_binding(&self) -> Result<String, RecoveryError> {
        Ok("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into())
    }

    fn filesystem_id(&self, _directory: &File) -> Result<String, RecoveryError> {
        Ok("11111111:22222222".into())
    }

    fn os_boot_id(&self) -> std::io::Result<String> {
        Ok("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee".into())
    }
}

pub(in crate::service) fn fixture_host() -> Arc<dyn RecoveryHost> {
    Arc::new(FixtureRecoveryHost)
}
