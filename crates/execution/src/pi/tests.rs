use super::*;
use crate::process::{CommandProbeError, CommandRequest};
use std::fs;
use std::sync::Mutex;
use std::time::Duration;

const COMPLETE_HELP: &str = "pi - fixture\nUsage:\n  pi [options] [--] [@files...] [messages...]\n  --mode <mode> Output mode: text, json, or rpc\n  --session-dir <dir> Directory for session storage and lookup\n  --extension, -e <path> Load extension\n  --append-system-prompt <text> Append prompt\n  --approve, -a Trust project files for this run\n";
const LOWER_BOUND_HELP: &str = "pi - fixture\nUsage:\n  pi [options] [@files...] [messages...]\n  --mode <mode> Output mode: text, json, or rpc\n  --session-dir <dir> Directory for session storage and lookup\n  --extension, -e <path> Load extension\n  --append-system-prompt <text> Append prompt\n  --approve, -a Trust project files for this run\n";

struct FakeRunner {
    invocations: Mutex<Vec<Vec<String>>>,
    version: CommandOutput,
    capabilities: CommandOutput,
}

impl CommandRunner for FakeRunner {
    fn run(&self, command: CommandRequest<'_>) -> Result<CommandOutput, CommandProbeError> {
        assert!(command.program.is_absolute());
        assert_eq!(command.timeout, Duration::from_secs(30));
        assert!(command.clear_environment);
        assert_isolated(command.environment, command.current_directory);
        self.invocations.lock().unwrap().push(
            command
                .args
                .iter()
                .map(|argument| (*argument).to_owned())
                .collect(),
        );
        match command.args {
            ["--version"] => {
                assert_eq!(command.maximum_stdout_bytes, 4 * 1024);
                Ok(self.version.clone())
            }
            args if args == CAPABILITY_PROBE_ARGUMENTS => {
                assert_eq!(command.maximum_stdout_bytes, 64 * 1024);
                Ok(self.capabilities.clone())
            }
            _ => Err(CommandProbeError::Spawn),
        }
    }
}

fn assert_isolated(environment: &[(&OsStr, &OsStr)], current_directory: Option<&Path>) {
    let current_directory = current_directory.unwrap();
    let root = current_directory.parent().unwrap();
    assert_eq!(current_directory, root.join("project"));
    assert_eq!(environment.len(), 12);
    assert_eq!(
        environment
            .iter()
            .find(|(candidate, _)| *candidate == OsStr::new("PATH"))
            .map(|(_, value)| *value),
        Some(OsStr::new("/controlled/bin"))
    );
    for (name, expected) in [
        ("HOME", root.join("home")),
        ("PI_CODING_AGENT_DIR", root.join("agent")),
        ("XDG_CONFIG_HOME", root.join("config")),
        ("XDG_CACHE_HOME", root.join("cache")),
        ("XDG_DATA_HOME", root.join("data")),
        ("XDG_STATE_HOME", root.join("state")),
    ] {
        assert_eq!(
            environment
                .iter()
                .find(|(candidate, _)| *candidate == OsStr::new(name))
                .map(|(_, value)| *value),
            Some(expected.as_os_str())
        );
    }
    for (name, expected) in [
        ("PI_OFFLINE", "1"),
        ("PI_SKIP_VERSION_CHECK", "1"),
        ("PI_TELEMETRY", "0"),
        ("FORCE_COLOR", "0"),
        ("NO_COLOR", "1"),
    ] {
        assert_eq!(
            environment
                .iter()
                .find(|(candidate, _)| *candidate == OsStr::new(name))
                .map(|(_, value)| *value),
            Some(OsStr::new(expected))
        );
    }
}

fn output(bytes: &[u8]) -> CommandOutput {
    CommandOutput {
        success: true,
        stdout: bytes.to_vec(),
        truncated: false,
    }
}

#[test]
fn bounded_compatibility_policy_constructs_the_complete_validated_value() {
    let executable = std::env::current_exe().unwrap();
    let runner = FakeRunner {
        invocations: Mutex::new(Vec::new()),
        version: output(b"0.99.1\n"),
        capabilities: output(COMPLETE_HELP.as_bytes()),
    };

    let installation =
        validate_pi_installation_with(&executable, OsStr::new("/controlled/bin"), &runner).unwrap();

    assert_eq!(
        installation.executable(),
        fs::canonicalize(executable).unwrap()
    );
    assert_eq!(installation.version().as_str(), "0.99.1");
    assert_eq!(installation.profile().as_str(), "PiJsonV1");
    assert_eq!(
        installation.capabilities().required(),
        REQUIRED_CAPABILITIES
    );
    assert_eq!(
        *runner.invocations.lock().unwrap(),
        [
            vec!["--version".to_owned()],
            CAPABILITY_PROBE_ARGUMENTS.map(str::to_owned).to_vec(),
        ]
    );
}

#[test]
fn lower_bound_and_intervening_patches_retain_exact_versions() {
    let executable = std::env::current_exe().unwrap();
    for (version, help) in [
        ("0.84.2", LOWER_BOUND_HELP),
        ("0.85.1", COMPLETE_HELP),
        ("0.86.1", COMPLETE_HELP),
        ("0.87.1", COMPLETE_HELP),
        ("0.99.1", COMPLETE_HELP),
        ("1.2.0", COMPLETE_HELP),
        ("1.3.0", COMPLETE_HELP),
    ] {
        let runner = FakeRunner {
            invocations: Mutex::new(Vec::new()),
            version: output(format!("{version}\n").as_bytes()),
            capabilities: output(help.as_bytes()),
        };

        let installation =
            validate_pi_installation_with(&executable, OsStr::new("/controlled/bin"), &runner)
                .unwrap();

        assert_eq!(installation.version().as_str(), version);
        assert_eq!(installation.profile(), AgentCompatibilityProfile::PiJsonV1);
    }
}

// REQ-AGENT-1, REQ-AGENT-7
// INV-HARNESS-1
#[test]
fn version_range_and_capabilities_are_both_required() {
    let executable = std::env::current_exe().unwrap();
    for unsupported in [
        "0.83.0", "0.84.1", "0.88.0", "0.95.0", "0.99.0", "0.99.2", "0.100.0", "1.1.0", "1.2.1",
    ] {
        let runner = FakeRunner {
            invocations: Mutex::new(Vec::new()),
            version: output(format!("{unsupported}\n").as_bytes()),
            capabilities: output(COMPLETE_HELP.as_bytes()),
        };
        assert_eq!(
            validate_pi_installation_with(&executable, OsStr::new("/controlled/bin"), &runner,),
            Err(PiInstallationFailure::Unsupported(
                PiIncompatibility::Version(unsupported.to_owned())
            ))
        );
        assert_eq!(
            *runner.invocations.lock().unwrap(),
            [vec!["--version".to_owned()]]
        );
    }

    for malformed in ["0.84.2-rc.1", "0.084.2", "0.84"] {
        let runner = FakeRunner {
            invocations: Mutex::new(Vec::new()),
            version: output(format!("{malformed}\n").as_bytes()),
            capabilities: output(COMPLETE_HELP.as_bytes()),
        };
        assert_eq!(
            validate_pi_installation_with(&executable, OsStr::new("/controlled/bin"), &runner,),
            Err(PiInstallationFailure::Malformed {
                probe: PiProbe::Version,
                version: None,
            })
        );
    }

    for (advertisement, replacement, capability) in [
        (
            "--mode <mode>",
            "--output <mode>",
            PiCapability::JsonEventStream,
        ),
        (
            "--session-dir <dir>",
            "--session-root <dir>",
            PiCapability::CustomSessionDirectory,
        ),
        (
            "--extension, -e <path>",
            "--plugin, -e <path>",
            PiCapability::ExtensionLoading,
        ),
        (
            "--append-system-prompt <text>",
            "--system-prompt <text>",
            PiCapability::SystemPromptAppend,
        ),
        (
            "--approve, -a",
            "--permit, -p",
            PiCapability::InvocationScopedProjectTrust,
        ),
    ] {
        let runner = FakeRunner {
            invocations: Mutex::new(Vec::new()),
            version: output(b"0.99.1\n"),
            capabilities: output(COMPLETE_HELP.replace(advertisement, replacement).as_bytes()),
        };
        assert_eq!(
            validate_pi_installation_with(&executable, OsStr::new("/controlled/bin"), &runner),
            Err(PiInstallationFailure::Unsupported(
                PiIncompatibility::Capability {
                    capability,
                    version: "0.99.1".to_owned(),
                }
            ))
        );
    }
}

fn validate_with(
    version: &str,
    help: &str,
) -> (
    Result<ValidatedPiInstallation, PiInstallationFailure>,
    Vec<Vec<String>>,
) {
    let runner = FakeRunner {
        invocations: Mutex::new(Vec::new()),
        version: output(format!("{version}\n").as_bytes()),
        capabilities: output(help.as_bytes()),
    };
    let result = validate_pi_installation_with(
        &std::env::current_exe().unwrap(),
        OsStr::new("/controlled/bin"),
        &runner,
    );
    let invocations = runner.invocations.into_inner().unwrap();
    (result, invocations)
}

// REQ-AGENT-1, REQ-AGENT-4, REQ-AGENT-5, REQ-AGENT-7, REQ-AGENT-65
// INV-HARNESS-1, INV-HARNESS-12
#[test]
fn stable_releases_newer_than_the_qualification_release_are_admitted_untested() {
    let both_probes = [
        vec!["--version".to_owned()],
        CAPABILITY_PROBE_ARGUMENTS.map(str::to_owned).to_vec(),
    ];
    for version in ["1.3.1", "1.4.0", "2.0.0"] {
        let (result, invocations) = validate_with(version, COMPLETE_HELP);
        let installation = result.unwrap();

        assert_eq!(installation.version().as_str(), version);
        assert_eq!(installation.profile(), AgentCompatibilityProfile::PiJsonV1);
        assert_eq!(installation.qualification(), HarnessQualification::Untested);
        assert_eq!(
            installation.capabilities().required(),
            REQUIRED_CAPABILITIES
        );
        assert_eq!(invocations, both_probes);
        let advisory = installation.untested_advisory().unwrap();
        assert_eq!(advisory.harness(), "Pi");
        assert_eq!(advisory.profile(), AgentCompatibilityProfile::PiJsonV1);
        assert_eq!(advisory.version(), version);
        assert_eq!(advisory.qualification_version(), "1.3.0");
        let line = advisory.to_string();
        assert!(line.contains(version) && line.contains("1.3.0"), "{line}");
    }

    for version in ["1.3.0", "0.99.1"] {
        let (result, invocations) = validate_with(version, COMPLETE_HELP);
        let installation = result.unwrap();
        assert_eq!(
            installation.qualification(),
            HarnessQualification::SupportedRange
        );
        assert_eq!(installation.untested_advisory(), None);
        assert_eq!(invocations, both_probes);
    }

    let (result, invocations) = validate_with(
        "1.4.0",
        &COMPLETE_HELP.replace("--approve, -a", "--permit, -p"),
    );
    assert_eq!(
        result,
        Err(PiInstallationFailure::Unsupported(
            PiIncompatibility::Capability {
                capability: PiCapability::InvocationScopedProjectTrust,
                version: "1.4.0".to_owned(),
            }
        ))
    );
    assert_eq!(invocations, both_probes);

    let (result, invocations) = validate_with("1.4.0-rc.1", COMPLETE_HELP);
    assert_eq!(
        result,
        Err(PiInstallationFailure::Malformed {
            probe: PiProbe::Version,
            version: None,
        })
    );
    assert_eq!(invocations, [vec!["--version".to_owned()]]);

    for version in ["0.99.2", "1.2.1"] {
        let (result, invocations) = validate_with(version, COMPLETE_HELP);
        assert_eq!(
            result,
            Err(PiInstallationFailure::Unsupported(
                PiIncompatibility::Version(version.to_owned())
            ))
        );
        assert_eq!(invocations, [vec!["--version".to_owned()]]);
    }
}

// INV-HARNESS-12
#[test]
fn version_fixtures_derive_the_qualification_by_the_admission_policy() {
    let anchor = ValidatedPiInstallation::fixture(PathBuf::from("/pi"));
    assert_eq!(anchor.version().as_str(), PI_JSON_V1_QUALIFICATION_VERSION);
    assert_eq!(anchor.qualification(), HarnessQualification::SupportedRange);

    let newer =
        ValidatedPiInstallation::fixture_with_version(PathBuf::from("/pi"), (1, 4, 0), "1.4.0");
    assert_eq!(newer.version().as_str(), "1.4.0");
    assert_eq!(newer.qualification(), HarnessQualification::Untested);
    assert_eq!(newer.profile(), anchor.profile());
    assert_eq!(newer.capabilities(), anchor.capabilities());
}
