use std::io::{self, Write};
use std::path::PathBuf;

use clap::Args;

use crate::exit_code::{ExitCode, OutcomeClass};
use useful_execution::format_bytes;
use useful_runner::RequestFailure;
use useful_runner::{AssignmentCounts, Operation, Response, StatusSnapshot};

pub(super) const ABOUT: &str = "Show live Runner Serve status";

#[derive(Debug, Args)]
pub(super) struct Command {
    /// Read the control socket path from the closed runner operator
    /// configuration; the runner home under the useful home is the default, read
    /// without creating anything.
    #[arg(long, value_name = "PATH")]
    config: Option<PathBuf>,
}

impl Command {
    pub(super) fn execute(self) -> super::super::CommandResult {
        let socket_path = super::configured_control_socket_path(self.config.as_deref())?;
        let response =
            useful_runner::request(&socket_path, Operation::Status).map_err(|error| {
                let outcome = match error {
                    RequestFailure::NotReachable => OutcomeClass::Unreachable,
                    RequestFailure::Protocol(_) => OutcomeClass::Protocol,
                };
                super::super::CommandFailure::for_outcome(anyhow::Error::new(error), outcome)
            })?;
        let Response::Status(status) = response else {
            return Err(super::super::CommandFailure::for_outcome(
                anyhow::anyhow!("Runner Serve control protocol is invalid"),
                OutcomeClass::Protocol,
            ));
        };
        write_status(&mut io::stdout().lock(), &status).map_err(anyhow::Error::new)?;
        Ok(ExitCode::Success)
    }
}

fn write_status(output: &mut impl Write, status: &StatusSnapshot) -> io::Result<()> {
    writeln!(
        output,
        "process:        {}",
        enum_json(status.process_state)?
    )?;
    writeln!(output, "boot:           {}", status.boot_id)?;
    writeln!(
        output,
        "uptime:         {}",
        format_uptime(status.uptime_milliseconds)
    )?;
    if let Some(log_path) = &status.log_path {
        writeln!(output, "log:            {log_path}")?;
    }
    if let Some(disk) = &status.disk {
        writeln!(
            output,
            "disk:           {} free of {} at {}",
            format_bytes(disk.available_bytes),
            format_bytes(disk.total_bytes),
            disk.path
        )?;
        if disk.low {
            writeln!(
                output,
                "disk warning:   below the minimum of {}",
                format_bytes(disk.minimum_free_bytes)
            )?;
        }
    }
    writeln!(
        output,
        "connection:     {}",
        enum_json(status.connection_state)?
    )?;
    if let Some(last_connected_at) = &status.last_connected_at {
        writeln!(output, "last connected: {last_connected_at}")?;
    }
    if let Some(credential_id) = &status.current_credential_id {
        writeln!(output, "credential:     {credential_id}")?;
    }
    if let Some(credential_id) = &status.pending_credential_id {
        writeln!(output, "pending:        {credential_id}")?;
    }
    writeln!(
        output,
        "assignments:    {}",
        format_assignments(status.assignment_counts)?
    )?;
    for held in &status.held_runs {
        writeln!(
            output,
            "run:            {} · {}",
            held.run_id,
            enum_json(held.state)?
        )?;
    }
    if let Some(failure) = status.last_connection_failure {
        writeln!(output, "last failure:   {}", enum_json(failure)?)?;
    }
    if let Some(readiness) = &status.agent_logins {
        super::agent_login::write_readiness(output, readiness)?;
    }
    Ok(())
}

pub(super) fn enum_json(value: impl serde::Serialize) -> io::Result<String> {
    let value = serde_json::to_value(value)?;
    value
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| io::Error::other("Runner Serve control state is not a string"))
}

fn format_uptime(milliseconds: u64) -> String {
    let seconds = milliseconds / 1_000;
    let days = seconds / 86_400;
    let hours = seconds % 86_400 / 3_600;
    let minutes = seconds % 3_600 / 60;
    let remaining_seconds = seconds % 60;
    if days > 0 {
        format!("{days}d {hours}h")
    } else if hours > 0 {
        format!("{hours}h {minutes}m")
    } else if minutes > 0 {
        format!("{minutes}m {remaining_seconds}s")
    } else {
        format!("{remaining_seconds}s")
    }
}

fn format_assignments(counts: AssignmentCounts) -> io::Result<String> {
    let total = counts
        .total()
        .ok_or_else(|| io::Error::other("assignment count overflow"))?;
    let details = [
        ("preparing", counts.preparing),
        ("accepted", counts.accepted),
        ("running", counts.running),
        ("waiting", counts.waiting),
        ("finishing", counts.finishing),
        ("reporting", counts.reporting),
    ]
    .into_iter()
    .filter(|(_, count)| *count != 0)
    .map(|(state, count)| format!("{state}: {count}"))
    .collect::<Vec<_>>();
    if details.is_empty() {
        Ok(format!("{total} total"))
    } else {
        Ok(format!("{total} total ({})", details.join(", ")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use useful_runner::{ConnectionState, ProcessState};

    // REQ-RUNNERENROLL-42, REQ-RUNNERENROLL-80
    #[test]
    fn renders_idle_and_nonzero_assignment_counts() {
        assert_eq!(
            format_assignments(AssignmentCounts::default()).unwrap(),
            "0 total"
        );
        assert_eq!(
            format_assignments(AssignmentCounts {
                running: 1,
                ..AssignmentCounts::default()
            })
            .unwrap(),
            "1 total (running: 1)"
        );
    }

    #[test]
    fn status_rejects_unrepresentable_counts_and_states() {
        assert_eq!(
            format_assignments(AssignmentCounts {
                preparing: u64::MAX,
                running: 1,
                ..AssignmentCounts::default()
            })
            .unwrap_err()
            .kind(),
            io::ErrorKind::Other
        );
        struct Unserializable;
        impl serde::Serialize for Unserializable {
            fn serialize<S>(&self, _: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                Err(serde::ser::Error::custom("invalid state"))
            }
        }
        assert_eq!(
            enum_json(Unserializable).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    // REQ-RUNNERENROLL-42
    #[test]
    fn renders_the_closed_live_snapshot_without_absent_optional_fields() {
        let status = StatusSnapshot {
            process_state: ProcessState::Running,
            boot_id: "rbt_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            uptime_milliseconds: 273_600_000,
            log_path: None,
            disk: None,
            connection_state: ConnectionState::Connected,
            last_connected_at: Some("2026-08-06T19:00:00Z".to_owned()),
            current_credential_id: Some("rrc_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned()),
            pending_credential_id: None,
            assignment_counts: AssignmentCounts {
                running: 1,
                ..AssignmentCounts::default()
            },
            last_connection_failure: None,
            agent_logins: None,
            held_runs: Vec::new(),
        };
        let mut output = Vec::new();
        write_status(&mut output, &status).unwrap();
        let expected = [
            "process:        running\n",
            "boot:           rbt_01k0z6r1w8f4jy2m7q9v3x5abc\n",
            "uptime:         3d 4h\n",
            "connection:     connected\n",
            "last connected: 2026-08-06T19:00:00Z\n",
            "credential:     ",
            "rrc_01k0z6r1w8f4jy2m7q9v3x5abc",
            "\nassignments:    1 total (running: 1)\n",
        ]
        .concat();
        assert_eq!(String::from_utf8(output).unwrap(), expected);
    }

    // REQ-RUNNERENROLL-42: the log the process writes is shown directly after its uptime.
    #[test]
    fn renders_the_log_path_after_the_uptime() {
        let status = StatusSnapshot {
            process_state: ProcessState::Running,
            boot_id: "rbt_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            uptime_milliseconds: 10_000,
            log_path: Some("/var/lib/runner/logs/runner.log".to_owned()),
            disk: None,
            connection_state: ConnectionState::Connecting,
            last_connected_at: None,
            current_credential_id: None,
            pending_credential_id: None,
            assignment_counts: AssignmentCounts::default(),
            last_connection_failure: None,
            agent_logins: None,
            held_runs: Vec::new(),
        };
        let mut output = Vec::new();
        write_status(&mut output, &status).unwrap();
        let output = String::from_utf8(output).unwrap();
        let lines = output.lines().collect::<Vec<_>>();
        let uptime = lines
            .iter()
            .position(|line| line.starts_with("uptime:"))
            .unwrap();
        assert_eq!(
            lines[uptime + 1],
            "log:            /var/lib/runner/logs/runner.log"
        );
        assert_eq!(
            lines.iter().filter(|line| line.starts_with("log:")).count(),
            1
        );
    }

    /// The lines `write_status` prints for a snapshot whose `disk` member is `disk`, with
    /// a log path, absent when `disk` is null.
    fn status_lines_with_disk(disk: serde_json::Value) -> Vec<String> {
        let mut status = serde_json::json!({
            "processState": "running",
            "bootId": "rbt_01k0z6r1w8f4jy2m7q9v3x5abc",
            "uptimeMilliseconds": 10_000,
            "logPath": "/var/lib/useful/runner/logs/runner.log",
            "connectionState": "connected",
            "assignmentCounts": {
                "preparing": 0, "accepted": 0, "running": 0, "finishing": 0, "reporting": 0
            }
        });
        if !disk.is_null() {
            status["disk"] = disk;
        }
        let status: StatusSnapshot = serde_json::from_value(status).unwrap();
        let mut output = Vec::new();
        write_status(&mut output, &status).unwrap();
        String::from_utf8(output)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect()
    }

    fn disk_reading(available_bytes: u64, low: bool) -> serde_json::Value {
        serde_json::json!({
            "path": "/var/lib/useful/runner/work",
            "availableBytes": available_bytes,
            "totalBytes": 394_u64 << 30,
            "minimumFreeBytes": 5_u64 << 30,
            "low": low,
            "readAt": "2026-11-19T14:00:30Z"
        })
    }

    // REQ-RUNNERENROLL-42: the disk reading is shown directly after the log path, and a
    // warning giving the minimum directly after it while the disk is low; a snapshot
    // without a reading shows neither.
    #[test]
    fn renders_the_disk_reading_after_the_log_and_a_warning_while_it_is_low() {
        let log = |lines: &[String]| {
            lines
                .iter()
                .position(|line| line.starts_with("log:"))
                .unwrap()
        };
        let disk_line = "disk:           120 GiB free of 394 GiB at /var/lib/useful/runner/work";

        let ok = status_lines_with_disk(disk_reading(120 << 30, false));
        assert_eq!(ok[log(&ok) + 1], disk_line);
        assert!(!ok.iter().any(|line| line.starts_with("disk warning:")));

        let low = status_lines_with_disk(disk_reading(6_186_599, true));
        assert_eq!(
            low[log(&low) + 1],
            "disk:           5.9 MiB free of 394 GiB at /var/lib/useful/runner/work"
        );
        assert_eq!(
            low[log(&low) + 2],
            "disk warning:   below the minimum of 5.0 GiB"
        );

        let none = status_lines_with_disk(serde_json::Value::Null);
        assert!(!none.iter().any(|line| line.starts_with("disk")));
    }
}
