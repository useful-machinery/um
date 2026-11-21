//! The step driver's start gate (INV-ENGINE-46): one question the driver asks before it
//! prepares a command or agent step of the workflow's own steps, answered by whoever runs
//! the engine. With no gate the driver asks nothing.

use std::time::Duration;

/// How long a held step waits, on the runtime's clock, before the driver reads the gate
/// again.
pub(crate) const START_GATE_REREAD: Duration = Duration::from_secs(30);

/// What the gate answers when the driver is about to prepare a step.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StartGateReading {
    /// The step may start.
    Open,
    /// The disk is low: the step waits, still `starting`, until a later reading is open.
    DiskLow {
        available_bytes: u64,
        minimum_free_bytes: u64,
    },
}

/// The start gate a runner supplies to the engine. The driver reads it on a blocking
/// worker, so a read may block.
pub trait StepStartGate: Send + Sync + 'static {
    fn read(&self) -> StartGateReading;
}
