//! An assignment answer's log as a document of rows, built once per answer: a heading per
//! named stream and each line of its text made visible, or a step's records one row each,
//! with each row's place in its file or in its step's output kept beside it, by which a
//! paused view holds its top line through later answers (REQ-RUNNERENROLL-146,
//! REQ-LOCALRUN-47, REQ-LOCALRUN-48, REQ-LOCALRUN-66, REQ-LOCALRUN-67).

use super::*;
use crate::workflow::presentation_feed::normalize_retained_prefix;

/// Where a row stands in its log: its stream's index in the log, the bytes of the stream's
/// file before its line (the stream's left-out bytes plus the line's offset in the text) or,
/// for a step's record, the record's ordinal in the step's output (its older records not
/// shown plus its index), none for a heading or a note, which come before the stream's
/// lines, and its index among the rows of the same line. Places rise from row to row.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct LogPlace {
    pub(super) stream: usize,
    pub(super) offset: Option<u64>,
    pub(super) record: usize,
}

/// A log as rows of text and tone, each with its place.
pub(super) struct LogDocument {
    /// What the log is and what was left out of it, the pane's first row.
    pub(super) heading: String,
    /// Whether this is the run log, beside which a step in flight says its own output is
    /// kept when it settles.
    pub(super) run_log: bool,
    pub(super) rows: Vec<OutputRow>,
    /// Beside each row, its place.
    pub(super) places: Vec<LogPlace>,
    /// The rows that head a named stream, which the full-screen log keeps in view.
    pub(super) stream_headings: Vec<usize>,
    /// The display width of the widest row.
    pub(super) width: usize,
}

impl LogDocument {
    pub(super) fn new(log: &RunnerWatchLog) -> Self {
        let left_out = |bytes: u64| format!("{bytes} B left out");
        let unnamed_left_out = log
            .streams
            .iter()
            .find(|stream| stream.name.is_none() && stream.omitted_bytes != 0)
            .map(|stream| left_out(stream.omitted_bytes));
        let step_output = matches!(log.source, RunnerWatchLogSource::Step { .. });
        let mut heading = match &log.source {
            RunnerWatchLogSource::Step {
                step,
                invocation,
                earlier_invocations_omitted,
            } => {
                let mut facts = vec![format!("OUTPUT {}", visible_text(step))];
                facts.extend(invocation.map(|invocation| format!("invocation {invocation}")));
                match earlier_invocations_omitted {
                    0 => {}
                    1 => facts.push("1 earlier invocation not shown".to_owned()),
                    count => facts.push(format!("{count} earlier invocations not shown")),
                }
                facts
            }
            RunnerWatchLogSource::RunLog => vec!["RUN LOG".to_owned()],
            RunnerWatchLogSource::StepRecords {
                step: Some(step),
                omitted_records,
                omitted_bytes,
            } => {
                let mut facts = vec![format!("OUTPUT {}", visible_text(step))];
                // REQ-LOCALRUN-40.
                if *omitted_records != 0 || *omitted_bytes != 0 {
                    facts.push(match omitted_records {
                        1 => "1 older record not shown".to_owned(),
                        count => format!("{count} older records not shown"),
                    });
                    facts.push(left_out(*omitted_bytes));
                }
                facts
            }
            RunnerWatchLogSource::StepRecords { step: None, .. } => vec![
                "RUN LOG".to_owned(),
                "this run has none; its output is shown by step".to_owned(),
            ],
            RunnerWatchLogSource::Unknown(word) => vec![format!("LOG {}", visible_text(word))],
        };
        heading.extend(unnamed_left_out);
        let mut document = Self {
            heading: heading.join(" · "),
            run_log: log.source == RunnerWatchLogSource::RunLog,
            rows: Vec::new(),
            places: Vec::new(),
            stream_headings: Vec::new(),
            width: 0,
        };
        if step_output && log.streams.is_empty() {
            // Not an empty stream: the step retained none (REQ-LOCALRUN-67).
            document.push(
                "no output is retained for this step".to_owned(),
                Tone::Muted,
                0,
                None,
            );
        } else if step_output {
            // REQ-LOCALRUN-66.
            document.push(
                "stdout is shown before stderr; their relative order is not recorded".to_owned(),
                Tone::Blocked,
                0,
                None,
            );
        }
        if let RunnerWatchLogSource::StepRecords {
            step: Some(_),
            omitted_records,
            ..
        } = log.source
        {
            document.append_records(omitted_records, &log.records);
        }
        for (index, stream) in log.streams.iter().enumerate() {
            document.append_stream(index, stream);
        }
        document.width = document_width(&document.rows);
        document
    }

    fn push(&mut self, text: String, tone: Tone, stream: usize, offset: Option<u64>) {
        let record = self
            .places
            .last()
            .filter(|last| last.stream == stream && last.offset == offset)
            .map_or(0, |last| last.record.saturating_add(1));
        self.rows.push(OutputRow { text, tone });
        self.places.push(LogPlace {
            stream,
            offset,
            record,
        });
    }

    /// A step's records, one row each under the run interface's label for its source and in
    /// its tone, a word this release does not know under the word itself (REQ-RUNNERENROLL-135).
    /// A row's place is its record's ordinal in the step's whole output, the same in every
    /// later answer. A step with no records says so in one row.
    fn append_records(&mut self, omitted_records: u64, records: &[RunnerWatchLogRecord]) {
        if records.is_empty() {
            self.push(
                "no output has been received".to_owned(),
                Tone::Muted,
                0,
                None,
            );
        }
        for (ordinal, record) in (omitted_records..).zip(records) {
            let (label, tone) = WorkflowRunLogSource::from_word(&record.source).map_or_else(
                || (visible_text(&record.source), Tone::Neutral),
                |source| {
                    let presentation = log_source_presentation(source);
                    (presentation.label.to_owned(), presentation.payload_tone)
                },
            );
            let marker = if record.continuation { "↪" } else { "│" };
            self.push(
                format!(
                    "{label:<LOG_SOURCE_WIDTH$} {marker} {}",
                    visible_text(&record.text)
                ),
                tone,
                0,
                Some(ordinal),
            );
        }
    }

    fn append_stream(&mut self, index: usize, stream: &RunnerWatchLogStream) {
        let heading = stream.name.as_ref().map(|name| {
            self.push(String::new(), Tone::Primary, index, None);
            let row = self.rows.len().saturating_sub(1);
            self.stream_headings.push(row);
            (row, name)
        });
        let mut offset = stream.omitted_bytes;
        let mut lines = 0_usize;
        for piece in stream.text.split_inclusive('\n') {
            let line = piece.strip_suffix('\n').unwrap_or(piece);
            let records = normalize_retained_prefix(line.as_bytes()).records;
            if records.is_empty() {
                self.push(String::new(), Tone::Neutral, index, Some(offset));
            }
            for record in records {
                let prefix = if record.continuation { "↪ " } else { "" };
                self.push(
                    format!("{prefix}{}", record.payload),
                    Tone::Neutral,
                    index,
                    Some(offset),
                );
            }
            lines = lines.saturating_add(1);
            offset = offset.saturating_add(u64::try_from(piece.len()).unwrap_or(u64::MAX));
        }
        // A named stream's heading says what it holds, an empty one that it is empty.
        if let Some((row, name)) = heading {
            let mut facts = vec![visible_text(name).to_uppercase()];
            facts.extend(
                stream
                    .retained_bytes
                    .map(|bytes| format!("retained {bytes} B")),
            );
            facts.extend(
                stream
                    .discarded_bytes
                    .map(|bytes| format!("discarded {bytes} B")),
            );
            facts.push(format!("{} B left out", stream.omitted_bytes));
            facts.push(match lines {
                0 => "empty".to_owned(),
                1 => "1 line".to_owned(),
                lines => format!("{lines} lines"),
            });
            if let Some(heading) = self.rows.get_mut(row) {
                heading.text = facts.join(" · ");
            }
        }
    }
}

impl KeyedRows for LogDocument {
    type Key = LogPlace;

    fn row_count(&self) -> usize {
        self.rows.len()
    }

    fn key(&self, row: usize) -> LogPlace {
        self.places.get(row).copied().unwrap_or(LogPlace {
            stream: usize::MAX,
            offset: None,
            record: 0,
        })
    }
}

/// The document the pane shows, or none while an answer has given it none.
impl KeyedRows for Option<&LogDocument> {
    type Key = LogPlace;

    fn row_count(&self) -> usize {
        self.map_or(0, KeyedRows::row_count)
    }

    fn key(&self, row: usize) -> LogPlace {
        self.map_or(
            LogPlace {
                stream: 0,
                offset: None,
                record: 0,
            },
            |document| document.key(row),
        )
    }
}
