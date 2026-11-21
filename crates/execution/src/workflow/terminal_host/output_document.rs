//! A document of rows drawn full-screen, each row on one line with panning, which the
//! archived viewer's retained output (REQ-LOCALRUN-69) and the runner viewer's log
//! (REQ-RUNNERENROLL-146) share: its rows, its viewport and its renderer.

use super::*;

/// One row of a document.
pub(super) struct OutputRow {
    pub(super) text: String,
    pub(super) tone: Tone,
}

/// The display width of a document's widest row, as far as its view pans.
pub(super) fn document_width(rows: &[OutputRow]) -> usize {
    rows.iter()
        .map(|row| display_width(&row.text))
        .max()
        .unwrap_or(0)
}

/// A document's rows as a viewport holds them: each row has a key, and the keys rise from
/// row to row.
pub(super) trait KeyedRows {
    type Key: Copy + Ord;

    fn row_count(&self) -> usize;
    fn key(&self, row: usize) -> Self::Key;
}

/// A document that does not change keys each row by its index.
impl KeyedRows for [OutputRow] {
    type Key = usize;

    fn row_count(&self) -> usize {
        self.len()
    }

    fn key(&self, row: usize) -> usize {
        row
    }
}

/// What a document's view shows: the first row in view and the columns panned past. It
/// either follows the last rows or is paused, held at its top row by that row's key, so that
/// new rows put the row with the key, or else the first row past it, at the top.
pub(super) struct OutputViewport<Key> {
    pub(super) top: usize,
    pub(super) horizontal_offset: usize,
    pub(super) available_width: usize,
    pub(super) available_rows: usize,
    pub(super) following: bool,
    held: Option<Key>,
}

impl<Key> Default for OutputViewport<Key> {
    fn default() -> Self {
        Self {
            top: 0,
            horizontal_offset: 0,
            available_width: 0,
            available_rows: 0,
            following: false,
            held: None,
        }
    }
}

impl<Key: Copy + Ord> OutputViewport<Key> {
    /// A view that opens at the last rows and follows them.
    pub(super) fn following() -> Self {
        Self {
            following: true,
            ..Self::default()
        }
    }

    pub(super) fn synchronize<Rows: KeyedRows<Key = Key> + ?Sized>(
        &mut self,
        rows: &Rows,
        document_width: usize,
        width: usize,
        height: usize,
    ) {
        self.available_width = width;
        self.available_rows = height;
        self.anchor(rows);
        self.horizontal_offset = self
            .horizontal_offset
            .min(document_width.saturating_sub(width));
    }

    /// Puts the followed bottom or the held row at the top, within the rows there are.
    fn anchor<Rows: KeyedRows<Key = Key> + ?Sized>(&mut self, rows: &Rows) {
        let bottom = maximum_document_top(rows.row_count(), self.available_rows);
        if self.following {
            self.top = bottom;
            return;
        }
        if let Some(held) = self.held {
            // The first row whose key is the held one or past it.
            let (mut low, mut high) = (0, rows.row_count());
            while low < high {
                let middle = low + (high - low) / 2;
                if rows.key(middle) < held {
                    low = middle + 1;
                } else {
                    high = middle;
                }
            }
            self.top = low;
        }
        self.top = self.top.min(bottom);
    }

    /// Moves the view; any movement pauses it, one that cannot move included
    /// (REQ-LOCALRUN-46).
    pub(super) fn navigate<Rows: KeyedRows<Key = Key> + ?Sized>(
        &mut self,
        rows: &Rows,
        navigation: VerticalNavigation,
    ) {
        self.anchor(rows);
        let bottom = maximum_document_top(rows.row_count(), self.available_rows);
        let page = self.available_rows.max(1);
        let half_page = (page / 2).max(1);
        self.top = match navigation {
            VerticalNavigation::Up => self.top.saturating_sub(1),
            VerticalNavigation::Down => self.top.saturating_add(1).min(bottom),
            VerticalNavigation::PageUp => self.top.saturating_sub(page),
            VerticalNavigation::PageDown => self.top.saturating_add(page).min(bottom),
            VerticalNavigation::HalfPageUp => self.top.saturating_sub(half_page),
            VerticalNavigation::HalfPageDown => self.top.saturating_add(half_page).min(bottom),
            VerticalNavigation::Top => 0,
            VerticalNavigation::Bottom => bottom,
        };
        self.following = false;
        self.held = (self.top < rows.row_count()).then(|| rows.key(self.top));
    }

    pub(super) fn follow(&mut self) {
        self.following = true;
        self.held = None;
    }

    pub(super) fn pan_left(&mut self) {
        self.horizontal_offset = self.horizontal_offset.saturating_sub(1);
    }

    pub(super) fn pan_right(&mut self, document_width: usize) {
        self.horizontal_offset = self
            .horizontal_offset
            .saturating_add(1)
            .min(document_width.saturating_sub(self.available_width));
    }

    /// The rows of a document of `row_count` rows below the view.
    pub(super) fn rows_below(&self, row_count: usize) -> usize {
        row_count.saturating_sub(self.top.saturating_add(self.available_rows))
    }
}

pub(super) fn maximum_document_top(row_count: usize, available_rows: usize) -> usize {
    row_count.saturating_sub(available_rows)
}

/// What a document's header says: a title, a state at its right and rows kept in view above
/// the document whatever its position.
pub(super) struct DocumentHeading<'a> {
    pub(super) title: String,
    pub(super) state: Option<(String, Tone)>,
    pub(super) pinned: Vec<&'a OutputRow>,
}

impl DocumentHeading<'_> {
    pub(super) fn titled(title: String) -> Self {
        Self {
            title,
            state: None,
            pinned: Vec::new(),
        }
    }
}

/// The columns and rows a document's rows have in `area`, below a header of a title and
/// `pinned` rows.
pub(super) fn output_document_dimensions(area: Rect, pinned: usize) -> (usize, usize) {
    let rows = output_document_areas(log_block(Borders::TOP, false).inner(area), pinned)[1];
    (usize::from(rows.width), usize::from(rows.height))
}

fn output_document_areas(area: Rect, pinned: usize) -> [Rect; 2] {
    let header = u16::try_from(pinned)
        .unwrap_or(u16::MAX)
        .saturating_add(1)
        .max(LOG_HEADER_HEIGHT);
    let rows = Layout::vertical([Constraint::Length(header), Constraint::Min(0)]).split(area);
    [rows[0], rows[1]]
}

/// Draws a document's header and the rows its viewport shows, each on one line, panned.
pub(super) fn render_output_document<Key>(
    frame: &mut Frame<'_>,
    area: Rect,
    heading: DocumentHeading<'_>,
    rows: &[OutputRow],
    viewport: &OutputViewport<Key>,
    color: bool,
) {
    let block = log_block(Borders::TOP, color);
    let content = block.inner(area);
    frame.render_widget(block, area);
    let [header, body] = output_document_areas(content, heading.pinned.len());
    let width = usize::from(header.width);
    let title_row = Rect::new(header.x, header.y, header.width, header.height.min(1));
    let title_width = heading.state.as_ref().map_or(width, |(state, _)| {
        width.saturating_sub(display_width(state).saturating_add(2))
    });
    let mut lines = vec![Line::from(Span::styled(
        ellipsize(&heading.title, title_width),
        tone_style(color, Tone::Muted),
    ))];
    lines.extend(heading.pinned.iter().map(|row| {
        Line::from(Span::styled(
            ellipsize(&row.text, width),
            tone_style(color, row.tone),
        ))
    }));
    frame.render_widget(Paragraph::new(lines), header);
    if let Some((state, tone)) = heading.state {
        frame.render_widget(
            Paragraph::new(Span::styled(state, tone_style(color, tone)))
                .alignment(Alignment::Right),
            title_row,
        );
    }
    let lines = rows
        .iter()
        .skip(viewport.top)
        .take(usize::from(body.height))
        .map(|row| Line::from(Span::styled(row.text.clone(), tone_style(color, row.tone))))
        .collect::<Vec<_>>();
    frame.render_widget(
        Paragraph::new(lines).scroll((
            0,
            u16::try_from(viewport.horizontal_offset).unwrap_or(u16::MAX),
        )),
        body,
    );
}
