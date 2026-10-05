use super::*;

pub(super) struct TerminalInput {
    pub(super) events: EventStream,
}

impl TerminalInput {
    pub(super) fn new() -> Self {
        Self {
            events: EventStream::new(),
        }
    }

    pub(super) async fn next_event(&mut self) -> io::Result<TerminalInputEvent> {
        match self.events.next().await {
            Some(Ok(event)) => Ok(terminal_input_event(event)),
            Some(Err(error)) => Err(error),
            None => Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "terminal input closed",
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TerminalInputEvent {
    Up,
    Down,
    PageUp,
    PageDown,
    HalfPageUp,
    HalfPageDown,
    Top,
    Bottom,
    PanLeft,
    PanRight,
    Follow,
    ToggleLogChannel(char),
    Help,
    Enter,
    Escape,
    Quit,
    Cancel,
    Resize,
    Other,
}

pub(super) fn terminal_input_event(event: Event) -> TerminalInputEvent {
    match event {
        Event::Key(key)
            if matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat)
                && key.modifiers.contains(KeyModifiers::CONTROL)
                && !key.modifiers.contains(KeyModifiers::ALT) =>
        {
            match key.code {
                KeyCode::Char('c') => TerminalInputEvent::Cancel,
                KeyCode::Char('u') => TerminalInputEvent::HalfPageUp,
                KeyCode::Char('d') => TerminalInputEvent::HalfPageDown,
                _ => TerminalInputEvent::Other,
            }
        }
        Event::Key(key)
            if key.kind == KeyEventKind::Press
                && !key
                    .modifiers
                    .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT)
                && matches!(key.code, KeyCode::Char('1'..='9')) =>
        {
            match key.code {
                KeyCode::Char(channel @ '1'..='9') => TerminalInputEvent::ToggleLogChannel(channel),
                _ => TerminalInputEvent::Other,
            }
        }
        Event::Key(key)
            if matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat)
                && !key
                    .modifiers
                    .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) =>
        {
            match key.code {
                KeyCode::Up | KeyCode::Char('k') => TerminalInputEvent::Up,
                KeyCode::Down | KeyCode::Char('j') => TerminalInputEvent::Down,
                KeyCode::PageUp | KeyCode::Char('b') => TerminalInputEvent::PageUp,
                KeyCode::PageDown | KeyCode::Char('f') | KeyCode::Char(' ') => {
                    TerminalInputEvent::PageDown
                }
                KeyCode::Char('u') => TerminalInputEvent::HalfPageUp,
                KeyCode::Char('d') => TerminalInputEvent::HalfPageDown,
                KeyCode::Char('g') => TerminalInputEvent::Top,
                KeyCode::Char('G') => TerminalInputEvent::Bottom,
                KeyCode::Left | KeyCode::Char('h') => TerminalInputEvent::PanLeft,
                KeyCode::Right | KeyCode::Char('l') => TerminalInputEvent::PanRight,
                KeyCode::Char('F') => TerminalInputEvent::Follow,
                KeyCode::Char('?') => TerminalInputEvent::Help,
                KeyCode::Enter => TerminalInputEvent::Enter,
                KeyCode::Esc => TerminalInputEvent::Escape,
                KeyCode::Char('q') => TerminalInputEvent::Quit,
                _ => TerminalInputEvent::Other,
            }
        }
        Event::Resize(_, _) => TerminalInputEvent::Resize,
        _ => TerminalInputEvent::Other,
    }
}

pub(super) struct TerminalRestore {
    pub(super) original_input_mode: Termios,
    pub(super) alternate_screen: bool,
    pub(super) restored: bool,
}

pub(super) fn begin_restoration(restored: &mut bool) -> bool {
    if *restored {
        return false;
    }
    *restored = true;
    true
}

impl TerminalRestore {
    pub(super) fn enter_raw_mode() -> io::Result<Self> {
        let input = io::stdin();
        let original_input_mode = tcgetattr(&input).map_err(io::Error::from)?;
        let mut restore = Self {
            original_input_mode: original_input_mode.clone(),
            alternate_screen: false,
            restored: false,
        };
        let mut raw_input_mode = original_input_mode;
        raw_input_mode.make_raw();
        if let Err(error) = tcsetattr(&input, OptionalActions::Now, &raw_input_mode) {
            restore.restored = true;
            return Err(error.into());
        }
        Ok(restore)
    }

    pub(super) fn restore(&mut self) -> io::Result<()> {
        if !begin_restoration(&mut self.restored) {
            return Ok(());
        }
        let mut output = io::stdout();
        let input = io::stdin();
        attempt_terminal_restoration(
            self.alternate_screen,
            &mut output,
            |output| queue!(output, LeaveAlternateScreen),
            |output| queue!(output, Show),
            Write::flush,
            || {
                tcsetattr(&input, OptionalActions::Now, &self.original_input_mode)
                    .map_err(io::Error::from)
            },
        )
    }
}

pub(super) fn attempt_terminal_restoration<Output: Write>(
    alternate_screen: bool,
    output: &mut Output,
    mut leave_alternate_screen: impl FnMut(&mut Output) -> io::Result<()>,
    mut show_cursor: impl FnMut(&mut Output) -> io::Result<()>,
    mut flush_output: impl FnMut(&mut Output) -> io::Result<()>,
    mut restore_input_mode: impl FnMut() -> io::Result<()>,
) -> io::Result<()> {
    let mut first_error = None;
    if alternate_screen {
        retain_first_error(leave_alternate_screen(output), &mut first_error);
        retain_first_error(show_cursor(output), &mut first_error);
        retain_first_error(flush_output(output), &mut first_error);
    }
    retain_first_error(restore_input_mode(), &mut first_error);
    first_error.map_or(Ok(()), Err)
}

impl Drop for TerminalRestore {
    fn drop(&mut self) {
        let _ = self.restore();
    }
}

pub(super) fn selected_output_area() -> io::Result<Rect> {
    let size = tcgetwinsize(io::stdout()).map_err(io::Error::from)?;
    if size.ws_col == 0 || size.ws_row == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "terminal reported an empty window",
        ));
    }
    Ok(Rect::new(0, 0, size.ws_col, size.ws_row))
}

pub(super) fn retain_first_error(result: io::Result<()>, first_error: &mut Option<io::Error>) {
    if let Err(error) = result
        && first_error.is_none()
    {
        *first_error = Some(error);
    }
}
