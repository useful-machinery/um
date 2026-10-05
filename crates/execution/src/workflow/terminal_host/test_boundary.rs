use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum BoundaryAction {
    Setup,
    Draw(Rect),
    Input(TerminalInputEvent),
    InputFailure,
    Lifecycle(TerminalLifecycleEvent),
    Resize(Rect),
    Restore,
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct BoundaryFailures {
    pub(super) setup: bool,
    pub(super) draw_at: Option<usize>,
    pub(super) panic_at: Option<usize>,
    pub(super) restore: bool,
}

pub(super) enum ScriptedInput {
    Event(TerminalInputEvent),
    Failure,
    Panic,
}

pub(super) struct ScriptedTerminalBoundary {
    area: Rect,
    pub(super) resize_areas: VecDeque<Rect>,
    input: tokio::sync::mpsc::UnboundedReceiver<ScriptedInput>,
    actions: tokio::sync::mpsc::UnboundedSender<BoundaryAction>,
    pub(super) failures: BoundaryFailures,
    pub(super) draw_count: usize,
}

impl ScriptedTerminalBoundary {
    pub(super) fn new(
        area: Rect,
        resize_areas: impl IntoIterator<Item = Rect>,
        failures: BoundaryFailures,
    ) -> (
        Self,
        tokio::sync::mpsc::UnboundedSender<ScriptedInput>,
        tokio::sync::mpsc::UnboundedReceiver<BoundaryAction>,
    ) {
        let (input_sender, input) = tokio::sync::mpsc::unbounded_channel();
        let (actions, action_receiver) = tokio::sync::mpsc::unbounded_channel();
        (
            Self {
                area,
                resize_areas: resize_areas.into_iter().collect(),
                input,
                actions,
                failures,
                draw_count: 0,
            },
            input_sender,
            action_receiver,
        )
    }

    pub(super) fn record(&self, action: BoundaryAction) {
        let _ = self.actions.send(action);
    }
}

impl TerminalBoundary for ScriptedTerminalBoundary {
    fn setup(&mut self) -> io::Result<Rect> {
        self.record(BoundaryAction::Setup);
        if self.failures.setup {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "injected setup failure",
            ));
        }
        Ok(self.area)
    }

    fn next_event(&mut self) -> impl Future<Output = io::Result<TerminalInputEvent>> + Send {
        let actions = self.actions.clone();
        async move {
            match self.input.recv().await {
                Some(ScriptedInput::Event(event)) => {
                    let _ = actions.send(BoundaryAction::Input(event));
                    Ok(event)
                }
                Some(ScriptedInput::Failure) => {
                    let _ = actions.send(BoundaryAction::InputFailure);
                    Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "injected input failure",
                    ))
                }
                Some(ScriptedInput::Panic) => {
                    std::panic::panic_any("injected terminal input panic")
                }
                None => Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "scripted terminal input closed",
                )),
            }
        }
    }

    fn resize(&mut self) -> io::Result<Rect> {
        if let Some(area) = self.resize_areas.pop_front() {
            self.area = area;
        }
        self.record(BoundaryAction::Resize(self.area));
        Ok(self.area)
    }

    fn notify_lifecycle(&mut self, event: TerminalLifecycleEvent) -> io::Result<()> {
        self.record(BoundaryAction::Lifecycle(event));
        Ok(())
    }

    fn restore(&mut self) -> io::Result<()> {
        self.record(BoundaryAction::Restore);
        if self.failures.restore {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "injected restore failure",
            ));
        }
        Ok(())
    }
}

impl WorkflowTerminalBoundary for ScriptedTerminalBoundary {
    fn draw_workflow(
        &mut self,
        _snapshot: &WorkflowRunViewSnapshot,
        interaction: &mut HostInteraction,
        _color: bool,
    ) -> io::Result<()> {
        self.draw_count = self.draw_count.saturating_add(1);
        self.record(BoundaryAction::Draw(interaction.terminal_area));
        if self.failures.panic_at == Some(self.draw_count) {
            std::panic::panic_any("injected widget panic");
        }
        if self.failures.draw_at == Some(self.draw_count) {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "injected draw failure",
            ));
        }
        Ok(())
    }
}

pub(super) async fn wait_for_action(
    actions: &mut tokio::sync::mpsc::UnboundedReceiver<BoundaryAction>,
    expected: BoundaryAction,
) {
    loop {
        let action = actions
            .recv()
            .await
            .expect("terminal action channel closed");
        if action == expected {
            return;
        }
    }
}

pub(super) fn buffer_text(buffer: &ratatui::buffer::Buffer) -> String {
    buffer
        .content()
        .iter()
        .fold(String::new(), |mut rendered, cell| {
            rendered.push_str(cell.symbol());
            rendered
        })
}
