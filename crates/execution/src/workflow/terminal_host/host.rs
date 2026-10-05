use super::*;

pub(super) struct TerminalStartup {
    pub(super) activation: oneshot::Receiver<()>,
    pub(super) ready: oneshot::Sender<Result<(), PresentationFailure>>,
}

pub struct WorkflowTerminalHost {
    pub(super) ready: Option<oneshot::Receiver<Result<(), PresentationFailure>>>,
    pub(super) activation: Option<oneshot::Sender<()>>,
    pub(super) shutdown: Option<oneshot::Sender<()>>,
    pub(super) task: tokio::task::JoinHandle<Result<TerminalHostExit, PresentationFailure>>,
    pub(super) cancellation: CancellationSource,
    pub(super) execution_active: Arc<AtomicBool>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TerminalHostExit {
    Quit,
    Stopped,
}

impl WorkflowTerminalHost {
    pub fn start<Clock>(
        view: WorkflowRunViewModel<Clock>,
        cancellation: CancellationSource,
        color: bool,
    ) -> Result<Self, PresentationFailure>
    where
        Clock: crate::workflow::run_timing::ObservationClock,
    {
        Self::start_with_boundary(view, cancellation, color, SystemTerminalBoundary::new())
    }

    pub fn start_with_boundary<Clock, Boundary>(
        view: WorkflowRunViewModel<Clock>,
        cancellation: CancellationSource,
        color: bool,
        boundary: Boundary,
    ) -> Result<Self, PresentationFailure>
    where
        Clock: crate::workflow::run_timing::ObservationClock,
        Boundary: WorkflowTerminalBoundary,
    {
        let (activation, activation_receiver) = oneshot::channel();
        let (ready_sender, ready) = oneshot::channel();
        let (shutdown, mut shutdown_receiver) = oneshot::channel();
        let task_cancellation = cancellation.clone();
        let execution_active = Arc::new(AtomicBool::new(false));
        let task_execution_active = Arc::clone(&execution_active);
        let runtime = tokio::runtime::Handle::current();
        // Watch notifications coalesce frames while a slow terminal is drawing.
        let task = tokio::task::spawn_blocking(move || {
            runtime.block_on(async move {
                let mut unwind_guard = TerminalTaskUnwindGuard::new(
                    task_cancellation.clone(),
                    Arc::clone(&task_execution_active),
                );
                let mut terminal = RestoringTerminal::new(boundary);
                let result = AssertUnwindSafe(run_terminal_host(
                    &mut terminal,
                    view,
                    task_cancellation,
                    task_execution_active,
                    color,
                    TerminalStartup {
                        activation: activation_receiver,
                        ready: ready_sender,
                    },
                    &mut shutdown_receiver,
                ))
                .catch_unwind()
                .await;
                match result {
                    Ok(result) => {
                        unwind_guard.disarm();
                        result
                    }
                    Err(payload) => {
                        let _ = terminal.restore();
                        Err(report_terminal_panic(payload))
                    }
                }
            })
        });
        Ok(Self {
            ready: Some(ready),
            activation: Some(activation),
            shutdown: Some(shutdown),
            task,
            cancellation,
            execution_active,
        })
    }

    /// Await initial setup and drawing without occupying a Tokio worker.
    pub async fn await_ready(&mut self) -> Result<(), PresentationFailure> {
        let Some(ready) = self.ready.take() else {
            return Err(PresentationFailure::operation(
                PresentationFailureOperation::TerminalTask,
            ));
        };
        ready.await.unwrap_or_else(|_| {
            Err(PresentationFailure::operation(
                PresentationFailureOperation::TerminalTask,
            ))
        })
    }

    pub fn activate_execution(&mut self) -> Result<(), PresentationFailure> {
        let Some(activation) = self.activation.take() else {
            return Err(PresentationFailure::operation(
                PresentationFailureOperation::TerminalTask,
            ));
        };
        self.execution_active.store(true, Ordering::SeqCst);
        if activation.send(()).is_err() {
            self.execution_active.store(false, Ordering::SeqCst);
            return Err(PresentationFailure::operation(
                PresentationFailureOperation::TerminalTask,
            ));
        }
        Ok(())
    }

    pub async fn wait(mut self) -> Result<TerminalHostExit, PresentationFailure> {
        drop(self.activation.take());
        let shutdown = self.shutdown.take();
        let cancellation = self.cancellation.clone();
        let result = self.task.await;
        drop(shutdown);
        Self::join_result(&cancellation, &self.execution_active, result)
    }

    pub async fn stop(mut self) -> Result<TerminalHostExit, PresentationFailure> {
        drop(self.activation.take());
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        let cancellation = self.cancellation.clone();
        let result = self.task.await;
        Self::join_result(&cancellation, &self.execution_active, result)
    }

    pub(super) fn join_result(
        cancellation: &CancellationSource,
        execution_active: &AtomicBool,
        result: Result<Result<TerminalHostExit, PresentationFailure>, tokio::task::JoinError>,
    ) -> Result<TerminalHostExit, PresentationFailure> {
        match result {
            Ok(result) => result,
            Err(error) => {
                if execution_active.load(Ordering::SeqCst) {
                    cancellation.request_cancellation(CancellationReason::CallerOutputFailure);
                }
                if error.is_panic() {
                    Err(report_terminal_panic(error.into_panic()))
                } else {
                    Err(PresentationFailure::operation(
                        PresentationFailureOperation::TerminalTask,
                    ))
                }
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TerminalLifecycleEvent {
    HelpOpened,
    QuitEligible,
}

pub trait TerminalBoundary: Send + 'static {
    fn setup(&mut self) -> io::Result<Rect>;

    fn next_event(&mut self) -> impl Future<Output = io::Result<TerminalInputEvent>> + Send;

    fn resize(&mut self) -> io::Result<Rect>;

    fn restore(&mut self) -> io::Result<()>;

    // Called on the render loop: implementations must not wait for an external consumer.
    fn notify_lifecycle(&mut self, _event: TerminalLifecycleEvent) -> io::Result<()> {
        Ok(())
    }
}

pub trait WorkflowTerminalBoundary: TerminalBoundary {
    fn draw_workflow(
        &mut self,
        snapshot: &WorkflowRunViewSnapshot,
        interaction: &mut HostInteraction,
        color: bool,
    ) -> io::Result<()>;
}

pub(super) fn report_terminal_panic(payload: Box<dyn std::any::Any + Send>) -> PresentationFailure {
    let message = payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| {
            payload
                .downcast_ref::<&str>()
                .map(|text| (*text).to_owned())
        })
        .unwrap_or_else(|| "non-string panic payload".to_owned());
    let failure = PresentationFailure {
        panic_message: Some(message),
        ..PresentationFailure::operation(PresentationFailureOperation::TerminalTask)
    };
    // The caller renders this failure after the terminal has been restored.
    failure
}

pub(super) struct TerminalTaskUnwindGuard {
    pub(super) cancellation: CancellationSource,
    pub(super) execution_active: Arc<AtomicBool>,
    pub(super) armed: bool,
}

impl TerminalTaskUnwindGuard {
    pub(super) fn new(cancellation: CancellationSource, execution_active: Arc<AtomicBool>) -> Self {
        Self {
            cancellation,
            execution_active,
            armed: true,
        }
    }

    pub(super) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for TerminalTaskUnwindGuard {
    fn drop(&mut self) {
        if self.armed && self.execution_active.load(Ordering::SeqCst) {
            self.cancellation
                .request_cancellation(CancellationReason::CallerOutputFailure);
        }
    }
}

pub(super) struct RestoringTerminal<Boundary: TerminalBoundary> {
    pub(super) boundary: Boundary,
    pub(super) restored: bool,
}

impl<Boundary: TerminalBoundary> RestoringTerminal<Boundary> {
    pub(super) fn new(boundary: Boundary) -> Self {
        Self {
            boundary,
            restored: false,
        }
    }

    pub(super) fn restore(&mut self) -> io::Result<()> {
        if !begin_restoration(&mut self.restored) {
            return Ok(());
        }
        self.boundary.restore()
    }
}

impl<Boundary: TerminalBoundary> Drop for RestoringTerminal<Boundary> {
    fn drop(&mut self) {
        let _ = self.restore();
    }
}

pub(super) async fn run_terminal_host<Clock, Boundary>(
    terminal: &mut RestoringTerminal<Boundary>,
    view: WorkflowRunViewModel<Clock>,
    cancellation: CancellationSource,
    execution_active: Arc<AtomicBool>,
    color: bool,
    startup: TerminalStartup,
    shutdown: &mut oneshot::Receiver<()>,
) -> Result<TerminalHostExit, PresentationFailure>
where
    Clock: crate::workflow::run_timing::ObservationClock,
    Boundary: WorkflowTerminalBoundary,
{
    let area = match terminal.boundary.setup() {
        Ok(area) => area,
        Err(error) => {
            let failure = presentation_failure(PresentationFailureOperation::TerminalSetup, &error);
            let _ = startup.ready.send(Err(failure.clone()));
            return Err(failure);
        }
    };
    let mut interaction = HostInteraction {
        terminal_area: area,
        ..HostInteraction::default()
    };
    if let Err(error) = terminal.boundary.draw_workflow(
        &view.snapshot_for_render(interaction.selected),
        &mut interaction,
        color,
    ) {
        let failure = presentation_failure(PresentationFailureOperation::TerminalDraw, &error);
        let _ = startup.ready.send(Err(failure));
        return fail_terminal(
            terminal,
            PresentationFailureOperation::TerminalDraw,
            &error,
            &cancellation,
            execution_active.load(Ordering::SeqCst),
        );
    }
    let _ = startup.ready.send(Ok(()));
    let activated = tokio::select! {
        biased;
        _ = &mut *shutdown => false,
        activation = startup.activation => activation.is_ok(),
    };
    if !activated {
        return restore_terminal(terminal, TerminalHostExit::Stopped, &cancellation, false);
    }
    run_terminal(
        terminal,
        view,
        cancellation,
        execution_active,
        color,
        shutdown,
        interaction,
    )
    .await
}

pub(super) async fn run_terminal<Clock, Boundary>(
    terminal: &mut RestoringTerminal<Boundary>,
    view: WorkflowRunViewModel<Clock>,
    cancellation: CancellationSource,
    execution_active: Arc<AtomicBool>,
    color: bool,
    shutdown: &mut oneshot::Receiver<()>,
    mut interaction: HostInteraction,
) -> Result<TerminalHostExit, PresentationFailure>
where
    Clock: crate::workflow::run_timing::ObservationClock,
    Boundary: WorkflowTerminalBoundary,
{
    let mut changes = view.subscribe();
    let mut redraw = redraw_interval();
    redraw.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let _ = redraw.tick().await;
    // The execution may advance before this task subscribes, so the first timed tick
    // refreshes the setup-time frame even when no later notification is observed.
    let mut dirty = true;

    loop {
        tokio::select! {
            biased;
            _ = &mut *shutdown => {
                return restore_terminal(
                    terminal,
                    TerminalHostExit::Stopped,
                    &cancellation,
                    execution_active.load(Ordering::SeqCst),
                );
            }
            event = terminal.boundary.next_event() => {
                let event = match event {
                    Ok(event) => event,
                    Err(error) => {
                        let snapshot = view.snapshot_for_render(interaction.selected);
                        let active = workflow_is_executing(&snapshot);
                        execution_active.store(active, Ordering::SeqCst);
                        return fail_terminal(
                            terminal,
                            PresentationFailureOperation::TerminalInput,
                            &error,
                            &cancellation,
                            active,
                        );
                    }
                };
                let snapshot = view.snapshot_for_render(interaction.selected);
                notify_quit_eligibility(&mut terminal.boundary, &snapshot);
                let active = workflow_is_executing(&snapshot);
                execution_active.store(active, Ordering::SeqCst);
                if event == TerminalInputEvent::Resize {
                    match terminal.boundary.resize() {
                        Ok(area) => interaction.terminal_area = area,
                        Err(error) => {
                            return fail_terminal(
                                terminal,
                                PresentationFailureOperation::TerminalDraw,
                                &error,
                                &cancellation,
                                active,
                            );
                        }
                    }
                } else {
                    let control = interaction.handle_key(event, &snapshot, &cancellation);
                    if event == TerminalInputEvent::Help && interaction.help_visible {
                        let _ = terminal
                            .boundary
                            .notify_lifecycle(TerminalLifecycleEvent::HelpOpened);
                    }
                    if control == HostControl::Quit {
                        return restore_terminal(
                            terminal,
                            TerminalHostExit::Quit,
                            &cancellation,
                            false,
                        );
                    }
                }
                let snapshot = view.snapshot_for_render(interaction.selected);
                notify_quit_eligibility(&mut terminal.boundary, &snapshot);
                let active = workflow_is_executing(&snapshot);
                execution_active.store(active, Ordering::SeqCst);
                if let Err(error) = terminal
                    .boundary
                    .draw_workflow(&snapshot, &mut interaction, color)
                {
                    return fail_terminal(
                        terminal,
                        PresentationFailureOperation::TerminalDraw,
                        &error,
                        &cancellation,
                        active,
                    );
                }
                dirty = false;
            }
            changed = changes.changed() => {
                if changed.is_ok() {
                    let _ = changes.borrow_and_update();
                    let snapshot = view.snapshot_for_render(interaction.selected);
                    notify_quit_eligibility(&mut terminal.boundary, &snapshot);
                    execution_active.store(workflow_is_executing(&snapshot), Ordering::SeqCst);
                    dirty = true;
                }
            }
            _ = redraw.tick() => {
                let snapshot = view.snapshot_for_render(interaction.selected);
                notify_quit_eligibility(&mut terminal.boundary, &snapshot);
                let active = workflow_is_executing(&snapshot);
                execution_active.store(active, Ordering::SeqCst);
                if dirty || !snapshot.timing.frozen {
                    if let Err(error) = terminal
                        .boundary
                        .draw_workflow(&snapshot, &mut interaction, color)
                    {
                        return fail_terminal(
                            terminal,
                            PresentationFailureOperation::TerminalDraw,
                            &error,
                            &cancellation,
                            active,
                        );
                    }
                    dirty = false;
                }
            }
        }
    }
}

pub(super) fn notify_quit_eligibility<Boundary: TerminalBoundary>(
    boundary: &mut Boundary,
    snapshot: &WorkflowRunViewSnapshot,
) {
    if snapshot.quit_eligible {
        let _ = boundary.notify_lifecycle(TerminalLifecycleEvent::QuitEligible);
    }
}

pub(super) fn workflow_is_executing(snapshot: &WorkflowRunViewSnapshot) -> bool {
    matches!(snapshot.workflow, WorkflowState::Executing { .. })
}

#[expect(
    clippy::disallowed_methods,
    reason = "redraw_interval is the terminal host boundary for coalesced redraw timing"
)]
pub(super) fn redraw_interval() -> tokio::time::Interval {
    tokio::time::interval(REDRAW_INTERVAL)
}

pub(super) fn restore_terminal<Boundary: TerminalBoundary>(
    terminal: &mut RestoringTerminal<Boundary>,
    exit: TerminalHostExit,
    cancellation: &CancellationSource,
    execution_active: bool,
) -> Result<TerminalHostExit, PresentationFailure> {
    terminal.restore().map_or_else(
        |error| {
            if execution_active {
                cancellation.request_cancellation(CancellationReason::CallerOutputFailure);
            }
            Err(presentation_failure(
                PresentationFailureOperation::TerminalRestore,
                &error,
            ))
        },
        |()| Ok(exit),
    )
}

pub(super) fn fail_terminal<Boundary: TerminalBoundary>(
    terminal: &mut RestoringTerminal<Boundary>,
    operation: PresentationFailureOperation,
    error: &io::Error,
    cancellation: &CancellationSource,
    execution_active: bool,
) -> Result<TerminalHostExit, PresentationFailure> {
    if execution_active {
        cancellation.request_cancellation(CancellationReason::CallerOutputFailure);
    }
    let failure = presentation_failure(operation, error);
    let _ = terminal.restore();
    Err(failure)
}

pub(super) fn presentation_failure(
    operation: PresentationFailureOperation,
    error: &io::Error,
) -> PresentationFailure {
    PresentationFailure {
        operation,
        error_kind: Some(error.kind()),
        result_directory: None,
        panic_message: None,
    }
}

pub(super) struct SystemTerminalBoundary {
    pub(super) surface: Option<TerminalSurface>,
    pub(super) input: TerminalInput,
    pub(super) restore: Option<TerminalRestore>,
}

impl SystemTerminalBoundary {
    pub(super) fn new() -> Self {
        Self {
            surface: None,
            input: TerminalInput::new(),
            restore: None,
        }
    }

    pub(super) fn surface_mut(&mut self) -> io::Result<&mut TerminalSurface> {
        self.surface.as_mut().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotConnected,
                "terminal surface is not set up",
            )
        })
    }
}

impl TerminalBoundary for SystemTerminalBoundary {
    fn setup(&mut self) -> io::Result<Rect> {
        self.restore = Some(TerminalRestore::enter_raw_mode()?);
        let area = selected_output_area()?;
        let mut output = io::stdout();
        if let Some(restore) = &mut self.restore {
            restore.alternate_screen = true;
        }
        execute!(output, EnterAlternateScreen, Hide)?;
        let terminal = Terminal::with_options(
            CrosstermBackend::new(output),
            TerminalOptions {
                viewport: Viewport::Fixed(area),
            },
        )?;
        self.surface = Some(TerminalSurface {
            terminal,
            graph: None,
        });
        Ok(area)
    }

    fn next_event(&mut self) -> impl Future<Output = io::Result<TerminalInputEvent>> + Send {
        self.input.next_event()
    }

    fn resize(&mut self) -> io::Result<Rect> {
        self.surface_mut()?.resize()
    }

    fn restore(&mut self) -> io::Result<()> {
        self.restore
            .as_mut()
            .map_or(Ok(()), TerminalRestore::restore)
    }
}

impl WorkflowTerminalBoundary for SystemTerminalBoundary {
    fn draw_workflow(
        &mut self,
        snapshot: &WorkflowRunViewSnapshot,
        interaction: &mut HostInteraction,
        color: bool,
    ) -> io::Result<()> {
        self.surface_mut()?.draw(snapshot, interaction, color)
    }
}

pub(super) struct TerminalSurface {
    pub(super) terminal: Terminal<CrosstermBackend<io::Stdout>>,
    pub(super) graph: Option<DagLayout>,
}

impl TerminalSurface {
    pub(super) fn draw(
        &mut self,
        snapshot: &WorkflowRunViewSnapshot,
        interaction: &mut HostInteraction,
        color: bool,
    ) -> io::Result<()> {
        clamp_step_selection(&mut interaction.selected, snapshot.steps.len());
        let graph = self
            .graph
            .get_or_insert_with(|| DagLayout::for_steps(&snapshot.steps));
        self.terminal
            .draw(|frame| render(frame, snapshot, graph, interaction, color))?;
        Ok(())
    }

    pub(super) fn resize(&mut self) -> io::Result<Rect> {
        let area = selected_output_area()?;
        self.terminal.resize(area)?;
        Ok(area)
    }
}
