//! The interactive browser: terminal setup, the event loop, and teardown.

use std::time::Duration;

use color_eyre::Result;
use crossterm::event::{Event, EventStream, KeyEventKind};
use futures::StreamExt;
use keylens_conn::Conn;
use tokio::sync::mpsc;
use tracing::warn;

use crate::app::App;
use crate::ui;
use crate::worker::{Request, Update, Worker};

/// Bounded so a burst of keystrokes applies backpressure instead of queuing unbounded
/// scan work the user has already scrolled past.
const CHANNEL_SIZE: usize = 32;
/// Server stats refresh interval.
const INFO_TICK: Duration = Duration::from_secs(5);

pub async fn run(url: &str) -> Result<()> {
    let conn = Conn::connect(url, "browse").await?;
    let server = conn.server().clone();

    let (req_tx, req_rx) = mpsc::channel::<Request>(CHANNEL_SIZE);
    let (up_tx, mut up_rx) = mpsc::channel::<Update>(CHANNEL_SIZE);
    let up_tx2 = up_tx.clone();

    let worker = Worker::new(conn);
    let worker_handle = tokio::spawn(worker.run(req_rx, up_tx));

    let mut app = App::new(server, url.to_string(), req_tx.clone());
    // Detection first: it's cheap, and its result decides whether a queues tab exists and
    // which streams the event reader follows.
    req_tx.send(Request::Detect).await.ok();
    req_tx.send(Request::Rescan { pattern: None, kind: None }).await.ok();

    let streamer = StreamerHandle { url: url.to_string(), updates: up_tx2, task: None };

    // `init` puts the terminal in raw mode and installs a panic hook that restores it --
    // without that, a panic leaves the user's shell unusable.
    let mut terminal = ratatui::init();
    let result = event_loop(&mut terminal, &mut app, &mut up_rx, &req_tx, streamer).await;
    ratatui::restore();

    // Dropping the sender ends the worker's recv loop.
    drop(req_tx);
    worker_handle.abort();

    result
}

/// Owns the event-stream reader task, which can only start once detection tells us which
/// queues exist.
struct StreamerHandle {
    url: String,
    updates: mpsc::Sender<Update>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl StreamerHandle {
    /// Attach to the detected queues. Idempotent -- a second detection does not spawn a
    /// second reader.
    async fn attach(&mut self, prefix: String, queues: Vec<String>) {
        if self.task.is_some() || queues.is_empty() {
            return;
        }

        // Its own connection: `XREAD BLOCK` holds the connection for the duration of the
        // block, so sharing the worker's would stall every key lookup behind it.
        let conn = match Conn::connect(&self.url, "events").await {
            Ok(c) => c,
            Err(e) => {
                warn!(error = %e, "could not open a connection for the event stream");
                return;
            }
        };

        let tx = self.updates.clone();
        self.task = Some(tokio::spawn(crate::events::run(conn, prefix, queues, tx)));
    }
}

impl Drop for StreamerHandle {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

async fn event_loop(
    terminal: &mut ratatui::DefaultTerminal,
    app: &mut App,
    updates: &mut mpsc::Receiver<Update>,
    requests: &mpsc::Sender<Request>,
    mut streamer: StreamerHandle,
) -> Result<()> {
    let mut events = EventStream::new();
    let mut info_tick = tokio::time::interval(INFO_TICK);
    info_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    terminal.draw(|f| ui::draw(f, app))?;

    loop {
        // Redraw only when something actually changed. A fixed render tick would burn CPU
        // on an idle terminal, which is exactly the thing people notice in a TUI.
        let mut dirty = false;

        tokio::select! {
            maybe_event = events.next() => {
                match maybe_event {
                    Some(Ok(Event::Key(key))) if key.kind == KeyEventKind::Press => {
                        app.handle_key(key).await;
                        dirty = true;
                    }
                    Some(Ok(Event::Resize(_, _))) => dirty = true,
                    Some(Err(e)) => return Err(e.into()),
                    None => app.quit = true,
                    _ => {}
                }
            }

            Some(update) = updates.recv() => {
                // Detection is what tells us which streams to follow, so the reader is
                // started here rather than at connect time.
                if let Update::Detected(detections) = &update
                    && let Some(d) = detections.first()
                {
                    streamer.attach(d.prefix.clone(), d.targets.clone()).await;
                }
                app.apply(update);
                dirty = true;
            }

            _ = info_tick.tick() => {
                // Stats go stale while you browse; refreshing them is cheap.
                requests.try_send(Request::RefreshInfo).ok();
            }
        }

        if app.quit {
            return Ok(());
        }
        if dirty {
            terminal.draw(|f| ui::draw(f, app))?;
        }
    }
}
