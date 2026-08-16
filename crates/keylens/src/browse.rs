//! The interactive browser: terminal setup, the event loop, and teardown.

use std::time::Duration;

use color_eyre::Result;
use crossterm::event::{Event, EventStream, KeyEventKind};
use futures::StreamExt;
use keylens_bullmq::EventsStatus;
use keylens_conn::{Conn, redact_url};
use tokio::sync::mpsc;
use tracing::warn;

use crate::app::App;
use crate::config::Target;
use crate::ui;
use crate::worker::{Request, Update, Worker};

/// Bounded so a burst of keystrokes applies backpressure instead of queuing unbounded
/// scan work the user has already scrolled past.
const CHANNEL_SIZE: usize = 32;
/// Bounds on the server stats refresh. The interval between them is derived from the
/// measured round trip -- see [`info_tick`].
const INFO_TICK_MIN: Duration = Duration::from_secs(5);
const INFO_TICK_MAX: Duration = Duration::from_secs(60);
/// How many round trips apart to space stats refreshes.
const INFO_TICK_RTTS: u32 = 20;

/// Bounds on the selection debounce. See [`select_debounce`].
const SELECT_DEBOUNCE_MIN: Duration = Duration::from_millis(90);
const SELECT_DEBOUNCE_MAX: Duration = Duration::from_millis(400);

/// How often to refresh server stats, given how far away the server is.
///
/// `INFO` returns several KB and costs a round trip the key browser wants. On localhost a
/// fixed 5s tick is free. Against a server measured at 390ms it spends a fifth of the
/// connection on stats nobody is looking at, and at the 1.2s that same link averaged under
/// packet loss the refreshes no longer even fit between ticks.
fn info_tick(rtt: Duration) -> Duration {
    if rtt.is_zero() {
        // Zero means the PING failed, not that the server is infinitely close.
        return INFO_TICK_MIN;
    }
    (rtt * INFO_TICK_RTTS).clamp(INFO_TICK_MIN, INFO_TICK_MAX)
}

/// How long the cursor must sit still before the key under it is fetched.
///
/// Reading a key is a round trip, so fetching on every cursor move means holding `j` for a
/// second queues dozens of them — and against a remote server every reply but the last is
/// thrown away as stale anyway. Short enough to feel immediate on a deliberate move, long
/// enough that scrolling through a list costs one request at the end of it.
///
/// Scaled by distance because the right answer is not a constant: at 35ms a 90ms wait is
/// most of the cost of the fetch, while at 390ms it is noise next to a round trip the user
/// is going to wait for regardless — and there, one wasted fetch is worth far more than
/// the 100ms spent avoiding it.
fn select_debounce(rtt: Duration) -> Duration {
    if rtt.is_zero() {
        return SELECT_DEBOUNCE_MIN;
    }
    (rtt / 2).clamp(SELECT_DEBOUNCE_MIN, SELECT_DEBOUNCE_MAX)
}

/// Deadline for the browser's connect.
///
/// Long on purpose. The browser draws what it is doing and quits on `q`, so this is only
/// a backstop against a connection that is black-holed and will never answer — not a
/// guess about how far away the server is. Guessing that is what made a perfectly healthy
/// managed database look broken.
const BROWSE_CONNECT_TIMEOUT: Duration = Duration::from_secs(60);

pub async fn run(target: &Target) -> Result<()> {
    let url = target.url.as_str();
    // The terminal comes up *before* the connection, so there is something on screen from
    // the first moment. Connecting behind a blank terminal is what forced a tight deadline
    // in the first place: with nothing drawn, a slow link and a hang look identical.
    let mut terminal = ratatui::init();
    let outcome = connect_with_splash(&mut terminal, url).await;

    let conn = match outcome {
        Ok(Some(conn)) => conn,
        // The user pressed q while it was still connecting.
        Ok(None) => {
            ratatui::restore();
            return Ok(());
        }
        Err(e) => {
            ratatui::restore();
            return Err(e);
        }
    };

    let server = conn.server().clone();
    // Read before the worker takes ownership. Everything the event loop paces itself by is
    // derived from this one measurement.
    let rtt = conn.rtt();

    let (req_tx, req_rx) = mpsc::channel::<Request>(CHANNEL_SIZE);
    let (up_tx, mut up_rx) = mpsc::channel::<Update>(CHANNEL_SIZE);
    let up_tx2 = up_tx.clone();

    let worker = Worker::with_prefix(conn, target.prefix.clone());
    let worker_handle = tokio::spawn(worker.run(req_rx, up_tx));

    let mut app = App::new(server, redact_url(url), req_tx.clone());
    // Detection first: it's cheap, and its result decides whether a queues tab exists and
    // which streams the event reader follows.
    req_tx.send(Request::Detect).await.ok();
    req_tx
        .send(Request::Rescan {
            pattern: None,
            kind: None,
        })
        .await
        .ok();

    let streamer = StreamerHandle {
        url: url.to_string(),
        updates: up_tx2,
        task: None,
    };

    let result = event_loop(&mut terminal, &mut app, &mut up_rx, &req_tx, streamer, rtt).await;
    ratatui::restore();

    // `app` owns a clone of the request sender and outlives this scope, so dropping this
    // one does *not* end the worker's recv loop -- the abort is what actually stops it.
    // Dropped first anyway so the worker isn't handed new work on the way out.
    drop(req_tx);
    worker_handle.abort();

    result
}

/// Connect while showing the splash, so the wait is visible and interruptible.
///
/// Returns `Ok(None)` if the user gave up and pressed `q`. A failure is drawn in the
/// terminal and acknowledged with a keypress rather than dumped to a shell that has
/// already been restored.
async fn connect_with_splash(
    terminal: &mut ratatui::DefaultTerminal,
    url: &str,
) -> Result<Option<Conn>> {
    let display_url = redact_url(url);
    let started = std::time::Instant::now();
    let mut connecting = Box::pin(Conn::connect_with_timeout(
        url,
        "browse",
        BROWSE_CONNECT_TIMEOUT,
    ));

    let mut events = EventStream::new();
    // Fast enough that the elapsed counter looks live, slow enough to cost nothing.
    let mut tick = tokio::time::interval(Duration::from_millis(200));

    loop {
        terminal.draw(|f| {
            ui::draw_connecting(f, &display_url, &status_line(started.elapsed()), None);
        })?;

        tokio::select! {
            result = &mut connecting => {
                return match result {
                    Ok(conn) => Ok(Some(conn)),
                    Err(e) => {
                        // Show it here: by the time `main` prints an error the terminal is
                        // already restored and the message can scroll past unread.
                        let message = e.to_string();
                        await_dismiss(terminal, &display_url, &message, &mut events).await?;
                        Err(e.into())
                    }
                };
            }
            maybe_event = events.next() => {
                if let Some(Ok(Event::Key(key))) = maybe_event
                    && key.kind == KeyEventKind::Press
                    && is_quit(&key)
                {
                    return Ok(None);
                }
            }
            _ = tick.tick() => {}
        }
    }
}

/// What the splash says while it waits. Naming the slow part beats a bare spinner.
fn status_line(elapsed: Duration) -> String {
    let secs = elapsed.as_secs();
    if secs < 3 {
        "connecting…".to_string()
    } else {
        format!("connecting… {secs}s   (q to cancel)")
    }
}

/// Draw the failure and wait for a keypress.
async fn await_dismiss(
    terminal: &mut ratatui::DefaultTerminal,
    url: &str,
    message: &str,
    events: &mut EventStream,
) -> Result<()> {
    terminal.draw(|f| ui::draw_connecting(f, url, "could not connect", Some(message)))?;
    while let Some(Ok(event)) = events.next().await {
        if let Event::Key(key) = event
            && key.kind == KeyEventKind::Press
        {
            break;
        }
    }
    Ok(())
}

fn is_quit(key: &crossterm::event::KeyEvent) -> bool {
    use crossterm::event::{KeyCode, KeyModifiers};
    matches!(key.code, KeyCode::Char('q') | KeyCode::Esc)
        || (key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c'))
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
    ///
    /// Returns as soon as the task is spawned. It must: this is called from the UI event
    /// loop, and opening the reader's connection is a TLS handshake plus a full capability
    /// probe, bounded by a 20s timeout. Awaiting that here froze the terminal — no
    /// keystrokes, no redraws — for the whole handshake, right at the moment the first
    /// keys had landed and the user was starting to scroll.
    fn attach(&mut self, prefix: String, queues: Vec<String>) {
        if self.task.is_some() || queues.is_empty() {
            return;
        }

        let url = self.url.clone();
        let tx = self.updates.clone();

        self.task = Some(tokio::spawn(async move {
            // Its own connection: `XREAD BLOCK` holds the connection for the duration of
            // the block, so sharing the worker's would stall every key lookup behind it.
            let conn = match Conn::connect(&url, "events").await {
                Ok(c) => c,
                Err(e) => {
                    warn!(error = %e, "could not open a connection for the event stream");
                    // Say so in the UI. Interactive logs go to a sink, so a silent return
                    // leaves the queue table claiming it is still "attaching…" forever.
                    tx.send(Update::EventsStatus(EventsStatus::Unavailable(
                        e.to_string(),
                    )))
                    .await
                    .ok();
                    return;
                }
            };

            crate::events::run(conn, prefix, queues, tx).await;
        }));
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
    rtt: Duration,
) -> Result<()> {
    let mut events = EventStream::new();
    let debounce = select_debounce(rtt);
    let mut info_tick = tokio::time::interval(info_tick(rtt));
    info_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // When the pending selection should be fetched. Pushed back by every keypress, so a
    // run of cursor moves collapses into the one fetch the user actually meant.
    let mut fetch_at: Option<tokio::time::Instant> = None;

    terminal.draw(|f| ui::draw(f, app))?;

    loop {
        // Redraw only when something actually changed. A fixed render tick would burn CPU
        // on an idle terminal, which is exactly the thing people notice in a TUI.
        let mut dirty = false;

        tokio::select! {
            // Disabled unless something is actually waiting, so an idle terminal stays idle.
            () = async {
                match fetch_at {
                    Some(at) => tokio::time::sleep_until(at).await,
                    None => std::future::pending().await,
                }
            } => {
                fetch_at = None;
                app.flush_selection().await;
                // Still pending means the worker was saturated; come back to it.
                if app.selection_pending() {
                    fetch_at = Some(tokio::time::Instant::now() + debounce);
                }
                dirty = true;
            }

            maybe_event = events.next() => {
                match maybe_event {
                    Some(Ok(Event::Key(key))) if key.kind == KeyEventKind::Press => {
                        app.handle_key(key).await;
                        if app.selection_pending() {
                            fetch_at = Some(tokio::time::Instant::now() + debounce);
                        }
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
                    streamer.attach(d.prefix.clone(), d.targets.clone());
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pacing_scales_with_distance() {
        // The two links this was tuned against: a droplet at 35ms and a managed endpoint
        // at 390ms, both DigitalOcean, measured from the same machine.
        let near = Duration::from_millis(35);
        let far = Duration::from_millis(390);

        // Near, the floors win -- there is nothing to save by backing off.
        assert_eq!(info_tick(near), INFO_TICK_MIN);
        assert_eq!(select_debounce(near), SELECT_DEBOUNCE_MIN);

        // Far, both back off, because a round trip is now a scarce resource.
        assert!(info_tick(far) > INFO_TICK_MIN);
        assert!(select_debounce(far) > SELECT_DEBOUNCE_MIN);
    }

    #[test]
    fn a_failed_ping_falls_back_to_the_floor_not_to_zero() {
        // `Conn::rtt` reports zero when the PING did not come back. Reading that as "this
        // server is infinitely close" would pick the most aggressive pacing for a server
        // we know least about.
        assert_eq!(info_tick(Duration::ZERO), INFO_TICK_MIN);
        assert_eq!(select_debounce(Duration::ZERO), SELECT_DEBOUNCE_MIN);
    }

    #[test]
    fn pacing_is_capped_so_a_terrible_link_still_refreshes() {
        // Backing off without a ceiling means a link bad enough stops updating entirely,
        // which reads as a frozen UI rather than a slow one.
        let awful = Duration::from_secs(10);
        assert_eq!(info_tick(awful), INFO_TICK_MAX);
        assert_eq!(select_debounce(awful), SELECT_DEBOUNCE_MAX);
    }
}
