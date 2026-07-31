//! The interactive browser: terminal setup, the event loop, and teardown.

use std::time::Duration;

use color_eyre::Result;
use crossterm::event::{Event, EventStream, KeyEventKind};
use futures::StreamExt;
use keylens_conn::Conn;
use tokio::sync::mpsc;

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

    let worker = Worker::new(conn);
    let worker_handle = tokio::spawn(worker.run(req_rx, up_tx));

    let mut app = App::new(server, url.to_string(), req_tx.clone());
    req_tx.send(Request::Rescan { pattern: None, kind: None }).await.ok();

    // `init` puts the terminal in raw mode and installs a panic hook that restores it --
    // without that, a panic leaves the user's shell unusable.
    let mut terminal = ratatui::init();
    let result = event_loop(&mut terminal, &mut app, &mut up_rx, &req_tx).await;
    ratatui::restore();

    // Dropping the sender ends the worker's recv loop.
    drop(req_tx);
    worker_handle.abort();

    result
}

async fn event_loop(
    terminal: &mut ratatui::DefaultTerminal,
    app: &mut App,
    updates: &mut mpsc::Receiver<Update>,
    requests: &mpsc::Sender<Request>,
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
