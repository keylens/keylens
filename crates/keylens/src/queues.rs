//! Rendering for the BullMQ lens: queue table, job list, job detail.

use keylens_bullmq::State;
use keylens_ui::PaneState;
use keylens_ui::format;
use keylens_ui::theme::Theme;
use ratatui::style::Color;
use ratatui::text::{Line, Span};

use crate::app::{App, QueueLevel};

/// Column width for a state's count, including the gap before it.
const COL: usize = 10;
/// Bounds on the inline sparkline. One cell is one second of history, so the width is
/// also how far back the graph reaches -- it has to fit, or the recent end (the only end
/// anyone looks at) gets clipped off the right of the pane.
const SPARK_MIN: usize = 5;
const SPARK_MAX: usize = 40;
/// Width of the events-per-second column.
const RATE_W: usize = 7;

/// State columns that always earn their space. `failed` is why people open a queue
/// dashboard at all; `wait` and `active` are how you tell a backlog from a stall.
const REQUIRED: [State; 3] = [State::Waiting, State::Active, State::Failed];
/// The rest, in the order they get dropped as the pane narrows (last dropped first).
const OPTIONAL: [State; 4] = [
    State::Completed,
    State::Delayed,
    State::Prioritized,
    State::WaitingChildren,
];

/// Decide which columns fit in `width`.
///
/// An 80-column terminal cannot hold seven state columns plus a graph, and silently
/// clipping the right-hand edge hides the throughput graph -- the one thing this view
/// exists for. So columns are dropped deliberately, least useful first.
fn layout(width: usize, name_w: usize) -> (Vec<State>, usize) {
    let mut used = 2 + name_w + 9 + REQUIRED.len() * COL;
    let mut shown: Vec<State> = REQUIRED.to_vec();

    for state in OPTIONAL {
        if used + COL > width {
            break;
        }
        shown.push(state);
        used += COL;
    }

    // Render in the canonical state order regardless of the order they were added.
    shown.sort_by_key(|s| State::ALL.iter().position(|a| a == s).unwrap_or(usize::MAX));

    // The graph needs its two-space gap, a minimum run of cells, and the rate column.
    let spark_w = match width.checked_sub(used + 2 + RATE_W) {
        Some(spare) if spare >= SPARK_MIN => spare.min(SPARK_MAX),
        _ => 0,
    };

    (shown, spark_w)
}

fn placeholder<T>(state: &PaneState<T>) -> Option<Vec<Line<'static>>> {
    state.placeholder().map(|msg| {
        let style = if state.is_error() {
            Theme::error()
        } else {
            Theme::dim()
        };
        vec![
            Line::raw(""),
            Line::from(Span::styled(format!("  {msg}"), style)),
        ]
    })
}

/// Colour a state's count: zero is noise, failed is red, the rest are informational.
fn count_style(state: State, n: u64) -> ratatui::style::Style {
    if n == 0 {
        return Theme::dim();
    }
    match state {
        State::Failed => Theme::error(),
        State::Active => Theme::ok(),
        State::Delayed | State::WaitingChildren => Theme::warn(),
        _ => Theme::number(),
    }
}

pub fn title(app: &App) -> String {
    match app.level {
        QueueLevel::Queues => match app.detections.first() {
            Some(d) => format!("queues — {}", d.summary),
            None => "queues".to_string(),
        },
        QueueLevel::Jobs => match app.selected_queue() {
            Some(q) => format!("{} — {}", q.name, app.job_state.label()),
            None => "jobs".to_string(),
        },
        QueueLevel::Job => match app.selected_job() {
            Some(j) => format!("job {}", j.id),
            None => "job".to_string(),
        },
    }
}

pub fn render(app: &App, width: u16) -> Vec<Line<'static>> {
    match app.level {
        QueueLevel::Queues => queue_table(app, width),
        QueueLevel::Jobs => job_list(app),
        QueueLevel::Job => job_detail(app),
    }
}

/// The live throughput cells: a sparkline of the last [`SPARK`] seconds plus a rate.
///
/// This is the whole differentiation thesis in two columns. BullBoard polls counts on a
/// timer; these cells are driven by the events stream, so they move the instant a job
/// completes or fails rather than at the next poll.
fn throughput_cells(app: &App, queue: &str, now: i64, spark_w: usize) -> Vec<Span<'static>> {
    let Some(series) = app.throughput.series(queue) else {
        let msg = if app.throughput.attached {
            "idle"
        } else {
            "…"
        };
        return vec![
            Span::styled(format!("  {msg:<spark_w$}"), Theme::dim()),
            Span::styled(format!("{:>RATE_W$}", "-"), Theme::dim()),
        ];
    };

    // Failures are colour, not a second series: a queue that is busy *and* failing has to
    // read differently at a glance from one that is merely busy.
    let totals = series.window(now, spark_w, |b| b.total);
    let failed: u64 = series.window(now, spark_w, |b| b.failed).iter().sum();
    let rate = series.rate(now, 10);

    let spark_style = if failed > 0 {
        Theme::error()
    } else {
        Theme::ok()
    };
    let rate_style = if rate > 0.0 {
        Theme::number()
    } else {
        Theme::dim()
    };

    vec![
        Span::styled(format!("  {}", format::sparkline(&totals)), spark_style),
        Span::styled(format!("{rate:>RATE_W$.1}"), rate_style),
    ]
}

fn queue_table(app: &App, width: u16) -> Vec<Line<'static>> {
    if let Some(p) = placeholder(&app.queues) {
        return p;
    }
    let queues = app.queues.ready().expect("checked above");
    let now_secs = now_ms() / 1000;

    if queues.is_empty() {
        return vec![
            Line::raw(""),
            Line::from(Span::styled("  no queues found", Theme::dim())),
        ];
    }

    // +2 so the longest name still has a gap before the status column.
    let name_w = queues
        .iter()
        .map(|q| q.name.chars().count())
        .max()
        .unwrap_or(10)
        .clamp(10, 30)
        + 2;

    let (columns, spark_w) = layout(width as usize, name_w);

    let mut header = vec![
        Span::styled(format!("  {:<name_w$}", "queue"), Theme::label()),
        Span::styled(format!("{:<9}", "status"), Theme::label()),
    ];
    for s in &columns {
        header.push(Span::styled(
            format!("{:>COL$}", s.short_label()),
            Theme::label(),
        ));
    }
    if spark_w > 0 {
        let spark_header = format!("last {spark_w}s");
        header.push(Span::styled(
            format!("  {:<spark_w$}", format::truncate(&spark_header, spark_w)),
            Theme::label(),
        ));
        header.push(Span::styled(format!("{:>RATE_W$}", "ev/s"), Theme::label()));
    }
    let mut lines = vec![Line::from(header), Line::raw("")];

    for (i, q) in queues.iter().enumerate() {
        let selected = i == app.queue_selected;
        let marker = if selected { "▶ " } else { "  " };

        let mut spans = vec![
            Span::styled(marker, Theme::accent()),
            Span::styled(
                format!("{:<name_w$}", format::truncate(&q.name, name_w - 2)),
                if selected {
                    Theme::title()
                } else {
                    Theme::key_name()
                },
            ),
        ];

        // Paused comes from `meta.paused`, never from the legacy `paused` list.
        spans.push(if q.paused {
            Span::styled(format!("{:<9}", "paused"), Theme::chip(Color::Yellow))
        } else {
            Span::styled(format!("{:<9}", "running"), Theme::ok())
        });

        for s in &columns {
            let n = q.count(*s);
            spans.push(Span::styled(
                format!("{:>COL$}", format::count(n)),
                count_style(*s, n),
            ));
        }

        if spark_w > 0 {
            spans.extend(throughput_cells(app, &q.name, now_secs, spark_w));
        }
        lines.push(Line::from(spans));
    }

    lines.push(Line::raw(""));
    let total_rate = app.throughput.total_rate(now_secs, 10);
    lines.push(Line::from(vec![
        Span::styled(
            format!(
                "  {} queues, {} paused",
                queues.len(),
                queues.iter().filter(|q| q.paused).count()
            ),
            Theme::dim(),
        ),
        Span::styled(
            if app.throughput.attached {
                format!("   ● live · {total_rate:.1} events/sec across all queues")
            } else {
                "   ○ attaching to event streams…".to_string()
            },
            if app.throughput.attached {
                Theme::ok()
            } else {
                Theme::dim()
            },
        ),
    ]));

    lines
}

fn job_list(app: &App) -> Vec<Line<'static>> {
    let mut lines = vec![state_bar(app), Line::raw("")];

    if let Some(p) = placeholder(&app.jobs) {
        lines.extend(p);
        return lines;
    }
    let jobs = app.jobs.ready().expect("checked above");

    if jobs.is_empty() {
        lines.push(Line::from(Span::styled(
            format!("  no {} jobs", app.job_state.label()),
            Theme::dim(),
        )));
        return lines;
    }

    lines.push(Line::from(vec![
        Span::styled(format!("  {:<20}", "job id"), Theme::label()),
        Span::styled(app.job_state.score_label().to_string(), Theme::label()),
    ]));

    let now = now_ms();
    for (i, job) in jobs.iter().enumerate() {
        let selected = i == app.job_selected;
        lines.push(Line::from(vec![
            Span::styled(if selected { "▶ " } else { "  " }, Theme::accent()),
            Span::styled(
                format!("{:<20}", format::truncate(&job.id, 19)),
                if selected {
                    Theme::title()
                } else {
                    Theme::value()
                },
            ),
            Span::styled(
                job.score.map(|s| format_score(s, now)).unwrap_or_default(),
                Theme::dim(),
            ),
        ]));
    }

    lines
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// The state selector, showing where `[` and `]` will take you.
fn state_bar(app: &App) -> Line<'static> {
    let mut spans = vec![Span::styled("  ", Theme::base())];
    for s in State::ALL {
        let active = s == app.job_state;
        let count = app.selected_queue().map(|q| q.count(s)).unwrap_or(0);
        // Short labels here too: seven chips at full length overflow an 80-column pane.
        spans.push(Span::styled(
            format!(" {} {} ", s.short_label(), format::count(count)),
            if active {
                Theme::tab_active()
            } else {
                Theme::tab_inactive()
            },
        ));
        spans.push(Span::raw(" "));
    }
    Line::from(spans)
}

/// ZSET scores are millisecond timestamps for the finished states and packed due-times
/// for `delayed`. A raw epoch helps nobody; relative time is the actual question.
///
/// Anything below the year-2001 threshold isn't a timestamp at all — `prioritized` scores
/// are priority values — so those are shown as-is.
fn format_score(score: f64, now_ms: i64) -> String {
    let ms = score as i64;
    if ms > 1_000_000_000_000 {
        format::ago(ms, now_ms)
    } else {
        format!("{score}")
    }
}

fn job_detail(app: &App) -> Vec<Line<'static>> {
    if let Some(p) = placeholder(&app.job) {
        return p;
    }

    let Some(detail) = app.job.ready().expect("checked above") else {
        return vec![
            Line::raw(""),
            Line::from(Span::styled(
                "  this job no longer exists — it was removed by retention",
                Theme::dim(),
            )),
        ];
    };

    let job = &detail.job;
    let mut lines = vec![
        Line::from(vec![
            Span::styled(format!("  {}", job.id), Theme::title()),
            Span::styled(format!("  {}", job.name), Theme::accent()),
        ]),
        Line::raw(""),
    ];

    // Timings first: on a failed job, "how long did it run" is usually the second
    // question after "what broke".
    lines.push(field("attempts", job.attempts_label()));
    if let Some(w) = job.wait_ms() {
        lines.push(field("waited", format::ttl(Some(w))));
    }
    if let Some(d) = job.duration_ms() {
        lines.push(field("ran for", format::ttl(Some(d))));
    }
    if !job.progress.is_empty() {
        lines.push(field("progress", job.progress.clone()));
    }
    if let Some(p) = job.priority.filter(|p| *p != 0) {
        lines.push(field("priority", p.to_string()));
    }
    if let Some(d) = job.delay.filter(|d| *d != 0) {
        lines.push(field("delay", format::ttl(Some(d))));
    }
    if !job.parent_key.is_empty() {
        lines.push(field("parent", job.parent_key.clone()));
    }

    if job.has_failed() {
        lines.push(Line::raw(""));
        lines.push(heading("failure"));
        lines.push(Line::from(Span::styled(
            format!("  {}", job.failed_reason),
            Theme::error(),
        )));

        for (i, trace) in job.stacktrace.iter().enumerate() {
            lines.push(Line::raw(""));
            if job.stacktrace.len() > 1 {
                lines.push(Line::from(Span::styled(
                    format!("  attempt {}", i + 1),
                    Theme::label(),
                )));
            }
            // The trace is one string with real newlines inside it.
            for frame in trace.lines() {
                let trimmed = frame.trim_start();
                let style = if trimmed.starts_with("at ") {
                    Theme::dim()
                } else {
                    Theme::warn()
                };
                lines.push(Line::from(Span::styled(format!("  {frame}"), style)));
            }
        }
    }

    lines.push(Line::raw(""));
    lines.push(heading("payload"));
    lines.extend(json_block(&job.data));

    if !job.return_value.is_empty() {
        lines.push(Line::raw(""));
        lines.push(heading("return value"));
        lines.extend(json_block(&job.return_value));
    }

    if !job.opts.is_empty() {
        lines.push(Line::raw(""));
        lines.push(heading("opts"));
        lines.extend(json_block(&job.opts));
    }

    if !detail.logs.is_empty() {
        lines.push(Line::raw(""));
        lines.push(heading("logs"));
        for log in &detail.logs {
            lines.push(Line::from(Span::styled(format!("  {log}"), Theme::value())));
        }
    }

    lines
}

fn field(name: &str, value: impl Into<String>) -> Line<'static> {
    Line::from(vec![
        Span::styled(format!("  {name:<14}"), Theme::label()),
        Span::styled(value.into(), Theme::value()),
    ])
}

fn heading(text: &str) -> Line<'static> {
    Line::from(Span::styled(format!(" ▌{text}"), Theme::heading()))
}

fn json_block(raw: &str) -> Vec<Line<'static>> {
    match format::pretty_json(raw) {
        Some(pretty) => pretty
            .lines()
            .map(|l| Line::from(Span::styled(format!("  {l}"), Theme::value())))
            .collect(),
        None => vec![Line::from(Span::styled(format!("  {raw}"), Theme::value()))],
    }
}
