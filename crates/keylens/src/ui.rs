//! Rendering.

use keylens_conn::KeyValue;
use keylens_ui::banner;
use keylens_ui::format;
use keylens_ui::theme::Theme;
use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Layout, Rect};
use ratatui::style::Stylize;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, BorderType, Clear, List, ListItem, Paragraph, Wrap};

use crate::app::{App, Focus, Mode, View};
use crate::{panes, queues};

/// A consumer holding entries this long without acknowledging is worth flagging. Chosen
/// to be well past a normal processing window without waiting for a human to notice.
const STUCK_IDLE_MS: i64 = 30_000;

pub fn draw(frame: &mut Frame, app: &mut App) {
    if app.splash {
        draw_splash(frame, app, frame.area());
        return;
    }

    let [status, tabs, body, hint] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Length(1),
        Constraint::Min(0),
        Constraint::Length(1),
    ])
    .areas(frame.area());

    draw_status(frame, app, status);
    draw_tabs(frame, app, tabs);

    match app.view {
        View::Keys => {
            let [left, right] =
                Layout::horizontal([Constraint::Percentage(40), Constraint::Percentage(60)])
                    .areas(body);
            draw_tree(frame, app, left);
            draw_value(frame, app, right);
        }
        view => draw_server_pane(frame, app, body, view),
    }

    draw_hint(frame, app, hint);

    if app.mode == Mode::Help {
        draw_help(frame, frame.area());
    }
}

/// The splash shown while connecting, and when connecting fails.
///
/// Deliberately available before an `App` exists: the terminal comes up before the
/// connection does, so there is never a blank screen to interpret.
pub fn draw_connecting(frame: &mut Frame, url: &str, status: &str, error: Option<&str>) {
    let area = frame.area();
    let mut lines = banner::splash(area.width, url, None, status);

    if let Some(message) = error {
        lines.push(Line::raw(""));
        for line in message.lines() {
            lines.push(Line::from(Span::styled(line.to_string(), Theme::error())));
        }
        lines.push(Line::raw(""));
        lines.push(Line::from(Span::styled(
            "press any key to exit",
            Theme::dim(),
        )));
    }

    let top = area.height.saturating_sub(lines.len() as u16) / 2;
    let inner = Rect {
        x: area.x,
        y: area.y + top,
        width: area.width,
        height: area.height.saturating_sub(top),
    };

    frame.render_widget(Clear, area);
    frame.render_widget(
        Paragraph::new(lines)
            .alignment(Alignment::Center)
            .wrap(Wrap { trim: false }),
        inner,
    );
}

fn draw_splash(frame: &mut Frame, app: &App, area: Rect) {
    let server = (!app.server.version.is_empty() && app.server.version != "unknown")
        .then(|| (app.server.vendor.label(), app.server.version.as_str()));

    let status = match &app.error {
        Some(e) => e.clone(),
        None => app.status.clone(),
    };

    let lines = banner::splash(area.width, &app.url, server, &status);

    // Centre vertically so the wordmark doesn't sit awkwardly at the top of a tall window.
    let content_height = lines.len() as u16;
    let top = area.height.saturating_sub(content_height) / 2;
    let inner = Rect {
        x: area.x,
        y: area.y + top,
        width: area.width,
        height: area.height.saturating_sub(top),
    };

    frame.render_widget(Clear, area);
    frame.render_widget(Paragraph::new(lines).alignment(Alignment::Center), inner);
}

fn draw_status(frame: &mut Frame, app: &App, area: Rect) {
    let server = &app.server;
    let mut spans = vec![
        Span::styled(" KEYLENS ", Theme::brand()),
        Span::raw(" "),
        Span::styled(
            format!(" {} ", server.vendor.label()),
            Theme::chip(Theme::BRAND_B),
        ),
        Span::raw(" "),
        Span::styled(server.version.clone(), Theme::number()),
        Span::styled(format!("  {}", server.mode), Theme::label()),
        Span::styled(format!("  {}", app.url), Theme::dim()),
    ];

    // Active filters are chips, not prose: a filter you can't see is a filter you forget
    // you set and then report as a bug.
    if let Some(p) = &app.pattern {
        spans.push(Span::raw(" "));
        spans.push(Span::styled(
            format!(" match {p} "),
            Theme::chip(ratatui::style::Color::Yellow),
        ));
    }
    if let Some(k) = app.kind_filter {
        spans.push(Span::raw(" "));
        spans.push(Span::styled(
            format!(" type {} ", k.label()),
            Theme::chip(Theme::kind(Some(k)).fg.unwrap_or(Theme::BRAND_A)),
        ));
    }
    if app.loading {
        spans.push(Span::raw(" "));
        spans.push(Span::styled("● scanning", Theme::warn()));
    }

    frame.render_widget(Line::from(spans), area);
}

fn draw_tabs(frame: &mut Frame, app: &App, area: Rect) {
    let mut spans = vec![Span::raw(" ")];
    // Tabs are whatever this keyspace earned: `queues` shows up only once a lens matched.
    for (i, view) in app.views().into_iter().enumerate() {
        let active = view == app.view;
        spans.push(Span::styled(
            format!(" {} {} ", i + 1, view.label()),
            if active {
                Theme::tab_active()
            } else {
                Theme::tab_inactive()
            },
        ));
        spans.push(Span::raw(" "));
    }
    frame.render_widget(Line::from(spans), area);
}

fn draw_server_pane(frame: &mut Frame, app: &App, area: Rect, view: View) {
    let (title, lines) = match view {
        View::Stats => (
            view.label().to_string(),
            panes::stats(&app.server, area.width),
        ),
        View::Slowlog => (view.label().to_string(), panes::slowlog(app)),
        View::Clients => (view.label().to_string(), panes::clients(app)),
        View::Cluster => (view.label().to_string(), panes::cluster(app)),
        View::PubSub => (view.label().to_string(), panes::pubsub(app)),
        // -2 for the panel border.
        View::Queues => (
            queues::title(app),
            queues::render(app, area.width.saturating_sub(2)),
        ),
        View::Keys => unreachable!("the keys view has its own split layout"),
    };

    frame.render_widget(
        Paragraph::new(lines)
            .block(Theme::panel(&title, true))
            .scroll((app.pane_scroll, 0)),
        area,
    );
}

fn draw_tree(frame: &mut Frame, app: &mut App, area: Rect) {
    let title = if app.loading {
        "keys — scanning…".to_string()
    } else {
        app.status.clone()
    };
    let block = Theme::panel(&title, app.focus == Focus::Tree);

    let items: Vec<ListItem> = app
        .rows
        .iter()
        .map(|row| {
            let indent = "  ".repeat(row.depth);
            let marker = if row.is_branch {
                if row.expanded { "▾ " } else { "▸ " }
            } else {
                "· "
            };

            let mut spans = vec![
                Span::raw(indent),
                Span::styled(
                    marker,
                    if row.is_branch {
                        Theme::accent()
                    } else {
                        Theme::dim()
                    },
                ),
                if row.is_branch {
                    Span::styled(row.label.clone(), Theme::branch())
                } else {
                    Span::styled(row.label.clone(), Theme::key_name())
                },
            ];

            if row.is_branch {
                spans.push(Span::styled(
                    format!(" ({})", format::count(row.subtree_keys as u64)),
                    Theme::dim(),
                ));
            }
            // A node can be both branch and key, so this is not an `else`.
            if row.is_key
                && let Some(kind) = row.kind
            {
                spans.push(Span::raw(" "));
                spans.push(Span::styled(kind.tag(), Theme::kind(Some(kind))));
            }

            ListItem::new(Line::from(spans))
        })
        .collect();

    let list = List::new(items)
        .block(block)
        .highlight_style(Theme::selected());
    app.list_state.select(Some(app.selected));
    frame.render_stateful_widget(list, area, &mut app.list_state);
}

fn draw_value(frame: &mut Frame, app: &App, area: Rect) {
    let focused = app.focus == Focus::Value;

    if let Some(err) = &app.error {
        let p = Paragraph::new(Line::from(Span::styled(err.clone(), Theme::error())))
            .block(Theme::panel("value", focused))
            .wrap(Wrap { trim: false });
        frame.render_widget(p, area);
        return;
    }

    let Some((meta, value)) = &app.detail else {
        let hint = match app.selected_row() {
            Some(row) if row.is_branch => "select a key to inspect it",
            Some(_) => "loading…",
            None => "no keys — press r to rescan, / to filter",
        };
        frame.render_widget(
            Paragraph::new(Line::from(Span::styled(format!("  {hint}"), Theme::dim())))
                .block(Theme::panel("value", focused)),
            area,
        );
        return;
    };

    let mut lines: Vec<Line> = Vec::new();

    lines.push(Line::from(Span::styled(meta.key.clone(), Theme::title())));
    lines.push(Line::from(vec![
        Span::styled(
            format!(" {} ", meta.kind.label()),
            Theme::chip(Theme::kind(Some(meta.kind)).fg.unwrap_or(Theme::BRAND_B)),
        ),
        Span::styled("  ttl ", Theme::dim()),
        Span::styled(format::ttl(meta.ttl_ms), Theme::number()),
        Span::styled("  size ", Theme::dim()),
        Span::styled(format::count(meta.size), Theme::number()),
        Span::styled("  mem ", Theme::dim()),
        Span::styled(format::bytes(meta.memory), Theme::number()),
    ]));
    lines.push(Line::raw(""));

    // For a stream, groups and consumers come *before* the entries: "which consumer is
    // stuck" is the question, and the entries are the background detail.
    if let Some(stream) = &app.stream {
        lines.extend(stream_lines(stream));
        lines.push(Line::raw(""));
        lines.push(Line::from(Span::styled(" ▌entries", Theme::heading())));
    }

    lines.extend(value_lines(value, area.width.saturating_sub(4) as usize));

    let p = Paragraph::new(lines)
        .block(Theme::panel("value", focused))
        .wrap(Wrap { trim: false })
        .scroll((app.detail_scroll, 0));
    frame.render_widget(p, area);
}

/// Stream metadata, consumer groups and per-consumer pending/idle.
///
/// Every other tool shows a stream as a list of entries. The operational question is
/// almost never "what's in it" — it's "which consumer stopped acknowledging, and how far
/// behind has the group fallen".
fn stream_lines(s: &keylens_conn::StreamInfo) -> Vec<Line<'static>> {
    let mut lines = vec![Line::from(Span::styled(" ▌stream", Theme::heading()))];

    lines.push(Line::from(vec![
        Span::styled("  length ", Theme::label()),
        Span::styled(format::count(s.length), Theme::number()),
        Span::styled("  added ", Theme::label()),
        Span::styled(
            s.entries_added
                .map(format::count)
                .unwrap_or_else(|| "-".into()),
            Theme::number(),
        ),
        Span::styled("  last id ", Theme::label()),
        Span::styled(s.last_generated_id.clone(), Theme::value()),
    ]));

    if s.groups.is_empty() {
        lines.push(Line::from(Span::styled(
            "  no consumer groups — this stream is read with XREAD",
            Theme::dim(),
        )));
        return lines;
    }

    lines.push(Line::raw(""));
    lines.push(Line::from(Span::styled(
        " ▌consumer groups",
        Theme::heading(),
    )));

    for g in &s.groups {
        let lag_style = match g.lag {
            Some(0) => Theme::ok(),
            Some(_) => Theme::warn(),
            None => Theme::dim(),
        };

        lines.push(Line::from(vec![
            Span::styled(format!("  {}", g.name), Theme::accent()),
            Span::styled("  consumers ", Theme::label()),
            Span::styled(g.consumer_count.to_string(), Theme::number()),
            Span::styled("  pending ", Theme::label()),
            Span::styled(
                format::count(g.pending),
                if g.pending > 0 {
                    Theme::warn()
                } else {
                    Theme::ok()
                },
            ),
            Span::styled("  lag ", Theme::label()),
            // `unknown` is the honest rendering of a nil lag; 0 would claim the group is
            // caught up when Redis simply cannot tell.
            Span::styled(
                g.lag
                    .map(|l| l.to_string())
                    .unwrap_or_else(|| "unknown".into()),
                lag_style,
            ),
        ]));

        if !g.pending_min_id.is_empty() {
            lines.push(Line::from(vec![
                Span::styled("    pending range ", Theme::label()),
                Span::styled(
                    format!("{} … {}", g.pending_min_id, g.pending_max_id),
                    Theme::dim(),
                ),
            ]));
        }

        // Worst offenders first: a group with fifty consumers should not make you scroll
        // to find the one that stopped acknowledging.
        let mut consumers: Vec<_> = g.consumers.iter().collect();
        consumers.sort_by_key(|c| (std::cmp::Reverse(c.pending), std::cmp::Reverse(c.idle_ms)));

        for c in consumers {
            // A consumer holding entries while idle for a long time is the failure mode
            // this pane exists to surface.
            let stuck = c.pending > 0 && c.idle_ms > STUCK_IDLE_MS;
            let name_style = if stuck {
                Theme::error()
            } else {
                Theme::value()
            };

            // The flag leads the row rather than trailing it: a trailing note wraps onto
            // its own line in a narrow value pane, which is exactly where this matters.
            lines.push(Line::from(vec![
                Span::styled(if stuck { "    ! " } else { "      " }, Theme::error()),
                Span::styled(format!("{:<22}", format::truncate(&c.name, 21)), name_style),
                Span::styled("pending ", Theme::label()),
                Span::styled(
                    format!("{:<8}", format::count(c.pending)),
                    if c.pending > 0 {
                        Theme::warn()
                    } else {
                        Theme::dim()
                    },
                ),
                Span::styled("idle ", Theme::label()),
                Span::styled(
                    format::ttl(Some(c.idle_ms)),
                    if stuck { Theme::error() } else { Theme::dim() },
                ),
            ]));
        }
    }

    if s.groups_truncated {
        lines.push(Line::from(Span::styled(
            "  … more groups not shown",
            Theme::dim(),
        )));
    }

    lines
}

fn value_lines(value: &KeyValue, width: usize) -> Vec<Line<'static>> {
    // Leave room for the field column before truncating values.
    let val_width = width.saturating_sub(24).max(20);

    match value {
        KeyValue::String(s) => {
            // Job payloads are JSON far more often than not; a minified blob is unreadable.
            match format::pretty_json(s) {
                Some(pretty) => pretty.lines().map(|l| json_line(l.to_string())).collect(),
                None => s
                    .lines()
                    .map(|l| Line::from(Span::styled(l.to_string(), Theme::value())))
                    .collect(),
            }
        }

        KeyValue::Hash(fields) => fields
            .iter()
            .map(|(f, v)| {
                Line::from(vec![
                    Span::styled(format!("{:<20} ", format::truncate(f, 20)), Theme::field()),
                    Span::styled(
                        format::truncate(&format::single_line(v), val_width),
                        Theme::value(),
                    ),
                ])
            })
            .collect(),

        KeyValue::List(items) => items
            .iter()
            .enumerate()
            .map(|(i, v)| {
                Line::from(vec![
                    Span::styled(format!("{i:<6} "), Theme::dim()),
                    Span::styled(
                        format::truncate(&format::single_line(v), val_width),
                        Theme::value(),
                    ),
                ])
            })
            .collect(),

        KeyValue::Set(members) => members
            .iter()
            .map(|m| {
                Line::from(Span::styled(
                    format::truncate(&format::single_line(m), width),
                    Theme::value(),
                ))
            })
            .collect(),

        KeyValue::ZSet(scored) => scored
            .iter()
            .map(|(m, score)| {
                Line::from(vec![
                    Span::styled(format!("{score:<14} "), Theme::number()),
                    Span::styled(
                        format::truncate(&format::single_line(m), val_width),
                        Theme::value(),
                    ),
                ])
            })
            .collect(),

        KeyValue::Stream(entries) => {
            let mut out = Vec::new();
            for e in entries {
                out.push(Line::from(Span::styled(e.id.clone(), Theme::accent())));
                for (f, v) in &e.fields {
                    out.push(Line::from(vec![
                        Span::raw("  "),
                        Span::styled(format!("{:<18} ", format::truncate(f, 18)), Theme::field()),
                        Span::styled(
                            format::truncate(&format::single_line(v), val_width),
                            Theme::value(),
                        ),
                    ]));
                }
            }
            out
        }

        KeyValue::Missing => vec![Line::from(Span::styled(
            "key no longer exists",
            Theme::dim(),
        ))],

        // Not an error: this server has no cursor-based read for the type, so keylens
        // measured the key first and declined rather than pulling the whole thing.
        KeyValue::TooLarge { what, limit, unit } => vec![
            Line::from(Span::styled(
                format!(
                    "this {what} is larger than {} {unit}",
                    format::count(*limit as u64)
                ),
                Theme::warn(),
            )),
            Line::from(Span::styled(
                "this server has no cursor-based read for it, so keylens will not fetch it whole",
                Theme::dim(),
            )),
        ],

        KeyValue::Unsupported(why) => vec![Line::from(Span::styled(why.clone(), Theme::warn()))],
    }
}

/// Colour a pretty-printed JSON line: keys in yellow, everything else plain.
///
/// Deliberately not a real tokenizer -- splitting on the first `":` is enough to make a
/// payload scannable, and a payload viewer should never be able to fail on odd input.
fn json_line(line: String) -> Line<'static> {
    match line.find("\": ") {
        Some(idx) => {
            let (key, rest) = line.split_at(idx + 2);
            Line::from(vec![
                Span::styled(key.to_string(), Theme::field()),
                Span::styled(rest.to_string(), Theme::value()),
            ])
        }
        None => Line::from(Span::styled(line, Theme::value())),
    }
}

fn draw_hint(frame: &mut Frame, app: &App, area: Rect) {
    // Derived, not written down. The tab count changes when a lens matches, and a hint
    // that says `1-6` next to seven tabs is a hint that has stopped being true.
    let views = format!("1-{}", app.views().len());

    let keys: Vec<(&str, &str)> = match app.mode {
        Mode::Search => Vec::new(),
        _ if app.view == View::Keys => vec![
            ("j/k", "move"),
            ("↵", "open"),
            ("/", "filter"),
            ("t", "type"),
            ("m", "more"),
            ("r", "rescan"),
            ("E/C", "expand"),
            ("⇥", "pane"),
            (views.as_str(), "view"),
            ("?", "help"),
            ("q", "quit"),
        ],
        _ if app.view == View::Queues => vec![
            ("j/k", "move"),
            ("↵", "open"),
            ("h", "back"),
            ("[/]", "state"),
            ("r", "reload"),
            (views.as_str(), "view"),
            ("?", "help"),
            ("q", "quit"),
        ],
        _ => vec![
            ("j/k", "scroll"),
            ("r", "reload"),
            (views.as_str(), "view"),
            ("?", "help"),
            ("q", "quit"),
        ],
    };

    if app.mode == Mode::Search {
        frame.render_widget(
            Line::from(vec![
                Span::styled(" match ", Theme::chip(ratatui::style::Color::Yellow)),
                Span::raw(" "),
                Span::styled(app.search_input.clone(), Theme::value()),
                Span::styled("█", Theme::accent()),
                Span::styled("   ↵ apply   esc cancel", Theme::dim()),
            ]),
            area,
        );
        return;
    }

    let mut spans = vec![Span::raw(" ")];
    for (key, what) in keys {
        spans.push(Span::styled(key.to_string(), Theme::accent()));
        spans.push(Span::styled(format!(" {what}  "), Theme::dim()));
    }
    frame.render_widget(Line::from(spans), area);
}

fn draw_help(frame: &mut Frame, area: Rect) {
    let rows = [
        ("1 - 7", "switch view"),
        ("r", "reload the current view"),
        ("[ / ]", "queues: cycle job state"),
        ("j / k, ↓ / ↑", "move or scroll"),
        ("g / G", "top / bottom"),
        ("enter, space, →", "expand branch or open key"),
        ("h, ←", "collapse or jump to parent"),
        ("/", "filter by pattern (server-side MATCH)"),
        ("esc", "clear filter"),
        ("t", "cycle type filter"),
        ("m", "scan more keys"),
        ("E / C", "expand all / collapse all"),
        ("tab", "switch pane"),
        ("q, ctrl-c", "quit"),
    ];

    let width = 62.min(area.width.saturating_sub(4));
    let height = (rows.len() as u16 + 4).min(area.height.saturating_sub(2));
    let popup = Rect {
        x: area.x + (area.width.saturating_sub(width)) / 2,
        y: area.y + (area.height.saturating_sub(height)) / 2,
        width,
        height,
    };

    let mut lines = vec![Line::raw("")];
    for (keys, what) in rows {
        lines.push(Line::from(vec![
            Span::raw("  "),
            Span::styled(format!("{keys:<18}"), Theme::accent()),
            Span::styled(what, Theme::value()),
        ]));
    }

    frame.render_widget(Clear, popup);
    frame.render_widget(
        Paragraph::new(lines).block(
            Block::bordered()
                .border_type(BorderType::Rounded)
                .border_style(Theme::title())
                .title(" help ".bold()),
        ),
        popup,
    );
}
