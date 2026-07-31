//! Rendering for the server panes: stats, slowlog, clients, cluster, pub/sub.

use keylens_conn::ServerInfo;
use keylens_ui::format;
use keylens_ui::theme::Theme;
use keylens_ui::PaneState;
use ratatui::text::{Line, Span};

use crate::app::App;

/// A `key   value` row, so every pane lines up the same way.
fn field(name: &str, value: impl Into<String>) -> Line<'static> {
    Line::from(vec![
        Span::styled(format!("  {name:<26}"), Theme::label()),
        Span::styled(value.into(), Theme::value()),
    ])
}

/// A field whose value is a number worth spotting at a glance.
fn number(name: &str, value: impl Into<String>) -> Line<'static> {
    Line::from(vec![
        Span::styled(format!("  {name:<26}"), Theme::label()),
        Span::styled(value.into(), Theme::number()),
    ])
}

/// A field that should turn yellow or red when it climbs.
fn pressure(name: &str, raw: Option<u64>) -> Line<'static> {
    let text = raw.map(format::count).unwrap_or_else(|| "-".into());
    let style = match raw {
        Some(0) | None => Theme::dim(),
        Some(_) => Theme::warn(),
    };
    Line::from(vec![
        Span::styled(format!("  {name:<26}"), Theme::label()),
        Span::styled(text, style),
    ])
}

/// A labelled meter, e.g. `hit rate  ████████░░  89.6%`.
fn gauge(name: &str, fraction: f64, text: String, width: usize, inverted: bool) -> Line<'static> {
    let style = if inverted { Theme::gauge_inverted(fraction) } else { Theme::gauge(fraction) };
    Line::from(vec![
        Span::styled(format!("  {name:<26}"), Theme::label()),
        Span::styled(format::bar(fraction, width), style),
        Span::styled(format!("  {text}"), Theme::value()),
    ])
}

fn heading(text: &str) -> Line<'static> {
    Line::from(Span::styled(format!(" ▌{text}"), Theme::heading()))
}

/// Render a pane's placeholder, or `None` when it has data to show.
fn placeholder<T>(state: &PaneState<T>) -> Option<Vec<Line<'static>>> {
    state.placeholder().map(|msg| {
        let style = if state.is_error() { Theme::error() } else { Theme::dim() };
        vec![Line::raw(""), Line::from(Span::styled(format!("  {msg}"), style))]
    })
}

/// The `INFO`-derived dashboard. No extra commands: everything here comes from the
/// `INFO` payload already refreshed on a tick.
pub fn stats(info: &ServerInfo, width: u16) -> Vec<Line<'static>> {
    let g = |k: &str| info.get(k).unwrap_or("-").to_string();
    // Meters shrink with the pane so they never wrap.
    let bar_width = ((width as usize).saturating_sub(46)).clamp(10, 30);
    let mut lines = vec![
        heading("server"),
        field("vendor", info.vendor.label().to_string()),
        number("version", info.version.clone()),
        field("mode", info.mode.clone()),
        field(
            "uptime",
            info.get_u64("uptime_in_seconds")
                .map(|s| format::ttl(Some(s as i64 * 1000)))
                .unwrap_or_else(|| "-".into()),
        ),
        field("role", g("role")),
    ];

    lines.push(Line::raw(""));
    lines.push(heading("memory"));
    // A meter only means something against a limit. With `maxmemory` unset -- the default
    // -- there is no denominator, so show the number and say so rather than inventing one.
    match (info.get_u64("used_memory"), info.get_u64("maxmemory")) {
        (Some(used), Some(max)) if max > 0 => lines.push(gauge(
            "used",
            used as f64 / max as f64,
            format!("{} / {}", g("used_memory_human"), g("maxmemory_human")),
            bar_width,
            false,
        )),
        _ => {
            lines.push(number("used", g("used_memory_human")));
            lines.push(field("maxmemory", "unset (no limit)"));
        }
    }
    lines.push(number("peak", g("used_memory_peak_human")));
    lines.push(number("rss", g("used_memory_rss_human")));
    lines.push(field("policy", g("maxmemory_policy")));
    // A fragmentation ratio well above 1 is the usual explanation for "Redis is using
    // more memory than my keys should".
    let frag = info.get_f64("mem_fragmentation_ratio");
    lines.push(Line::from(vec![
        Span::styled(format!("  {:<26}", "fragmentation"), Theme::label()),
        Span::styled(
            frag.map(|f| format!("{f:.2}")).unwrap_or_else(|| "-".into()),
            match frag {
                Some(f) if f >= 1.5 => Theme::warn(),
                Some(_) => Theme::ok(),
                None => Theme::dim(),
            },
        ),
    ]));

    lines.push(Line::raw(""));
    lines.push(heading("throughput"));
    lines.push(number("ops/sec", g("instantaneous_ops_per_sec")));
    lines.push(number(
        "total commands",
        info.get_u64("total_commands_processed").map(format::count).unwrap_or_else(|| "-".into()),
    ));
    match info.hit_rate() {
        Some(r) => {
            lines.push(gauge("hit rate", r, format!("{:.1}%", r * 100.0), bar_width, true))
        }
        None => lines.push(field("hit rate", "n/a (no reads yet)")),
    }
    lines.push(number(
        "keyspace hits/misses",
        format!(
            "{} / {}",
            info.get_u64("keyspace_hits").map(format::count).unwrap_or_else(|| "-".into()),
            info.get_u64("keyspace_misses").map(format::count).unwrap_or_else(|| "-".into())
        ),
    ));

    lines.push(Line::raw(""));
    lines.push(heading("clients & pressure"));
    lines.push(number("connected", g("connected_clients")));
    // Blocked clients are *not* a warning sign. Any BullMQ worker parked on `BZPOPMIN`
    // counts here, so a healthy queue system sits permanently non-zero.
    lines.push(number(
        "blocked",
        info.get_u64("blocked_clients").map(format::count).unwrap_or_else(|| "-".into()),
    ));
    lines.push(pressure("rejected connections", info.get_u64("rejected_connections")));
    lines.push(pressure("evicted keys", info.get_u64("evicted_keys")));
    lines.push(number(
        "expired keys",
        info.get_u64("expired_keys").map(format::count).unwrap_or_else(|| "-".into()),
    ));

    lines.push(Line::raw(""));
    lines.push(heading("persistence & replication"));
    lines.push(field("aof enabled", g("aof_enabled")));
    lines.push(field("last bgsave", g("rdb_last_bgsave_status")));
    lines.push(field("connected replicas", g("connected_slaves")));
    if info.get("master_link_status").is_some() {
        lines.push(field("master link", g("master_link_status")));
    }

    // Per-db key counts come as `db0:keys=12,expires=3,avg_ttl=0`.
    let dbs: Vec<_> = info
        .fields
        .iter()
        .filter(|(k, _)| k.starts_with("db") && k[2..].chars().all(|c| c.is_ascii_digit()))
        .collect();
    if !dbs.is_empty() {
        lines.push(Line::raw(""));
        lines.push(heading("keyspace"));
        for (db, stats) in dbs {
            lines.push(field(db, stats.clone()));
        }
    }

    lines
}

pub fn slowlog(app: &App) -> Vec<Line<'static>> {
    if let Some(p) = placeholder(&app.slowlog) {
        return p;
    }
    let entries = app.slowlog.ready().expect("checked above");

    if entries.is_empty() {
        return vec![
            Line::raw(""),
            Line::from(Span::styled("  no slow commands logged", Theme::dim())),
            Line::from(Span::styled(
                "  (raise the threshold with CONFIG SET slowlog-log-slower-than)",
                Theme::dim(),
            )),
        ];
    }

    let mut lines = vec![Line::from(vec![
        Span::styled(format!("  {:<8}", "id"), Theme::dim()),
        Span::styled(format!("{:>12}  ", "duration"), Theme::dim()),
        Span::styled(format!("{:<22}", "client"), Theme::dim()),
        Span::styled("command", Theme::dim()),
    ])];

    for e in entries {
        // Anything over 10ms is worth the eye going to it.
        let style = if e.duration_us >= 10_000 { Theme::error() } else { Theme::warn() };
        let duration = if e.duration_us >= 1000 {
            format!("{:.1}ms", e.duration_us as f64 / 1000.0)
        } else {
            format!("{}µs", e.duration_us)
        };

        let client = if e.client_name.is_empty() {
            e.client_addr.clone()
        } else {
            format!("{} ({})", e.client_addr, e.client_name)
        };

        lines.push(Line::from(vec![
            Span::styled(format!("  {:<8}", e.id), Theme::dim()),
            Span::styled(format!("{duration:>12}  "), style),
            Span::raw(format!("{:<22}", format::truncate(&client, 21))),
            Span::raw(format::truncate(&e.command, 60)),
        ]));
    }

    lines
}

pub fn clients(app: &App) -> Vec<Line<'static>> {
    if let Some(p) = placeholder(&app.clients) {
        return p;
    }
    let list = app.clients.ready().expect("checked above");

    let mut lines = vec![Line::from(vec![
        Span::styled(format!("  {:<8}", "id"), Theme::dim()),
        Span::styled(format!("{:<24}", "addr"), Theme::dim()),
        Span::styled(format!("{:<18}", "name"), Theme::dim()),
        Span::styled(format!("{:>7}", "age"), Theme::dim()),
        Span::styled(format!("{:>7}", "idle"), Theme::dim()),
        Span::styled(format!("{:>4}", "db"), Theme::dim()),
        Span::styled(format!("{:>5}", "sub"), Theme::dim()),
        Span::styled("  cmd", Theme::dim()),
    ])];

    for c in list {
        // A long-idle connection is usually a leak or a forgotten subscriber.
        let idle_style = if c.idle > 300 { Theme::warn() } else { Theme::base() };
        lines.push(Line::from(vec![
            Span::styled(format!("  {:<8}", format::truncate(&c.id, 7)), Theme::dim()),
            Span::raw(format!("{:<24}", format::truncate(&c.addr, 23))),
            Span::styled(format!("{:<18}", format::truncate(&c.name, 17)), Theme::accent()),
            Span::raw(format!("{:>7}", format::ttl(Some(c.age as i64 * 1000)))),
            Span::styled(format!("{:>7}", format::ttl(Some(c.idle as i64 * 1000))), idle_style),
            Span::raw(format!("{:>4}", c.db)),
            Span::raw(format!("{:>5}", c.sub)),
            Span::raw(format!("  {}", format::truncate(&c.cmd, 24))),
        ]));
    }

    lines.push(Line::raw(""));
    lines.push(Line::from(Span::styled(
        format!("  {} connected", list.len()),
        Theme::dim(),
    )));

    lines
}

pub fn cluster(app: &App) -> Vec<Line<'static>> {
    if let Some(p) = placeholder(&app.cluster) {
        return p;
    }
    let t = app.cluster.ready().expect("checked above");

    if !t.enabled {
        return vec![
            Line::raw(""),
            Line::from(Span::styled("  cluster mode is not enabled", Theme::dim())),
            Line::from(Span::styled(
                "  this is a standalone server",
                Theme::dim(),
            )),
        ];
    }

    let state_style = if t.state == "ok" { Theme::ok() } else { Theme::error() };
    let mut lines = vec![
        heading("cluster"),
        Line::from(vec![
            Span::styled(format!("  {:<26}", "state"), Theme::dim()),
            Span::styled(t.state.clone(), state_style),
        ]),
        field("slots assigned", format!("{} / 16384", t.slots_assigned)),
        field("known nodes", t.known_nodes.to_string()),
        field("shards", t.size.to_string()),
        Line::raw(""),
        heading("nodes"),
    ];

    for n in &t.nodes {
        let role = if n.master { "master" } else { "replica" };
        let role_style = if n.master { Theme::accent() } else { Theme::dim() };
        let link_style = if n.link_state == "connected" { Theme::ok() } else { Theme::error() };

        lines.push(Line::from(vec![
            Span::raw("  "),
            Span::styled(if n.myself { "* " } else { "  " }, Theme::title()),
            Span::raw(format!("{:<24}", format::truncate(&n.addr, 23))),
            Span::styled(format!("{role:<9}"), role_style),
            Span::styled(format!("{:<11}", n.link_state), link_style),
            Span::raw(n.slots.join(" ")),
        ]));
    }

    lines
}

pub fn pubsub(app: &App) -> Vec<Line<'static>> {
    if let Some(p) = placeholder(&app.pubsub) {
        return p;
    }
    let channels = app.pubsub.ready().expect("checked above");

    if channels.is_empty() {
        return vec![
            Line::raw(""),
            Line::from(Span::styled("  no active channels", Theme::dim())),
            Line::from(Span::styled(
                "  PUBSUB only lists channels with at least one subscriber",
                Theme::dim(),
            )),
        ];
    }

    let mut lines = vec![Line::from(vec![
        Span::styled(format!("  {:<52}", "channel"), Theme::dim()),
        Span::styled("subscribers", Theme::dim()),
    ])];

    for c in channels {
        lines.push(Line::from(vec![
            Span::raw(format!("  {:<52}", format::truncate(&c.name, 51))),
            Span::styled(c.subscribers.to_string(), Theme::accent()),
        ]));
    }

    lines
}
