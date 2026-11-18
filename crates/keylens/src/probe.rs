//! `keylens probe` -- connect, detect, and report what will and won't work here.

use std::sync::Arc;

use color_eyre::Result;
use keylens_bullmq::{BullMqLens, State};
use keylens_conn::{Conn, Feature, redact_url};
use keylens_lens::Registry;
use keylens_ui::format::terminal_safe as safe;

use crate::config::Target;

pub async fn run(target: &Target, show_queues: bool) -> Result<()> {
    let url = &target.url;
    println!("connecting to {}", safe(&redact_url(url)));
    let conn = Conn::connect(url, "probe").await?;
    let server = conn.server();

    println!();
    println!("server");
    println!("  vendor      {}", server.vendor.label());
    println!("  version     {}", safe(&server.version));
    println!("  mode        {}", safe(&server.mode));
    if let Some(mem) = server.get("used_memory_human") {
        println!("  memory      {}", safe(mem));
    }
    if let Some(clients) = server.get("connected_clients") {
        println!("  clients     {}", safe(clients));
    }
    match server.hit_rate() {
        Some(r) => println!("  hit rate    {:.1}%", r * 100.0),
        None => println!("  hit rate    n/a (no reads yet)"),
    }

    println!();
    println!("capabilities");
    let caps = conn.capabilities();
    for feature in Feature::ALL {
        let availability = caps.get(feature);
        let mark = if availability.is_available() {
            "ok  "
        } else {
            "--  "
        };
        match availability.reason() {
            None => println!("  {mark}{:<16}", feature.label()),
            Some(why) => println!(
                "  {mark}{:<16} {} ({})",
                feature.label(),
                safe(why),
                feature.affects()
            ),
        }
    }
    if !caps.modules.is_empty() {
        println!("  modules: {}", safe(&caps.modules.join(", ")));
    }

    println!();
    println!("lenses");
    let mut registry = Registry::new();
    registry.register(Arc::new(match &target.prefix {
        Some(p) => BullMqLens::new(p.clone()),
        None => BullMqLens::default(),
    }));

    let detections = registry.detect_all(&conn).await;
    if detections.is_empty() {
        println!("  none detected -- the general browser still applies");
    }
    for d in &detections {
        println!(
            "  {:<10} {:?}  {}",
            d.lens_id,
            d.confidence,
            safe(&d.summary)
        );
    }

    if show_queues {
        if let Some(d) = detections.iter().find(|d| d.lens_id == "bullmq") {
            let lens = BullMqLens::new(d.prefix.clone());
            println!();
            println!("bullmq queues (prefix `{}`)", safe(&d.prefix));
            print_queue_table(&lens, &conn).await?;
        } else {
            println!();
            println!("bullmq queues: none detected");
        }
    }

    Ok(())
}

async fn print_queue_table(lens: &BullMqLens, conn: &Conn) -> Result<()> {
    let list = lens.all_queues(conn).await?;
    let queues = &list.queues;
    if queues.is_empty() {
        println!("  (no queues)");
        return Ok(());
    }

    let name_w = queues
        .iter()
        .map(|q| safe(&q.name).chars().count())
        .max()
        .unwrap_or(5)
        .max(5);

    print!("  {:<name_w$}  {:>7}", "queue", "status");
    for s in State::ALL {
        print!("  {:>9}", s.label());
    }
    println!();

    for q in queues {
        let status = match q.paused {
            Some(true) => "paused",
            Some(false) => "running",
            None => "?",
        };
        print!("  {:<name_w$}  {:>7}", safe(&q.name), status);
        for s in State::ALL {
            // `?` for a count the server would not give; never a made-up 0.
            print!("  {:>9}", keylens_ui::format::count_or_unknown(q.count(s)));
        }
        println!();
    }
    if !list.complete {
        println!("  (detection stopped early on a large keyspace; more queues may exist)");
    }

    Ok(())
}
