//! `keylens probe` -- connect, detect, and report what will and won't work here.

use std::sync::Arc;

use color_eyre::Result;
use keylens_bullmq::{BullMqLens, State};
use keylens_conn::{Conn, Feature};
use keylens_lens::Registry;

pub async fn run(url: &str, show_queues: bool) -> Result<()> {
    println!("connecting to {url}");
    let conn = Conn::connect(url, "probe").await?;
    let server = conn.server();

    println!();
    println!("server");
    println!("  vendor      {}", server.vendor.label());
    println!("  version     {}", server.version);
    println!("  mode        {}", server.mode);
    if let Some(mem) = server.get("used_memory_human") {
        println!("  memory      {mem}");
    }
    if let Some(clients) = server.get("connected_clients") {
        println!("  clients     {clients}");
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
        let mark = if availability.is_available() { "ok  " } else { "--  " };
        match availability.reason() {
            None => println!("  {mark}{:<16}", feature.label()),
            Some(why) => println!(
                "  {mark}{:<16} {} ({})",
                feature.label(),
                why,
                feature.affects()
            ),
        }
    }
    if !caps.modules.is_empty() {
        println!("  modules: {}", caps.modules.join(", "));
    }

    println!();
    println!("lenses");
    let mut registry = Registry::new();
    registry.register(Arc::new(BullMqLens::default()));

    let detections = registry.detect_all(&conn).await;
    if detections.is_empty() {
        println!("  none detected -- the general browser still applies");
    }
    for d in &detections {
        println!("  {:<10} {:?}  {}", d.lens_id, d.confidence, d.summary);
    }

    if show_queues {
        if let Some(d) = detections.iter().find(|d| d.lens_id == "bullmq") {
            let lens = BullMqLens::new(d.prefix.clone());
            println!();
            println!("bullmq queues (prefix `{}`)", d.prefix);
            print_queue_table(&lens, &conn).await?;
        } else {
            println!();
            println!("bullmq queues: none detected");
        }
    }

    Ok(())
}

async fn print_queue_table(lens: &BullMqLens, conn: &Conn) -> Result<()> {
    let queues = lens.all_queues(conn).await?;
    if queues.is_empty() {
        println!("  (no queues)");
        return Ok(());
    }

    let name_w = queues.iter().map(|q| q.name.len()).max().unwrap_or(5).max(5);

    print!("  {:<name_w$}  {:>7}", "queue", "status");
    for s in State::ALL {
        print!("  {:>9}", s.label());
    }
    println!();

    for q in &queues {
        let status = if q.paused { "paused" } else { "running" };
        print!("  {:<name_w$}  {:>7}", q.name, status);
        for s in State::ALL {
            print!("  {:>9}", q.count(s));
        }
        println!();
    }

    Ok(())
}
