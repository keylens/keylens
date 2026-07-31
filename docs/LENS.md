# Writing a lens

A lens teaches keylens what a family of keys *means*. `bull:emails:failed` is a ZSET to
`redis-cli`; to the BullMQ lens it's a dead-letter queue with retryable jobs and stack
traces.

A lens is exactly three things:

1. a **detector** — a cheap probe that says "this keyspace looks like X",
2. a **model** — the domain objects that pattern implies,
3. a **view** — how to render them (UI layer, keyed by lens id).

You can add one without touching core. That's the point.

## The trait

```rust
#[async_trait]
pub trait Lens: Send + Sync {
    fn id(&self) -> &'static str;
    fn name(&self) -> &'static str;
    async fn detect(&self, conn: &Conn) -> Result<Option<Detection>>;
}
```

## Detector rules

These are hard requirements, not style preferences. Detection runs on **every connect**,
often against production.

- **Never issue `KEYS`.** Use `Conn::scan_page` with a bounded `COUNT` and a page cap. A
  workspace test (`no_dangerous_commands.rs`) fails the build if `KEYS` appears in source.
- **Bound your work.** The BullMQ lens caps at 40 pages / 500 queues. A keyspace with 50M
  unrelated keys must not make connecting slow.
- **Check capabilities.** Managed hosts (Upstash, ElastiCache, MemoryDB) block subsets of
  `CONFIG`, `CLIENT`, `MEMORY`, `MONITOR`, `DEBUG`. Ask `conn.capabilities()` before
  relying on a command; degrade rather than error.
- **`Ok(None)` means "not present."** Reserve `Err` for genuine failures. A detector that
  errors is logged and skipped — a broken lens must never stop someone from connecting,
  because the general browser works fine without any lens.

## Confidence

Report honestly. The UI shows this, so the user can see when we're guessing.

| Level | Meaning |
|---|---|
| `Weak` | Shape matches, but so would other things. Offer it; don't auto-open. |
| `Likely` | Structure is distinctive enough to name. |
| `Certain` | Version markers or unambiguous keys present. |

The BullMQ lens returns `Certain` only when it reads `bullmq:<version>` out of the queue's
`meta` hash — otherwise `Weak`, because an older Bull v3 keyspace looks similar.

## Version detection is not optional

Upstream libraries change their key layout between majors, and a lens that assumes a
version silently reports wrong numbers. Two real examples from BullMQ:

- v4 introduced `prioritized` (a ZSET); v3 used `priority`.
- Pausing **does not** rename `wait` → `paused`. It sets `meta.paused = 1`. A lens that
  reads the legacy `paused` LIST reports paused queues as running.

Detect the version from the keyspace, record it on the `Detection`, and branch on it.

## Registering

```rust
let mut registry = Registry::new();
registry.register(Arc::new(BullMqLens::default()));
let detections = registry.detect_all(&conn).await; // strongest confidence first
```

## Testing

Pure key-layout logic (name parsing, key building, state→command mapping) should be unit
tested with no Redis. Anything touching a live server belongs in the integration harness
so it runs across the whole vendor matrix — Redis, Valkey, Dragonfly.

If your lens targets a library with its own test fixtures, drive the **real library** in
`fixtures/` and assert your model matches what that library reports. That's what catches
upstream layout drift on release day instead of in a bug report.
