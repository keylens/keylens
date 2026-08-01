# Changelog

All notable changes to keylens are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [0.1.3] — 2026-08-01

Connecting to a distant server works, and you can watch it happen.

### Changed

- **The browser draws before it connects.** The terminal comes up immediately and shows
  `connecting… 4s (q to cancel)`, and a failure is rendered in place rather than dumped to
  a shell that has already been restored. Connecting behind a blank terminal is what
  forced a tight deadline in the first place: with nothing on screen, a slow link and a
  hang look identical. The deadline is now only a backstop against a black-holed
  connection — 60s for the browser, cancellable at any point.

### Fixed

- **The client's own per-step timeouts were too tight for a remote server.** They were set
  to 5s, but the client's handshake alone is four round trips — at 1.4s round trip, an
  ordinary managed database on another continent, that exceeds 5s and a healthy connection
  was killed before keylens's own deadline was ever consulted. Raised to 30s; the overall
  bound belongs to the caller, not to each step.
- **Connecting to a remote server timed out before the UI could open.** The capability
  probe ran eleven commands one after another. Against a managed host ~1.4s away that is
  fifteen seconds of blank screen — long enough to look like a hang, and long enough to
  blow the connect deadline outright, which is exactly what happened against a
  DigitalOcean managed Valkey. The probes are independent reads, so they now go in a
  single pipeline: one round trip instead of eleven, and it falls back to serial calls if
  a cluster rejects the pipeline.
- **`GETRANGE` and `HSCAN`/`SSCAN` were never actually probed.** Both capabilities existed
  but nothing tested for them, so they always read as unsupported and keylens took the
  measure-then-read-whole fallback on servers that support the cursor variants perfectly
  well — a hash over 200 fields reported "too large" on plain Redis. A test now asserts
  that every capability in `Feature::ALL` has a probe, and that no probe is a mutating
  command.
- BullMQ lens detection scanned the keyspace 500 keys at a time, which is eight round
  trips on a modest keyspace and forty on a large one. `COUNT` is nearly free for the
  server, so it now scans 4,000 at a time — one round trip instead of eight, measured.
- A handshake timeout over `rediss://` no longer claims to know what went wrong. It lists
  what to check in order — IP allowlisting (DigitalOcean's "Trusted Sources"), password
  url-encoding, and a `redis-cli` command to isolate the problem — plus how to capture a
  trace.

### Changed

- Recached is detected as its own vendor, and the fixture-backed tests assert what keylens
  does with whatever the server offers rather than pinning a dependency's missing feature.
  Recached 0.2.3 added `INFO`, and a test written as "Recached has no INFO" would have
  started failing the moment it improved.

## [0.1.2] — 2026-08-01

### Fixed

- **Connecting to a TLS-only port with `redis://` could hang forever.** The connection is
  accepted and then closed, after which the client queued commands and retried in the
  background instead of failing them — so `INFO` never returned and keylens sat there with
  no error at all. The whole handshake now has a 10s deadline and reports what happened.
- Where the same mistake produced an error rather than a hang, it was `Protocol Error:
  Expected string`, which gave no clue what was wrong. Both paths now explain that the
  port requires TLS and print the corrected `rediss://` url ready to paste. This is the
  first thing anyone hits pointing keylens at DigitalOcean, Aiven, Upstash or ElastiCache
  with encryption in transit. The hint is offered only for plaintext urls — over
  `rediss://` these failures mean something else, and guessing would send you the wrong
  way.

## [0.1.1] — 2026-08-01

Documentation and presentation. No behaviour changes to the browser, the lens system, or
anything that talks to a server.

### Added

- A full **Installation** section: Homebrew, the install script and its `VERSION` /
  `INSTALL_DIR` knobs, `cargo install`, per-platform archives, checksum verification, and
  building from source.
- A full **Usage** section: connection URL forms, named connections with the config path
  for each platform, every command, a complete key reference per view, and the
  environment variables.
- `docs/demo.tape` — the README demo is rendered from a script, so it stays honest as the
  UI changes.

### Changed

- keylens is described as a TUI for **Redis, Valkey and Recached** throughout, matching
  what v0.1.0 actually supports.
- `--name` help no longer hardcodes a Linux config path that is wrong on macOS and
  Windows; it points at `keylens connections`, which prints the real one.
- The splash falls back to a short tagline on terminals narrower than the full line. The
  splash does not wrap, so the longer three-vendor tagline would otherwise be cut
  mid-word.

## [0.1.0] — 2026-08-01

First release. **Read-only by construction** — safe to point at production.

### Key browser

- Cursor-paged `SCAN` tree, auto-split on `:`, with single-child chain folding so
  BullMQ's `bull:q:42` + `bull:q:42:logs` shape doesn't turn every job id into a folder
  holding one thing.
- Server-side pattern filter (`/`) and type filter (`t`).
- Viewers for string, hash, list, set, zset and stream, with JSON pretty-printing.
- Key metadata: type, TTL, element count, and `MEMORY USAGE` where the host permits it.

### Server panes

- `INFO` dashboard with meters for memory and hit rate — meters appear only when there's
  a real denominator, so an unset `maxmemory` shows a number rather than a fabricated bar.
- Slowlog, client list, cluster topology and pub/sub channels.
- Commands blocked by a managed host render as *unavailable on this server* with the
  reason, not as an error.

### BullMQ lens

- The `queues` tab appears only when a lens matches the keyspace.
- Queue table with per-state counts fetched in a single pipelined round trip, and paused
  state read from `meta.paused` — current BullMQ does not rename `wait` to `paused`.
- Drill-down from queue → state → job, with `[`/`]` to cycle job state.
- Job detail with a stack trace **per attempt**, attempts made/allowed, wait and run
  durations, payload, opts and logs.
- **Live throughput** from the events stream via a single blocking `XREAD` across every
  queue — per-queue sparklines and events/sec at sub-second resolution, without polling.

### Streams

- `XINFO STREAM`/`GROUPS`/`CONSUMERS` plus the `XPENDING` summary, rendered above the
  entries: the question is which consumer stopped acknowledging, not what's in the stream.
- Consumers ranked worst-first with a stalled flag; a nil `lag` shows as `unknown` rather
  than `0`, because Redis genuinely cannot compute it after trimming.

### Compatibility

- Every capability is probed at connect. A server that lacks `INFO` still connects and
  browses — the stats pane says so instead of the whole connection failing.
- Verified against Redis 8, Valkey 8 and Recached. Where a server has no `HSCAN`/`SSCAN`/
  `GETRANGE`, keylens measures the key with `HLEN`/`SCARD`/`STRLEN` first and reads it
  whole only when small; an oversized key reports that rather than being fetched.
- Vendors distinguished at runtime: Redis, Valkey, Dragonfly, KeyDB, Garnet, Recached.

### Safety

- `KEYS`, `FLUSHALL`, `FLUSHDB`, `MONITOR`, `DEBUG`, `HGETALL` and `SMEMBERS` are never
  issued. A workspace test fails the build if any appears in source — an unbounded
  collection read blocks a production server exactly as hard as `KEYS` does.

### Interface

- Block-letter splash, magenta→cyan palette using indexed ANSI so it inherits your
  terminal theme, chips for vendor and active filters, rounded panels.
- `NO_COLOR` and `--no-color` honoured, including in error output. Colour is stripped;
  bold and dim are kept, so the layout stays readable.
- `keylens probe` for a non-interactive capability and queue report.
- `keylens connections` lists named connections from the config file, with passwords
  masked.
- In the browser, logs go to `KEYLENS_LOG_FILE` or nowhere — never to stderr, which would
  paint over the rendered frame.

[Unreleased]: https://github.com/keylens/keylens/compare/v0.1.3...HEAD
[0.1.3]: https://github.com/keylens/keylens/releases/tag/v0.1.3
[0.1.2]: https://github.com/keylens/keylens/releases/tag/v0.1.2
[0.1.1]: https://github.com/keylens/keylens/releases/tag/v0.1.1
[0.1.0]: https://github.com/keylens/keylens/releases/tag/v0.1.0
