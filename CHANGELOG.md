# Changelog

All notable changes to keylens are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [0.1.5] — 2026-08-08

No behaviour change. The compatibility claims and the tests behind them were describing a
Recached that shipped three releases ago.

### Fixed

- **Two live tests asserted a dependency's *missing* features.** `reads_values_without_
  hscan_sscan_or_getrange` opened with `assert!(!has(GetRange))` and
  `assert!(!has(CursorCollectionScan))`, and the oversized-hash test required the refusal
  that only the no-`HSCAN` path produces. Both passed solely because the compose profile
  pins Recached 0.2.3 — the release before those commands landed — so pointing
  `KEYLENS_TEST_RECACHED_URL` at anything current failed a test about keylens because a
  server got better. This is the failure the `INFO` test one screen up already warned
  about in a comment, committed in the same file.

  They now assert the property that holds on either path: values read back correctly
  whichever branch the capabilities select, and an oversized hash is bounded by `HSCAN`'s
  `COUNT` where that exists and declined where it doesn't — never `HGETALL`. Which path
  ran is printed, not required. Verified against both 0.2.3 and 0.3.2 containers; each
  takes a different branch and both pass.

- **A live test depended on a key an earlier test forgot to delete.** `connects_and_
  browses_whatever_info_the_server_offers` ended in `assert!(!keys.is_empty())`, but
  nothing seeds the Recached fixture — the BullMQ producer only feeds Redis. It passed on
  whatever the previously-run test left behind, and failed outright against a freshly
  started container, which is exactly how anyone runs it the first time. It seeds and
  cleans up its own key now.

### Changed

- **The compatibility matrix matches Recached 0.3.2.** It claimed Recached had no
  `PUBSUB CHANNELS`, `MEMORY USAGE` or `MODULE LIST`; all three shipped in Recached 0.3.0,
  so the pub/sub pane, the memory breakdown and the module check work there now. `SLOWLOG`
  and `CLUSTER` were also collapsed into one "not implemented" row despite failing
  differently — `SLOWLOG` is an unknown command, while `CLUSTER` is refused with Redis's
  own `ERR This instance has cluster support disabled`, which `classify()` deliberately
  reports as the server's own sentence rather than as a missing command. Separate rows.
  The column is now the output of `keylens probe` against `v0.3.2`, and says so, since
  `docs/COMPAT.md` — the generated version that would stop this recurring — still doesn't
  exist.

- **The README no longer calls the Recached image a private package.** It is a public GHCR
  package and needs no `docker login`; `docker-compose.yml` has said so in a comment the
  whole time, two files away. The profile exists so the ordinary two-server fixture doesn't
  pull a third image, which is the real reason. Both files now also record that the `v0.2.3`
  pin is deliberate — the client-side fallback needs a server that actually lacks the
  cursor reads — and that the fixture therefore isn't what the compatibility table
  describes.

---

## [0.1.4] — 2026-08-01

Browsing a distant server is usable, and a dead connection says so instead of hanging.

Measured against two DigitalOcean endpoints from the same machine: a droplet at 35ms with
no loss, and a managed Valkey cluster at 390ms with heavy packet loss (PING averaged 1173ms
and tailed to 9753ms). Everything below is aimed at the second one.

### Fixed

- **A command could be awaited forever.** fred defaults `default_command_timeout` to zero,
  which means no timeout at all, and `Builder::from_config` leaves the reconnect policy
  unset — so a connection dropped by the load balancer in front of a managed database
  stayed dropped, and the request sent into it never returned. Because the worker handled
  requests one at a time, that single stuck `await` stalled every key selected afterwards:
  the value pane sat on `loading…` and never moved. Commands are now bounded at 20s, the
  client reconnects with exponential backoff, and a half-open socket is detected rather
  than waited on.
- **`TCP_NODELAY` was never set.** Nagle held small writes waiting for more to coalesce
  while the peer's delayed ACK held the reply that would release them. A request/response
  protocol has nothing to coalesce, so this was pure added latency on every command. TCP
  keepalive is now set too, so a connection dropped by a NAT is found by the kernel rather
  than by the user's next keypress.

### Changed

- **Reading a key takes one round trip, down from three.** Both the size command and the
  read command depend on the key's type, so they used to wait for `TYPE` to come back.
  keylens now asks for every type's size and value at once and keeps the pair the type
  turns out to justify — the five wrong ones fail with `WRONGTYPE`, which Redis rejects on
  the type check before doing any work. At 35ms this saves 70ms nobody notices; at 390ms
  it is the difference between a key opening in 0.4s and in 1.2s, on every keypress.
- **Selecting a key no longer queues behind a scan.** `Rescan` and `Detect` each walk up to
  40 sequential `SCAN` pages — sixteen seconds at 390ms — and every keypress made during
  that wait used to sit in line behind them. Key reads now run on their own task over the
  same multiplexed connection, and a superseded read is cancelled rather than left to spend
  round trips on a reply the UI has already decided to discard.
- **The stats refresh paces itself by distance.** A single timed `PING` at connect sizes
  both the `INFO` tick and the selection debounce. A fixed 5s tick spent a fifth of a
  390ms connection on stats nobody was looking at; refreshes now spread out as the server
  gets further away, and are skipped entirely while the previous one is still outstanding.


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
