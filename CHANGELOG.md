# Changelog

All notable changes to keylens are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.7] — 2026-09-05

Security and correctness pass over untrusted input, plus the toolchain gates that catch it.

### Fixed

- URL redaction no longer leaks passwords containing `/`, `?` or `#`. Managed-host
  passwords are base64 and routinely contain them; `keylens connections` and `keylens probe`
  print stored URLs without parsing them first, so these were echoed in full.
- Redaction now fails closed: an ambiguous URL loses its whole userinfo rather than part of it.
- BullMQ job paging can no longer wrap a large offset or limit into a negative Redis bound,
  which reads as "to the end of the collection" — the unbounded read the function exists to
  prevent. Same fix in the ranged value read.
- Durations built from server-supplied seconds are checked, not cast. An oversized `INFO` or
  `CLIENT LIST` field panicked debug builds and rendered a negative duration in release.
- Job timestamp subtraction is checked rather than trusting the ordering guard alone.
- A malformed config file is reported instead of being read as an empty one, which had the
  tool claiming no config file existed while naming the path to the user's own.
- Unknown config keys are rejected, so a misspelled `prefix` fails loudly instead of
  silently scanning the wrong keyspace.
- Missing-connection errors now distinguish no config file, an empty one, and an
  unresolvable config directory.

### Changed

- `CONFIG GET` is restricted to named non-secret parameters. It previously accepted any
  parameter, including `requirepass` and a `*` glob.
- The read-only allowlist is documented as a discipline boundary rather than a sandbox;
  lenses are compiled in and could bypass it.
- Job hash fields are capped on arrival, bounding what is retained and rendered. Redis has
  no ranged hash read, so the transfer itself is still unbounded.
- Pane rendering takes its value and its placeholder from one exhaustive match, removing six
  hand-asserted invariants from the render path.
- `overflow-checks` is enabled for release builds, so a wrong number surfaces instead of
  being displayed with confidence.
- Dependencies updated for an unsound `lru` (RUSTSEC-2026-0253) and a yanked `bytesize`.

### Added

- Workspace lint policy: `unsafe_code` forbidden, and `unwrap`/`expect`/`panic`/`todo`
  denied outside tests.
- `cargo-deny` configuration and a CI job for advisories, licences, banned crates and yanked
  versions, on a weekly schedule as well as on push.
- The toolchain is pinned rather than floating on `stable`, which had left the clippy gate
  failing. The MSRV job verifies it is still testing the declared floor.
- First doc examples in the library crates; doc tests now execute.

## [0.1.6] — 2026-08-17

Safety, Redis Cluster correctness, and roadmap accuracy following a full code review.

### Added

- Redis Cluster scans cover every primary, and arbitrary-key batches route correctly across
  hash slots. Multi-queue event streams report an explicit unavailable state when their keys
  cannot share one slot.
- The public connection command surface is read-only by construction; raw mutating commands
  exist only behind a live-test feature.
- CI checks the declared Rust 1.90 minimum supported version.

### Changed

- Servers without `GETRANGE`, `HSCAN` or `SSCAN` show an explicit unsupported state. The
  former measure-then-read fallback was racy.
- Type filters fall back to bounded client-side `TYPE` batches when `SCAN … TYPE` is absent.
- Relicensed to dual `MIT OR Apache-2.0`. Strictly more permissive than 0.1.5, so no
  existing user is affected.
- The four library crates carry crates.io metadata; they were published unsearchable and
  rendering blank.
- Lens documentation distinguishes the public extension point from the host-integrated UI.
- README restructured: badges, contents, comparison table, FAQ.

### Fixed

- Passwords in Redis URLs are redacted from the TUI, probe output, connection listing and
  TLS guidance.
- Rejected worker requests no longer leave panes permanently on `loading…`.
- The README no longer claims a demo GIF that was never committed.

## [0.1.5] — 2026-08-08

No behaviour change. The compatibility claims and their tests described a Recached that
shipped three releases earlier.

### Fixed

- Two live tests asserted a *dependency's missing features*, so they failed when the server
  improved. They now assert the property that holds on either path.
- A live test depended on a key an earlier test left behind, and failed against a fresh
  container. It seeds and cleans up its own key.

### Changed

- The compatibility matrix matches Recached 0.3.2, and separates commands that fail
  differently instead of collapsing them into one row.
- The README no longer calls the Recached image a private package, and both it and the
  compose file record why the fixture pin is deliberate.

## [0.1.4] — 2026-08-01

Browsing a distant server is usable, and a dead connection says so instead of hanging.
Measured against a 35ms droplet and a 390ms managed Valkey cluster with heavy packet loss.

### Fixed

- A command could be awaited forever: no default command timeout and no reconnect policy, so
  one stuck `await` stalled every key selected afterwards. Commands are bounded, the client
  reconnects with backoff, and half-open sockets are detected.
- `TCP_NODELAY` was never set, adding Nagle-plus-delayed-ACK latency to every command. TCP
  keepalive is set too.

### Changed

- Reading a key takes one round trip instead of three, by speculating across every type and
  keeping the pair the type justifies.
- Selecting a key no longer queues behind a scan; reads run on their own task and superseded
  reads are cancelled.
- The stats refresh and selection debounce pace themselves by measured round-trip latency.

## [0.1.3] — 2026-08-01

Connecting to a distant server works, and you can watch it happen.

### Changed

- The browser draws before it connects, showing progress and rendering failures in place.
  The deadline is now only a backstop.
- Recached is detected as its own vendor.

### Fixed

- The client's per-step timeouts were too tight for a remote server and killed healthy
  connections before the caller's own deadline applied.
- The capability probe ran eleven serial commands, which blew the connect deadline on a
  distant host. It is one pipeline, with a serial fallback for clusters.
- `GETRANGE` and `HSCAN`/`SSCAN` were never probed, so servers that support them took the
  fallback path. A test now asserts every capability has a non-mutating probe.
- BullMQ detection scans 4,000 keys per page instead of 500.
- A handshake timeout over `rediss://` lists what to check instead of guessing.

## [0.1.2] — 2026-08-01

### Fixed

- Connecting to a TLS-only port with `redis://` could hang forever. The handshake now has a
  deadline and reports what happened.
- The bare `Protocol Error: Expected string` is replaced with an explanation and a corrected
  `rediss://` URL ready to paste. Offered only for plaintext URLs.

## [0.1.1] — 2026-08-01

Documentation and presentation. No behaviour changes.

### Added

- Full installation and usage sections: every install path, URL form, command, key binding
  and environment variable.
- `docs/demo.tape`, so the demo is rendered from a script.

### Changed

- Described as a TUI for Redis, Valkey and Recached throughout.
- `--name` help points at `keylens connections` instead of a hardcoded Linux path.
- The splash falls back to a short tagline on narrow terminals.

## [0.1.0] — 2026-08-01

First release. **Read-only by construction** — safe to point at production.

- Cursor-paged `SCAN` key tree, auto-split on `:` with single-child chain folding, plus
  server-side pattern and type filters.
- Viewers for string, hash, list, set, zset and stream, with JSON pretty-printing and key
  metadata.
- `INFO` dashboard, slowlog, client list, cluster topology and pub/sub panes. Host-blocked
  commands render as unavailable with a reason rather than as errors.
- BullMQ lens: queue table with pipelined per-state counts, correct paused detection,
  queue → state → job drill-down, per-attempt stack traces, and live throughput from the
  events stream via a single blocking `XREAD`.
- Stream consumer-group detail ranked worst-first, with a nil `lag` shown as unknown.
- Every capability probed at connect; a server without `INFO` still browses. Verified
  against Redis 8, Valkey 8 and Recached, with six vendors distinguished at runtime.
- `KEYS`, `FLUSHALL`, `FLUSHDB`, `MONITOR`, `DEBUG`, `HGETALL` and `SMEMBERS` are never
  issued; a workspace test fails the build if any appears in source.
- Themed splash and palette using indexed ANSI, `NO_COLOR` honoured, `keylens probe` and
  `keylens connections`, and logs that never paint over the rendered frame.
