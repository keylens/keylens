# Changelog

All notable changes to keylens are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.8] — 2026-11-17

Correctness, reliability and security hardening. No new features: every change makes an
existing view more truthful, an existing failure recoverable, or an existing surface
harder to misuse. Each was reproduced against 0.1.7 before it was fixed; see
[docs/releases/0.1.8.md](docs/releases/0.1.8.md) for the evidence and the release-gate
report.

### Fixed

- **Stale replies rendered under the wrong item.** A job detail had no identity (open job
  A, then B: A's reply rendered under B); a job page was matched on its state only, so one
  queue's `failed` page could render under another's; an old key's error, or a failed
  `INFO` refresh, replaced the selected key's value. Every request now carries an id and
  only the newest reply of each kind is applied.
- **Unknown rendered as zero.** A failed `LLEN`/`ZCARD` showed "0 waiting", an unread meta
  hash showed "running", a failed size showed `size 0`, and a negative `XINFO` count
  wrapped to 18 quintillion. Unknown values now render as `?`.
- **A failed scan reported itself complete**, after dropping the keys it had gathered.
  Failure is now permanent for that scan, keeps its keys, and says "incomplete".
- **Partial values looked whole.** The first bounded page of a value is labelled
  ("showing 200 of 5,000"); `HSCAN`/`SSCAN` coverage comes from the cursor and the page
  cap; an exactly-64 KiB string is no longer marked truncated.
- **"Key no longer exists" for keys that exist**: under a key-pattern ACL, `TYPE` is denied
  and was read as type `none`. A key that changes type mid-read now says so.
- **A half-open connection wedged the browser.** A `SCAN` page had no response timeout
  (still waiting after 60s, measured), and the worker serves requests in order. Pages are
  now bounded by the command timeout.
- **Live throughput died for the session after an outage** (0.1.7 gave up after ~115s of
  outage and never resumed). The events reader now reconnects with jittered backoff capped
  at 30s, shows "reconnecting", and stops at once only on a denial (`NOPERM`, wrong
  password, unknown command).
- **A rotated password produced endless "Request timed out"** while the client retried the
  stale password forever. keylens now stops reconnecting and says the credentials were
  rejected.
- **Sentinel passwords were printed in clear** by `probe`, `connections` and the status
  bar (`?sentinelPassword=`). Credential-like query parameters are masked.
- **Terminal escape injection** through `keylens probe --queues`: a queue (key) name
  containing `ESC ] 0 ; … BEL` retitled the terminal and `ESC [ 2 J` cleared it. Control
  and bidi characters from the server are now printed as visible escapes.
- **The installer installed a tampered archive.** `install.sh` now verifies the archive
  against `checksums.txt` and fails closed.
- Stream group detail took two sequential round trips per group (42 for 20 groups); it is
  now one pipeline (3 round trips).
- Keys whose names are not valid UTF-8 were dropped without a trace; they are now counted
  in the status bar (they still cannot be opened).

### Changed

- The connection layer refuses read-only commands with unbounded arguments
  (`LRANGE k 0 -1`, `XRANGE` without `COUNT`, `SLOWLOG GET -1`, `SCAN COUNT 10⁸`,
  `MEMORY USAGE … SAMPLES 0`, `XINFO STREAM … FULL`) before they reach the server, with
  `ConnError::Unbounded`. `keylens_conn::bounds` documents, per command, what is bounded on
  the wire and what cannot be.
- What keylens keeps is bounded: 16 KiB per element, 128 fields per stream entry, 1,000
  rows (with the true total) for `CLIENT LIST`, `PUBSUB CHANNELS` and consumers, 500,000
  keys in the tree.
- `KEYLENS_LOG_FILE` is created owner-only (`0600`) and a symlinked path is refused. A
  config file holding passwords that others can read, and a password in `--url`, each get
  a one-line warning.
- CI and release: read-only default token, every action pinned to a commit SHA (with
  Dependabot), no persisted checkout credentials, the release tag passed via `env`,
  actionlint from a checksum-verified release, `--locked` builds, and signed build
  provenance for release archives (`gh attestation verify`).
- Dependencies updated for RUSTSEC-2026-0285, -0346, -0348, -0349 (rustls), -0382
  (rustls-webpki) and -0386 (tokio), which had failed the weekly supply-chain job since
  2026-09-21.
- The Sentinel url example in the README was wrong; it is
  `redis-sentinel://host:26379/0?sentinelServiceName=mymaster`.

### Added

- Test suites against real servers: `faults` (a proxy that blackholes, refuses, severs or
  delays), `adversarial` (hostile names, huge values, thousands of consumers and channels,
  malformed BullMQ records), and `topology` (Redis Cluster and Sentinel), all in CI, plus
  both Recached read paths. Compose gains the `topology` profile and Recached 0.5.2, and
  the Recached image path is fixed (`ghcr.io/recached-sh`).
- `cargo run --example bench` and `bench/run.sh`: per-scenario wall time, server command
  count, retained bytes, peak RSS and CPU, with injectable latency.
- Library: `ConnOptions`/`Conn::connect_with`, `Coverage`/`KeyRead`, `Capped<T>`,
  `Recovery`, `ConnError::{Unbounded, AuthLost}`, `KeyValue::Changed`,
  `EventsStatus::Reconnecting`, `QueueList`. Several public signatures changed to carry
  unknowns (`Option`) and coverage; see the release notes.

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
