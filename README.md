<h1 align="center">keylens</h1>

<p align="center">
  <strong>A terminal UI for Redis, Valkey and Recached that understands your keys.</strong><br>
  A fast, read-only Redis TUI with a pluggable <strong>lens</strong> system — point it at
  production on day one.
</p>

<p align="center">
  <a href="https://crates.io/crates/keylens"><img alt="keylens on crates.io" src="https://img.shields.io/crates/v/keylens.svg"></a>
  <a href="https://crates.io/crates/keylens"><img alt="Downloads on crates.io" src="https://img.shields.io/crates/d/keylens.svg"></a>
  <a href="https://github.com/keylens/keylens/actions/workflows/ci.yml"><img alt="CI status" src="https://github.com/keylens/keylens/actions/workflows/ci.yml/badge.svg"></a>
  <a href="#license"><img alt="License: MIT OR Apache-2.0" src="https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg"></a>
  <img alt="Runs on macOS, Linux and Windows" src="https://img.shields.io/badge/platform-macOS%20%7C%20Linux%20%7C%20Windows-lightgrey.svg">
  <img alt="Read-only" src="https://img.shields.io/badge/v0.1-read--only-brightgreen.svg">
</p>

<!--
  A rendered demo belongs here, and it is the highest-value thing this README is missing.
  `docs/demo.tape` produces it; see "Demo" below. Once docs/demo.gif is committed:
  <p align="center"><img src="docs/demo.gif" alt="keylens browsing a Redis keyspace and showing live BullMQ queue throughput" width="900"></p>
-->

Every Redis client shows you keys. None of them understand what your keys *mean*.

`bull:emails:failed` is a ZSET to `redis-cli`. It's a dead-letter queue to you.
`celery-task-meta-*` is 4,000 unrelated strings to RedisInsight. Your cache keys are a
namespace with a hit rate and a TTL distribution, not a flat list.

keylens is a general Redis, Valkey and Recached browser with a pluggable **lens** system
on top. A lens detects a known keyspace pattern and renders domain UI instead of raw keys.
BullMQ is the first one.

Every capability is **probed at connect, never assumed**, so keylens works against any
RESP server and tells you plainly what that server can't do. Verified against Redis 8,
Valkey 8, and [Recached](https://github.com/recached-dev/recached) — see
[compatibility](#compatibility).

> **v0.1 is read-only.** That's a feature — you can point it at production on day one.

**keylens** is a single-binary **Redis TUI**, **Valkey TUI** and **Recached TUI** written
in **Rust** — a terminal alternative to `redis-cli`, **RedisInsight** and Redis Desktop
Manager, and a faster read on **BullMQ** queues than **BullBoard**. It browses the
keyspace with `SCAN`
(never `KEYS`), inspects all six value types plus **streams and consumer groups**, shows
an `INFO` dashboard, slowlog, clients, cluster and pub/sub panes, and renders **live job
throughput** from BullMQ's events stream. Works against **Upstash**, **ElastiCache**,
**MemoryDB**, **Aiven**, **DigitalOcean**, **Dragonfly** and **KeyDB** — capabilities are
probed and missing commands degrade instead of erroring. macOS, Linux and Windows.

---

## Contents

- [Why keylens](#why-keylens)
- [Installation](#installation)
- [Usage](#usage) — [quick start](#quick-start) · [connecting](#connecting) · [named connections](#named-connections) · [commands](#commands) · [keys](#keys) · [environment](#environment)
- [What it looks like](#what-it-looks-like)
- [Compatibility](#compatibility)
- [FAQ](#faq)
- [Demo](#demo)
- [Development](#development)
- [Design constraints](#design-constraints)
- [Writing a lens](#writing-a-lens)
- [Roadmap](#roadmap)
- [License](#license)

---

## Why keylens

|  | keylens | `redis-cli` | RedisInsight / Desktop Manager | BullBoard / Taskforce |
|---|---|---|---|---|
| **Runs in** | your terminal, one binary | your terminal | a desktop app or a browser | a web app you deploy |
| **Understands your keyspace** | yes — pluggable lenses | no | no | BullMQ only |
| **Job queue view** | BullMQ, with per-attempt stack traces | no | no | yes |
| **Live throughput** | event-level, from the events stream | no | no | polled counts |
| **Safe on production** | read-only in v0.1, `KEYS` never issued | you can run anything | varies | writes by design |
| **Managed hosts** | probed, degrades per command | manual | partial | n/a |
| **Streams + consumer groups** | group state first, worst consumer ranked | raw `XINFO` | limited | n/a |
| **Install** | one binary, no runtime | bundled with Redis | app install | Node service |
| **Extensible** | write a lens — Sidekiq, Celery, RQ | no | no | no |

Read that as scope, not scoring. `redis-cli` is the right tool for running a command.
BullBoard is the right tool if you want to retry jobs from a web UI. keylens is for the
moment you're staring at an unfamiliar keyspace, or a queue that's backing up, and you
want to understand it from a terminal without touching it.

---

## Installation

A single binary, no runtime, nothing to deploy.

**Install script** — macOS and Linux, detects your platform:

```sh
curl -fsSL https://github.com/keylens/keylens/releases/latest/download/install.sh | bash
```

It installs to `/usr/local/bin` and asks for `sudo` only if that directory isn't writable.
Two knobs:

```sh
VERSION=v0.1.0 curl -fsSL .../install.sh | bash      # pin a release
INSTALL_DIR=~/.local/bin curl -fsSL .../install.sh | bash
```

**Cargo** — any platform with a Rust toolchain:

```sh
cargo install keylens
```

**Homebrew** — not yet. A tap is coming; plain `brew install keylens` additionally needs
homebrew-core, which has a notability bar this project hasn't cleared yet. Use the install
script or `cargo install` meanwhile.

**Manual** — grab an archive from
[releases](https://github.com/keylens/keylens/releases/latest):

| Platform | Asset |
|---|---|
| macOS, Apple Silicon | `keylens-aarch64-apple-darwin.tar.gz` |
| macOS, Intel | `keylens-x86_64-apple-darwin.tar.gz` |
| Linux, x86-64 | `keylens-x86_64-unknown-linux-gnu.tar.gz` |
| Linux, arm64 | `keylens-aarch64-unknown-linux-gnu.tar.gz` |
| Windows | `keylens-x86_64-pc-windows-msvc.zip` |

```sh
tar -xzf keylens-<target>.tar.gz
sudo mv keylens /usr/local/bin/
```

Every release ships a `checksums.txt`. Worth verifying if you're piping to a shell:

```sh
curl -fsSLO https://github.com/keylens/keylens/releases/latest/download/checksums.txt
sha256sum --check --ignore-missing checksums.txt
```

**From source** — needs Rust 1.90+ (edition 2024):

```sh
git clone https://github.com/keylens/keylens
cd keylens
cargo build --release          # binary at target/release/keylens
```

Confirm it works:

```sh
keylens --version
```

## Usage

### Quick start

```sh
keylens                                    # browse redis://127.0.0.1:6379
keylens --url redis://cache.internal:6379  # a specific server
keylens probe                              # non-interactive capability report
```

Point it at an unfamiliar server with `probe` first. It reports the vendor, which panes
will work, and which commands the host blocks — before you're staring at a UI wondering
why something is empty.

### Connecting

`--url` accepts the usual schemes:

```sh
keylens --url redis://127.0.0.1:6379            # plain
keylens --url redis://127.0.0.1:6379/3          # database 3
keylens --url rediss://cache.example.com:6380   # TLS
keylens --url redis://:password@host:6379       # password only
keylens --url redis://user:password@host:6379   # ACL user
keylens --url redis-sentinel://host:26379/mymaster
keylens --url redis-cluster://node1:6379
```

Managed hosts work as-is — DigitalOcean, Aiven, Upstash, ElastiCache, MemoryDB, Dragonfly,
KeyDB. Copy the connection string your provider gives you.

**Most managed hosts are TLS-only, so the scheme is `rediss://`, not `redis://`.** Using
the plaintext scheme against a TLS port is the single most common connection mistake;
keylens detects it and prints the corrected url rather than leaving you with a protocol
error. If your server uses a private CA, point at it with `SSL_CERT_FILE`:

```sh
SSL_CERT_FILE=/path/to/ca.crt keylens --url rediss://redis.internal:6379
```

Since a URL on the command line lands in your shell history and in `ps`, prefer the
environment variable or a named connection for anything with a password:

```sh
export KEYLENS_URL=rediss://user:pass@prod.example.com:6379
keylens
```

### Named connections

Nicer than retyping URLs, and it keeps credentials out of your history.

```sh
keylens connections     # prints the config path for your platform, and any entries
```

| Platform | Config file |
|---|---|
| macOS | `~/Library/Application Support/keylens/config.toml` |
| Linux | `~/.config/keylens/config.toml` |
| Windows | `%APPDATA%\keylens\config.toml` |

```toml
[[connections]]
name = "local"
url  = "redis://127.0.0.1:6379"

[[connections]]
name = "prod"
url  = "rediss://user:pass@prod.example.com:6379"
readonly = true          # reserved for v0.2 mutations; v0.1 is read-only throughout

[[connections]]
name = "queues"
url  = "redis://jobs.internal:6379"
prefix = "bull"          # hint for the BullMQ lens if you use a custom prefix
```

Then:

```sh
keylens --name prod
keylens --name queues probe --queues
```

`keylens connections` masks passwords in its output, since it's exactly the command you
run while screen-sharing.

### Commands

| Command | What it does |
|---|---|
| `keylens` / `keylens browse` | Open the interactive browser |
| `keylens probe` | Vendor, version, capabilities, detected lenses |
| `keylens probe --queues` | Adds a BullMQ queue table with per-state counts |
| `keylens connections` | List named connections and the config path |

### Keys

Number keys switch views. `queues` only appears when a lens matches your keyspace, so the
numbering shifts by one when it does — the tab bar always shows the current mapping.

**Everywhere**

| Key | Action |
|---|---|
| `1`–`7` | Switch view |
| `r` | Reload the current view |
| `?` | Help overlay |
| `q`, `Ctrl-C` | Quit |

**Keys view**

| Key | Action |
|---|---|
| `j` / `k`, `↓` / `↑` | Move |
| `g` / `G` | Top / bottom |
| `Enter`, `Space`, `→`, `l` | Expand a branch, or open a key |
| `h`, `←` | Collapse, or jump to the parent |
| `/` | Filter by pattern — server-side `SCAN MATCH` |
| `Esc` | Clear the filter |
| `t` | Cycle the type filter |
| `m` | Scan more keys |
| `E` / `C` | Expand all / collapse all |
| `Tab` | Switch between tree and value pane |

A bare search term becomes a substring match (`emails` → `*emails*`). Type your own glob
(`bull:*:meta`) and it's used exactly as written.

**Queues view**

| Key | Action |
|---|---|
| `j` / `k` | Move |
| `Enter`, `→`, `l` | Drill in: queues → jobs → job detail |
| `h`, `←`, `Esc` | Back out one level |
| `[` / `]` | Cycle which job state is listed |

**Server panes** (stats, slowlog, clients, cluster, pubsub)

| Key | Action |
|---|---|
| `j` / `k` | Scroll |
| `PgUp` / `PgDn` | Scroll by a page |
| `g` | Back to top |

### Environment

| Variable | Effect |
|---|---|
| `KEYLENS_URL` | Default connection URL |
| `NO_COLOR` | Disable colour (any value). `--no-color` does the same |
| `KEYLENS_LOG` | Log filter, e.g. `debug` or `keylens_conn=trace` |
| `KEYLENS_LOG_FILE` | Where to write logs in the browser |
| `SSL_CERT_FILE` | CA bundle for `rediss://` against a private CA |

Logs never go to stderr while the browser is running — they'd paint over the frame. Set
`KEYLENS_LOG_FILE` to capture them:

```sh
KEYLENS_LOG=debug KEYLENS_LOG_FILE=/tmp/keylens.log keylens
```

`probe` and `connections` log to stderr as usual.

---

## What it looks like

`keylens` with no arguments opens the browser:

```
        ██╗  ██╗███████╗██╗   ██╗██╗     ███████╗███╗   ██╗███████╗
        ██║ ██╔╝██╔════╝╚██╗ ██╔╝██║     ██╔════╝████╗  ██║██╔════╝
        █████╔╝ █████╗   ╚████╔╝ ██║     █████╗  ██╔██╗ ██║███████╗
        ██╔═██╗ ██╔══╝    ╚██╔╝  ██║     ██╔══╝  ██║╚██╗██║╚════██║
        ██║  ██╗███████╗   ██║   ███████╗███████╗██║ ╚████║███████║
        ╚═╝  ╚═╝╚══════╝   ╚═╝   ╚══════╝╚══════╝╚═╝  ╚═══╝╚══════╝

      a TUI for Redis, Valkey and Recached that understands your keys

                    Redis  8.10.0  redis://127.0.0.1:6379
                             scanning keyspace…
                  read-only — safe to point at production
```

Six views, switched with `1`–`6`:

```
 KEYLENS   Redis  8.10.0  standalone  redis://127.0.0.1:6379  match *:meta
  1 keys   2 stats   3 slowlog   4 clients   5 cluster   6 pubsub
╭ 5 keys ─────────────────────────────╮╭ value ──────────────────────────────────╮
│▾ bull (5)                           ││bull:emails:meta                         │
│  · emails:meta hash                 ││ hash   ttl -  size 2  mem 88 B          │
│  · exports:meta hash                ││                                         │
│  · image-processing:meta hash       ││opts.maxLenEvents    10000               │
│  · reports:meta hash                ││version              bullmq:6.0.2        │
│  · webhooks:meta hash               ││                                         │
╰─────────────────────────────────────╯╰─────────────────────────────────────────╯
 j/k move  ↵ open  / filter  t type  m more  r rescan  1-6 view  ? help  q quit
```

The **queues** tab only exists when a lens matched — keylens grows a tab because of what's
in your keyspace, not because a flag was flipped. The sparklines are **live**, driven by
BullMQ's events stream rather than by polling:

```
╭ queues — bullmq 6.0.2 - 5 queues ────────────────────────────────────────────────────────╮
│  queue             status       wait   active     done   failed  last 8s            ev/s│
│                                                                                          │
│▶ emails            running         0        0      300       56  █▄▅▅·▄▇·            8.5│
│  exports           running         0        0      300       69  ▆▇▃▅█▅▅·           11.9│
│  image-processing  running         0        2      300      500  ▃▇▄▇▆▄█▂           10.8│
│  reports           paused      6,389        0      300      146  ·█▇▄▃▅▄·            4.9│
│                                                                                          │
│  5 queues, 1 paused   ● live · 44.1 events/sec across all queues                        │
╰──────────────────────────────────────────────────────────────────────────────────────────╯
```

BullBoard polls `getJobCounts` on a timer, which is why its graphs are coarse. BullMQ
already writes every state transition to a Redis STREAM, so **one blocking `XREAD` across
every queue** gives event-level throughput at sub-second resolution and near-zero server
load. The graph moves the instant a job fails. On Redis Cluster, those event stream keys
must share a hash slot (normally through a shared BullMQ hash-tag prefix); otherwise the
pane explains that live throughput is unavailable instead of issuing an invalid cross-slot
read.

Narrow panes drop columns deliberately — least useful first — rather than clipping the
graph off the right-hand edge.

Drill in for the failed job, and you get the trace for **each attempt**:

```
╭ job 7012 ──────────────────────────────────────────────────────────────────────────╮
│  7012  image-processing                                                            │
│  attempts      2/2                                                                 │
│  waited        1s                                                                  │
│  ran for       378ms                                                               │
│                                                                                    │
│ ▌failure                                                                           │
│  offset is out of bounds: requested 16384, buffer length 1024                       │
│                                                                                    │
│  attempt 1                                                                         │
│  RangeError: offset is out of bounds: requested 16384, buffer length 1024           │
│      at decodeFrame (file:///app/producer.mjs:59:9)                                │
│      at resizeImage (file:///app/producer.mjs:65:10)                               │
╰────────────────────────────────────────────────────────────────────────────────────╯
```

Queue counts are one pipelined round trip for the whole table, not eight commands per
queue — the difference between instant and twenty seconds on a remote server.

Selecting a **stream** leads with consumer-group state, because the operational question
is almost never "what's in it" — it's *which consumer stopped acknowledging*:

```
 ▌stream
  length 1,506  added 1,506  last id 1785518199335-0

 ▌consumer groups
  processors  consumers 2  pending 27  lag 0
    pending range 1785517610863-0 … 1785517615743-0
    ! worker-stuck          pending 27      idle 9m43s
      worker-healthy        pending 0       idle 52ms

 ▌entries
  …
```

Consumers are ranked worst-first, and a nil `lag` renders as `unknown` rather than `0` —
Redis genuinely cannot compute it after trimming, and claiming the group is caught up
would be a lie.

The stats view is an `INFO` dashboard with meters — no extra commands, and the meters only
appear when there's a real denominator (a memory bar means nothing without `maxmemory`):

```
 ▌throughput
  ops/sec                   434
  total commands            1,614,661
  hit rate                  ██████████████████████░░░░░░░░  74.7%
  keyspace hits/misses      443,560 / 150,555
```

Single-child chains fold into one row, so BullMQ's `bull:q:42` + `bull:q:42:logs` shape
doesn't turn every job id into a folder holding one thing.

There's also a non-interactive report:

```
$ keylens probe --queues
```

```
server
  vendor      Redis
  version     8.10.0
  mode        standalone
  hit rate    89.6%

capabilities
  ok  CONFIG
  ok  SLOWLOG
  ok  SCAN TYPE
  ...
  modules: timeseries, vectorset, search, bf, ReJSON

lenses
  bullmq     Certain  bullmq 6.0.2 - 5 queues

bullmq queues (prefix `bull`)
  queue              status    waiting     active  prioritized    delayed  waiting-children  completed     failed
  emails            running          0          0            0         76                 0        300         18
  reports            paused        415          0           45         47                 2         93          5
```

## Compatibility

keylens probes each server at connect and degrades to what it finds, rather than assuming
a feature set. A pane whose command is missing says *unavailable on this server* and names
the reason.

| | Redis 8 | Valkey 8 | Recached 0.3 |
|---|---|---|---|
| Key browser, all 6 value types | ✅ | ✅ | ✅ |
| Server stats (`INFO`) | ✅ | ✅ | ✅ since 0.2.3 |
| Bounded reads (`HSCAN`/`SSCAN`/`GETRANGE`) | ✅ | ✅ | ✅ since 0.2.4 |
| Clients pane (`CLIENT LIST`) | ✅ | ✅ | ✅ since 0.2.4 |
| Pub/sub pane (`PUBSUB CHANNELS`) | ✅ | ✅ | ✅ since 0.3.0 |
| Memory breakdown (`MEMORY USAGE`) | ✅ | ✅ | ✅ since 0.3.0 |
| Module-backed viewers (`MODULE LIST`) | ✅ | ✅ | ✅ since 0.3.0, always empty |
| Server-side type filter (`SCAN … TYPE`) | ✅ | ✅ | — filtered client-side |
| Slowlog | ✅ | ✅ | — not implemented |
| Cluster topology | ✅ | ✅ | — standalone only |
| Streams + consumer groups | ✅ | ✅ | — no stream types |
| BullMQ lens | ✅ | ✅ | — needs streams |

The Recached column is what `keylens probe` reports against `ghcr.io/recached-dev/recached:v0.3.2`;
it is maintained by hand, which is why [ROADMAP.md](ROADMAP.md) wants it generated from the probe
instead. Nothing keys off these versions at runtime — keylens gates on the capability it detected,
never on a version string — so a Recached that gains a command lights the pane up with no change here.

Recached 0.2.4 added `GETRANGE`, `HSCAN`, `SSCAN` and `ZSCAN`, so keylens asks it for bounded
reads directly, the same as Redis. Against 0.2.3 and earlier the probe finds them missing and
marks that value viewer unavailable. keylens does not measure and then read the value whole:
another client could grow it between those commands, defeating the bound.

Recached 0.3.0 added `PUBSUB CHANNELS`/`NUMSUB`/`NUMPAT`, `MEMORY USAGE` and `MODULE LIST`, so the
pub/sub pane, the memory breakdown and the module check all work there now; earlier releases
carried `SUBSCRIBE`/`PUBLISH` but no way to enumerate what was subscribed, which is the question
the pane is built on.

What still degrades, and how keylens tells the difference:

- **`SCAN … TYPE`** answers `ERR syntax error`, not `unknown command` — the reply an older Redis
  gives too. The probe reads it as unsupported rather than as a blocked command, and the type
  filter runs client-side.
- **`SLOWLOG`** is `unknown command`, so the slowlog pane says *not implemented by this server*.
- **`CLUSTER`** is refused with Redis's own sentence, `ERR This instance has cluster support
  disabled`, so the pane shows that instead of claiming the command is missing. `INFO` carries
  `cluster_enabled:0` alongside it.
- **No stream types**, so the stream viewer and the BullMQ lens have nothing to read.

The same machinery is what makes keylens behave on Upstash, ElastiCache and MemoryDB,
which block subsets of `CONFIG`, `CLIENT`, `MEMORY` and `DEBUG`.

## FAQ

### What is a lens?

A detector and a domain model, plus a host-integrated view. A lens recognises a keyspace pattern — BullMQ's
`bull:<queue>:*` shape, say — and replaces the raw key tree with UI built for that domain:
queue states, job detail, live throughput. keylens grows a tab because of what's in your
keyspace, not because you flipped a flag. Writing one is documented in
[docs/LENS.md](docs/LENS.md). In v0.1, detection can live in a separate crate, but a new
interactive domain view still needs explicit registration in the binary; a generic view
host is on the roadmap.

### Is it safe to run against production?

That's what v0.1 is designed for. It is read-only throughout — there is no code path that
writes — and `KEYS` is never issued, only cursor-paged `SCAN` with a bounded `COUNT`. A
workspace test fails the build if `KEYS`, `HGETALL` or `SMEMBERS` appear in command source.
Unbounded collection reads are treated the same way as `KEYS`, because
`HGETALL` on a two-million-field hash blocks the server just as hard. Mutations are a v0.2
question and will be gated behind the `readonly` connection flag that already exists.

### Does it work with Upstash, ElastiCache, MemoryDB or Aiven?

Yes. Managed hosts block subsets of `CONFIG`, `CLIENT`, `MEMORY`, `MONITOR` and `DEBUG`;
keylens probes each capability at connect and renders an explicit "unavailable on this
server" state for the panes it can't fill, rather than failing to start. Run
`keylens probe` against an unfamiliar host to see exactly what it will and won't do.
Note that most managed hosts are TLS-only, so the scheme is `rediss://`.

### Does it support Redis Cluster and Sentinel?

Yes — `redis-cluster://` and `redis-sentinel://host:26379/mymaster`. There's a cluster
topology pane, which distinguishes "this is a standalone server" from "your host blocked
`CLUSTER`" because those are the same probe failure but not the same problem.

### How is this different from RedisInsight?

RedisInsight is a desktop application and shows you keys and types. keylens is a single
binary in your terminal — usable over SSH, in a container, on a jump box — and its point
is interpretation rather than display: a lens turns a keyspace into the thing it
represents. It's also read-only by design, which RedisInsight isn't.

### How is the BullMQ view different from BullBoard?

BullBoard polls `getJobCounts` on a timer, which is why its graphs are coarse. BullMQ
already writes every state transition to a Redis stream, so keylens runs **one blocking
`XREAD` across every queue** and gets event-level throughput at sub-second resolution with
near-zero server load — the graph moves the instant a job fails. Job detail shows the
stack trace for *each attempt*, not just the last. Redis Cluster requires those streams to
share a hash slot; otherwise live throughput is explicitly unavailable. BullBoard remains
the right tool for retrying and removing jobs; keylens v0.1 won't write.

### Which BullMQ versions work?

Detection reads `meta.version`, and the lens is written against current BullMQ (v6) while
still reading older field names — `attemptsMade` is stored as `atm` in v6, and reading
only the long name reports every job as attempt 0. Note that BullMQ v6 made the storage
driver an optional peer dependency, so a v6 user can run BullMQ on Postgres with no Redis
keyspace at all; there's nothing for the lens to read in that case.

### Does it need Docker, Node, or a runtime?

No. It's a single static-ish binary with no runtime. Docker is only used for the
development fixtures.

### Can I use it as a library?

Yes — `keylens-conn` (connection and capability probing), `keylens-lens` (the extension
point), `keylens-ui` (ratatui widgets) and `keylens-bullmq` are all published on
crates.io. The dual MIT/Apache-2.0 license is there so linking them is uncontroversial.

---

## Demo

`docs/demo.tape` renders the demo from a script, so it stays honest as the UI changes.
The GIF itself is not committed yet — render it locally:

```sh
brew install vhs                  # pulls ttyd + ffmpeg
docker compose up -d --build      # the workload the demo shows
cargo build --release
vhs docs/demo.tape                # writes docs/demo.gif
```

The tape warms up off-camera for 25 seconds before recording. The sparklines are real —
they start empty and fill from the events stream — so capturing immediately would show a
flat graph and undersell the one thing the demo exists to show.

## Development

Bring up the fixtures — Redis, Valkey, and a BullMQ producer generating a realistic
workload (real multi-frame stack traces, retries, delayed and prioritized jobs, a
parent/child flow, and a queue that pauses and resumes every 45s):

```sh
docker compose up -d --build
```

| Service | Address |
|---|---|
| Redis 8 | `redis://127.0.0.1:6379` |
| Valkey 8 | `redis://127.0.0.1:6380` |
| Recached (optional) | `redis://127.0.0.1:6381` |

Recached sits behind a compose profile so the ordinary two-server fixture doesn't pull a third
image. The image is a public GHCR package — no `docker login`:

```sh
docker compose --profile recached up -d
KEYLENS_TEST_RECACHED_URL=redis://127.0.0.1:6381 cargo test --test live -- --ignored
```

Its live tests skip cleanly when that variable isn't set, so CI stays green without it.

The profile pins `v0.2.3` on purpose: it is the last release without `HSCAN`/`SSCAN`/`GETRANGE`,
and the explicit unsupported path needs a real server that lacks them to test against. That
means the fixture is *not* what the compatibility table above describes — point
`KEYLENS_TEST_RECACHED_URL` at a `v0.3.2` container to exercise the current one.

Then:

```sh
cargo run -p keylens -- --url redis://127.0.0.1:6379            # browse
cargo run -p keylens -- --url redis://127.0.0.1:6379 probe --queues
cargo run -p keylens -- --url redis://127.0.0.1:6380 probe      # vendor detection

cargo test                                        # hermetic: no Redis, no terminal
cargo test --test live -- --ignored               # against the fixtures above
```

Rendering is tested headlessly with ratatui's `TestBackend`, so pane layout and every
value viewer are covered in CI without a terminal.

## Design constraints

These are enforced, not aspirational:

- **`KEYS` is never issued.** Only cursor-paged `SCAN` with a bounded `COUNT`. The same
  applies to unbounded collection reads — `HGETALL` on a two-million-field hash blocks the
  server just as hard, so hashes and sets are read with `HSCAN`/`SSCAN` and lists, zsets
  and streams with explicit ranges. A workspace test fails the build if any of those
  literals appear in source.
- **Vendor is detected, never assumed.** `INFO server_name` distinguishes Redis, Valkey,
  Recached, Dragonfly, KeyDB and Garnet — and a server that answers no `INFO` at all still
  connects and browses. Feature gating keys off detected capabilities, never a version.
- **Managed hosts degrade, they don't error.** Upstash, ElastiCache and MemoryDB block
  subsets of `CONFIG`/`CLIENT`/`MEMORY`/`MONITOR`/`DEBUG`. keylens probes at connect time
  and renders an explicit "unavailable on this server" state.
- **Lens correctness is grounded in upstream source, not memory.** For example, current
  BullMQ pauses by setting `meta.paused = 1` — it does *not* rename `wait` to `paused`.
  Reading the legacy list would report paused queues as running.

## Writing a lens

See [docs/LENS.md](docs/LENS.md). The public crate supports detectors and shared detection
metadata today. A complete Sidekiq, Celery, RQ, or Horizon view still needs a small core
integration until the generic lens host on the roadmap lands.

## Roadmap

[ROADMAP.md](ROADMAP.md) covers what's next and, more usefully, what keylens knowingly
doesn't do yet — value paging past the first 200 elements, mutations, and the second lens.
The ordering there is a judgement call and the issue tracker is where to argue with it.

## License

Dual-licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option. `SPDX-License-Identifier: MIT OR Apache-2.0` — © 2026 ThinkGrid Labs.

This is the Rust ecosystem's convention, and it matters here because keylens ships
libraries, not just a binary: `keylens-lens` is a public extension point, so anyone
writing a lens links against these crates. Apache-2.0 carries an express patent grant,
which is what a company's legal review wants to see; MIT keeps the crates usable from
GPLv2 projects that Apache-2.0 alone would exclude.

Unless you state otherwise, any contribution you intentionally submit for inclusion in
this work shall be dual-licensed as above, with no additional terms or conditions.

---

<sub>Keywords: Redis TUI, Valkey TUI, Recached, redis-cli alternative, RedisInsight
alternative, Redis GUI, Redis browser, keyspace explorer, BullMQ dashboard, BullBoard
alternative, job queue monitoring, Redis streams, consumer groups, Redis slowlog, Rust,
ratatui, terminal UI, Upstash, ElastiCache, MemoryDB, Dragonfly, KeyDB.</sub>
