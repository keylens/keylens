# keylens

**A TUI for Redis, Valkey and Recached that understands your keys.**

Every Redis client shows you keys. None of them understand what your keys *mean*.

`bull:emails:failed` is a ZSET to `redis-cli`. It's a dead-letter queue to you.
`celery-task-meta-*` is 4,000 unrelated strings to RedisInsight. Your cache keys are a
namespace with a hit rate and a TTL distribution, not a flat list.

keylens is a general Redis, Valkey and Recached browser with a pluggable **lens** system
on top. A lens detects a known keyspace pattern and renders domain UI instead of raw keys.
BullMQ is the first one.

Every capability is **probed at connect, never assumed**, so keylens works against any
RESP server and tells you plainly what that server can't do. Verified against Redis 8,
Valkey 8, and [Recached](https://github.com/thinkgrid-labs/recached) — see
[compatibility](#compatibility).

> **v0.1 is read-only.** That's a feature — you can point it at production on day one.

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

## Status

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
load. The graph moves the instant a job fails.

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

| | Redis 8 | Valkey 8 | Recached |
|---|---|---|---|
| Key browser, all 6 value types | ✅ | ✅ | ✅ |
| Stats / slowlog / clients / cluster / pub-sub | ✅ | ✅ | — no `INFO` etc. |
| Streams + consumer groups | ✅ | ✅ | — no stream types |
| BullMQ lens | ✅ | ✅ | — needs streams |

Recached has no `HSCAN`/`SSCAN`/`GETRANGE`, so keylens measures a key with `HLEN`/`SCARD`/
`STRLEN` first and reads it whole only when it's small. The bound is preserved — it's
enforced client-side instead of requested server-side — and an oversized key says so
rather than being fetched.

The same machinery is what makes keylens behave on Upstash, ElastiCache and MemoryDB,
which block subsets of `CONFIG`, `CLIENT`, `MEMORY` and `DEBUG`.

## Regenerating the demo

The GIF at the top is rendered from a script, so it stays honest as the UI changes:

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

Recached sits behind a compose profile, since its image is a private package today:

```sh
docker compose --profile recached up -d
KEYLENS_TEST_RECACHED_URL=redis://127.0.0.1:6381 cargo test --test live -- --ignored
```

Its live tests skip cleanly when that variable isn't set, so CI stays green without it.

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

See [docs/LENS.md](docs/LENS.md). A lens is a detector, a model, and a view; you can add
Sidekiq, Celery, RQ or Horizon support without touching core.

## License

[Apache-2.0](LICENSE)
