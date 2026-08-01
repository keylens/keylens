# keylens

**A TUI for Redis, Valkey and Recached that understands your keys.**

Every Redis client shows you keys. None of them understand what your keys *mean*.

`bull:emails:failed` is a ZSET to `redis-cli`. It's a dead-letter queue to you.
`celery-task-meta-*` is 4,000 unrelated strings to RedisInsight. Your cache keys are a
namespace with a hit rate and a TTL distribution, not a flat list.

keylens is a general Redis/Valkey browser with a pluggable **lens** system on top. A lens
detects a known keyspace pattern and renders domain UI instead of raw keys. BullMQ is the
first one.

Every capability is **probed at connect, never assumed**, so keylens works against any
RESP server and tells you plainly what that server can't do. Verified against Redis 8,
Valkey 8, and [Recached](https://github.com/thinkgrid-labs/recached) — see
[compatibility](#compatibility).

> **v0.1 is read-only.** That's a feature — you can point it at production on day one.

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
  Dragonfly, KeyDB and Garnet. Feature gating keys off detected capabilities.
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
