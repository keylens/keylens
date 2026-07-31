# keylens

**A TUI for Redis and Valkey that understands your keys.**

Every Redis client shows you keys. None of them understand what your keys *mean*.

`bull:emails:failed` is a ZSET to `redis-cli`. It's a dead-letter queue to you.
`celery-task-meta-*` is 4,000 unrelated strings to RedisInsight. Your cache keys are a
namespace with a hit rate and a TTL distribution, not a flat list.

keylens is a general Redis/Valkey browser with a pluggable **lens** system on top. A lens
detects a known keyspace pattern and renders domain UI instead of raw keys. BullMQ is the
first one.

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

           a TUI for Redis and Valkey that understands your keys

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
