# Benchmarks: 0.1.8 vs 0.1.7

Measured 2026-11-17 on one machine (macOS 15, x86-64, Docker Desktop), against the compose
**Valkey 8** on :6380 (no other traffic, so server command counts are exact) unless noted.
Both versions built `--release` from the same `examples/bench.rs`; the 0.1.7 copy differs
only where the public API changed. One scenario per process under `/usr/bin/time -l`;
**medians of 5 runs** (3 for the re-measured rows marked †). RTT is injected by the fault
proxy in `tests/support` (half each way, per chunk), so bulk transfers at 50 ms are
bandwidth-limited and pessimistic.

Reproduce:

```sh
docker compose up -d --wait
cargo build --release --example bench
target/release/examples/bench seed
REPS=5 bench/run.sh target/release/examples/bench path/to/0.1.7/bench
```

## Reads, command volume and memory

| scenario | RTT | 0.1.7 wall | 0.1.8 wall | server cmds 0.1.7 → 0.1.8 | kept bytes 0.1.7 → 0.1.8 | peak RSS 0.1.7 → 0.1.8 |
|---|---|---|---|---|---|---|
| open list (5,000) | 0 | 1.5 ms | 1.5 ms | 15 → 15 | 1,490 → 1,490 | 4.0 → 4.1 MiB |
| open list (5,000) | 50 ms | 81.5 ms | 82.4 ms | 15 → 15 | = | 4.0 → 4.0 MiB |
| open hash (5,000) | 50 ms | 109.0 ms | 81.9 ms | 15 → 15 | = | 4.0 → 4.0 MiB |
| open set (500, intset) | 50 ms | 82.5 ms | 82.1 ms | 15 → 15 | = | 4.1 → 4.1 MiB |
| open zset (5,000) | 50 ms | 82.3 ms | 81.8 ms | 15 → 16‡ | = | 4.0 → 4.1 MiB |
| open stream (5,000) | 50 ms | 83.5 ms | 84.3 ms | 15 → 15 | = | 4.1 → 4.1 MiB |
| open string (32 MiB) | 50 ms | 108.9 ms | 108.9 ms | 15 → 15 | 65,555 → 65,555 | 4.2 → 4.3 MiB |
| **open list of 200 × 1 MiB elements** † | 0 | 4.86 s | 4.66 s | 17 → 17 | **209.7 MB → 3.28 MB** | **400 → 208 MiB** |
| open list of 200 × 1 MiB elements † | 50 ms | timeout (20 s) | timeout (20 s) | — | — | 50 → 51 MiB |
| **stream with 20 groups** † | 0 | 19.6 ms | 4.5 ms | 22 → 22 | — | 4.1 → 4.2 MiB |
| **stream with 20 groups** † | 50 ms | **1,199 ms** | **189 ms** | 22 → 22 | — | 4.1 → 4.1 MiB |
| queue table (20 queues) | 50 ms | 136.6 ms | 138.4 ms | 161 → 161 | — | 4.4 → 4.4 MiB |
| **`Conn::cmd LRANGE k 0 -1` (1M items)** | 0 | 1,055 ms, accepted | refused, 0 ms | **1 → 0** | 1,000,000 → 0 | **162 → 4.0 MiB** |
| `Conn::cmd LRANGE k 0 -1` (1M items) | 50 ms | 5,508 ms, accepted | refused, 0 ms | 3 → 0 | 1,000,000 → 0 | 161 → 3.9 MiB |

‡ One run of five sent an extra command; the other four sent 15. Within noise.

Reading the table honestly:

- **Key opens are unchanged**: one pipeline of 15 commands, same wall time, same memory.
  The new coverage and unknown-vs-zero logic costs nothing measurable.
- **The 200 × 1 MiB list** shows the line between storage and wire bounds. What keylens
  keeps fell 64×, and peak RSS halved now that elements are capped before they are copied.
  But ~200 MiB still crosses the wire and sits in the reply buffer, because `LRANGE` cannot
  bound element size. At 50 ms through the bandwidth-limited proxy both versions hit the
  20 s command timeout; 0.1.7 reported it as "unexpected reply shape … expected 15 replies,
  got 1", 0.1.8 as the timeout it is.
- **Stream group detail** is 6.3× faster at 50 ms (one pipeline instead of 2 round trips per
  group) at the same command count.
- **`LRANGE 0 -1` through the public API** is the class of read 0.1.8 refuses outright.

## UI responsiveness (tree size)

Apply a 500-key batch and redraw a 160×50 frame, repeated until the tree holds N keys.

| keys | 0.1.7 total | 0.1.8 total | worst frame 0.1.7 → 0.1.8 | peak RSS |
|---|---|---|---|---|
| 10,000 | 21 ms | 21 ms | 1.26 → 1.78 ms | 6.3 MiB both |
| 100,000 | 214 ms | 218 ms | 1.20 → 1.57 ms | 17.6 MiB both |
| 500,000 | 1,174 ms | 1,172 ms | 2.55 → 1.84 ms | 67.7 MiB both |

Unchanged within noise. 0.1.8 stops the tree at 500,000 keys (0.1.7 grew without limit).

## Faults (Redis 8, BullMQ producer running)

| scenario | 0.1.7 | 0.1.8 |
|---|---|---|
| SCAN page on a half-open link | still waiting at 60 s; worker wedged | fails at the command timeout; next request answered |
| events reader, 25 s accept-then-close outage | not measured | "reconnecting", live 9.3 s after restore |
| events reader, outage longer than ~115 s | **gave up permanently**; no events after restore | reconnects; at most one attempt per ≤ 30 s |
| events reader, 90 s real restart (`docker stop`/`start`) | recovered; 0.42 s CPU over 150 s | recovered; 0.41 s CPU over 150 s |
| ACL denies `XREAD` | 5 attempts, ~15 s, then unavailable | unavailable in < 3 s, no retries |
| password rotated mid-session | "Request timed out" forever; stale password retried forever | `AuthLost` within a few commands; auth-failure counter flat; fail-fast |
| **accept-then-close outage, CPU** (`bench outage:10`, 4 runs each) | 12.6 / 18.5 / 12.8 / 19.0 s CPU in ~21 s | 12.3 / 19.3 / 0.03 / 19.1 s CPU in ~21 s |

The last row is a **pre-existing upstream issue**, unchanged by this release. When a peer
accepts TCP and closes immediately (a load balancer with no healthy backend, some TLS
terminators) while a command is buffered, fred's reconnect path (`reconnect_with_policy` →
`Router::retry_buffer`, from a symbolized sample) spins on a core until the server comes
back. A genuine restart (connection refused) does not trigger it: 0.4 s CPU over a 90 s outage.
An idle connection through the same outage costs 0.05 s.
