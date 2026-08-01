# keylens — Roadmap

Where keylens is going and, more usefully, what it is knowingly missing today.

[`PLAN.md`](PLAN.md) is the internal build plan that got v0.1 shipped and is largely
historical now. This file is the forward-looking one, and it is where to argue with the
priorities.

**Today: v0.1.4 — read-only, single binary, one lens (BullMQ).**

Nothing here has a date. Items are ordered within each section by how much they undercut
what keylens already claims to do, which is a more honest ranking than a calendar.

---

## Invariants

Every item below is constrained by these. A feature that cannot be built without breaking
one does not get built.

- **`KEYS` is never issued.** Cursor-paged `SCAN` with a bounded `COUNT`, always. Enforced
  by a grep test over the workspace, not by review.
- **No unbounded collection read.** `HGETALL` on a two-million-field hash blocks a server
  exactly as hard as `KEYS`, and a key browser is the thing that will meet that hash. Every
  read is cursor-paged or explicitly ranged; where a server lacks the bounded command, the
  key is *measured* first and declined if it is too big.
- **Capabilities are probed, never assumed.** A blocked command produces *unavailable on
  this server* and names the reason. This is what makes keylens behave on Upstash,
  ElastiCache, MemoryDB and DigitalOcean rather than erroring at them.
- **Read-only is the default and always will be.** Mutations are opt-in, per-connection
  disable-able, and confirmed. "Point it at production" is the promise the project is built
  on; nothing is worth trading it for.

---

## Now — finish what v0.1 already promises

These are gaps in shipped features, not new features. They rank above v0.2 because a tool
that half-does what it advertises costs more trust than one that does less on purpose.

### Value paging

The value pane shows the first 200 elements of a collection and stops. There is no way to
reach the 201st.

For lists, zsets and streams the read is already offset-addressable and only ever called
with `0` ([`worker.rs`](crates/keylens/src/worker.rs), `detail`). For hashes and sets it is
harder and the limitation is real: `HSCAN`/`SSCAN` resume from an opaque cursor returned by
the previous reply, not from an element index, so paging them needs the caller to carry
that cursor back in — which the current signature cannot express. See the note on
`read_value` in [`value.rs`](crates/keylens-conn/src/value.rs).

Strings truncate at 64 KB with a marker, which is honest but terminal.

A key browser that cannot show you the whole key is not finished. This is the single
biggest gap.

### Cluster: multi-key pipelines span hash slots

`Conn::pipeline` documents that a cluster can reject a pipeline spanning multiple hash
slots, and says callers that may span slots should fall back to sequential calls. One
caller does not: `type_keys` pipelines up to 1,000 arbitrary keys in a single batch, which
on a real cluster is close to guaranteed to span slots.

The failure is quiet — types are treated as a nicety, so the tree renders with every tag
missing rather than erroring. Quiet is worse. Either group by slot or fall back per batch.

### Surface connection health

keylens now measures round-trip latency with a timed `PING` at connect and uses it to pace
the stats refresh and the selection debounce — but never shows it.

It should. A user staring at a slow pane cannot currently distinguish "this server is
400 ms away" from "keylens is broken", and those call for completely different responses.
Latency, vendor, and the reconnect state fred already exposes belong in the status bar.

### `docs/COMPAT.md`

The compatibility matrix lives in the README and is maintained by hand, which is why it
went stale: it claimed Recached had no `INFO` for two releases after Recached gained it.

The fix is to generate the matrix from the capability probe against each server in the
test matrix, so it cannot drift from what keylens actually detects. `PLAN.md` promised this
file; it does not exist.

### Command console

Listed in v0.1 scope, never built. A read-only command allowlist with history, for the
moments when the panes do not have the shape of the question you are asking.

---

## Next — v0.2, mutations

The whole design is already worked out in [`PLAN.md` §2.5 and §3](PLAN.md); this is a
pointer, not a restatement. The parts that matter:

- **Ported Lua only, never composed commands.** BullMQ ships 49 scripts and reimplementing
  retry as `ZREM` + `LPUSH` will corrupt a production queue under concurrency. Vendor their
  scripts with the upstream commit recorded, and pick the variant by detected major.
- **Generic key mutations** — `DEL`, `EXPIRE`, `RENAME`, edit value — behind the same
  confirmation machinery.
- **Three layers of "no"**: a `--read-only` flag that hard-disables, per-connection
  `readonly = true` in config (the field already exists and is reserved for exactly this),
  and two-key confirm on anything destructive.
- **A concurrency test that hammers retry against a live worker** and proves no corruption.
  This is the gate. Mutations do not ship without it — a single corruption issue on GitHub
  permanently ends the project's claim to be safe to point at production.

---

## Later — the lens ecosystem

A lens is a detector, a data model, and a view. The extension point is real and documented
in [`docs/LENS.md`](docs/LENS.md); what it lacks is a second example.

**Cache-namespace lens, first.** Group keys by prefix and show memory share, TTL
distribution, and keys with no TTL at all — the classic memory leak. It is cheap, it is
useful on literally any Redis, and it is the proof that the lens system generalises beyond
job queues. Right now the honest critique is that keylens is a BullMQ tool wearing a Redis
costume; one non-queue lens retires that.

**Then the queue lenses people keep asking for**: Sidekiq, Celery, RQ, Laravel Horizon.
Each is self-contained and addable without touching core, which is the property that lets
this compound past one maintainer.

**Bull v3 read-only** was in v0.1 scope and only landed as far as detection tolerating it.
Still worth finishing — v3 keyspaces are everywhere and BullBoard's support for them is
mediocre.

---

## Not planned

Saying no to these in writing is what keeps the rest achievable.

- **No web UI, no daemon, no server component.** keylens is a binary you run.
- **No `CONFIG SET`** or config editing. Blocked on most managed hosts anyway, and the
  blast radius is wrong for a browsing tool.
- **No Redis Stack module authoring.** Read-only viewers for JSON/Search/TimeSeries, yes;
  authoring, no.
- **No mutation without confirmation, ever** — including a `--yes` flag. If a script needs
  to mutate a queue, it should use the queue library, not drive a TUI.

---

## Influencing this

Open an issue. The ordering above is a judgement call about which gaps hurt most, and it is
the part most likely to be wrong — particularly the ranking of value paging over mutations,
which assumes people mind partial data more than they mind read-only. Concrete reports beat
speculation, especially the shape of your keyspace and which pane you gave up on.
