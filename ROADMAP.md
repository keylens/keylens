# keylens — Roadmap

Where keylens is going and what it is knowingly missing today.

**Today: v0.1.7 — read-only, single binary, one lens (BullMQ).**

Nothing here has a date. Items are ordered within each section by how much they undercut
what keylens already claims to do.

---

## Invariants

A feature that cannot be built without breaking one of these does not get built.

- `KEYS` is never issued — cursor-paged `SCAN` with a bounded `COUNT`, enforced by a test.
- No unbounded collection read. Every read is cursor-paged or explicitly ranged.
- Capabilities are probed, never assumed. A blocked command degrades and names the reason.
- Read-only is the default and always will be. Mutations are opt-in, disable-able, confirmed.
- Credentials never reach a terminal, a log, or a diagnostic — redaction fails closed.
- Arithmetic on anything a server sent is checked, not cast.

---

## Now — finish what v0.1 already promises

- Value paging past the first 200 elements — the single biggest gap.
- Cursor carry-through for hash and set paging, which `HSCAN`/`SSCAN` require.
- Paging or streaming for strings, which currently truncate at 64 KB.
- Surface connection health: latency, vendor and reconnect state in the status bar.
- Generate `docs/COMPAT.md` from the capability probe so the matrix cannot go stale.
- Read-only command console with history.
- Bound job-field *transfer*, not just retention — needs a ranged hash read or a size probe.
- Clear the deferred lints: indexing, arithmetic side effects, and the cast family.
- Public API compatibility checks in CI.

---

## Next — v0.2, mutations

- Vendor BullMQ's own Lua scripts, pinned to an upstream commit and picked by detected major.
  Never recompose a mutation out of primitive commands.
- Generic key mutations: `DEL`, `EXPIRE`, `RENAME`, edit value.
- Three independent layers of "no": a `--read-only` flag, per-connection `readonly = true`,
  and two-key confirmation on anything destructive.
- A concurrency test that hammers retry against a live worker and proves no corruption.
  This is the gate; mutations do not ship without it.

---

## Later — the lens ecosystem

- A generic lens host, so a domain view registers itself instead of requiring host changes.
  Prerequisite for everything else in this section.
- A cache-namespace lens: prefix grouping, memory share, TTL distribution, keys with no TTL.
  The proof that lenses generalise beyond job queues.
- The queue lenses people keep asking for: Sidekiq, Celery, RQ, Laravel Horizon.
- Bull v3 read-only support, currently only tolerated by detection.
- Per-crate API guides in place of the shared root README symlinks.

---

## Not planned

- No web UI, no daemon, no server component. keylens is a binary you run.
- No `CONFIG SET` or config editing.
- No Redis Stack module authoring — read-only viewers, yes; authoring, no.
- No mutation without confirmation, ever, including a `--yes` flag.

---

## Influencing this

Open an issue. The ordering is a judgement call and the part most likely to be wrong —
particularly ranking value paging above mutations. Concrete reports beat speculation,
especially the shape of your keyspace and which pane you gave up on.
