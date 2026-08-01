# keylens demo — renders docs/demo.gif
#
#   brew install vhs          # pulls ttyd + ffmpeg
#   docker compose up -d --build
#   cargo build --release
#   vhs docs/demo.tape
#
# The GIF is the single biggest predictor of whether anyone tries this, so it opens on
# the thing nothing else does: live per-queue throughput off the BullMQ events stream,
# then a keyboard drill-down into a real stack trace. No narration, under ten seconds.

Output docs/demo.gif

Require docker

Set Shell bash
Set FontSize 16
Set Width 1400
Set Height 760
Set Padding 20
Set Theme "Catppuccin Mocha"
Set TypingSpeed 50ms
# One loop of a short GIF reads better than a fast blur.
Set PlaybackSpeed 1.0

# ── Warm-up, not recorded ───────────────────────────────────────────────────
#
# The sparklines are genuinely live: they start empty and fill from the events
# stream. Recording immediately would show an empty graph, which undersells the
# one feature the GIF exists to demonstrate. So launch, let ~25s of real traffic
# accumulate, switch to the queues tab, and only then start capturing.
Hide
Type "./target/release/keylens --url redis://127.0.0.1:6379"
Enter
Sleep 25s
Type "2"
Sleep 6s
Show

# ── Recorded from here ──────────────────────────────────────────────────────

# Live queue table: per-state counts, paused state, sparklines moving, ev/s.
Sleep 3.5s

# Into the failed jobs for the selected queue.
Down@400ms 2
Sleep 700ms
Enter
Sleep 2s

# Open one failed job: stack trace per attempt, timings, payload.
Enter
Sleep 4s

Hide
Type "q"
