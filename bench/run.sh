#!/usr/bin/env bash
# Run the release benchmark matrix and print a markdown table.
#
#   cargo build --release --example bench
#   bench/run.sh target/release/examples/bench [path/to/0.1.7/bench] > bench/results.md
#
# Each scenario runs in its own process, three times, under /usr/bin/time so peak RSS
# and CPU belong to that scenario alone. Medians are reported. The server must have no
# other traffic (default: the compose Valkey on :6380); seed it first with `bench seed`.
set -euo pipefail

BINS=("$@")
[ ${#BINS[@]} -ge 1 ] || { echo "usage: $0 <bench-bin> [<bench-bin> ...]" >&2; exit 2; }
REPS=${REPS:-3}
SCENARIOS=(open:list open:hash open:set open:zset open:stream open:str open:fat stream_info queues lrange_all)
RTTS=(0 50)
TREES=(tree:10000 tree:100000 tree:500000)

if [ "$(uname -s)" = Darwin ]; then TIME=(/usr/bin/time -l); else TIME=(/usr/bin/time -v); fi

# Peak RSS in MiB and user+sys CPU seconds from /usr/bin/time's stderr.
parse_time() {
  if [ "$(uname -s)" = Darwin ]; then
    awk '/maximum resident set size/ { rss = $1 / 1048576 }
         / real / { cpu = $3 + $5 }
         END { printf "%.1f %.2f\n", rss, cpu }' "$1"
  else
    awk -F: '/Maximum resident set size/ { rss = $2 / 1024 }
             /User time/ { u = $2 } /System time/ { s = $2 }
             END { printf "%.1f %.2f\n", rss, u + s }' "$1"
  fi
}

median() { sort -n | awk '{ a[NR] = $1 } END { print a[int((NR + 1) / 2)] }'; }

field() { sed -E "s/.*\"$1\":(\"[^\"]*\"|[^,}]*).*/\1/" | tr -d '"'; }

run_one() {
  local bin=$1 scenario=$2 rtt=$3 tmp walls=() rsss=() cpus=() json
  tmp=$(mktemp)
  for _ in $(seq "$REPS"); do
    json=$("${TIME[@]}" "$bin" "$scenario" "$rtt" 2>"$tmp")
    walls+=("$(echo "$json" | field wall_ms)")
    read -r rss cpu < <(parse_time "$tmp")
    rsss+=("$rss"); cpus+=("$cpu")
  done
  rm -f "$tmp"
  local version cmds kept result keys frame
  version=$(echo "$json" | field version)
  cmds=$(echo "$json" | grep -q '"commands"' && echo "$json" | field commands || echo "-")
  kept=$(echo "$json" | grep -q '"retained_bytes"' && echo "$json" | field retained_bytes || echo "-")
  result=$(echo "$json" | grep -q '"result"' && echo "$json" | field result | cut -c1-40 || echo "-")
  frame=$(echo "$json" | grep -q '"worst_frame_ms"' && echo "$json" | field worst_frame_ms || echo "-")
  keys=$(echo "$json" | grep -q '"keys"' && echo "$json" | field keys || echo "-")
  printf '| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n' \
    "$scenario" "$rtt" "$version" \
    "$(printf '%s\n' "${walls[@]}" | median)" "$cmds" "$kept" \
    "$(printf '%s\n' "${rsss[@]}" | median)" "$(printf '%s\n' "${cpus[@]}" | median)" \
    "$frame" "$result${keys:+ }$( [ "$keys" != "-" ] && echo "($keys keys)")"
}

echo "| scenario | RTT ms | version | wall ms (median) | server commands | retained bytes | peak RSS MiB | CPU s | worst frame ms | result |"
echo "|---|---|---|---|---|---|---|---|---|---|"
for s in "${SCENARIOS[@]}"; do
  for r in "${RTTS[@]}"; do
    for b in "${BINS[@]}"; do run_one "$b" "$s" "$r"; done
  done
done
for s in "${TREES[@]}"; do
  for b in "${BINS[@]}"; do run_one "$b" "$s" 0; done
done
