#!/bin/bash
# Streaming v2 feed (runs inside flu-namenode via stdin): move one daily GHCN file at a time from
# /flu_project/streaming_v2/source into /flu_project/streaming_v2/incoming (atomic HDFS rename) and print one JSON line
# per file with the arrival time in ms (Docker VM clock, shared with the Spark container). Interval: $1 seconds (default 3).
INTERVAL="${1:-3}"
for d in $(hdfs dfs -ls /flu_project/streaming_v2/source 2>/dev/null | awk '/day=/{print $8}' | sort); do
  day="${d##*day=}"
  src=$(hdfs dfs -ls "$d" 2>/dev/null | awk '/part-/{print $8}')
  [ -z "$src" ] && continue
  hdfs dfs -mv "$src" "/flu_project/streaming_v2/incoming/day_${day}.csv" 2>/dev/null
  printf '{"file": "day_%s.csv", "arrival_ms": %s}\n' "$day" "$(date +%s%3N)"
  sleep "$INTERVAL"
done
