#!/usr/bin/env bash
# Replay a captured Flussonic event_sink payload against a running event-service.
# Sends ONE event per HTTP request — exactly how Flussonic's event_sink behaves
# (it POSTs a single JSON object per event, not a batched array).
#
# Real end-to-end: events flow ingest → asynq → worker → Postgres/Redis.
#
# Usage:
#   scripts/replay.sh [file.json] [repeat] [delay_seconds]
#     file.json      fixture to replay (JSON array, NDJSON, or single object)
#     repeat         how many times to replay the whole set (default 1)
#     delay_seconds  pause between individual events (default 0)
#
# Env:
#   URL       endpoint (default http://localhost:4100/api/v1/webhooks/flussonic/event-sink)
#   SIGN_KEY  if set, sends X-Signature = sha1_hex(body || SIGN_KEY) per event
#
# Examples:
#   scripts/replay.sh                                   # post each fixture event once
#   scripts/replay.sh events.json 5 0.2                 # replay the file 5x, 200ms between events
#   SIGN_KEY=secret scripts/replay.sh events.json       # signed, per event
set -euo pipefail

FILE="${1:-internal/ingest/testdata/flussonic_events.json}"
REPEAT="${2:-1}"
DELAY="${3:-0}"
URL="${URL:-http://localhost:4100/api/v1/webhooks/flussonic/event-sink}"

if [[ ! -f "$FILE" ]]; then
  echo "fixture not found: $FILE" >&2
  exit 1
fi

# Flatten the fixture into one compact JSON object per line, accepting a JSON
# array, NDJSON, or a single object. Read into an array without `mapfile` so
# this still runs under macOS's stock Bash 3.2.
EVENTS=()
while IFS= read -r line; do
  EVENTS+=("$line")
done < <(python3 - "$FILE" <<'PY'
import json, sys
with open(sys.argv[1]) as f:
    text = f.read().strip()
try:
    data = json.loads(text)            # JSON array or single object
    data = data if isinstance(data, list) else [data]
except json.JSONDecodeError:
    data = [json.loads(l) for l in text.splitlines() if l.strip()]  # NDJSON
for ev in data:
    print(json.dumps(ev, separators=(",", ":")))
PY
)

if [[ ${#EVENTS[@]} -eq 0 ]]; then
  echo "no events parsed from $FILE" >&2
  exit 1
fi

sign() { # sha1_hex(body || SIGN_KEY)
  { printf '%s' "$1"; printf '%s' "$SIGN_KEY"; } | openssl dgst -sha1 | awk '{print $NF}'
}

total=$(( ${#EVENTS[@]} * REPEAT ))
echo "Replaying $FILE → $URL  (events=${#EVENTS[@]}, repeat=$REPEAT, total=$total, delay=${DELAY}s, per-event)"

n=0
for ((r = 1; r <= REPEAT; r++)); do
  for body in "${EVENTS[@]}"; do
    n=$((n + 1))
    sig_args=()
    if [[ -n "${SIGN_KEY:-}" ]]; then
      sig_args=(-H "X-Signature: $(sign "$body")")
    fi
    resp="$(curl -sS -X POST "$URL" \
      -H 'Content-Type: application/json' \
      ${sig_args[@]+"${sig_args[@]}"} \
      --data-binary "$body")"
    echo "[$n/$total] $resp"
    if (( $(echo "$DELAY > 0" | bc -l) )); then sleep "$DELAY"; fi
  done
done
