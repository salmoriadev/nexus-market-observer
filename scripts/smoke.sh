#!/usr/bin/env bash
set -euo pipefail

base_url="${BASE_URL:-http://localhost:8080}"
market="${NEXUS_MARKET:-BTC-USDX-PERP}"
attempts=60

request_json() {
  local path="$1"
  curl --fail --silent --show-error "${base_url}${path}"
}

echo "Waiting for the backend to publish a usable health response..."
for ((attempt = 1; attempt <= attempts; attempt += 1)); do
  if request_json "/health" > /tmp/nexus-smoke-health.json 2>/dev/null; then
    break
  fi
  if (( attempt == attempts )); then
    echo "Backend did not become ready" >&2
    exit 1
  fi
  sleep 2
done

request_json "/markets" > /tmp/nexus-smoke-markets.json
request_json "/markets/${market}" > /tmp/nexus-smoke-market.json

echo "Waiting for at least one persisted snapshot..."
for ((attempt = 1; attempt <= attempts; attempt += 1)); do
  request_json "/markets/${market}/history?limit=1" > /tmp/nexus-smoke-history.json
  if command -v jq > /dev/null 2>&1; then
    count="$(jq -r '.count' /tmp/nexus-smoke-history.json)"
  else
    count="$(sed -n 's/.*"count":\([0-9][0-9]*\).*/\1/p' /tmp/nexus-smoke-history.json)"
  fi
  if [[ "${count:-0}" -ge 1 ]]; then
    break
  fi
  if (( attempt == attempts )); then
    echo "No persisted snapshot was available before the timeout" >&2
    exit 1
  fi
  sleep 2
done

curl --fail --silent --show-error "${base_url}/metrics" > /tmp/nexus-smoke-metrics.txt
if ! sed -n '/nexus_market_updates_total/p' /tmp/nexus-smoke-metrics.txt | head -n 1 | grep -q .; then
  echo "Expected market metrics were not exposed" >&2
  exit 1
fi

echo "Smoke test passed for ${market}"

