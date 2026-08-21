#!/usr/bin/env bash
set -euo pipefail

env_file_value() {
  local name="$1"
  if [[ -f .env ]]; then
    sed -n "s/^${name}=//p" .env | tail -n 1
  fi
}

backend_port="${BACKEND_PORT:-$(env_file_value BACKEND_PORT)}"
base_url="${BASE_URL:-http://localhost:${backend_port:-8080}}"
prometheus_url="${PROMETHEUS_URL:-http://localhost:9090}"
grafana_port="${GRAFANA_PORT:-$(env_file_value GRAFANA_PORT)}"
grafana_url="${GRAFANA_URL:-http://localhost:${grafana_port:-3000}}"
grafana_user="${GRAFANA_ADMIN_USER:-$(env_file_value GRAFANA_ADMIN_USER)}"
grafana_password="${GRAFANA_ADMIN_PASSWORD:-$(env_file_value GRAFANA_ADMIN_PASSWORD)}"
grafana_user="${grafana_user:-admin}"
grafana_password="${grafana_password:-admin}"
market="${NEXUS_MARKET:-BTC-USDX-PERP}"
expected_source="${EXPECTED_DATA_SOURCE:-replay}"
expected_health_statuses="${EXPECTED_HEALTH_STATUSES:-}"
stale_after_seconds="${STALE_AFTER_SECONDS:-$(env_file_value STALE_AFTER_SECONDS)}"
stale_after_seconds="${stale_after_seconds:-15}"
if [[ ! "${stale_after_seconds}" =~ ^[0-9]+$ || "${stale_after_seconds}" -eq 0 ]]; then
  echo "STALE_AFTER_SECONDS must be a positive integer for the smoke test" >&2
  exit 1
fi
max_market_data_age_ms="${MAX_MARKET_DATA_AGE_MS:-$((stale_after_seconds * 1000))}"
if [[ ! "${max_market_data_age_ms}" =~ ^[0-9]+$ || "${max_market_data_age_ms}" -eq 0 ]]; then
  echo "MAX_MARKET_DATA_AGE_MS must be a positive integer" >&2
  exit 1
fi
attempts="${SMOKE_ATTEMPTS:-60}"
if [[ ! "${attempts}" =~ ^[0-9]+$ || "${attempts}" -eq 0 ]]; then
  echo "SMOKE_ATTEMPTS must be a positive integer" >&2
  exit 1
fi
smoke_dir="$(mktemp -d)"
trap 'rm -rf "${smoke_dir}"' EXIT

for command in curl jq; do
  command -v "${command}" >/dev/null 2>&1 || {
    echo "${command} is required by the smoke test" >&2
    exit 1
  }
done

if [[ -z "${expected_health_statuses}" ]]; then
  if [[ "${expected_source}" == "replay" ]]; then
    expected_health_statuses="healthy"
  else
    # Live REST can provide a valid book while WebSocket health remains
    # degraded; a future usable public socket may make it fully healthy.
    expected_health_statuses="healthy,degraded"
  fi
fi

request_json() {
  local path="$1"
  curl --fail --silent --show-error "${base_url}${path}"
}

echo "Waiting for a usable ${expected_source} health response..."
for ((attempt = 1; attempt <= attempts; attempt += 1)); do
  if request_json "/health" > "${smoke_dir}/health.json" 2>/dev/null; then
    source_mode="$(jq -r '.data_source.mode // empty' "${smoke_dir}/health.json")"
    health_status="$(jq -r '.status // empty' "${smoke_dir}/health.json")"
    market_data_stale="$(jq -r '.market_data.stale' "${smoke_dir}/health.json")"
    last_update_at="$(jq -r '.market_data.last_update_at // empty' "${smoke_dir}/health.json")"
    last_update_age_ms="$(jq -r '.market_data.last_update_age_ms // empty' "${smoke_dir}/health.json")"
    if [[ "${source_mode}" == "${expected_source}" \
      && ",${expected_health_statuses}," == *",${health_status},"* \
      && "${market_data_stale}" == "false" \
      && -n "${last_update_at}" \
      && "${last_update_age_ms}" =~ ^[0-9]+$ \
      && "${last_update_age_ms}" -le "${max_market_data_age_ms}" ]]; then
      break
    fi
  fi
  if (( attempt == attempts )); then
    echo "Backend did not become ready with source ${expected_source}" >&2
    exit 1
  fi
  sleep 2
done

request_json "/markets" > "${smoke_dir}/markets.json"
request_json "/markets/${market}" > "${smoke_dir}/market.json"

for response in markets market; do
  jq --exit-status --arg source "${expected_source}" \
    '.data_source == $source' "${smoke_dir}/${response}.json" > /dev/null
done

echo "Waiting for at least two persisted snapshots..."
for ((attempt = 1; attempt <= attempts; attempt += 1)); do
  request_json "/markets/${market}/history?limit=2" > "${smoke_dir}/history.json"
  count="$(jq -r '.count // 0' "${smoke_dir}/history.json")"
  if [[ "${count:-0}" -ge 2 ]]; then
    jq --exit-status --arg source "${expected_source}" \
      '.data_source == $source' "${smoke_dir}/history.json" > /dev/null
    break
  fi
  if (( attempt == attempts )); then
    echo "Two persisted snapshots were not available before the timeout" >&2
    exit 1
  fi
  sleep 2
done

if [[ "${expected_source}" == "replay" ]]; then
  echo "Waiting for replay gap and reconnect evidence..."
  for ((attempt = 1; attempt <= attempts; attempt += 1)); do
    curl --fail --silent --show-error "${base_url}/metrics" > "${smoke_dir}/metrics.txt"
    if grep -Eq 'nexus_ws_lagged_frames_total\{source="replay"\} [1-9]' "${smoke_dir}/metrics.txt" \
      && grep -Eq 'nexus_ws_reconnects_total\{source="replay"\} [1-9]' "${smoke_dir}/metrics.txt" \
      && grep -Eq 'nexus_snapshots_persisted_total\{market="BTC-USDX-PERP",source="replay"\} [1-9]' "${smoke_dir}/metrics.txt"; then
      break
    fi
    if (( attempt == attempts )); then
      echo "Replay recovery metrics were not observed before the timeout" >&2
      exit 1
    fi
    sleep 2
  done
else
  curl --fail --silent --show-error "${base_url}/metrics" > "${smoke_dir}/metrics.txt"
fi

grep -Eq '^nexus_market_updates_total\{' "${smoke_dir}/metrics.txt" || {
  echo "Expected market metrics were not exposed" >&2
  exit 1
}

curl --fail --silent --show-error "${prometheus_url}/-/healthy" > /dev/null
prometheus_query="${prometheus_url}/api/v1/query?query=up%7Bjob%3D%22nexus-market-observer%22%7D"
curl --fail --silent --show-error "${prometheus_query}" > "${smoke_dir}/prometheus.json"
jq --exit-status '
  .status == "success"
  and (.data.result | length > 0)
  and any(.data.result[]; .value[1] == "1")
' "${smoke_dir}/prometheus.json" > /dev/null || {
  echo "Prometheus is not reporting the backend target as up" >&2
  exit 1
}

curl --fail --silent --show-error "${grafana_url}/api/health" > /dev/null
curl --fail --silent --show-error \
  --user "${grafana_user}:${grafana_password}" \
  "${grafana_url}/api/dashboards/uid/nexus-market-observer" > "${smoke_dir}/dashboard.json"
jq --exit-status '.dashboard.title == "Nexus Market Observer"' \
  "${smoke_dir}/dashboard.json" > /dev/null || {
  echo "The provisioned Grafana dashboard was not found" >&2
  exit 1
}

jq --exit-status '
  any(
    .dashboard.panels[];
    .title == "API Latency (p95)"
    and any(
      .targets[];
      .expr == "max by (route) (nexus_http_request_duration_seconds{quantile=\"0.95\"})"
    )
  )
' "${smoke_dir}/dashboard.json" > /dev/null || {
  echo "The Grafana latency panel is not querying the exported summary metric" >&2
  exit 1
}

curl --fail --silent --show-error --get \
  --data-urlencode 'query=max by (route) (nexus_http_request_duration_seconds{quantile="0.95"})' \
  "${prometheus_url}/api/v1/query" > "${smoke_dir}/latency.json"
jq --exit-status '
  .status == "success"
  and (.data.result | length > 0)
' "${smoke_dir}/latency.json" > /dev/null || {
  echo "Prometheus has no data for the Grafana latency query" >&2
  exit 1
}

echo "Smoke test passed for ${market} using ${expected_source}"
