#!/usr/bin/env bash
set -euo pipefail

env_file_value() {
  local name="$1"
  if [[ -f .env ]]; then
    sed -n "s/^${name}=//p" .env | tail -n 1
  fi
}

base_url="${BASE_URL:-http://localhost:8080}"
prometheus_url="${PROMETHEUS_URL:-http://localhost:9090}"
grafana_port="${GRAFANA_PORT:-$(env_file_value GRAFANA_PORT)}"
grafana_url="${GRAFANA_URL:-http://localhost:${grafana_port:-3000}}"
grafana_user="${GRAFANA_ADMIN_USER:-$(env_file_value GRAFANA_ADMIN_USER)}"
grafana_password="${GRAFANA_ADMIN_PASSWORD:-$(env_file_value GRAFANA_ADMIN_PASSWORD)}"
grafana_user="${grafana_user:-admin}"
grafana_password="${grafana_password:-admin}"
market="${NEXUS_MARKET:-BTC-USDX-PERP}"
expected_source="${EXPECTED_DATA_SOURCE:-replay}"
attempts=60
smoke_dir="$(mktemp -d)"
trap 'rm -rf "${smoke_dir}"' EXIT

request_json() {
  local path="$1"
  curl --fail --silent --show-error "${base_url}${path}"
}

json_value() {
  local expression="$1"
  local file="$2"
  if command -v jq > /dev/null 2>&1; then
    jq -r "${expression}" "${file}"
  else
    case "${expression}" in
      '.count') sed -n 's/.*"count":\([0-9][0-9]*\).*/\1/p' "${file}" ;;
      '.data_source.mode') sed -n 's/.*"data_source":{"mode":"\([^"]*\)".*/\1/p' "${file}" ;;
      *) return 1 ;;
    esac
  fi
}

echo "Waiting for a usable ${expected_source} health response..."
for ((attempt = 1; attempt <= attempts; attempt += 1)); do
  if request_json "/health" > "${smoke_dir}/health.json" 2>/dev/null; then
    source_mode="$(json_value '.data_source.mode' "${smoke_dir}/health.json")"
    if [[ "${source_mode}" == "${expected_source}" ]]; then
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
  if command -v jq > /dev/null 2>&1; then
    jq --exit-status --arg source "${expected_source}" \
      '.data_source == $source' "${smoke_dir}/${response}.json" > /dev/null
  else
    grep -q "\"data_source\":\"${expected_source}\"" \
      "${smoke_dir}/${response}.json"
  fi
done

echo "Waiting for at least two persisted snapshots..."
for ((attempt = 1; attempt <= attempts; attempt += 1)); do
  request_json "/markets/${market}/history?limit=2" > "${smoke_dir}/history.json"
  count="$(json_value '.count' "${smoke_dir}/history.json")"
  if [[ "${count:-0}" -ge 2 ]]; then
    if command -v jq > /dev/null 2>&1; then
      jq --exit-status --arg source "${expected_source}" \
        '.data_source == $source' "${smoke_dir}/history.json" > /dev/null
    else
      grep -q "\"data_source\":\"${expected_source}\"" \
        "${smoke_dir}/history.json"
    fi
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

grep -q 'nexus_market_updates_total' "${smoke_dir}/metrics.txt" || {
  echo "Expected market metrics were not exposed" >&2
  exit 1
}

curl --fail --silent --show-error "${prometheus_url}/-/healthy" > /dev/null
prometheus_query="${prometheus_url}/api/v1/query?query=up%7Bjob%3D%22nexus-market-observer%22%7D"
curl --fail --silent --show-error "${prometheus_query}" > "${smoke_dir}/prometheus.json"
grep -q '"1"' "${smoke_dir}/prometheus.json" || {
  echo "Prometheus is not reporting the backend target as up" >&2
  exit 1
}

curl --fail --silent --show-error "${grafana_url}/api/health" > /dev/null
curl --fail --silent --show-error \
  --user "${grafana_user}:${grafana_password}" \
  "${grafana_url}/api/dashboards/uid/nexus-market-observer" > "${smoke_dir}/dashboard.json"
grep -q 'Nexus Market Observer' "${smoke_dir}/dashboard.json" || {
  echo "The provisioned Grafana dashboard was not found" >&2
  exit 1
}

echo "Smoke test passed for ${market} using ${expected_source}"
