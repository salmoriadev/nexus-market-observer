#!/usr/bin/env bash
set -euo pipefail

for command in docker jq; do
  command -v "${command}" >/dev/null 2>&1 || {
    echo "${command} is required by the Compose contract test" >&2
    exit 1
  }
done

compose_dir="$(mktemp -d)"
trap 'rm -rf "${compose_dir}"' EXIT

custom_database_url="postgres://custom:custom@db.example:6543/custom"
export BACKEND_PORT=18080
export DATABASE_URL="${custom_database_url}"
export GRAFANA_PORT=3300

docker compose \
  --env-file .env.example \
  --file docker-compose.yml \
  --project-name nexus-compose-contract \
  config --format json > "${compose_dir}/bridge.json"

jq --exit-status --arg database_url "${custom_database_url}" '
  .services.backend.environment.DATABASE_URL == $database_url
  and .services.backend.environment.HTTP_PORT == "8080"
  and any(
    .services.backend.ports[];
    .host_ip == "127.0.0.1"
    and .target == 8080
    and .published == "18080"
  )
  and all(.services[].ports[]?; .host_ip == "127.0.0.1")
' "${compose_dir}/bridge.json" > /dev/null || {
  echo "Bridge Compose configuration violated its port or database contract" >&2
  exit 1
}

docker compose \
  --env-file .env.example \
  --file docker-compose.yml \
  --file docker-compose.host-network.yml \
  --project-name nexus-compose-contract \
  config --format json > "${compose_dir}/host.json"

jq --exit-status --arg database_url "${custom_database_url}" '
  .services.backend.environment.DATABASE_URL == $database_url
  and .services.backend.environment.HTTP_HOST == "127.0.0.1"
  and .services.backend.environment.HTTP_PORT == "18080"
  and .services.backend.network_mode == "host"
  and (.services.backend.ports // [] | length == 0)
  and any(.services.backend.extra_hosts[]; . == "postgres=127.0.0.1")
  and .services.postgres.network_mode == "host"
  and (.services.postgres.ports // [] | length == 0)
  and .services.postgres.command == [
    "postgres",
    "-c",
    "listen_addresses=127.0.0.1"
  ]
  and .services.prometheus.environment.BACKEND_PORT == "18080"
  and .services.prometheus.entrypoint == ["/bin/sh", "-ec"]
  and (
    .services.prometheus.command
    | join("\n")
    | contains("127.0.0.1:$${BACKEND_PORT}")
  )
  and .services.prometheus.network_mode == "host"
  and .services.grafana.environment.GF_SERVER_HTTP_PORT == "3300"
' "${compose_dir}/host.json" > /dev/null || {
  echo "Host-network Compose configuration violated its port or database contract" >&2
  exit 1
}

docker compose \
  --env-file .env.example \
  --file docker-compose.yml \
  --file docker-compose.external-db.yml \
  --project-name nexus-compose-contract \
  config --format json > "${compose_dir}/external.json"

jq --exit-status --arg database_url "${custom_database_url}" '
  .services.backend.environment.DATABASE_URL == $database_url
  and ((.services.backend.depends_on // {}) | has("postgres") | not)
  and (.services | has("postgres") | not)
' "${compose_dir}/external.json" > /dev/null || {
  echo "External-database Compose configuration retained a bundled-database dependency" >&2
  exit 1
}

docker compose \
  --env-file .env.example \
  --file docker-compose.yml \
  --file docker-compose.host-network.yml \
  --file docker-compose.external-db.yml \
  --project-name nexus-compose-contract \
  config --format json > "${compose_dir}/host-external.json"

jq --exit-status --arg database_url "${custom_database_url}" '
  .services.backend.environment.DATABASE_URL == $database_url
  and .services.backend.environment.HTTP_PORT == "18080"
  and ((.services.backend.depends_on // {}) | has("postgres") | not)
  and (.services | has("postgres") | not)
' "${compose_dir}/host-external.json" > /dev/null || {
  echo "Host external-database Compose configuration violated its independence contract" >&2
  exit 1
}

echo "Bridge, host-network, and external-database Compose contracts passed"
