.PHONY: build run test test-integration test-all audit lint fmt up demo live down down-live logs logs-live smoke smoke-live

COMPOSE := docker compose
DEMO_PROJECT := nexus-market-observer-demo
LIVE_PROJECT := nexus-market-observer-live

build:
	cargo build --locked

run:
	DATABASE_URL="$${DATABASE_URL_LOCAL:-postgres://nexus:nexus@localhost:5432/nexus_market}" cargo run --locked

test:
	cargo test --locked

test-integration:
	DATABASE_URL="$${DATABASE_URL_LOCAL:-postgres://nexus:nexus@localhost:5432/nexus_market}" cargo test --locked --all-features --test persistence_test

test-all: test test-integration

audit:
	./scripts/audit.sh

lint:
	cargo clippy --locked --all-targets --all-features -- -D warnings

fmt:
	cargo fmt --all

up:
	$(MAKE) demo

demo:
	MARKET_DATA_SOURCE=replay $(COMPOSE) -p $(DEMO_PROJECT) up --build -d

live:
	MARKET_DATA_SOURCE=nexus_live $(COMPOSE) -p $(LIVE_PROJECT) up --build -d

down:
	$(COMPOSE) -p $(DEMO_PROJECT) down

down-live:
	$(COMPOSE) -p $(LIVE_PROJECT) down

logs:
	$(COMPOSE) -p $(DEMO_PROJECT) logs --follow backend

logs-live:
	$(COMPOSE) -p $(LIVE_PROJECT) logs --follow backend

smoke:
	EXPECTED_DATA_SOURCE=replay ./scripts/smoke.sh

smoke-live:
	EXPECTED_DATA_SOURCE=nexus_live ./scripts/smoke.sh
