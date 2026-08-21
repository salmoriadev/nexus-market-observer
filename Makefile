.PHONY: build run test lint fmt up down logs smoke

build:
	cargo build --locked

run:
	DATABASE_URL="$${DATABASE_URL_LOCAL:-postgres://nexus:nexus@localhost:5432/nexus_market}" cargo run --locked

test:
	cargo test --all-features

lint:
	cargo clippy --all-targets --all-features -- -D warnings

fmt:
	cargo fmt --all

up:
	docker compose up --build -d

down:
	docker compose down

logs:
	docker compose logs --follow backend

smoke:
	./scripts/smoke.sh

