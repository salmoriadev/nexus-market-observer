FROM rust:1.88.0-bookworm AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
RUN mkdir src \
    && printf 'fn main() {}\n' > src/main.rs \
    && cargo build --release --locked \
    && rm -rf src

COPY migrations ./migrations
COPY src ./src
RUN touch src/main.rs \
    && cargo build --release --locked

FROM debian:bookworm-slim AS runtime

WORKDIR /app
COPY --from=builder /app/target/release/nexus-market-intelligence /usr/local/bin/nexus-market-intelligence
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder --chown=65534:65534 /app/migrations ./migrations

USER 65534:65534
EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=3s --start-period=30s --retries=3 \
    CMD ["/usr/local/bin/nexus-market-intelligence", "healthcheck"]

ENTRYPOINT ["/usr/local/bin/nexus-market-intelligence"]
