#!/usr/bin/env bash
set -euo pipefail

if ! cargo audit --version >/dev/null 2>&1; then
  echo "cargo-audit is required; install it with: cargo install cargo-audit --version 0.22.2 --locked" >&2
  exit 1
fi

cargo audit

# The exceptions in .cargo/audit.toml are valid only while these crate names
# remain outside the all-features normal/build/dev graph, regardless of version.
active_exceptions="$({
  cargo tree --locked --all-features --edges normal,build,dev --prefix none \
    | grep -E '^(rkyv|rsa) v' || true
})"
if [[ -n "${active_exceptions}" ]]; then
  echo "A documented security exception became active in the build graph:" >&2
  printf '%s\n' "${active_exceptions}" >&2
  exit 1
fi

echo "Rust dependency audit passed; documented optional-dependency exceptions remain unreachable"
