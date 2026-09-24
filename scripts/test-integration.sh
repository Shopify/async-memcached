#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"
export PATH="$root/target/test-tools/bin:$PATH"
export RUST_BACKTRACE="${RUST_BACKTRACE:-1}"
export RUST_TEST_THREADS="${RUST_TEST_THREADS:-4}"

for binary in "${MEMCACHED_BIN:-memcached}" "${TOXIPROXY_BIN:-toxiproxy-server}"; do
  if ! command -v "$binary" >/dev/null 2>&1; then
    printf 'Required test executable not found: %s. See TESTING.md.\n' "$binary" >&2
    exit 1
  fi
done

exec cargo test --all-features --tests --no-fail-fast -- --include-ignored "$@"
