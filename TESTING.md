# Integration tests

## Test environment

The integration suite uses memcached processes and Toxiproxy to simulate network failures.
Scripted TCP peers cover malformed responses and interrupted protocol frames.

The harness follows the process model in Shopify/Dalli.
Each fixture owns its service processes and private directories.
Each test that uses memcached starts an empty cache.
The fixture stops its child processes after the test or a panic.
It waits for the child processes to exit.

The suite does not need an external memcached service or Toxiproxy service.
It does not need Ruby or a Dalli checkout.

### Requirements

The suite supports Linux and macOS.
Rust 1.98.0 matches the compiler version in CI.

The installer and harness require these tools:

- A C compiler
- `make`
- `curl`
- `tar`
- `install`
- `shasum`
- `id`

The installer provides these versions:

- memcached 1.6.41
- libevent 2.1.12
- Toxiproxy 2.11.0

These versions and their SHA-256 checksums appear in `scripts/install-test-tools.sh`.
The installer checks each checksum before it uses a download.

The installer supports these platforms:

- Linux x86-64
- Linux ARM64
- macOS x86-64
- macOS ARM64

The installer writes only to `target/test-tools`.
It does not require `sudo` or change system services.

The memcached fixture accepts version 1.6.40 or newer.
The versions from the installer define the reference environment.

### Test script behavior

The test script runs unit tests and every integration target with all crate features.
It runs ignored tests and continues to other targets after a failure.
A failed target produces a nonzero exit status.

The default concurrency is four test threads.
Each test uses private services, so tests can run in parallel.

### Executable selection

The script searches `target/test-tools/bin` before the system `PATH`.
Explicit executable paths override both locations.
An absent executable causes a failure, not a skipped test.

The harness selects loopback ports and private Unix sockets.
These variables no longer select test endpoints:

- `MEMCACHED_HOST`
- `MEMCACHED_PORT`
- `TOXIPROXY_HOST`
- `TOXIPROXY_PORT`

### Tests without service executables

The default Cargo command runs unit tests and scripted TCP tests.
It ignores tests that require memcached or Toxiproxy executables.
That command alone does not check the complete integration suite.

## Test procedures

Run each command from the repository root.

### Install the test tools

Install the test tools:

```bash
./scripts/install-test-tools.sh
```

If the host needs fewer concurrent build jobs, set `TEST_BUILD_JOBS`:

```bash
TEST_BUILD_JOBS=2 ./scripts/install-test-tools.sh
```

### Run the complete suite

Run the complete suite:

```bash
./scripts/test-integration.sh
```

To change the concurrency, set `RUST_TEST_THREADS`:

```bash
RUST_TEST_THREADS=8 ./scripts/test-integration.sh
```

Filter the tests by name:

```bash
./scripts/test-integration.sh cas
```

Run one integration target:

```bash
PATH="$PWD/target/test-tools/bin:$PATH" \
  cargo test --all-features --test resiliency_tests -- --include-ignored
```

Run the complete suite directly through Cargo:

```bash
PATH="$PWD/target/test-tools/bin:$PATH" \
  cargo test --all-features --tests --no-fail-fast -- --include-ignored
```

### Select executables

Select the executable paths:

```bash
MEMCACHED_BIN=/absolute/path/to/memcached \
TOXIPROXY_BIN=/absolute/path/to/toxiproxy-server \
  ./scripts/test-integration.sh
```

### Run tests without service executables

Run unit tests and scripted TCP tests:

```bash
cargo test --all-features
```

## Test targets

| Target | Coverage |
| --- | --- |
| `ascii_proto_integration_tests` | The target checks original ASCII commands and size boundaries. |
| `meta_proto_integration_tests` | The target checks original meta commands and response metadata. |
| `client_integration_tests` | The target checks value lifecycles and connections over both transports. |
| `pipeline_integration_tests` | The target checks batch contents and per-key outcomes. |
| `resiliency_tests` | The target checks transport failures and explicit reconnection. |
| `wire_integration_tests` | The target checks protocol frames and task cancellation with an owned client. |

## Harness settings

Each memcached fixture has these settings:

- Loopback TCP or a private Unix socket
- UDP disabled
- One worker thread
- A 128 MiB cache budget
- A 1 MiB item limit

The cache budget prevents eviction during the original test with 100 near-limit items.

Each Toxiproxy fixture owns a separate API process and proxy.
The fixture does not delete proxies from an external service.

Startup probes have a ten-second deadline.
New tests and fault operations have thirty-second deadlines.
Expiration tests poll for a cache miss instead of a fixed delay.

Scripted peers and Toxiproxy cover separate failure conditions.
Neither replaces the tests that use memcached.

## Diagnostic output

A fixture prints its server logs when a test panics.
Errors for absent executables and startup failures include a reference to this document.

## Diagnostic procedures

Display captured output:

```bash
./scripts/test-integration.sh --nocapture
```

Check the executable versions:

```bash
target/test-tools/bin/memcached -h
target/test-tools/bin/toxiproxy-server --version
```

If a checksum check fails, remove the affected file from `target/test-tools/downloads`.
Run the installer again.

## CI

The `test` job in `.github/workflows/ci.yml` runs the same scripts as local execution.
The workflow runs for pull requests to `main` and pushes to `main`.
Cargo discovers all integration targets, so new target files do not require separate CI commands.
The test job has a fifteen-minute limit.

## Scope

The suite checks the current Rust API.
The test changes do not add library features.
