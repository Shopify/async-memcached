# async-memcache

async-memcache is an async [memcached](https://memcached.org/) client implementation for Tokio.

*Warning*: This is a work in progress crate.

## Testing

The default test suite does not require `memcached` to be running. It will ignore tests that require `memcached` to be running.
```bash
$ cargo test
```

For the full test suite, you need to have `memcached` installed and running.

```bash
$ memcached -p 11211 -t 10 -c 10000 -m 1024
$ cargo test && cargo test -- --ignored
```

## Benchmark

To run the benchmark, you need to have `memcached` installed and running.

```bash
$ memcached -p 11211 -t 10 -c 10000 -m 1024
$ cargo bench
```

## Examples

You can run the examples with `cargo run --example <example-name>`. Examples require a running `memcached` server.

### TCP
Run a memcached server (with optional very verbose flag) that accepts a TCP connection and execute the basic examples:
```bash
$ memcached -vv
$ cargo run --package async-memcached --example basic
```

### Unix
Run a memcached server (with optional very verbose flag) that accepts a UDS connection and execute the UNIX examples:
```bash
$ memcached -vv -s /tmp/memcached.sock
$ cargo run --package async-memcached --example unix
```

## Profiling

Install `samply` with `cargo install samply`.

```bash
$ samply record cargo run --package async-memcached --example unix
```

## Features

This crate only targets the ASCII protocol, as the binary protocol has been deprecated and is no
longer actively being improved.

- [x] TCP connection
- [ ] UDP connection
- [x] UNIX domain socket connection
- [ ] Authentication
- [ ] TLS

## Releasing a new version

Developers should keep a running log of changes being made with each PR in `CHANGELOG.md`, under the `[Unreleased] - ReleaseDate`
header to ensure that accurate change records are kept as part of the release cycle.

To release a new version of this crate, you must be part of the [Owners group listed on crates.io](https://crates.io/crates/async-memcached).  If you are not part of this group and require a release that includes recently merged changes, please [open an issue on Github](https://github.com/Shopify/async-memcached/issues).

When using the `cargo-release` crate to release a new version, the `version` field in `Cargo.toml` will be bumped automatically according to the `<LEVEL>` argument that has been provided.  Additionally, `CHANGELOG.md` will have version and date fields updated
automatically.  See the `cargo-release` [documentation for more info](https://github.com/crate-ci/cargo-release) on versioning and options.

### `cargo publish` workflow:
<details>
<summary>Expand this section for `cargo publish` workflow details</summary>

- Ensure the your `main` branch is up to date:
```bash
$ git checkout main
$ git pull
```
- Checkout a new branch with the release version name:
```bash
$ git checkout -b "release v<VERSION>"
```
- Update the `version` field in `Cargo.toml` to reflect the desired new version of the crate, following [semantic versioning best practices](semver.org).
- Update the `CHANGELOG.md` such that the latest changes are under a header with the new version & release date:
```
## [Unreleased] - ReleaseDate

### Added
- a cool new feature

### Changed
- something to be more optimized
```
Should be updated to:
```
## [Unreleased] - ReleaseDate

## [Major.Minor.Patch] - 20YY-MM-DD

### Added
- a cool new feature

### Changed
- something to be more optimized
```
- Push your changes to the remote, get approval and merge your PR
- Update your `main` branch again:
```bash
$ git checkout main
$ git pull
```
- Run `cargo publish --dry-run` to perform a dry run, ensuring that your publishing process will proceed as expected.
- Run `cargo publish` to [publish the new version](https://doc.rust-lang.org/cargo/commands/cargo-publish.html) of this crate to crates.io.

</details>

### `cargo release` workflow:

<details>
<summary>Expand this section for `cargo release` workflow details</summary>

- Ensure the `main` branch is up to date:
```bash
$ git checkout main
$ git pull
```
- Checkout a new branch with the release version name:
```bash
$ git checkout -b "release v<VERSION>"
```
- Open a PR on GitHub and fill out the PR template for a release.  Provide the `cargo release` dryrun output in the PR body.

<details>
<summary>Expand this selection to see example dryrun output with the `Cargo.toml` and `CHANGELOG.md` files changed:</summary>

```bash
$ cargo release patch -v
[2024-09-09T17:37:47Z DEBUG reqwest::connect] starting new connection: https://index.crates.io/
[2024-09-09T17:37:48Z DEBUG cargo_release::steps] Files changed in async-memcached since v0.3.0: [
        "/async-memcached/CHANGELOG.md",
        "/async-memcached/benches/bench.rs",
        "/async-memcached/src/connection.rs",
        "/async-memcached/src/lib.rs",
        "/async-memcached/src/value_serializer.rs",
    ]
[2024-09-09T17:37:48Z DEBUG globset] glob converted to regex: Glob { glob: "**/*", re: "(?-u)^(?:/?|.*/)[^/]*$", opts: GlobOptions { case_insensitive: false, literal_separator: true, backslash_escape: true, empty_alternates: false }, tokens: Tokens([RecursivePrefix, ZeroOrMore]) }
[2024-09-09T17:37:48Z DEBUG globset] built glob set; 0 literals, 1 basenames, 0 extensions, 0 prefixes, 0 suffixes, 0 required extensions, 1 regexes
   Upgrading async-memcached from 0.3.0 to 0.3.1
[2024-09-09T17:37:48Z DEBUG cargo_release::ops::cargo] change:
    --- /async-memcached/Cargo.toml   original
    +++ /async-memcached/Cargo.toml   updated
    @@ -1,6 +1,6 @@
     [package]
     name = "async-memcached"
    -version = "0.3.0"
    +version = "0.3.1"
     authors = ["Toby Lawrence <toby@nuclearfurnace.com>"]
     edition = "2018"
     readme = "README.md"

[2024-09-09T17:37:48Z DEBUG cargo_release::steps::release] updating lock file
[2024-09-09T17:37:48Z DEBUG cargo_release::ops::replace] processing replacements for file async-memcached/CHANGELOG.md
   Replacing in CHANGELOG.md
--- CHANGELOG.md        original
+++ CHANGELOG.md        replaced
@@ -10,6 +10,8 @@

 ## [Unreleased] - ReleaseDate

+## [0.3.1] - 2024-09-09
+
 ### Changed

 - `set` and `add` methods can now accept `uint`-type argument for value in addition to `&str` and `&String` types.  The original implementation used an `AsRef` trait bound, which has been replaced with a custom `AsMemcachedValue` trait bound that should cover all of the applicable incoming types.
 ```
</details>

- Once your PR is approved, run `cargo release <LEVEL> -v --execute` and select yes (`y`) to confirm that you would like to publish to crates.io.  This will automatically update the `version` field in `Cargo.toml` and replace the `## [Unreleased] - ReleaseDate` header in `CHANGELOG.md` with the appropriate version and date automatically, and then publish the crate with those changes included.  These changes will also be pushed to your remote branch.
- Merge your PR so that `main` and the released crate version are in parity.

</details>

## License

MIT
