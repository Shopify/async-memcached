# Changelog

All notable changes to this project will be documented in this file.
Keep a running log of your changes with each PR under the `[Un-released] - Release Date` header.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

<!-- next-header -->

## [Unreleased] - ReleaseDate

## [0.11.1] - 2026-11-23

### Fixed

- Meta flags that do not start with an ASCII letter are rejected with an `ErrorKind::Client` error before anything is written ([GHSA-5f6h-jcc2-54mg](https://github.com/Shopify/async-memcached/security/advisories/GHSA-5f6h-jcc2-54mg)). memcached 1.6.40 and newer reject an `ms` line whose flag starts with a byte that is neither a digit nor in the ASCII range `A` to `z` (0x41 to 0x7a), such as `!`, before they read the length of the value. They then run the value as commands, which closing the connection cannot undo. Every meta flag starts with an ASCII letter, so the client also rejects other first bytes, such as `1` and `[`, which memcached rejects without running the value. Such a flag previously reached memcached, which answered `CLIENT_ERROR`. It now fails with `Meta flag does not start with an ASCII letter`, and `meta_set`, `meta_set_multi` and `meta_get_multi` keep the connection open.
- `get_multi`, `meta_get_multi` and `meta_set_multi` write each key as the bytes that they checked, and `set_multi` and `add_multi` read a reply for each key that they wrote and for no other key ([GHSA-5f6h-jcc2-54mg](https://github.com/Shopify/async-memcached/security/advisories/GHSA-5f6h-jcc2-54mg)). They called `as_ref` on a key once to check it and again to write it or to match its reply, so a key type whose `as_ref` returns other bytes on a later call could put unchecked bytes on the wire or misalign the replies. Types that return the same bytes on every call, such as `&str`, `String`, `&[u8]` and `Vec<u8>`, were not affected.

## [0.11.0] - 2026-11-18

### Upgrade notes

0.11.0 has no public type or signature changes, so code that compiles against 0.10 compiles unchanged. These calls behave differently:

- Pass each meta flag as its own element: `["v", "T60"]`, not `["v T60"]`. A flag that is empty or contains whitespace or control characters fails with an `ErrorKind::Client` error, and so does an empty opaque.
- `set`, `add`, `set_multi`, `add_multi` and `meta_set` close the connection when memcached answers `ERROR` or `CLIENT_ERROR`, and later calls on that client return `Error::ConnectionClosed`. A caller whose meta flags memcached rejects now loses the connection on every such call, so a pool opens a new one each time. Watch for connection churn after upgrading.
- `set_multi` and `add_multi` return `Err` for the whole call on those replies, instead of an error for one key.
- `version` closes the connection on any reply that does not start with `VERSION `, so later calls on that client return `Error::ConnectionClosed`. Behind a proxy that answers `version` with `ERROR`, such as Twemproxy, `version` already returned an error; now it also closes the connection.
- These inputs fail before anything is written: an empty key, an opaque longer than 31 bytes, and an `ms` command with more than 16 meta flags (at most 14 caller flags for `meta_set_multi`). A 32-byte opaque returns `ErrorKind::OpaqueTooLong` instead of an `ErrorKind::Client` error from memcached.
- Changed error messages: `Meta flag contains a line or string terminator` is now `Meta flag is empty or contains whitespace or control characters`, `Opaque token contains whitespace or control characters` is now `Opaque token is empty or contains whitespace or control characters`, and `ErrorKind::OpaqueTooLong` says 31 characters instead of 32.

### Added

- Property tests that use `proptest`, a new dev-dependency. They cover the response parsers and key validation at every entry point that takes a key.
- The `cancel_at_every_poll` test target. It drops each operation after each of its polls while the reply arrives one byte at a time. Then the client must be closed, or the next request must receive its own reply.

### Fixed

- Every entry point that takes a key rejects an empty key before it writes anything ([GHSA-76fx-vq52-f53f](https://github.com/Shopify/async-memcached/security/advisories/GHSA-76fx-vq52-f53f), [GHSA-f9wj-cj54-79vv](https://github.com/Shopify/async-memcached/security/advisories/GHSA-f9wj-cj54-79vv)). An empty key left the key token out of the command line. For a storage command such as `set`, memcached rejected the line with an error and then ran the value as a command, and the reply to that command answered a later request on the same connection. Other commands read their next argument as the key: `delete_no_reply("")` deleted the key `noreply`, and `increment_no_reply` and `decrement_no_reply` sent a malformed command. The single-key calls, `meta_get_multi` and `meta_set_multi` now return an `ErrorKind::Client` error. `get_multi` and `delete_multi_no_reply` leave an empty key out of the request, and `set_multi` and `add_multi` leave it out and report that error for it.
- `set`, `add`, `set_multi`, `add_multi` and `meta_set` close the connection when memcached answers `ERROR` or `CLIENT_ERROR` ([GHSA-76fx-vq52-f53f](https://github.com/Shopify/async-memcached/security/advisories/GHSA-76fx-vq52-f53f)). Such a reply can mean that memcached rejected the command line before it read the length of the value, and then read the value as commands. Closing the connection keeps the replies to those commands from answering later requests. `set_multi` and `add_multi` now return that error for the whole call instead of for one key. A `SERVER_ERROR` reply still leaves the connection open.
- `meta_set` and `meta_set_multi` refuse an `ms` command with more than 16 meta flags before they write anything, counting the `q`, `k` and `O` flags that the client adds ([GHSA-76fx-vq52-f53f](https://github.com/Shopify/async-memcached/security/advisories/GHSA-76fx-vq52-f53f)). memcached 1.6.39 and older reject such a line before they read the length of the value, and then run the value as commands, which closing the connection cannot undo. `meta_set_multi` adds `k` and `q` to every line, so it takes at most 14 other flags.
- `version` requires the `VERSION ` prefix in its reply ([GHSA-76fx-vq52-f53f](https://github.com/Shopify/async-memcached/security/advisories/GHSA-76fx-vq52-f53f)). It previously returned the tail of any line of 8 bytes or more, so the reply to another command could pass as a version. Any other line now closes the connection and returns an error, and so do unread bytes of an earlier reply in the client's buffer, so a connection pool that checks connections with `version` discards a connection that is out of sync. Behind a proxy that answers `version` with `ERROR`, such as Twemproxy, each check now also closes the connection.
- Meta flags that are empty or contain whitespace, control characters or the DEL byte are rejected with an `ErrorKind::Client` error before anything is written, and so is an empty opaque ([GHSA-f9wj-cj54-79vv](https://github.com/Shopify/async-memcached/security/advisories/GHSA-f9wj-cj54-79vv)). Previously only `\r`, `\n` and `\0` were rejected, so a flag with a space in it added tokens to the command, and an empty flag or opaque wrote an empty or bare `O` token. Pass each flag as its own element of `meta_flags`.
- An opaque longer than 31 bytes fails with `ErrorKind::OpaqueTooLong` before anything is written. memcached limits the `O<opaque>` token to 32 bytes, so it answered a 32-byte opaque with `CLIENT_ERROR`, and after `meta_set` that reply would now also close the connection.
- `scripts/install-test-tools.sh` builds memcached on macOS 27. That SDK does not declare `kSBXProfileNoInternet`, which memcached's macOS sandbox code uses, so the build failed. The installer now skips that sandbox on macOS; the test fixture does not need it.

## [0.10.0] - 2026-11-06

### Changed

- **Breaking:** Replaced the unmaintained `fxhash` crate ([RUSTSEC-2025-0057](https://rustsec.org/advisories/RUSTSEC-2025-0057)) with `rustc-hash`. `set_multi`, `add_multi` and `stats` now return `rustc_hash::FxHashMap` instead of `fxhash::FxHashMap`. Both are `std::collections::HashMap` with a different hasher type, so code that only calls map methods compiles unchanged. Code that names the `fxhash` type must switch to `rustc_hash::FxHashMap` from `rustc-hash` 2.x or use type inference.
- Replaced the `toxiproxy_rust` dev-dependency with a small Toxiproxy HTTP client in the test support code. `toxiproxy_rust` pulled in `reqwest` 0.11, which depends on the vulnerable `h2` 0.3 ([RUSTSEC-2026-0258](https://rustsec.org/advisories/RUSTSEC-2026-0258)) and the unmaintained `rustls-pemfile` ([RUSTSEC-2025-0134](https://rustsec.org/advisories/RUSTSEC-2025-0134)). `toxiproxy_rust` has been a dev-dependency since 0.8.1, so this change does not affect users of 0.8.1 or later. Versions 0.5.0 to 0.8.0 list it as a normal dependency; their users drop it by upgrading.

## [0.9.0] - 2026-10-19

### Added

- Added `Client::is_closed` so callers (for example a connection pool's `has_broken` check) can discard a connection closed by an incomplete operation.
- `MetadumpIter::is_complete()` reports whether the dump reached `END`.

### Changed

- **Breaking:** Added `Error::ConnectionClosed` for I/O attempted after an incomplete operation closes the client. Exhaustive matches on `Error` must handle the new variant.
  - A closed client never recovers: every later call returns `ConnectionClosed` without touching the network. Discard the client and connect a new one.
  - Treat `ConnectionClosed` like `Error::Io` wherever you decide to reconnect, retry, or report an error. Checks such as `matches!(e, Error::Io(_))` compile unchanged but no longer catch this case.
  - Check `Client::is_closed()` before reusing a client: in a pool's `recycle` or `has_broken`, and when reusing a cached client slot. Otherwise the first request after a cancellation fails on the closed client.
- **Breaking:** `MetadumpIter` keeps the `Client` mutably borrowed until it is dropped, even after `next()` returns `None`, because dropping it early closes the connection. Drop it or end its scope before reusing the client.
- `MetadumpIter::next` now ends the iterator after every `Some(Err(..))`: a `BUSY` or `BADCLASS` refusal, or a network or protocol error, which also closes the connection. It previously kept returning `Some(Err(..))` and re-reading the connection on every call. `None` after an error means the dump was cut short; use `is_complete()` to tell.

### Fixed

- Close the connection when any operation is cancelled or fails before its response is complete, including early batch errors. Discard buffered writes and consume each parsed response once. This now covers every entry point: the ASCII commands (`get`, `get_multi`, `set`, `set_multi`, `add`, `add_multi`, `delete`, `increment`, `decrement` and their `_no_reply` variants), the meta commands, and `version`, `stats`, `flush_all` and `dump_keys`. Previously only the meta commands were guarded, so cancelling an ASCII `get` mid-read left the stale `VALUE` block to be returned as the answer to the next request on that connection.
- Dropping a `MetadumpIter` before it yields `None` closes the connection instead of leaving the unread dump to be parsed as the next response. Cancelling an individual `next()` call remains safe.
- `version` and `flush_all` report a server that closes the connection mid-reply as `Error::Io(UnexpectedEof)` and close the client, like every other entry point. They previously returned a protocol error and left `is_closed()` false, so the next request was wasted on the dead socket.
- A leaked operation or `MetadumpIter` (`mem::forget`, `Box::leak`) no longer lets its unread reply answer the next request. `is_closed()` reports it, and the next call closes the connection and returns `Error::ConnectionClosed`.
- `dump_keys` parses metadump entries from current memcached 1.6 releases, which append `flags=` and a trailing space after `size`. Entries from those servers previously failed to parse, so a non-empty cache could not be dumped.

## [0.8.1] - 2026-10-15

### Added

- The suite includes 49 new integration tests. The Shopify/Dalli suite guided these tests.
- The test harness starts isolated memcached and Toxiproxy processes.
- The installer uses fixed versions of the test tools.
- `TESTING.md` provides instructions for the integration tests.

### Changed

- Original integration tests now check values and per-key results.
- Oversized batch failures now use fixed positions.
- CI now discovers and runs every integration target. It also runs ignored tests.

### Fixed

- Validated keys, opaque tokens, and meta flags before writing line-oriented memcached text-protocol commands, preventing command injection through token or line delimiters.

## [0.8.0] - 2026-09-18

### Added

- Added `MetaProtocol::meta_get_multi`, which reads many keys in one round trip by pipelining quiet `mg ... k q` commands behind a single `mn`. Hits are returned with `key` populated; misses are absent.
- Added `MetaProtocol::meta_set_multi`, which stores many items in one round trip by pipelining quiet `ms ... k q` commands behind a single `mn`. Items the server refused (`NS`, `EX`, `NF`) are returned with `key` and `status` populated; an empty result means every item was stored.
- Re-exported `MetaValue` from the crate root so callers can name the type the meta protocol methods return.

### Changed

- **Breaking:** `mg` values are now returned byte-for-byte. The parser previously stripped trailing ASCII whitespace from every value, which corrupted binary payloads ending in a whitespace byte. Counters that memcached pads with trailing spaces after a shrinking `ma` are no longer trimmed; callers reading counters through `mg` should trim the padding themselves.

### Fixed

- A zero-length `mg` value (for example after a tombstoning `md … x`) left the empty data block's `\r\n` in the read buffer, misframing the next response on the connection. The terminator is now consumed.

## [0.7.0] - 2026-07-27

### Fixed

- Fixed meta protocol error parsing

## [0.6.0] - 2026-01-26

### Changed

- Updated `nom` dependency from 7.1 to 8.0 and refactored parser code to use the new `.parse()` method syntax.
- Updated `btoi` dependency from 0.4 to 0.5.
- Updated `rand` dev-dependency from 0.8 to 0.9.
- Updated `criterion` dev-dependency from 0.5 to 0.8.

## [0.5.0] - 2025-03-31

### Added
- Added `Toxiproxy` resiliency testing.
- Added crate-level validation for key lengths.  Allowing a key that is too long through to the memcached protocol can lead to multiple errors being returned for a single operation, which leaves unexpected and unread bytes on the buffer.  Future operations can be parsed incorrectly if this is left unchecked, but validating key length is a simple check to prevent this behaviour.
- Added parsing capabilities to support all meta flags.
- Added `meta_get` method to the meta protocol.
- Implemented Meta NoOp parsing for quiet mode and multi-op.
- Added `meta_set` method to the meta protocol.
- Added `meta_delete` method to the meta protocol.
- Added `meta_increment` and `meta_decrement` methods to the meta protocol to cover Meta Arithmetic functionality.
- Added panic guard on `drive_receive` to prevent panics on `split_to` calls where the buffer is empty.

### Changed
- Refactored some wall-clock benchmarks to yield more realistic results for expensive set operations.

## [0.4.0] - 2024-09-20

### Added

- Added `set_multi` method to the ASCII protocol.
- Added `flush_all` method to the ASCII protocol.
- Added `delete_multi_no_reply` method to the ASCII protocol.
- Added `add_multi` method to the ASCII protocol.

### Changed

- Changed the name of `get_many`.  This method has been renamed to `get_multi`.  `get_many` will persist as an alias, but it is now deprecated and will be removed in a future release.
- Instances of `std::collections::HashMap` have been changed to `fxhash::FxHashMap` to improve performance.
- Outlined the process of releasing a new crate version in `README.md`.

## [0.3.1] - 2024-09-09

### Changed

- `set` and `add` methods can now accept `uint`-type argument for value in addition to `&str` and `&String` types.  The original implementation used an `AsRef` trait bound, which has been replaced with a custom `AsMemcachedValue` trait bound that should cover all of the applicable incoming types.

- Fixed a bug related to DNS lookup that was preventing successful project builds in some cases.

## [0.3.0] - 2024-08-30

### Added

- Added arithmetic methods to the ASCII protocol:
  - `increment`
  - `increment_no_reply`
  - `decrement`
  - `decrement_no_reply`
- Added benchmarking suite

### Changed

- Disabled Nagle's Algorithm
- Improved README instructions and sample code for `tcp` and `uds` connections

## [0.2.0] - 2024-07-12

### Added

Implement Unix domain socket support.

## [0.1.6] - 2020-09-14

### Changed

- Changed `Client::get` to return `Option<Value>` in the non-error case to indicate hit vs miss.
- Fixed a bug where reads in a particular situation would stall if the client attempted a follow-up
  read after getting an "incomplete" protocol parse result during the last loop iteration.

### Added

- Added `Client::stats` to get a list of statistics from the server.

## [0.1.5] - 2020-09-13

### Changed

- Make `ttl` optional on `Client::set`.
- Expose metadump "BUSY" and "BADCLASS" responses via `Error`.
- Break out `ErrorKind::Generic` as `ErrorKind::NonexistentCommand` to allow for actual generic
  errors while properly capturing `ERROR\r\n` responses from memcached.

### Added

- A ton of documentation on public types.

## [0.1.4] - 2020-09-12

### Added

- Added support for dumping keys via the LRU crawler interface.

## [0.1.2] - 2020-07-13

### Changed

- Bug fixes / parsing changes.

## [0.1.1] - 2020-07-13

### Changed

- Bug fixes / parsing changes.

## [0.1.0] - 2020-07-13

### Added

- Initial commit.  Basic get/set support.
