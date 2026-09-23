# Changelog

All notable changes to this project will be documented in this file.
Keep a running log of your changes with each PR under the `[Un-released] - Release Date` header.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

<!-- next-header -->

## [Unreleased] - ReleaseDate

### Added

- Added `Client::is_closed` so callers (for example a connection pool's `has_broken` check) can discard a connection closed by an incomplete operation.

### Changed

- **Breaking:** Added `Error::ConnectionClosed` for I/O attempted after an incomplete operation closes the client. Exhaustive matches on `Error` must handle the new variant.
- `MetadumpIter::next` now ends the iterator after a `BUSY` refusal, matching `BADCLASS` and `END`. It previously kept returning `Some(Err(..))` and re-reading the connection on every call.

### Fixed

- Close the connection when any operation is cancelled or fails before its response is complete, including early batch errors. Discard buffered writes and consume each parsed response once. This now covers every entry point: the ASCII commands (`get`, `get_multi`, `set`, `set_multi`, `add`, `add_multi`, `delete`, `increment`, `decrement` and their `_no_reply` variants), the meta commands, and `version`, `stats`, `flush_all` and `dump_keys`. Previously only the meta commands were guarded, so cancelling an ASCII `get` mid-read left the stale `VALUE` block to be returned as the answer to the next request on that connection.
- Dropping a `MetadumpIter` before it yields `None` closes the connection instead of leaving the unread dump to be parsed as the next response. Cancelling an individual `next()` call remains safe.

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
