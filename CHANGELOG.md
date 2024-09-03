# Changelog

All notable changes to this project will be documented in this file.
Keep a running log of your changes with each PR under the `[Unreleased] - ReleaseDate` header.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

<!-- next-header -->

## [Unreleased] - ReleaseDate

### Changed

- `set` and `add` methods can now accept `uint`-type argument for value in addition to `&str` and `String` types

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
