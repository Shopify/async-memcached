# async-memcached

## What this codebase does

async-memcached is a Tokio-based memcached client crate for Rust, published to crates.io. One
`Client` owns one connection (`Connection::Tcp` or `Connection::Unix`, built from a DSN by
`Connection::new`) and speaks the memcached text protocol: the classic ASCII commands in
`proto/ascii_protocol.rs` and the meta commands in `proto/meta_protocol.rs`, with nom parsers
in `parser/`. It is a library only: no listener, no deploy config, no secrets. Keys, values,
flags and DSNs all come from the embedding application.

## Auth shape

- No authentication and no TLS (the README lists both as unimplemented); confidentiality and
  integrity rest entirely on the network path to memcached.
- The client's safety boundary is input validation before bytes hit the wire:
  `Client::validate_key` / `is_valid_key` (1-250 bytes, no whitespace, controls or DEL),
  `validate_opaque` (1-31 bytes), `validate_meta_flags`, and `check_meta_set_flag_count`.
- Connection integrity: `start_operation` returns an `OperationGuard` whose `Drop` closes the
  connection unless `complete()` ran; `fail_on_rejected_line` closes it after `ERROR` or
  `CLIENT_ERROR` on a command that carried a value.

## Threat model

The main risk is command injection on the text protocol: a caller-influenced key, opaque or
meta flag containing space, CR or LF splits a token or starts a second command (`flush_all`,
writes to other keys). Next is response desynchronization: if a cancelled, failed or leaked
operation leaves unread bytes and the connection is reused, the next caller reads someone
else's value — cross-request data leakage in a shared cache. Last, a malicious or compromised
server can send huge declared lengths or malformed replies to exhaust memory or cause panics.

## Project-specific patterns to flag

- Any `write_all` of caller data in `proto/` or `lib.rs` that does not first pass through the
  matching `validate_*` helper (e.g. a new command, or a multi-key path such as `get_multi`
  that writes keys in a loop).
- Any early return or `?` between `start_operation()` and `operation.complete()` that should
  close the connection but completes it, or a call to `complete()` before the full reply
  (including value bytes and `END`/`mn`) has been consumed.
- Parsers that trust a server-declared byte count (`take(len)` in `ascii_parser.rs`, the meta
  value size in `meta_parser.rs`) and let `drive_receive` grow `buf` without an upper bound, or
  that index or slice without checking length (`unwrap`, `[i]` on parser output).
- `unsafe` blocks, `unwrap()`/`expect()` on network-derived data, or integer casts of parsed
  sizes (`as usize`) that can truncate or overflow.

## Known false-positives

- `MetadumpIter` and `dump_keys` returning every key, and `flush_all`, are intended admin
  commands exposed to the embedding application.
- `Addr::Unknown` falling back to treating an unparsed DSN as `host:port` is intended.
- `tests/`, `benches/`, `examples/`, `scripts/` and `src/parser/proptests.rs` are test and
  tooling code; `tests/support/toxiproxy.rs` deliberately injects faults.
