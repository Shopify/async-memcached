# async-memcache

async-memcache is an async [memcached](https://memcached.org/) client implementation for Tokio.

*Warning*: This is a work in progress crate.

## Testing

The default test suite does not require `memcached` to be running. It will ignore tests that require `memcached` to be running.
```
cargo test
```

For the full test suite, you need to have `memcached` installed and running.

```
cargo test && cargo test -- --ignored
```

## Benchmark

To run the benchmark, you need to have `memcached` installed and running.

```
memcached -p 11211 -t 10 -c 10000 -m 1024
cargo bench
```

## Examples

You can run the examples with `cargo run --example <example-name>`. Example require `memcached` running.

TCP:
```
cargo run --package async-memcached --example basic 
```

Unix:
```
memcached -p 11211 -t 10 -c 10000 -m 1024
memcached -vv -s /tmp/memcached.sock
cargo run --package async-memcached --example unix
```

## Profiling

Install `samply` with `cargo install samply`.

`samply record cargo run --package async-memcached --example unix`

## Features

This crate only targets the ASCII protocol, as the binary protocol has been deprecated and is no
longer actively being improved.

- [x] TCP connection
- [ ] UDP connection
- [x] UNIX domain socket connection
- [ ] Authentication
- [ ] TLS

## License

MIT
