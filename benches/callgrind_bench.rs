use async_memcached::Client;

use std::{hint::black_box, sync::Mutex};

use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use once_cell::sync::Lazy;
use tokio::runtime::Runtime;

const LARGE_PAYLOAD_SIZE: usize = 1000 * 1024; // Memcached's ~default maximum payload size

static CLIENT: Lazy<Mutex<Client>> = Lazy::new(|| {
    let rt = Runtime::new().unwrap();
    let client = rt.block_on(async {
        Client::new("tcp://127.0.0.1:11211")
            .await
            .expect("failed to create client")
    });
    Mutex::new(client)
});

#[library_benchmark]
async fn bench_get_small() {
    let mut client = CLIENT.lock().unwrap();
    let small_key = "foo";

    let _ = black_box(client.get(small_key).await.unwrap());
}

#[library_benchmark]
async fn bench_get_large() {
    let mut client = CLIENT.lock().unwrap();
    let large_key = "a".repeat(230);

    let _ = black_box(client.get(large_key).await.unwrap());
}

library_benchmark_group!(
    name = bench_cache_group;
    benchmarks =
        bench_get_small,
        bench_get_large,
);

main!(library_benchmark_groups = bench_cache_group);
