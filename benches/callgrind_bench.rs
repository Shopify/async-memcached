use async_memcached::Client;

use std::{hint::black_box, sync::Mutex};

use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use once_cell::sync::Lazy;
use tokio::runtime::Runtime;

const SMALL_KEY_SIZE: usize = 10;
const LARGE_KEY_SIZE: usize = 250; // Memcached's ~default maximum key size
const SMALL_PAYLOAD_SIZE: usize = 128;
const LARGE_PAYLOAD_SIZE: usize = 1000 * 1024; // Memcached's ~default maximum payload size

const MULTI_KEY_VEC: &[&str] = &["key1", "key2", "key3"];

const MULTI_KV_VEC: &[(&str, &str)] = &[("key1", "value1"), ("key2", "value2"), ("key3", "value3")];

static CLIENT: Lazy<Mutex<Client>> = Lazy::new(|| {
    let rt = Runtime::new().unwrap();
    let client = rt.block_on(async {
        Client::new("tcp://127.0.0.1:11211")
            .await
            .expect("failed to create client")
    });

    Mutex::new(client)
});

// 'get' method benchmarks
#[library_benchmark]
async fn bench_get_small() {
    let mut client = CLIENT.lock().unwrap();
    let small_key = "a".repeat(SMALL_KEY_SIZE);

    let _ = black_box(client.get(&small_key).await.unwrap());
}

#[library_benchmark]
async fn bench_get_large() {
    let mut client = CLIENT.lock().unwrap();
    let large_key = "a".repeat(LARGE_KEY_SIZE);

    let _ = black_box(client.get(&large_key).await.unwrap());
}

#[library_benchmark]
async fn bench_get_multi_small() {
    let mut client = CLIENT.lock().unwrap();

    let _ = black_box(client.get_multi(MULTI_KEY_VEC).await.unwrap());
}

// 'set' method benchmarks
#[library_benchmark]
async fn bench_set_small() {
    let mut client = CLIENT.lock().unwrap();
    let small_key = "a".repeat(SMALL_KEY_SIZE);
    let small_payload = "b".repeat(SMALL_PAYLOAD_SIZE);

    let _ = black_box(
        client
            .set(&small_key, &small_payload, None, None)
            .await
            .unwrap(),
    );
}

#[library_benchmark]
async fn bench_set_large() {
    let mut client = CLIENT.lock().unwrap();
    let small_key = "a".repeat(SMALL_KEY_SIZE);
    let large_payload = "b".repeat(LARGE_PAYLOAD_SIZE);

    let _ = black_box(
        client
            .set(&small_key, &large_payload, None, None)
            .await
            .unwrap(),
    );
}

#[library_benchmark]
async fn bench_set_multi_small() {
    let mut client = CLIENT.lock().unwrap();

    let _ = black_box(client.set_multi(MULTI_KV_VEC, None, None).await.unwrap());
}

// 'add' method benchmarks
#[library_benchmark]
async fn bench_add_small() {
    let mut client = CLIENT.lock().unwrap();
    let small_key = "a".repeat(SMALL_KEY_SIZE);
    let small_payload = "b".repeat(SMALL_PAYLOAD_SIZE);

    let _ = black_box(
        client
            .add(&small_key, &small_payload, None, None)
            .await
            .unwrap(),
    );
}

#[library_benchmark]
async fn bench_add_large() {
    let mut client = CLIENT.lock().unwrap();
    let small_key = "a".repeat(SMALL_KEY_SIZE);
    let large_payload = "b".repeat(LARGE_PAYLOAD_SIZE);

    let _ = black_box(
        client
            .add(&small_key, &large_payload, None, None)
            .await
            .unwrap(),
    );
}

#[library_benchmark]
async fn bench_add_multi_small() {
    let mut client = CLIENT.lock().unwrap();

    let _ = black_box(client.add_multi(MULTI_KV_VEC, None, None).await.unwrap());
}

// 'delete' method benchmarks
#[library_benchmark]
async fn bench_delete() {
    let mut client = CLIENT.lock().unwrap();
    let small_key = "a".repeat(SMALL_KEY_SIZE);

    let _ = black_box(client.delete(&small_key).await.unwrap());
}

#[library_benchmark]
async fn bench_delete_multi_no_reply_small() {
    let mut client = CLIENT.lock().unwrap();

    let _ = black_box(client.delete_multi_no_reply(MULTI_KEY_VEC).await.unwrap());
}

// 'increment' method benchmarks
#[library_benchmark]
async fn bench_increment() {
    let mut client = CLIENT.lock().unwrap();
    let small_key = "a".repeat(SMALL_KEY_SIZE);
    let _ = client.set(&small_key, 0_u64, None, None).await.unwrap();

    let _ = black_box(client.increment(&small_key, 1).await.unwrap());
}

#[library_benchmark]
async fn bench_increment_no_reply() {
    let mut client = CLIENT.lock().unwrap();
    let small_key = "a".repeat(SMALL_KEY_SIZE);
    let _ = client.set(&small_key, 0_u64, None, None).await.unwrap();

    let _ = black_box(client.increment_no_reply(&small_key, 1).await.unwrap());
}

// 'decrement' method benchmarks
#[library_benchmark]
async fn bench_decrement() {
    let mut client = CLIENT.lock().unwrap();
    let small_key = "a".repeat(SMALL_KEY_SIZE);
    let _ = client
        .set(&small_key, 1000000_u64, None, None)
        .await
        .unwrap();

    let _ = black_box(client.decrement(&small_key, 1).await.unwrap());
}

#[library_benchmark]
async fn bench_decrement_no_reply() {
    let mut client = CLIENT.lock().unwrap();
    let small_key = "a".repeat(SMALL_KEY_SIZE);
    let _ = client
        .set(&small_key, 1000000_u64, None, None)
        .await
        .unwrap();

    let _ = black_box(client.decrement_no_reply(&small_key, 1).await.unwrap());
}

library_benchmark_group!(
    name = bench_cache_group;
    benchmarks =
        bench_get_small,
        bench_get_large,
        bench_get_multi_small,
        bench_set_small,
        bench_set_large,
        bench_set_multi_small,
        bench_add_small,
        bench_add_large,
        bench_add_multi_small,
        bench_delete,
        bench_delete_multi_no_reply_small,
        bench_increment,
        bench_increment_no_reply,
        bench_decrement,
        bench_decrement_no_reply,
);

main!(library_benchmark_groups = bench_cache_group);
