use async_memcached::Client;
use criterion::{criterion_group, criterion_main, Criterion};
use rand::Rng;
use tokio::runtime::Runtime;

const LARGE_PAYLOAD_SIZE: usize = 1000 * 1024; // Memcached's ~default maximum payload size

async fn setup_client() -> Client {
    Client::new("tcp://127.0.0.1:11211")
        .await
        .expect("failed to create client")
}

fn bench_get(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    rt.block_on(async {
        let mut client = setup_client().await;
        client.set("foo", "bar", None, None).await.unwrap();
    });

    c.bench_function("get_small", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let mut client = setup_client().await;
            let start = std::time::Instant::now();
            for _ in 0..iters {
                let _ = client.get("foo").await;
            }
            start.elapsed()
        });
    });
}

fn bench_set_with_string(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("set_small_with_string", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let mut client = setup_client().await;
            let start = std::time::Instant::now();
            for _ in 0..iters {
                let _ = client.set("foo", "bar", None, None).await;
            }
            start.elapsed()
        });
    });
}

fn bench_set_with_large_string(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("set_with_large_string", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let mut client = setup_client().await;
            let large_payload = "a".repeat(LARGE_PAYLOAD_SIZE);
            let start = std::time::Instant::now();
            for _ in 0..iters {
                let _ = client
                    .set("large_foo", large_payload.as_str(), None, None)
                    .await;
            }
            start.elapsed()
        });
    });
}

fn bench_set_no_reply_with_string(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("set_small_no_reply_with_string", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let mut client = setup_client().await;
            let start = std::time::Instant::now();
            for _ in 0..iters {
                let _ = client.set_no_reply("foo", "bar", None, None).await;
            }
            start.elapsed()
        });
    });
}

fn bench_set_no_reply_with_large_string(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("set_no_reply_large_with_string", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let mut client = setup_client().await;
            let large_payload = "a".repeat(LARGE_PAYLOAD_SIZE);
            let start = std::time::Instant::now();
            for _ in 0..iters {
                let _ = client
                    .set_no_reply("large_foo", large_payload.as_str(), None, None)
                    .await;
            }
            start.elapsed()
        });
    });
}

fn bench_set_with_u64(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("set_small_with_int", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let mut client = setup_client().await;
            let start = std::time::Instant::now();
            for _ in 0..iters {
                let _ = client.set("foo", 1_u64, None, None).await;
            }
            start.elapsed()
        });
    });
}

fn bench_add_with_string(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("add_small_with_string", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let mut client = setup_client().await;
            let start = std::time::Instant::now();
            for _ in 0..iters {
                let _ = client.add("foo", "bar", None, None).await;
            }
            start.elapsed()
        });
    });
}

fn bench_add_with_u64(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("add_small_with_int", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let mut client = setup_client().await;
            let start = std::time::Instant::now();
            for _ in 0..iters {
                let _ = client.add("foo", 1_u64, None, None).await;
            }
            start.elapsed()
        });
    });
}

fn bench_get_many(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let keys = &["foo", "bar", "baz"];

    rt.block_on(async {
        let mut client = setup_client().await;
        for key in keys {
            client.set(key, "zzz", None, None).await.unwrap();
        }
    });

    c.bench_function("get_many_small", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let mut client = setup_client().await;
            let start = std::time::Instant::now();
            for _ in 0..iters {
                let _ = client.get_many(keys).await;
            }
            start.elapsed()
        });
    });
}

fn bench_add_large_with_string(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("add_large_with_string", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let mut client = setup_client().await;
            let large_payload = "a".repeat(LARGE_PAYLOAD_SIZE);
            let start = std::time::Instant::now();
            for _ in 0..iters {
                let _ = client
                    .add("large_foo", large_payload.as_str(), None, None)
                    .await;
            }
            start.elapsed()
        });
    });
}

fn bench_set_multi_small_strings(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("set_multi_small_strings", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let mut client = setup_client().await;
            let kv = vec![("key1", "value1"), ("key2", "value2"), ("key3", "value3")];
            let start = std::time::Instant::now();
            for _ in 0..iters {
                let _ = client.set_multi(kv.clone(), None, None).await;
            }
            start.elapsed()
        });
    });
}

fn bench_set_multi_large_strings(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("set_multi_large_strings", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let mut client = setup_client().await;
            let large_payload = "a".repeat(LARGE_PAYLOAD_SIZE);
            let kv = vec![
                ("key1", &large_payload),
                ("key2", &large_payload),
                ("key3", &large_payload),
            ];
            let start = std::time::Instant::now();
            for _ in 0..iters {
                let _ = client.set_multi(kv.clone(), None, None).await;
            }
            start.elapsed()
        });
    });
}

fn bench_set_multi_with_100_large_string_values(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("set_multi_with_100_large_string_values", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let mut client = setup_client().await;

            let large_payload = "a".repeat(LARGE_PAYLOAD_SIZE);

            let mut keys = Vec::with_capacity(100);
            let key_strings: Vec<String> = (0..100).map(|i| format!("key{}", i)).collect();
            keys.extend(key_strings.iter().map(|s| s.as_str()));
            let values = vec![large_payload.as_str(); 100];

            let kv: Vec<(&str, &str)> = keys.into_iter().zip(values).collect();

            let start = std::time::Instant::now();
            for _ in 0..iters {
                let _ = client.set_multi(kv.clone(), None, None).await;
            }
            start.elapsed()
        });
    });
}

fn bench_set_multi_no_clone_with_100_large_string_values(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("set_multi_no_clone_with_100_large_string_values", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let mut client = setup_client().await;

            let large_payload = "a".repeat(LARGE_PAYLOAD_SIZE);

            let mut keys = Vec::with_capacity(100);
            let key_strings: Vec<String> = (0..100).map(|i| format!("key{}", i)).collect();
            keys.extend(key_strings.iter().map(|s| s.as_str()));
            let values = vec![large_payload.as_str(); 100];

            let kv: Vec<(&str, &str)> = keys.into_iter().zip(values).collect();

            let start = std::time::Instant::now();
            for _ in 0..iters {
                let _ = client.set_multi_no_clone(&kv, None, None).await;
            }
            start.elapsed()
        });
    });
}

fn bench_set_multi_test_two_with_100_large_string_values(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("set_multi_test_two_with_100_large_string_values", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let mut client = setup_client().await;

            let large_payload = "a".repeat(LARGE_PAYLOAD_SIZE);

            let mut keys = Vec::with_capacity(100);
            let key_strings: Vec<String> = (0..100).map(|i| format!("key{}", i)).collect();
            keys.extend(key_strings.iter().map(|s| s.as_str()));
            let values = vec![large_payload.as_str(); 100];

            let kv: Vec<(&str, &str)> = keys.into_iter().zip(values).collect();

            let start = std::time::Instant::now();
            for _ in 0..iters {
                let _ = client.set_multi_test_two(kv.clone(), None, None).await;
            }
            start.elapsed()
        });
    });
}

fn bench_set_multi_u64(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("set_multi_u64", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let mut client = setup_client().await;
            let kv = vec![("key1", 1_u64), ("key2", 2_u64), ("key3", 3_u64)];
            let start = std::time::Instant::now();
            for _ in 0..iters {
                let _ = client.set_multi(kv.clone(), None, None).await;
            }
            start.elapsed()
        });
    });
}

fn bench_get_large(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let large_payload = "a".repeat(LARGE_PAYLOAD_SIZE);

    rt.block_on(async {
        let mut client = setup_client().await;
        client
            .set("large_foo", large_payload.as_str(), None, None)
            .await
            .unwrap();
    });

    c.bench_function("get_large", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let mut client = setup_client().await;
            let start = std::time::Instant::now();
            for _ in 0..iters {
                let _ = client.get("large_foo").await;
            }
            start.elapsed()
        });
    });
}

fn bench_get_many_large(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let large_payload = "a".repeat(LARGE_PAYLOAD_SIZE);
    let keys = &["large_foo1", "large_foo2", "large_foo3"];

    rt.block_on(async {
        let mut client = setup_client().await;
        for key in keys {
            client
                .set(key, large_payload.as_str(), None, None)
                .await
                .unwrap();
        }
    });

    c.bench_function("get_many_large", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let mut client = setup_client().await;
            let start = std::time::Instant::now();
            for _ in 0..iters {
                let _ = client.get_many(keys).await;
            }
            start.elapsed()
        });
    });
}

fn bench_increment(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    rt.block_on(async {
        let mut client = setup_client().await;
        client.set("foo", 0_u64, None, None).await.unwrap();
    });

    c.bench_function("increment", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let mut client = setup_client().await;
            let start = std::time::Instant::now();
            for _ in 0..iters {
                let _ = client.increment("foo", 1).await;
            }
            start.elapsed()
        });
    });
}

fn bench_decrement(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    rt.block_on(async {
        let mut client = setup_client().await;
        client.set("baz", "99999999999", None, None).await.unwrap();
    });

    c.bench_function("decrement", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let mut client = setup_client().await;
            let start = std::time::Instant::now();
            for _ in 0..iters {
                let _ = client.decrement("foo", 1).await;
            }
            start.elapsed()
        });
    });
}

fn bench_increment_no_reply(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    rt.block_on(async {
        let mut client = setup_client().await;
        client.set("foo_two", "0", None, None).await.unwrap();
    });

    c.bench_function("increment_no_reply", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let mut client = setup_client().await;
            let start = std::time::Instant::now();
            for _ in 0..iters {
                let _ = client.increment_no_reply("foo_two", 1).await;
            }
            start.elapsed()
        });
    });
}

fn bench_decrement_no_reply(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    rt.block_on(async {
        let mut client = setup_client().await;
        client
            .set("baz_two", "99999999999", None, None)
            .await
            .unwrap();
    });

    c.bench_function("decrement_no_reply", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let mut client = setup_client().await;
            let start = std::time::Instant::now();
            for _ in 0..iters {
                let _ = client.decrement_no_reply("baz_two", 1).await;
            }
            start.elapsed()
        });
    });
}

criterion_group!(
    benches,
    // bench_get,
    // bench_get_many,
    // bench_get_large,
    // bench_get_many_large,
    bench_set_with_string,
    bench_set_with_large_string,
    bench_set_no_reply_with_string,
    bench_set_no_reply_with_large_string,
    // bench_set_with_u64,
    // bench_set_multi_small_strings,
    // bench_set_multi_large_strings,
    // bench_set_multi_no_clone_large_strings,
    // bench_set_multi_test_two_large_strings,
    // bench_set_multi_with_100_large_string_values,
    // bench_set_multi_no_clone_with_100_large_string_values,
    // bench_set_multi_test_two_with_100_large_string_values,
    // bench_set_multi_u64,
    // bench_add_with_string,
    // bench_add_with_u64,
    // bench_add_large_with_string,
    // bench_increment,
    // bench_increment_no_reply,
    // bench_decrement,
    // bench_decrement_no_reply,
);
criterion_main!(benches);
