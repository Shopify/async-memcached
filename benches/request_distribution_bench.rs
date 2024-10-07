use std::{env, sync::LazyLock};

use async_memcached::Client;
use criterion::{criterion_group, criterion_main, Criterion};
use rand::{rngs::StdRng, seq::SliceRandom, SeedableRng};
use tokio::runtime::Runtime;

const PAYLOAD_SIZES: [(&str, usize, usize); 6] = [
    // (percentile, size_in_bytes, count_in_randomized_set)
    ("p100", 1000 * 1024, 1), // Memcached's ~default maximum payload size
    ("p99", 300 * 1024, 4),   // 300 KB
    ("p95", 100 * 1024, 5),   // 100 KB
    ("p90", 40 * 1024, 15),   // 40 KB
    ("p75", 4 * 1024, 25),    // 4 KB
    ("p50", 128, 50),         // 128 bytes
];

static RANDOMIZED_KEY_VALUE_SEED: LazyLock<Vec<(String, usize)>> = LazyLock::new(|| {
    let mut pairs = Vec::new();

    for (percentile, payload_size, count) in PAYLOAD_SIZES.iter() {
        for i in 0..*count {
            let key = format!("{}-key-{}", percentile, i);
            pairs.push((key, *payload_size));
        }
    }

    // Use seeded RNG for consistent shuffling across runs
    let mut rng = StdRng::seed_from_u64(1337);
    pairs.shuffle(&mut rng);

    pairs
});

async fn setup_client() -> Client {
    let memcached_host = env::var("MEMCACHED_HOST").unwrap_or("127.0.0.1".to_string());
    let memcached_port = env::var("MEMCACHED_PORT").unwrap_or("11211".to_string());
    Client::new(format!("tcp://{}:{}", memcached_host, memcached_port))
        .await
        .expect("failed to create client")
}

// Single set benchmarks
fn bench_set_with_strings_across_payload_distribution(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    for (percentile, payload_size, _) in PAYLOAD_SIZES.iter() {
        let bench_name = format!("set_with_{}_strings", percentile);
        c.bench_function(&bench_name, |b| {
            b.to_async(&rt).iter_custom(|iters| async move {
                let mut client = setup_client().await;
                let payload = "a".repeat(*payload_size);
                let start = std::time::Instant::now();
                for i in 0..iters {
                    let _ = client
                        .set(
                            &format!("{}-foo-{}", percentile, i),
                            payload.as_str(),
                            None,
                            None,
                        )
                        .await;
                }
                start.elapsed()
            });
        });
    }
}

// set_multi benchmarks
fn bench_set_multi_with_strings_across_payload_distribution(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    for (percentile, payload_size, _) in PAYLOAD_SIZES.iter() {
        let bench_name = format!("set_multi_with_{}_strings", percentile);
        c.bench_function(&bench_name, |b| {
            b.to_async(&rt).iter_custom(|iters| async move {
                let mut client = setup_client().await;
                let payload = "a".repeat(*payload_size);

                let mut keys = Vec::with_capacity(100);
                let key_strings: Vec<String> = (0..100)
                    .map(|i| format!("{}-foo-{}", percentile, i))
                    .collect();
                keys.extend(key_strings.iter().map(|s| s.as_str()));
                let values = vec![payload.as_str(); 100];

                let kv: Vec<(&str, &str)> = keys.into_iter().zip(values).collect();

                let start = std::time::Instant::now();
                for _ in 0..iters {
                    let _ = client.set_multi(&kv, None, None).await;
                }
                start.elapsed()
            });
        });
    }
}

fn bench_set_multi_with_consistent_requests(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("set_multi_with_consistent_requests", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let mut client = setup_client().await;

            let mut keys = Vec::with_capacity(100);
            let mut values = Vec::with_capacity(100);

            for (key, value_size) in RANDOMIZED_KEY_VALUE_SEED.iter() {
                let payload = "a".repeat(*value_size);
                keys.push(key);
                values.push(payload);
            }

            let kv: Vec<(&str, &str)> = keys
                .iter()
                .map(|s| s.as_str())
                .zip(values.iter().map(|s| s.as_str()))
                .collect();

            let start = std::time::Instant::now();
            for _ in 0..iters {
                let _ = client.set_multi(&kv, None, None).await;
            }
            start.elapsed()
        });
    });
}

// Single add benchmarks
fn bench_add_with_strings_across_payload_distribution(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    for (percentile, payload_size, _) in PAYLOAD_SIZES.iter() {
        let bench_name = format!("add_with_{}_strings", percentile);
        c.bench_function(&bench_name, |b| {
            b.to_async(&rt).iter_custom(|iters| async move {
                let mut client = setup_client().await;
                let payload = "a".repeat(*payload_size);
                let start = std::time::Instant::now();
                for i in 0..iters {
                    let _ = client
                        .add(
                            &format!("{}-foo-{}", percentile, i),
                            payload.as_str(),
                            None,
                            None,
                        )
                        .await;
                }
                start.elapsed()
            });
        });
    }
}

// add_multi benchmarks
fn bench_add_multi_with_strings_across_payload_distribution(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    for (percentile, payload_size, _) in PAYLOAD_SIZES.iter() {
        let bench_name = format!("add_multi_with_{}_strings", percentile);
        c.bench_function(&bench_name, |b| {
            b.to_async(&rt).iter_custom(|iters| async move {
                let mut client = setup_client().await;
                let payload = "a".repeat(*payload_size);

                let mut keys = Vec::with_capacity(100);
                let key_strings: Vec<String> = (0..100)
                    .map(|i| format!("{}-foo-{}", percentile, i))
                    .collect();
                keys.extend(key_strings.iter().map(|s| s.as_str()));
                let values = vec![payload.as_str(); 100];

                let kv: Vec<(&str, &str)> = keys.into_iter().zip(values).collect();

                let start = std::time::Instant::now();
                for _ in 0..iters {
                    let _ = client.add_multi(&kv, None, None).await;
                }
                start.elapsed()
            });
        });
    }
}

fn bench_add_multi_with_consistent_requests(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("add_multi_with_consistent_requests", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let mut client = setup_client().await;

            let mut keys = Vec::with_capacity(100);
            let mut values = Vec::with_capacity(100);

            for (key, value_size) in RANDOMIZED_KEY_VALUE_SEED.iter() {
                let payload = "a".repeat(*value_size);
                keys.push(key);
                values.push(payload);
            }

            let kv: Vec<(&str, &str)> = keys
                .iter()
                .map(|s| s.as_str())
                .zip(values.iter().map(|s| s.as_str()))
                .collect();

            let start = std::time::Instant::now();
            for _ in 0..iters {
                let _ = client.add_multi(&kv, None, None).await;
            }
            start.elapsed()
        });
    });
}

criterion_group!(
    benches,
    bench_set_with_strings_across_payload_distribution,
    bench_set_multi_with_strings_across_payload_distribution,
    bench_set_multi_with_consistent_requests,
    bench_add_with_strings_across_payload_distribution,
    bench_add_multi_with_strings_across_payload_distribution,
    bench_add_multi_with_consistent_requests,
);
criterion_main!(benches);
