use async_memcached::Client;
use criterion::{criterion_group, criterion_main, Criterion};
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

fn bench_set(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("set_small", |b| {
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

fn bench_set_large(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("set_large", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let mut client = setup_client().await;
            let large_payload = "a".repeat(LARGE_PAYLOAD_SIZE);
            let start = std::time::Instant::now();
            for _ in 0..iters {
                let _ = client.set("large_foo", &large_payload, Some(5), None).await;
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
            .set("large_foo", &large_payload, Some(3600), None)
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
                .set(key, &large_payload, Some(3600), None)
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

criterion_group!(
    benches,
    bench_get,
    bench_set,
    bench_get_many,
    bench_set_large,
    bench_get_large,
    bench_get_many_large
);
criterion_main!(benches);
