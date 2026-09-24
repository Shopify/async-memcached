mod support;

use async_memcached::{AsciiProtocol, Error, MetaProtocol, Status};
use std::collections::BTreeMap;
use support::{run, Memcached};

fn entries(count: usize) -> Vec<(String, String)> {
    (0..count)
        .map(|index| {
            (
                format!("key-{index:04}"),
                format!("value-{index}\r\nEND\r\nMN\r\n \t"),
            )
        })
        .collect()
}

#[test]
#[ignore = "Requires the memcached executable"]
fn ascii_pipelines_return_every_value_and_flag_for_large_batches() {
    run(async {
        for server in [Memcached::tcp(), Memcached::unix()] {
            let mut client = server.client().await;
            for count in [1, 17, 1000] {
                let entries = entries(count);
                let pairs: Vec<_> = entries
                    .iter()
                    .map(|(key, value)| (key.as_str(), value.as_str()))
                    .collect();
                let results = client
                    .set_multi(&pairs, Some(300), Some(u32::MAX))
                    .await
                    .unwrap();
                assert_eq!(results.len(), count);
                assert!(results.values().all(Result::is_ok));
                let mut keys: Vec<_> = entries.iter().map(|(key, _)| key.as_str()).collect();
                keys.insert(0, "missing-first");
                keys.insert(keys.len() / 2, "missing-middle");
                keys.push("missing-last");
                let values = client.get_multi(&keys).await.unwrap();
                assert_eq!(values.len(), count);
                let mut actual = BTreeMap::new();
                for value in values {
                    assert_eq!(value.flags, Some(u32::MAX));
                    actual.insert(
                        String::from_utf8(value.key).unwrap(),
                        String::from_utf8(value.data.unwrap()).unwrap(),
                    );
                }
                assert_eq!(actual, entries.into_iter().collect());
                client.set("sentinel", "next", None, None).await.unwrap();
                assert_eq!(
                    client.get("sentinel").await.unwrap().unwrap().data,
                    Some(b"next".to_vec())
                );
            }
        }
    });
}

async fn check_meta_batches(server: Memcached, sizes: &[usize]) {
    let mut client = server.client().await;
    for &count in sizes {
        let entries = entries(count);
        let pairs: Vec<_> = entries
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect();
        assert!(client
            .meta_set_multi(&pairs, Some(&["T300", "F42", "k", "q"]))
            .await
            .unwrap()
            .is_empty());
        let mut keys: Vec<_> = entries.iter().map(|(key, _)| key.as_str()).collect();
        keys.insert(0, "missing-first");
        keys.insert(keys.len() / 2, "missing-middle");
        keys.push("missing-last");
        let values = client
            .meta_get_multi(&keys, Some(&["v", "f", "s", "c", "t", "k", "q"]))
            .await
            .unwrap();
        assert_eq!(values.len(), count);
        let mut actual = BTreeMap::new();
        for value in values {
            assert_eq!(value.flags, Some(42));
            assert!(value.cas.unwrap() > 0);
            assert!(matches!(value.ttl_remaining, Some(290..=300)));
            let data = value.data.unwrap();
            assert_eq!(value.size, Some(data.len() as u64));
            actual.insert(
                String::from_utf8(value.key.unwrap()).unwrap(),
                String::from_utf8(data).unwrap(),
            );
        }
        assert_eq!(actual, entries.into_iter().collect());
        client.set("sentinel", "next", None, None).await.unwrap();
        assert_eq!(
            client.get("sentinel").await.unwrap().unwrap().data,
            Some(b"next".to_vec())
        );
    }
}

#[test]
#[ignore = "Requires the memcached executable"]
fn tcp_meta_pipelines_return_every_value_and_flag_for_large_batches() {
    run(check_meta_batches(Memcached::tcp(), &[1, 17, 1000]));
}

#[test]
#[ignore = "Requires the memcached executable"]
fn unix_meta_pipelines_return_values_and_flags_for_small_batches() {
    run(check_meta_batches(Memcached::unix(), &[1, 17]));
}

#[test]
#[ignore = "Requires the memcached executable"]
fn pipeline_misses_and_empty_meta_batches_leave_no_pending_responses() {
    run(async {
        let server = Memcached::tcp();
        let mut client = server.client().await;
        let keys: [&str; 0] = [];
        let pairs: [(&str, &str); 0] = [];
        assert!(client
            .set_multi(&pairs, None, None)
            .await
            .unwrap()
            .is_empty());
        assert!(client
            .add_multi(&pairs, None, None)
            .await
            .unwrap()
            .is_empty());
        client.delete_multi_no_reply(&keys).await.unwrap();
        assert!(client
            .meta_get_multi(&keys, Some(&["v"]))
            .await
            .unwrap()
            .is_empty());
        assert!(client
            .meta_set_multi(&pairs, None)
            .await
            .unwrap()
            .is_empty());
        for keys in [vec!["absent"], vec!["first", "second", "third"]] {
            assert!(client
                .meta_get_multi(&keys, Some(&["v"]))
                .await
                .unwrap()
                .is_empty());
            assert_eq!(
                client.get_multi(&keys).await,
                Err(Error::Protocol(Status::NotFound))
            );
        }
        client
            .set("after-empty", "present", None, None)
            .await
            .unwrap();
        assert_eq!(
            client.get("after-empty").await.unwrap().unwrap().data,
            Some(b"present".to_vec())
        );
    });
}

#[test]
#[ignore = "Requires the memcached executable"]
fn duplicate_pipeline_keys_preserve_request_order_and_final_value() {
    run(async {
        let server = Memcached::tcp();
        let mut client = server.client().await;
        let pairs = [
            ("duplicate", "first"),
            ("other", "middle"),
            ("duplicate", "last"),
        ];
        let result = client.set_multi(&pairs, None, None).await.unwrap();
        assert_eq!(result.len(), 2);
        assert!(result.values().all(Result::is_ok));
        let ascii = client
            .get_multi(&["duplicate", "other", "duplicate"])
            .await
            .unwrap();
        assert_eq!(ascii.len(), 3);
        assert_eq!(ascii[0].data.as_deref(), Some(b"last".as_slice()));
        assert_eq!(ascii[1].data.as_deref(), Some(b"middle".as_slice()));
        assert_eq!(ascii[2], ascii[0]);
        assert!(client
            .meta_set_multi(&pairs, None)
            .await
            .unwrap()
            .is_empty());
        let meta = client
            .meta_get_multi(
                &["duplicate", "missing", "other", "duplicate"],
                Some(&["v"]),
            )
            .await
            .unwrap();
        assert_eq!(meta.len(), 3);
        assert_eq!(meta[0].key.as_deref(), Some(b"duplicate".as_slice()));
        assert_eq!(meta[0].data.as_deref(), Some(b"last".as_slice()));
        assert_eq!(meta[1].key.as_deref(), Some(b"other".as_slice()));
        assert_eq!(meta[1].data.as_deref(), Some(b"middle".as_slice()));
        assert_eq!(meta[2], meta[0]);
    });
}

#[test]
#[ignore = "Requires the memcached executable"]
fn meta_add_pipeline_reports_refusals_at_each_position() {
    run(async {
        let server = Memcached::tcp();
        let mut client = server.client().await;
        for key in ["first", "middle", "last"] {
            client.set(key, "original", None, None).await.unwrap();
        }
        let pairs = [
            ("first", "new"),
            ("new-a", "new"),
            ("middle", "new"),
            ("new-b", "new"),
            ("last", "new"),
        ];
        let failures = client.meta_set_multi(&pairs, Some(&["ME"])).await.unwrap();
        assert_eq!(failures.len(), 3);
        for (failure, key) in failures.iter().zip(["first", "middle", "last"]) {
            assert_eq!(failure.key.as_deref(), Some(key.as_bytes()));
            assert_eq!(failure.status, Some(Status::NotStored));
        }
        for (key, _) in pairs {
            let expected = if key.starts_with("new-") {
                "new"
            } else {
                "original"
            };
            assert_eq!(
                client.get(key).await.unwrap().unwrap().data.as_deref(),
                Some(expected.as_bytes())
            );
        }
    });
}

#[test]
#[ignore = "Requires the memcached executable"]
fn meta_cas_pipeline_distinguishes_conflicts_from_missing_keys() {
    run(async {
        let server = Memcached::tcp();
        let mut client = server.client().await;
        client.set("matching", "first", None, None).await.unwrap();
        client.set("conflict", "second", None, None).await.unwrap();
        let cas = client
            .meta_get("matching", false, None, Some(&["c"]))
            .await
            .unwrap()
            .unwrap()
            .cas
            .unwrap();
        let flag = format!("C{cas}");
        let failures = client
            .meta_set_multi(
                &[("matching", "new"), ("conflict", "new"), ("missing", "new")],
                Some(&[&flag]),
            )
            .await
            .unwrap();
        assert_eq!(failures.len(), 2);
        assert_eq!(failures[0].key.as_deref(), Some(b"conflict".as_slice()));
        assert_eq!(failures[0].status, Some(Status::Exists));
        assert_eq!(failures[1].key.as_deref(), Some(b"missing".as_slice()));
        assert_eq!(failures[1].status, Some(Status::NotFound));
        assert_eq!(
            client.get("matching").await.unwrap().unwrap().data,
            Some(b"new".to_vec())
        );
        assert_eq!(
            client.get("conflict").await.unwrap().unwrap().data,
            Some(b"second".to_vec())
        );
        assert_eq!(client.get("missing").await.unwrap(), None);
    });
}

#[test]
#[ignore = "Requires the memcached executable"]
fn meta_pipeline_distinguishes_empty_hits_tombstones_and_misses() {
    run(async {
        let server = Memcached::tcp();
        let mut client = server.client().await;
        assert!(client
            .meta_set_multi(
                &[
                    ("hit", "normal"),
                    ("empty", ""),
                    ("stale", "retained"),
                    ("dropped", "discarded")
                ],
                None
            )
            .await
            .unwrap()
            .is_empty());
        client
            .meta_delete("stale", false, None, Some(&["I"]))
            .await
            .unwrap();
        client
            .meta_delete("dropped", false, None, Some(&["I", "x"]))
            .await
            .unwrap();
        let values = client
            .meta_get_multi(
                &["missing", "hit", "empty", "stale", "dropped"],
                Some(&["v"]),
            )
            .await
            .unwrap();
        assert_eq!(values.len(), 4);
        let values: BTreeMap<_, _> = values
            .into_iter()
            .map(|value| (value.key.clone().unwrap(), value))
            .collect();
        let hit = &values[b"hit".as_slice()];
        let empty = &values[b"empty".as_slice()];
        let stale = &values[b"stale".as_slice()];
        let dropped = &values[b"dropped".as_slice()];
        assert_eq!(hit.data.as_deref(), Some(b"normal".as_slice()));
        assert_ne!(hit.is_stale, Some(true));
        assert_eq!(empty.data, None);
        assert_ne!(empty.is_stale, Some(true));
        assert_eq!(stale.data.as_deref(), Some(b"retained".as_slice()));
        assert_eq!(stale.is_stale, Some(true));
        assert_eq!(dropped.data, None);
        assert_eq!(dropped.is_stale, Some(true));
        assert_eq!(
            client.get("hit").await.unwrap().unwrap().data,
            Some(b"normal".to_vec())
        );
    });
}

#[test]
#[ignore = "Requires the memcached executable"]
fn metadata_only_pipeline_returns_keys_without_value_bytes() {
    run(async {
        let server = Memcached::tcp();
        let mut client = server.client().await;
        assert!(client
            .meta_set_multi(&[("a", "one"), ("b", "two")], Some(&["F42"]))
            .await
            .unwrap()
            .is_empty());
        let values = client
            .meta_get_multi(&["a", "absent", "b"], Some(&["f", "s"]))
            .await
            .unwrap();
        assert_eq!(values.len(), 2);
        for (value, key) in values.iter().zip(["a", "b"]) {
            assert_eq!(value.key.as_deref(), Some(key.as_bytes()));
            assert_eq!(value.flags, Some(42));
            assert_eq!(value.size, Some(3));
            assert_eq!(value.data, None);
        }
        assert_eq!(
            client.get("a").await.unwrap().unwrap().data,
            Some(b"one".to_vec())
        );
    });
}

#[test]
#[ignore = "Requires the memcached executable"]
fn batch_delete_removes_all_hits_and_tolerates_misses() {
    run(async {
        let server = Memcached::tcp();
        let mut client = server.client().await;
        let entries = entries(1000);
        let pairs: Vec<_> = entries
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect();
        assert!(client
            .meta_set_multi(&pairs, None)
            .await
            .unwrap()
            .is_empty());
        let mut keys: Vec<_> = entries.iter().map(|(key, _)| key.as_str()).collect();
        keys.push("absent");
        client.delete_multi_no_reply(&keys).await.unwrap();
        assert!(client
            .meta_get_multi(&keys, Some(&["v"]))
            .await
            .unwrap()
            .is_empty());
        client.delete_multi_no_reply(&keys).await.unwrap();
        client
            .set("after-delete", "present", None, None)
            .await
            .unwrap();
        assert_eq!(
            client.get("after-delete").await.unwrap().unwrap().data,
            Some(b"present".to_vec())
        );
    });
}
