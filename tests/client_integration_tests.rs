mod support;

use async_memcached::{AsciiProtocol, Client, Error, ErrorKind, MetaProtocol, Status};
use std::sync::Arc;
use support::{run, wait_for_expiration, Memcached, LARGE_PAYLOAD_SIZE, MAX_KEY_LENGTH};
use tokio::sync::Barrier;

async fn assert_value(client: &mut Client, key: &str, expected: &[u8]) {
    let value = client.get(key).await.unwrap().unwrap();
    assert_eq!(value.key, key.as_bytes());
    assert_eq!(value.data.as_deref(), Some(expected));
}

#[test]
#[ignore = "Requires the memcached executable"]
fn payloads_round_trip_across_protocols_and_transports() {
    run(async {
        let values = [
            Vec::new(),
            b"Not found".to_vec(),
            "ƒ©åÍÎ 日本語 🦀".as_bytes().to_vec(),
            (0..=255).collect::<Vec<u8>>(),
            b"\0\r\nEND\r\nMN\r\nVALUE key 0 1\r\n \t\r\n".to_vec(),
            vec![0xff; 32 * 1024],
        ];
        for server in [Memcached::tcp(), Memcached::unix()] {
            let mut client = server.client().await;
            for value in &values {
                client
                    .set("payload", value.as_slice(), None, Some(u32::MAX))
                    .await
                    .unwrap();
                assert_value(&mut client, "payload", value).await;
                let meta = client
                    .meta_get("payload", false, None, Some(&["v", "s", "f", "k"]))
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(meta.key.as_deref(), Some(b"payload".as_slice()));
                assert_eq!(meta.size, Some(value.len() as u64));
                assert_eq!(meta.flags, Some(u32::MAX));
                assert_eq!(meta.data.as_deref().unwrap_or_default(), value.as_slice());

                client
                    .meta_set("payload", value.as_slice(), false, None, Some(&["F42"]))
                    .await
                    .unwrap();
                let ascii = client.get("payload").await.unwrap().unwrap();
                assert_eq!(ascii.data.as_deref(), Some(value.as_slice()));
                assert_eq!(ascii.flags, Some(42));
                assert_eq!(client.get("missing").await.unwrap(), None);
            }
        }
    });
}

#[test]
#[ignore = "Requires the memcached executable"]
fn unsigned_value_types_preserve_their_full_range() {
    run(async {
        let server = Memcached::tcp();
        let mut client = server.client().await;
        client.set("u8", u8::MAX, None, None).await.unwrap();
        client.set("u16", u16::MAX, None, None).await.unwrap();
        client.set("u32", u32::MAX, None, None).await.unwrap();
        client.set("u64", u64::MAX, None, None).await.unwrap();
        client.set("usize", usize::MAX, None, None).await.unwrap();
        for (key, value) in [
            ("u8", u8::MAX.to_string()),
            ("u16", u16::MAX.to_string()),
            ("u32", u32::MAX.to_string()),
            ("u64", u64::MAX.to_string()),
            ("usize", usize::MAX.to_string()),
        ] {
            assert_value(&mut client, key, value.as_bytes()).await;
        }
    });
}

#[test]
#[ignore = "Requires the memcached executable"]
fn overwrite_changes_the_value_and_flags_without_stale_bytes() {
    run(async {
        let server = Memcached::tcp();
        let mut client = server.client().await;
        for value in [
            "long-value".repeat(2048),
            "x".into(),
            String::new(),
            "last".into(),
        ] {
            client
                .set("overwrite", &value, None, Some(123))
                .await
                .unwrap();
            assert_value(&mut client, "overwrite", value.as_bytes()).await;
        }
        client.set("overwrite", "new", None, None).await.unwrap();
        let result = client.get("overwrite").await.unwrap().unwrap();
        assert_eq!(result.data.as_deref(), Some(b"new".as_slice()));
        assert_eq!(result.flags, Some(0));
    });
}

#[test]
#[ignore = "Requires the memcached executable"]
fn refused_add_preserves_the_value_flags_and_expiration() {
    run(async {
        let server = Memcached::tcp();
        let mut client = server.client().await;
        client
            .set("existing", "original", None, Some(42))
            .await
            .unwrap();
        assert_eq!(
            client
                .add("existing", "replacement", Some(1), Some(99))
                .await,
            Err(Error::Protocol(Status::NotStored))
        );
        let value = client
            .meta_get("existing", false, None, Some(&["v", "f", "t"]))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(value.data.as_deref(), Some(b"original".as_slice()));
        assert_eq!(value.flags, Some(42));
        assert_eq!(value.ttl_remaining, Some(-1));
    });
}

#[test]
#[ignore = "Requires the memcached executable"]
fn delete_misses_and_noreply_commands_leave_the_connection_usable() {
    run(async {
        let server = Memcached::tcp();
        let mut client = server.client().await;
        assert_eq!(
            client.delete("absent").await,
            Err(Error::Protocol(Status::NotFound))
        );
        client.set("counter", 100_u64, None, None).await.unwrap();
        client.set("deleted", "value", None, None).await.unwrap();
        client.delete_no_reply("absent").await.unwrap();
        client.delete_no_reply("deleted").await.unwrap();
        client.increment_no_reply("counter", 20).await.unwrap();
        client.decrement_no_reply("counter", 10).await.unwrap();
        assert_value(&mut client, "counter", b"110").await;
        assert_eq!(client.get("deleted").await.unwrap(), None);
        assert_eq!(
            client.delete("deleted").await,
            Err(Error::Protocol(Status::NotFound))
        );
        assert_value(&mut client, "counter", b"110").await;
    });
}

#[test]
#[ignore = "Requires the memcached executable"]
fn ascii_set_and_add_expire_while_zero_ttl_items_survive() {
    run(async {
        let server = Memcached::tcp();
        let mut client = server.client().await;
        client.set("set", "set-value", Some(2), None).await.unwrap();
        client.add("add", "add-value", Some(2), None).await.unwrap();
        client
            .set("permanent", "keep", Some(0), None)
            .await
            .unwrap();
        assert_value(&mut client, "set", b"set-value").await;
        assert_value(&mut client, "add", b"add-value").await;
        wait_for_expiration(&mut client, "set").await;
        wait_for_expiration(&mut client, "add").await;
        assert_value(&mut client, "permanent", b"keep").await;
        client.add("add", "new", None, None).await.unwrap();
        assert_value(&mut client, "add", b"new").await;
    });
}

#[test]
#[ignore = "Requires the memcached executable"]
fn meta_set_and_pipeline_items_expire() {
    run(async {
        let server = Memcached::tcp();
        let mut client = server.client().await;
        client
            .meta_set("single", "one", false, None, Some(&["T2"]))
            .await
            .unwrap();
        assert!(client
            .meta_set_multi(&[("batch-a", "two"), ("batch-b", "three")], Some(&["T2"]))
            .await
            .unwrap()
            .is_empty());
        for (key, expected) in [("single", "one"), ("batch-a", "two"), ("batch-b", "three")] {
            assert_value(&mut client, key, expected.as_bytes()).await;
        }
        for key in ["single", "batch-a", "batch-b"] {
            wait_for_expiration(&mut client, key).await;
        }
        assert!(client
            .meta_get_multi(&["single", "batch-a", "batch-b"], Some(&["v"]))
            .await
            .unwrap()
            .is_empty());
    });
}

#[test]
#[ignore = "Requires the memcached executable"]
fn overwrite_resets_an_existing_ttl() {
    run(async {
        let server = Memcached::tcp();
        let mut client = server.client().await;
        client.set("clock", "expires", Some(2), None).await.unwrap();
        client.set("reset", "old", Some(2), None).await.unwrap();
        client.set("reset", "new", None, None).await.unwrap();
        wait_for_expiration(&mut client, "clock").await;
        assert_value(&mut client, "reset", b"new").await;
        assert_eq!(
            client
                .meta_get("reset", false, None, Some(&["t"]))
                .await
                .unwrap()
                .unwrap()
                .ttl_remaining,
            Some(-1)
        );
    });
}

#[test]
#[ignore = "Requires the memcached executable"]
fn absolute_and_past_expiration_values_follow_memcached_semantics() {
    run(async {
        let server = Memcached::tcp();
        let mut client = server.client().await;
        let time: i64 = client.stats().await.unwrap()["time"].parse().unwrap();
        client
            .set("future", "value", Some(time + 2), None)
            .await
            .unwrap();
        client
            .set("past", "value", Some(time - 60), None)
            .await
            .unwrap();
        client
            .set("negative", "value", Some(-1), None)
            .await
            .unwrap();
        assert_value(&mut client, "future", b"value").await;
        assert_eq!(client.get("past").await.unwrap(), None);
        assert_eq!(client.get("negative").await.unwrap(), None);
        wait_for_expiration(&mut client, "future").await;
    });
}

#[test]
#[ignore = "Requires the memcached executable"]
fn meta_get_can_touch_and_remove_expiration() {
    run(async {
        let server = Memcached::tcp();
        let mut client = server.client().await;
        client.set("clock", "expires", Some(2), None).await.unwrap();
        client.set("touch", "value", Some(2), None).await.unwrap();
        let touched = client
            .meta_get("touch", false, None, Some(&["v", "T60", "t"]))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(touched.data.as_deref(), Some(b"value".as_slice()));
        assert!(matches!(touched.ttl_remaining, Some(59..=60)));
        wait_for_expiration(&mut client, "clock").await;
        assert_value(&mut client, "touch", b"value").await;
        client
            .meta_get("touch", false, None, Some(&["T0"]))
            .await
            .unwrap();
        assert_eq!(
            client
                .meta_get("touch", false, None, Some(&["t"]))
                .await
                .unwrap()
                .unwrap()
                .ttl_remaining,
            Some(-1)
        );
        assert_eq!(
            client
                .meta_get("absent", false, None, Some(&["T60", "v"]))
                .await
                .unwrap(),
            None
        );
    });
}

#[test]
#[ignore = "Requires the memcached executable"]
fn tombstone_with_dropped_value_transitions_from_stale_to_miss() {
    run(async {
        let server = Memcached::tcp();
        let mut client = server.client().await;
        client.set("tombstone", "old", None, None).await.unwrap();
        client
            .meta_delete("tombstone", true, None, Some(&["I", "x", "T2"]))
            .await
            .unwrap();
        let stale = client
            .meta_get("tombstone", false, None, Some(&["v", "k", "t"]))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stale.key.as_deref(), Some(b"tombstone".as_slice()));
        assert_eq!(stale.is_stale, Some(true));
        assert_eq!(stale.is_recache_winner, Some(true));
        assert_eq!(stale.data, None);
        assert!(matches!(stale.ttl_remaining, Some(1..=2)));
        wait_for_expiration(&mut client, "tombstone").await;
        assert_eq!(
            client
                .meta_get("tombstone", false, None, Some(&["v"]))
                .await
                .unwrap(),
            None
        );
        client
            .meta_set("tombstone", "fresh", false, None, None)
            .await
            .unwrap();
        assert_value(&mut client, "tombstone", b"fresh").await;
    });
}

#[test]
#[ignore = "Requires the memcached executable"]
fn cas_round_trip_detects_other_writers_and_protects_delete() {
    run(async {
        let server = Memcached::tcp();
        let mut first = server.client().await;
        let mut second = server.client().await;
        first
            .meta_set("cas", "original", false, None, None)
            .await
            .unwrap();
        let original = first
            .meta_get("cas", false, None, Some(&["v", "c"]))
            .await
            .unwrap()
            .unwrap();
        let cas = original.cas.unwrap();
        assert!(cas > 0);
        second
            .meta_set("cas", "other-writer", false, None, None)
            .await
            .unwrap();
        let stale_cas = format!("C{cas}");
        assert_eq!(
            first
                .meta_set("cas", "lost-update", false, None, Some(&[&stale_cas]))
                .await,
            Err(Error::Protocol(Status::Exists))
        );
        assert_value(&mut first, "cas", b"other-writer").await;
        let current = first
            .meta_get("cas", false, None, Some(&["c"]))
            .await
            .unwrap()
            .unwrap()
            .cas
            .unwrap();
        assert_ne!(cas, current);
        let current_cas = format!("C{current}");
        let replaced = first
            .meta_set(
                "cas",
                "replacement",
                false,
                None,
                Some(&["MR", "c", &current_cas]),
            )
            .await
            .unwrap()
            .unwrap();
        let replacement_cas = replaced.cas.unwrap();
        assert_ne!(replacement_cas, current);
        assert_eq!(
            second
                .meta_delete("cas", false, None, Some(&[&current_cas]))
                .await,
            Err(Error::Protocol(Status::Exists))
        );
        assert_value(&mut second, "cas", b"replacement").await;
        second
            .meta_delete("cas", false, None, Some(&[&format!("C{replacement_cas}")]))
            .await
            .unwrap();
        assert_eq!(first.get("cas").await.unwrap(), None);
    });
}

#[test]
#[ignore = "Requires the memcached executable"]
fn concurrent_cas_writers_have_exactly_one_winner() {
    run(async {
        let server = Memcached::tcp();
        let mut client = server.client().await;
        client
            .set("cas-race", "original", None, None)
            .await
            .unwrap();
        let cas = client
            .meta_get("cas-race", false, None, Some(&["c"]))
            .await
            .unwrap()
            .unwrap()
            .cas
            .unwrap();
        let barrier = Arc::new(Barrier::new(8));
        let mut tasks = Vec::new();
        for id in 0..8 {
            let barrier = barrier.clone();
            let dsn = server.dsn();
            tasks.push(tokio::spawn(async move {
                let mut client = Client::new(dsn).await.unwrap();
                let value = format!("writer-{id}");
                let flag = format!("C{cas}");
                barrier.wait().await;
                let result = client
                    .meta_set("cas-race", &value, false, None, Some(&[&flag]))
                    .await;
                (value, result)
            }));
        }
        let mut winners = Vec::new();
        for task in tasks {
            let (value, result) = task.await.unwrap();
            match result {
                Ok(_) => winners.push(value),
                Err(error) => assert_eq!(error, Error::Protocol(Status::Exists)),
            }
        }
        assert_eq!(winners.len(), 1);
        assert_value(&mut client, "cas-race", winners[0].as_bytes()).await;
    });
}

#[test]
#[ignore = "Requires the memcached executable"]
fn independent_clients_increment_one_counter_atomically() {
    run(async {
        let server = Memcached::tcp();
        let mut client = server.client().await;
        client.set("counter", 0_u64, None, None).await.unwrap();
        let barrier = Arc::new(Barrier::new(8));
        let mut tasks = Vec::new();
        for id in 0..8 {
            let dsn = server.dsn();
            let barrier = barrier.clone();
            tasks.push(tokio::spawn(async move {
                let mut client = Client::new(dsn).await.unwrap();
                barrier.wait().await;
                let mut results = Vec::new();
                for _ in 0..50 {
                    let value = if id % 2 == 0 {
                        client.increment("counter", 1).await.unwrap()
                    } else {
                        let value = client
                            .meta_increment("counter", false, None, None, Some(&["v"]))
                            .await
                            .unwrap()
                            .unwrap();
                        btoi::btoi::<u64>(&value.data.unwrap()).unwrap()
                    };
                    results.push(value);
                }
                results
            }));
        }
        let mut results = Vec::new();
        for task in tasks {
            results.extend(task.await.unwrap());
        }
        results.sort_unstable();
        assert_eq!(results, (1..=400).collect::<Vec<u64>>());
        assert_value(&mut client, "counter", b"400").await;
    });
}

#[test]
#[ignore = "Requires the memcached executable"]
fn admin_commands_work_between_ascii_and_meta_operations() {
    run(async {
        for server in [Memcached::tcp(), Memcached::unix()] {
            let mut client = server.client().await;
            let version = client.version().await.unwrap();
            let before = client.stats().await.unwrap();
            assert_eq!(before["version"], version.trim());
            client.set("ascii", "one", None, None).await.unwrap();
            client
                .meta_set("meta", "two", false, None, None)
                .await
                .unwrap();
            assert_value(&mut client, "ascii", b"one").await;
            assert_value(&mut client, "meta", b"two").await;
            assert_eq!(client.get("missing").await.unwrap(), None);
            let stats = client.stats().await.unwrap();
            assert_eq!(
                stats["cmd_set"].parse::<u64>().unwrap(),
                before["cmd_set"].parse::<u64>().unwrap() + 2
            );
            assert_eq!(
                stats["get_hits"].parse::<u64>().unwrap(),
                before["get_hits"].parse::<u64>().unwrap() + 2
            );
            assert_eq!(
                stats["get_misses"].parse::<u64>().unwrap(),
                before["get_misses"].parse::<u64>().unwrap() + 1
            );
            assert_eq!(stats["curr_items"], "2");
            client.flush_all().await.unwrap();
            assert_eq!(client.get("ascii").await.unwrap(), None);
            assert_eq!(
                client
                    .meta_get("meta", false, None, Some(&["v"]))
                    .await
                    .unwrap(),
                None
            );
            client.set("after-flush", "new", None, None).await.unwrap();
            assert_value(&mut client, "after-flush", b"new").await;
            assert_eq!(client.version().await.unwrap(), version);
        }
    });
}

#[test]
#[ignore = "Requires the memcached executable"]
fn empty_key_dump_finishes_and_leaves_the_connection_usable() {
    run(async {
        let server = Memcached::tcp();
        let mut client = server.client().await;
        let mut dump = client.dump_keys().await.unwrap();
        assert_eq!(dump.next().await, None);
        assert_eq!(dump.next().await, None);
        client.set("after-dump", "new", None, None).await.unwrap();
        assert_value(&mut client, "after-dump", b"new").await;
    });
}

#[test]
#[ignore = "Requires the memcached executable"]
fn maximum_key_and_value_sizes_work_in_both_protocols() {
    run(async {
        let server = Memcached::tcp();
        let mut client = server.client().await;
        let key = "k".repeat(MAX_KEY_LENGTH);
        let value = vec![0xfe; LARGE_PAYLOAD_SIZE];
        client
            .meta_set(&key, value.as_slice(), false, None, None)
            .await
            .unwrap();
        assert_value(&mut client, &key, &value).await;
        let meta = client
            .meta_get(&key, false, None, Some(&["v", "k"]))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(meta.key.as_deref(), Some(key.as_bytes()));
        assert_eq!(meta.data, Some(value));
    });
}

#[test]
#[ignore = "Requires the memcached executable"]
fn all_single_key_operations_reject_overlong_keys_before_io() {
    run(async {
        let server = Memcached::tcp();
        let mut client = server.client().await;
        client
            .set("sentinel", "untouched", None, None)
            .await
            .unwrap();
        for key in ["x".repeat(251), "é".repeat(126)] {
            let error = || Error::Protocol(Status::Error(ErrorKind::KeyTooLong));
            assert_eq!(client.get(&key).await, Err(error()));
            assert_eq!(client.set(&key, "value", None, None).await, Err(error()));
            assert_eq!(client.add(&key, "value", None, None).await, Err(error()));
            assert_eq!(client.delete(&key).await, Err(error()));
            assert_eq!(client.delete_no_reply(&key).await, Err(error()));
            assert_eq!(client.increment(&key, 1).await, Err(error()));
            assert_eq!(client.decrement(&key, 1).await, Err(error()));
            assert_eq!(client.increment_no_reply(&key, 1).await, Err(error()));
            assert_eq!(client.decrement_no_reply(&key, 1).await, Err(error()));
            assert_eq!(client.meta_get(&key, false, None, None).await, Err(error()));
            assert_eq!(
                client.meta_set(&key, "value", false, None, None).await,
                Err(error())
            );
            assert_eq!(
                client.meta_delete(&key, false, None, None).await,
                Err(error())
            );
            assert_eq!(
                client.meta_increment(&key, false, None, None, None).await,
                Err(error())
            );
            assert_eq!(
                client.meta_decrement(&key, false, None, None, None).await,
                Err(error())
            );
            assert_value(&mut client, "sentinel", b"untouched").await;
        }
    });
}

#[test]
#[ignore = "Requires the memcached executable"]
fn opaque_length_validation_leaves_all_meta_operations_usable() {
    run(async {
        let server = Memcached::tcp();
        let mut client = server.client().await;
        let opaque = [b'o'; 33];
        let error = || Error::Protocol(Status::Error(ErrorKind::OpaqueTooLong));
        client.set("key", "123", None, None).await.unwrap();
        for quiet in [false, true] {
            assert_eq!(
                client.meta_get("key", quiet, Some(&opaque), None).await,
                Err(error())
            );
            assert_eq!(
                client
                    .meta_set("key", "changed", quiet, Some(&opaque), None)
                    .await,
                Err(error())
            );
            assert_eq!(
                client.meta_delete("key", quiet, Some(&opaque), None).await,
                Err(error())
            );
            assert_eq!(
                client
                    .meta_increment("key", quiet, Some(&opaque), None, None)
                    .await,
                Err(error())
            );
            assert_eq!(
                client
                    .meta_decrement("key", quiet, Some(&opaque), None, None)
                    .await,
                Err(error())
            );
            assert_value(&mut client, "key", b"123").await;
        }
    });
}

#[test]
#[ignore = "Requires the memcached executable"]
fn server_supported_opaque_tokens_echo_on_hits_and_misses() {
    run(async {
        let server = Memcached::tcp();
        let mut client = server.client().await;
        let opaque = [b'o'; 31];
        for quiet in [false, true] {
            client
                .meta_set("key", "123", quiet, Some(&opaque), None)
                .await
                .unwrap();
            let hit = client
                .meta_get("key", quiet, Some(&opaque), Some(&["v"]))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(hit.opaque_token.as_deref(), Some(opaque.as_slice()));
            assert_eq!(hit.data.as_deref(), Some(b"123".as_slice()));
            let increment = client
                .meta_increment("key", quiet, Some(&opaque), None, Some(&["v"]))
                .await
                .unwrap();
            if !quiet {
                assert_eq!(
                    increment.unwrap().opaque_token.as_deref(),
                    Some(opaque.as_slice())
                );
            }
            assert_value(&mut client, "key", b"124").await;
            client
                .meta_delete("key", quiet, Some(&opaque), None)
                .await
                .unwrap();
            assert_eq!(client.get("key").await.unwrap(), None);
        }
        let miss = client
            .meta_get("key", false, Some(&opaque), Some(&["v"]))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(miss.status, Some(Status::NotFound));
        assert_eq!(miss.opaque_token.as_deref(), Some(opaque.as_slice()));
        let rejected = client
            .meta_get("key", false, Some(&[b'o'; 32]), Some(&["v"]))
            .await;
        assert!(matches!(
            rejected,
            Err(Error::Protocol(Status::Error(ErrorKind::Client(_))))
        ));
        assert_eq!(client.get("key").await.unwrap(), None);
    });
}

#[test]
#[ignore = "Requires the memcached executable"]
fn invalid_read_delete_and_arithmetic_flags_leave_the_connection_usable() {
    run(async {
        let server = Memcached::tcp();
        let mut client = server.client().await;
        client.set("key", "123", None, None).await.unwrap();
        for quiet in [false, true] {
            let bad = Some(["!"].as_slice());
            let results = [
                client.meta_get("key", quiet, None, bad).await,
                client.meta_delete("key", quiet, None, bad).await,
                client.meta_increment("key", quiet, None, None, bad).await,
                client.meta_decrement("key", quiet, None, None, bad).await,
            ];
            for result in results {
                assert!(
                    matches!(
                        result,
                        Err(Error::Protocol(Status::Error(ErrorKind::Client(_))))
                    ),
                    "{:?}",
                    result
                );
            }
            assert_value(&mut client, "key", b"123").await;
            assert_eq!(
                client.meta_delete("absent", quiet, None, None).await,
                Err(Error::Protocol(Status::NotFound))
            );
            assert_value(&mut client, "key", b"123").await;
        }
    });
}

#[test]
#[ignore = "Requires the memcached executable"]
fn quiet_storage_modes_preserve_values_after_refused_operations() {
    run(async {
        let server = Memcached::tcp();
        let mut client = server.client().await;
        client
            .meta_set("key", "middle", true, None, Some(&["ME", "F42"]))
            .await
            .unwrap();
        assert_eq!(
            client
                .meta_set("key", "wrong", true, None, Some(&["ME"]))
                .await,
            Err(Error::Protocol(Status::NotStored))
        );
        assert_value(&mut client, "key", b"middle").await;
        client
            .meta_set("key", "-tail", true, None, Some(&["MA", "F99"]))
            .await
            .unwrap();
        client
            .meta_set("key", "head-", true, None, Some(&["MP", "F99"]))
            .await
            .unwrap();
        let value = client.get("key").await.unwrap().unwrap();
        assert_eq!(value.data.as_deref(), Some(b"head-middle-tail".as_slice()));
        assert_eq!(value.flags, Some(42));
        for mode in ["MR", "MA", "MP"] {
            assert_eq!(
                client
                    .meta_set("absent", "wrong", true, None, Some(&[mode]))
                    .await,
                Err(Error::Protocol(Status::NotStored))
            );
            assert_eq!(client.get("absent").await.unwrap(), None);
            assert_value(&mut client, "key", b"head-middle-tail").await;
        }
        client
            .meta_set("key", "replacement", true, None, Some(&["MR", "F99"]))
            .await
            .unwrap();
        let value = client.get("key").await.unwrap().unwrap();
        assert_eq!(value.data.as_deref(), Some(b"replacement".as_slice()));
        assert_eq!(value.flags, Some(99));
    });
}

#[test]
#[ignore = "Requires the memcached executable"]
fn meta_decrement_can_initialize_a_counter_and_preserve_its_ttl() {
    run(async {
        let server = Memcached::tcp();
        let mut client = server.client().await;
        let first = client
            .meta_decrement(
                "counter",
                false,
                None,
                Some(100),
                Some(&["N300", "J10", "v", "t"]),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.data.as_deref(), Some(b"10".as_slice()));
        assert!(matches!(first.ttl_remaining, Some(299..=300)));
        let next = client
            .meta_decrement("counter", false, None, Some(3), Some(&["N300", "J99", "v"]))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(next.data.as_deref(), Some(b"7".as_slice()));
        assert_eq!(client.increment("counter", 3).await.unwrap(), 10);
        let final_value = client
            .meta_get("counter", false, None, Some(&["v", "t"]))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(final_value.data.as_deref(), Some(b"10".as_slice()));
        assert!(matches!(final_value.ttl_remaining, Some(299..=300)));
    });
}

#[test]
#[ignore = "Requires the memcached executable"]
fn oversized_meta_values_fail_without_poisoning_the_connection() {
    run(async {
        let server = Memcached::tcp();
        let mut client = server.client().await;
        let oversized = vec![b'x'; 2 * 1024 * 1024];
        client.set("sentinel", "intact", None, None).await.unwrap();
        for quiet in [false, true] {
            let result = client
                .meta_set("too-large", oversized.as_slice(), quiet, None, None)
                .await;
            assert!(
                matches!(
                    result,
                    Err(Error::Protocol(Status::Error(ErrorKind::Server(_))))
                ),
                "{:?}",
                result
            );
            assert_eq!(client.get("too-large").await.unwrap(), None);
            assert_value(&mut client, "sentinel", b"intact").await;
        }
    });
}
