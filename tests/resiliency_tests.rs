mod support;

use async_memcached::{AsciiProtocol, Client, Error, MetaProtocol};
use std::time::Duration;
use support::toxiproxy::ToxicMemcached;
use support::{run, runtime, within, Memcached};

fn assert_disconnect<T: std::fmt::Debug>(result: Result<T, Error>) {
    match result {
        Err(Error::Io(error)) => assert!(
            matches!(
                error.kind(),
                std::io::ErrorKind::UnexpectedEof
                    | std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::NotConnected
            ),
            "Unexpected I/O error: {:?}",
            error
        ),
        other => panic!("Expected a connection error, got {:?}", other),
    }
}

#[test]
#[ignore = "Requires the memcached executable"]
fn test_set_multi_succeeds_with_clean_client() {
    run(async {
        let server = Memcached::tcp();
        let mut client = server.client().await;
        let pairs = [
            ("clean-key1", "value1"),
            ("clean-key2", "value2"),
            ("clean-key3", "value3"),
        ];
        let results = client.set_multi(&pairs, None, None).await.unwrap();
        assert_eq!(results.len(), pairs.len());
        assert!(results.values().all(Result::is_ok));
        for (key, expected) in pairs {
            assert_eq!(
                client.get(key).await.unwrap().unwrap().data.as_deref(),
                Some(expected.as_bytes())
            );
        }
    });
}

#[test]
#[ignore = "Requires the memcached and toxiproxy-server executables"]
fn test_set_multi_errors_with_toxic_client_via_with_down() {
    let fixture = ToxicMemcached::new();
    let rt = runtime();
    let mut client = rt.block_on(within(Client::new(fixture.dsn()))).unwrap();
    let pairs = [
        ("with-down-key1", "value1"),
        ("with-down-key2", "value2"),
        ("with-down-key3", "value3"),
    ];
    fixture
        .proxy
        .with_down(|| {
            rt.block_on(within(async {
                assert_disconnect(client.set_multi(&pairs, None, None).await);
            }));
        })
        .unwrap();
    drop(client);
    rt.block_on(within(async {
        let mut clean = fixture.memcached.client().await;
        for (key, _) in pairs {
            assert_eq!(clean.get(key).await.unwrap(), None);
        }
        let mut reconnected = Client::new(fixture.dsn()).await.unwrap();
        let result = reconnected.set_multi(&pairs, None, None).await.unwrap();
        assert!(result.values().all(Result::is_ok));
    }));
}

#[test]
#[ignore = "Requires the memcached and toxiproxy-server executables"]
fn test_set_multi_errors_on_upstream_with_toxic_client_via_limit_data() {
    let fixture = ToxicMemcached::new();
    let rt = runtime();
    let mut client = rt.block_on(within(Client::new(fixture.dsn()))).unwrap();
    let pairs = [
        ("upstream-key1", "value1"),
        ("upstream-key2", "value2"),
        ("upstream-key3", "value3"),
    ];
    let command: String = pairs
        .iter()
        .map(|(key, value)| format!("set {key} 0 0 {}\r\n{value}\r\n", value.len()))
        .collect();
    let byte_limit = command.len() - 10;
    fixture
        .proxy
        .with_limit_data("upstream".into(), byte_limit as u32, 1.0)
        .apply(|| {
            rt.block_on(within(async {
                assert_disconnect(client.set_multi(&pairs, None, None).await);
            }));
        })
        .unwrap();
    rt.block_on(within(async {
        let mut clean = fixture.memcached.client().await;
        for (key, expected) in &pairs[..2] {
            assert_eq!(
                clean.get(key).await.unwrap().unwrap().data.as_deref(),
                Some(expected.as_bytes())
            );
        }
        assert_eq!(clean.get(pairs[2].0).await.unwrap(), None);
    }));
}

#[test]
#[ignore = "Requires the memcached and toxiproxy-server executables"]
fn test_set_multi_errors_on_downstream_with_toxic_client_via_limit_data() {
    let fixture = ToxicMemcached::new();
    let rt = runtime();
    let mut client = rt.block_on(within(Client::new(fixture.dsn()))).unwrap();
    let pairs = [
        ("downstream-key1", "value1"),
        ("downstream-key2", "value2"),
        ("downstream-key3", "value3"),
    ];
    fixture
        .proxy
        .with_limit_data("downstream".into(), (b"STORED\r\n".len() + 1) as u32, 1.0)
        .apply(|| {
            rt.block_on(within(async {
                assert_disconnect(client.set_multi(&pairs, None, None).await);
            }));
        })
        .unwrap();
    rt.block_on(within(async {
        let mut clean = fixture.memcached.client().await;
        for (key, expected) in pairs {
            assert_eq!(
                clean.get(key).await.unwrap().unwrap().data.as_deref(),
                Some(expected.as_bytes())
            );
        }
    }));
}

#[derive(Clone, Copy, Debug)]
enum ReadOperation {
    AsciiGet,
    AsciiMulti,
    MetaGet,
    MetaMulti,
}

const READS: [ReadOperation; 4] = [
    ReadOperation::AsciiGet,
    ReadOperation::AsciiMulti,
    ReadOperation::MetaGet,
    ReadOperation::MetaMulti,
];

impl ReadOperation {
    async fn read(self, client: &mut Client) -> Result<Vec<u8>, Error> {
        Ok(match self {
            Self::AsciiGet => client.get("key").await?.unwrap().data.unwrap(),
            Self::AsciiMulti => {
                let values = client.get_multi(&["key", "missing"]).await?;
                assert_eq!(values.len(), 1);
                assert_eq!(values[0].key, b"key");
                values[0].data.clone().unwrap()
            }
            Self::MetaGet => client
                .meta_get("key", false, None, Some(&["v"]))
                .await?
                .unwrap()
                .data
                .unwrap(),
            Self::MetaMulti => {
                let values = client
                    .meta_get_multi(&["key", "missing"], Some(&["v"]))
                    .await?;
                assert_eq!(values.len(), 1);
                assert_eq!(values[0].key.as_deref(), Some(b"key".as_slice()));
                values[0].data.clone().unwrap()
            }
        })
    }
}

#[test]
#[ignore = "Requires the memcached and toxiproxy-server executables"]
fn single_and_multi_reads_report_proxy_shutdown() {
    for operation in READS {
        let fixture = ToxicMemcached::new();
        let rt = runtime();
        let mut client = rt.block_on(within(async {
            let mut client = Client::new(fixture.dsn()).await.unwrap();
            client.set("key", "value", None, None).await.unwrap();
            assert_eq!(operation.read(&mut client).await.unwrap(), b"value");
            client
        }));
        fixture
            .proxy
            .with_down(|| {
                rt.block_on(within(async {
                    assert_disconnect(operation.read(&mut client).await);
                }));
            })
            .unwrap();
        drop(client);
        rt.block_on(within(async {
            let mut reconnected = Client::new(fixture.dsn()).await.unwrap();
            assert_eq!(operation.read(&mut reconnected).await.unwrap(), b"value");
        }));
    }
}

#[test]
#[ignore = "Requires the memcached and toxiproxy-server executables"]
fn truncated_read_bodies_never_return_partial_values() {
    for operation in READS {
        let fixture = ToxicMemcached::new();
        let rt = runtime();
        let value = vec![b'x'; 1024];
        rt.block_on(within(async {
            fixture
                .memcached
                .client()
                .await
                .set("key", value.as_slice(), None, None)
                .await
                .unwrap();
        }));
        let mut client = rt.block_on(within(Client::new(fixture.dsn()))).unwrap();
        fixture
            .proxy
            .with_limit_data("downstream".into(), 128, 1.0)
            .apply(|| {
                rt.block_on(within(async {
                    assert_disconnect(operation.read(&mut client).await);
                }));
            })
            .unwrap();
        drop(client);
        rt.block_on(within(async {
            let mut reconnected = Client::new(fixture.dsn()).await.unwrap();
            assert_eq!(operation.read(&mut reconnected).await.unwrap(), value);
        }));
    }
}

#[test]
#[ignore = "Requires the memcached and toxiproxy-server executables"]
fn fragmented_requests_and_responses_preserve_payloads() {
    let fixture = ToxicMemcached::new();
    let rt = runtime();
    let value: Vec<u8> = (0..=255).cycle().take(1024).collect();
    fixture.proxy.with_slicer("upstream".into(), 7, 0, 100, 1.0);
    fixture
        .proxy
        .with_slicer("downstream".into(), 7, 0, 100, 1.0)
        .apply(|| {
            rt.block_on(within(async {
                let mut client = Client::new(fixture.dsn()).await.unwrap();
                client
                    .meta_set("key", value.as_slice(), false, None, None)
                    .await
                    .unwrap();
                for operation in READS {
                    assert_eq!(operation.read(&mut client).await.unwrap(), value);
                }
                client
                    .set("key", value.as_slice(), None, None)
                    .await
                    .unwrap();
                assert_eq!(
                    ReadOperation::MetaMulti.read(&mut client).await.unwrap(),
                    value
                );
            }));
        })
        .unwrap();
}

#[test]
#[ignore = "Requires the memcached and toxiproxy-server executables"]
fn caller_timeouts_allow_recovery_with_a_new_connection() {
    for operation in READS {
        let fixture = ToxicMemcached::new();
        let rt = runtime();
        let mut client = rt.block_on(within(async {
            let mut client = Client::new(fixture.dsn()).await.unwrap();
            client.set("key", "value", None, None).await.unwrap();
            client
        }));
        fixture
            .proxy
            .with_timeout("downstream".into(), 0, 1.0)
            .apply(|| {
                rt.block_on(within(async {
                    let result = tokio::time::timeout(
                        Duration::from_millis(100),
                        operation.read(&mut client),
                    )
                    .await;
                    assert!(
                        result.is_err(),
                        "The blocked response did not reach the caller deadline"
                    );
                }));
                // A canceled request can leave unread bytes. Its connection is not reusable.
                drop(client);
            })
            .unwrap();
        rt.block_on(within(async {
            let mut reconnected = Client::new(fixture.dsn()).await.unwrap();
            assert_eq!(operation.read(&mut reconnected).await.unwrap(), b"value");
        }));
    }
}

#[test]
#[ignore = "Requires the memcached and toxiproxy-server executables"]
fn meta_write_pipeline_reports_disconnect_and_can_be_retried_on_a_new_client() {
    let fixture = ToxicMemcached::new();
    let rt = runtime();
    let mut client = rt.block_on(within(Client::new(fixture.dsn()))).unwrap();
    let pairs = [("a", "one"), ("b", "two")];
    fixture
        .proxy
        .with_down(|| {
            rt.block_on(within(async {
                assert_disconnect(client.meta_set_multi(&pairs, None).await);
            }));
        })
        .unwrap();
    drop(client);
    rt.block_on(within(async {
        let mut reconnected = Client::new(fixture.dsn()).await.unwrap();
        assert!(reconnected
            .meta_set_multi(&pairs, None)
            .await
            .unwrap()
            .is_empty());
        for (key, expected) in pairs {
            assert_eq!(
                reconnected.get(key).await.unwrap().unwrap().data.as_deref(),
                Some(expected.as_bytes())
            );
        }
    }));
}

#[test]
#[ignore = "Requires the memcached executable"]
fn server_restart_invalidates_tcp_and_unix_connections() {
    run(async {
        for mut server in [Memcached::tcp(), Memcached::unix()] {
            let mut client = server.client().await;
            client.set("before", "old", None, None).await.unwrap();
            let dsn = server.dsn();
            server.restart();
            assert_eq!(server.dsn(), dsn);
            assert_disconnect(client.get("before").await);
            drop(client);
            let mut reconnected = server.client().await;
            assert_eq!(reconnected.get("before").await.unwrap(), None);
            reconnected.set("after", "new", None, None).await.unwrap();
            assert_eq!(
                reconnected.get("after").await.unwrap().unwrap().data,
                Some(b"new".to_vec())
            );
        }
    });
}

#[test]
#[ignore = "Requires the memcached executable"]
fn unavailable_tcp_and_unix_servers_return_connect_errors() {
    run(async {
        for mut server in [Memcached::tcp(), Memcached::unix()] {
            server.stop();
            assert!(matches!(
                Client::new(server.dsn()).await,
                Err(Error::Connect(_))
            ));
        }
    });
}
