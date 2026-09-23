use async_memcached::{AsciiProtocol, Client, Error, ErrorKind, MetaProtocol, Status};
use std::{future::Future, io, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

async fn pair() -> (Client, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = Client::new(format!("tcp://{}", listener.local_addr().unwrap()))
        .await
        .unwrap();
    let (peer, _) = listener.accept().await.unwrap();
    (client, peer)
}

async fn expect_request(peer: &mut TcpStream, expected: &[u8]) {
    let mut request = vec![0; expected.len()];
    timeout(TEST_TIMEOUT, peer.read_exact(&mut request))
        .await
        .expect("request timed out")
        .unwrap();
    assert_eq!(request, expected);
}

async fn expect_closed(peer: &mut TcpStream) {
    let mut byte = [0];
    match timeout(TEST_TIMEOUT, peer.read(&mut byte))
        .await
        .expect("connection was not closed")
    {
        Ok(0) => {}
        Err(error) if error.kind() == io::ErrorKind::ConnectionReset => {}
        result => panic!("unexpected bytes or error after close: {:?}", result),
    }
}

fn expect_connection_closed<T>(result: Result<T, Error>) {
    assert_eq!(result.map(|_| ()), Err(Error::ConnectionClosed));
}

async fn exchange<T>(
    peer: &mut TcpStream,
    request: &[u8],
    response: &[u8],
    operation: impl Future<Output = T>,
) -> T {
    timeout(TEST_TIMEOUT, async {
        let (result, ()) = tokio::join!(operation, async {
            expect_request(peer, request).await;
            peer.write_all(response).await.unwrap();
        });
        result
    })
    .await
    .expect("operation timed out")
}

async fn cancel_after_request<T>(
    peer: &mut TcpStream,
    expected: &[u8],
    operation: impl Future<Output = T>,
) {
    tokio::pin!(operation);
    tokio::select! {
        _ = &mut operation => panic!("operation completed without a response"),
        () = expect_request(peer, expected) => {}
    }
}

#[derive(Clone, Copy, Debug)]
enum MetaOperation {
    Get,
    Set,
    Delete,
    Increment,
    Decrement,
    GetMulti,
    SetMulti,
}

impl MetaOperation {
    fn request(self) -> &'static [u8] {
        match self {
            Self::Get => b"mg key v\r\n",
            Self::Set => b"ms key 5\r\nvalue\r\n",
            Self::Delete => b"md key\r\n",
            Self::Increment => b"ma key\r\n",
            Self::Decrement => b"ma key MD\r\n",
            Self::GetMulti => b"mg key v k q\r\nmg other v k q\r\nmn\r\n",
            Self::SetMulti => b"ms key 5 k q\r\nvalue\r\nms other 4 k q\r\nmore\r\nmn\r\n",
        }
    }

    async fn run(self, client: &mut Client) -> Result<(), Error> {
        match self {
            Self::Get => client
                .meta_get("key", false, None, Some(&["v"]))
                .await
                .map(|_| ()),
            Self::Set => client
                .meta_set("key", "value", false, None, None)
                .await
                .map(|_| ()),
            Self::Delete => client
                .meta_delete("key", false, None, None)
                .await
                .map(|_| ()),
            Self::Increment => client
                .meta_increment("key", false, None, None, None)
                .await
                .map(|_| ()),
            Self::Decrement => client
                .meta_decrement("key", false, None, None, None)
                .await
                .map(|_| ()),
            Self::GetMulti => client
                .meta_get_multi(&["key", "other"], Some(&["v"]))
                .await
                .map(|_| ()),
            Self::SetMulti => client
                .meta_set_multi(&[("key", "value"), ("other", "more")], None)
                .await
                .map(|_| ()),
        }
    }
}

#[tokio::test]
async fn cancellation_closes_each_meta_entry_point() {
    use MetaOperation::*;
    for operation in [Get, Set, Delete, Increment, Decrement, GetMulti, SetMulti] {
        let (mut client, mut peer) = pair().await;
        cancel_after_request(&mut peer, operation.request(), operation.run(&mut client)).await;
        assert!(
            client.is_closed(),
            "{:?} left the connection open",
            operation
        );
        expect_connection_closed(operation.run(&mut client).await);
        expect_closed(&mut peer).await;
    }
}

#[tokio::test]
async fn cancellation_after_success_rejects_delayed_response_and_later_writes() {
    let (mut client, mut peer) = pair().await;
    let first = exchange(
        &mut peer,
        b"mg prime v\r\n",
        b"VA 5\r\nprime\r\n",
        client.meta_get("prime", false, None, Some(&["v"])),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(first.data.as_deref(), Some(&b"prime"[..]));

    cancel_after_request(
        &mut peer,
        b"mg cancelled v\r\n",
        client.meta_get("cancelled", false, None, Some(&["v"])),
    )
    .await;

    assert!(client.is_closed());
    // The closed socket may reject the delayed response.
    let _ = timeout(TEST_TIMEOUT, peer.write_all(b"VA 5\r\nstale\r\n"))
        .await
        .expect("delayed response write timed out");
    timeout(TEST_TIMEOUT, async {
        expect_connection_closed(client.meta_get("next", false, None, Some(&["v"])).await);
        expect_connection_closed(client.set("next", "value", None, None).await);
    })
    .await
    .expect("closed client attempted I/O");
    expect_closed(&mut peer).await;
}

#[tokio::test]
async fn unpolled_operation_sends_nothing_and_preserves_connection() {
    let (mut client, mut peer) = pair().await;
    drop(client.meta_get("unused", false, None, Some(&["v"])));
    assert!(!client.is_closed());
    let result = exchange(
        &mut peer,
        b"mg next v\r\n",
        b"VA 4\r\nnext\r\n",
        client.meta_get("next", false, None, Some(&["v"])),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(result.data.as_deref(), Some(&b"next"[..]));
}

#[tokio::test]
async fn validation_failure_sends_nothing_and_preserves_connection() {
    let (mut client, mut peer) = pair().await;
    let long_key = "x".repeat(251);
    assert!(client.meta_get(&long_key, false, None, None).await.is_err());
    assert!(client
        .meta_get("key", false, Some(&[b'x'; 33]), None)
        .await
        .is_err());
    assert!(client
        .meta_set_multi(&[("valid", "v"), (long_key.as_str(), "v")], None)
        .await
        .is_err());
    assert!(client
        .meta_get_multi::<&str>(&[], None)
        .await
        .unwrap()
        .is_empty());
    assert!(!client.is_closed());
    assert_eq!(
        exchange(
            &mut peer,
            b"mg next v\r\n",
            b"EN\r\n",
            client.meta_get("next", false, None, Some(&["v"])),
        )
        .await
        .unwrap(),
        None
    );
}

#[tokio::test]
async fn fully_read_refusal_preserves_connection() {
    let (mut client, mut peer) = pair().await;
    let result = exchange(
        &mut peer,
        b"ms key 5 C1\r\nvalue\r\n",
        b"EX\r\n",
        client.meta_set("key", "value", false, None, Some(&["C1"])),
    )
    .await;
    assert_eq!(result, Err(Error::Protocol(Status::Exists)));
    assert!(!client.is_closed());
    let value = exchange(
        &mut peer,
        b"mg key v\r\n",
        b"VA 3\r\nold\r\n",
        client.meta_get("key", false, None, Some(&["v"])),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(value.data.as_deref(), Some(&b"old"[..]));
}

#[tokio::test]
async fn batch_error_closes_even_if_terminator_is_already_buffered() {
    for operation in [MetaOperation::GetMulti, MetaOperation::SetMulti] {
        let (mut client, mut peer) = pair().await;
        let result = exchange(
            &mut peer,
            operation.request(),
            b"SERVER_ERROR upstream error\r\nMN\r\n",
            operation.run(&mut client),
        )
        .await;
        assert_eq!(
            result,
            Err(Error::Protocol(Status::Error(ErrorKind::Server(
                "upstream error".into()
            ))))
        );
        assert!(client.is_closed());
        expect_connection_closed(client.meta_get("next", false, None, Some(&["v"])).await);
        expect_closed(&mut peer).await;
    }
}

#[tokio::test]
async fn completed_batch_preserves_refusals_and_connection() {
    let (mut client, mut peer) = pair().await;
    let refused = exchange(
        &mut peer,
        b"ms key 5 C1 k q\r\nvalue\r\nmn\r\n",
        b"EX kkey\r\nMN\r\n",
        client.meta_set_multi(&[("key", "value")], Some(&["C1"])),
    )
    .await
    .unwrap();
    assert_eq!(refused.len(), 1);
    assert_eq!(refused[0].status, Some(Status::Exists));
    assert!(!client.is_closed());

    let values = exchange(
        &mut peer,
        b"mg key v k q\r\nmg other v k q\r\nmn\r\n",
        b"VA 3 kkey\r\nold\r\nVA 4 kother\r\nmore\r\nMN\r\n",
        client.meta_get_multi(&["key", "other"], Some(&["v"])),
    )
    .await
    .unwrap();
    assert_eq!(values.len(), 2);
    assert_eq!(values[0].data.as_deref(), Some(&b"old"[..]));
    assert_eq!(values[1].data.as_deref(), Some(&b"more"[..]));
    assert!(!client.is_closed());
}

#[tokio::test]
async fn quiet_operation_cancellation_before_complete_terminator_closes_connection() {
    let (mut client, mut peer) = pair().await;
    {
        let operation = client.meta_get("key", true, None, Some(&["v"]));
        tokio::pin!(operation);
        tokio::select! {
            _ = &mut operation => panic!("operation completed without a full terminator"),
            () = async {
                expect_request(&mut peer, b"mg key v q\r\nmn\r\n").await;
                peer.write_all(b"VA 5\r\nvalue\r\nMN\r").await.unwrap();
            } => {}
        }
    }
    assert!(client.is_closed());
    expect_closed(&mut peer).await;
}

#[tokio::test]
async fn malformed_or_truncated_response_closes_connection() {
    for (response, expect_io_error) in [
        (&b"invalid response\r\n"[..], false),
        (&b"VA 5\r\npart"[..], true),
    ] {
        let (mut client, mut peer) = pair().await;
        let (result, ()) = timeout(TEST_TIMEOUT, async {
            tokio::join!(client.meta_get("key", false, None, Some(&["v"])), async {
                expect_request(&mut peer, b"mg key v\r\n").await;
                peer.write_all(response).await.unwrap();
                peer.shutdown().await.unwrap();
            })
        })
        .await
        .unwrap();
        if expect_io_error {
            assert!(
                matches!(result, Err(Error::Io(ref error)) if error.kind() == io::ErrorKind::UnexpectedEof)
            );
        } else {
            assert!(matches!(result, Err(Error::Protocol(_))));
        }
        assert!(client.is_closed());
        expect_connection_closed(client.meta_get("next", false, None, Some(&["v"])).await);
        expect_closed(&mut peer).await;
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum AsciiOperation {
    Get,
    GetMulti,
    GetMany,
    Set,
    SetMulti,
    Add,
    AddMulti,
    Delete,
    DeleteNoReply,
    DeleteMultiNoReply,
    Increment,
    IncrementNoReply,
    Decrement,
    DecrementNoReply,
}

impl AsciiOperation {
    const ALL: [Self; 14] = [
        Self::Get,
        Self::GetMulti,
        Self::GetMany,
        Self::Set,
        Self::SetMulti,
        Self::Add,
        Self::AddMulti,
        Self::Delete,
        Self::DeleteNoReply,
        Self::DeleteMultiNoReply,
        Self::Increment,
        Self::IncrementNoReply,
        Self::Decrement,
        Self::DecrementNoReply,
    ];

    /// Operations that wait for a server response and can therefore be cancelled between the
    /// request write and the response read.
    fn replies(self) -> bool {
        !matches!(
            self,
            Self::DeleteNoReply
                | Self::DeleteMultiNoReply
                | Self::IncrementNoReply
                | Self::DecrementNoReply
        )
    }

    fn request(self) -> &'static [u8] {
        match self {
            Self::Get => b"get key\r\n",
            Self::GetMulti | Self::GetMany => b"get key other\r\n",
            Self::Set => b"set key 0 0 5\r\nvalue\r\n",
            Self::SetMulti => b"set key 0 0 5\r\nvalue\r\nset other 0 0 4\r\nmore\r\n",
            Self::Add => b"add key 0 0 5\r\nvalue\r\n",
            Self::AddMulti => b"add key 0 0 5\r\nvalue\r\nadd other 0 0 4\r\nmore\r\n",
            Self::Delete => b"delete key\r\n",
            Self::DeleteNoReply => b"delete key noreply\r\n",
            Self::DeleteMultiNoReply => b"delete key noreply\r\ndelete other noreply\r\n",
            Self::Increment => b"incr key 1\r\n",
            Self::IncrementNoReply => b"incr key 1 noreply\r\n",
            Self::Decrement => b"decr key 1\r\n",
            Self::DecrementNoReply => b"decr key 1 noreply\r\n",
        }
    }

    /// A complete, successful response to `request()`.
    fn success_response(self) -> &'static [u8] {
        match self {
            Self::Get => b"VALUE key 0 5\r\nvalue\r\nEND\r\n",
            Self::GetMulti | Self::GetMany => {
                b"VALUE key 0 5\r\nvalue\r\nVALUE other 0 4\r\nmore\r\nEND\r\n"
            }
            Self::Set | Self::Add => b"STORED\r\n",
            Self::SetMulti | Self::AddMulti => b"STORED\r\nSTORED\r\n",
            Self::Delete => b"DELETED\r\n",
            Self::Increment | Self::Decrement => b"42\r\n",
            Self::DeleteNoReply
            | Self::DeleteMultiNoReply
            | Self::IncrementNoReply
            | Self::DecrementNoReply => b"",
        }
    }

    async fn run(self, client: &mut Client) -> Result<(), Error> {
        match self {
            Self::Get => client.get("key").await.map(|_| ()),
            Self::GetMulti => client.get_multi(["key", "other"]).await.map(|_| ()),
            Self::GetMany => {
                #[allow(deprecated)]
                let result = client.get_many(["key", "other"]).await;
                result.map(|_| ())
            }
            Self::Set => client.set("key", "value", None, None).await,
            Self::SetMulti => client
                .set_multi(&[("key", "value"), ("other", "more")], None, None)
                .await
                .map(|_| ()),
            Self::Add => client.add("key", "value", None, None).await,
            Self::AddMulti => client
                .add_multi(&[("key", "value"), ("other", "more")], None, None)
                .await
                .map(|_| ()),
            Self::Delete => client.delete("key").await,
            Self::DeleteNoReply => client.delete_no_reply("key").await,
            Self::DeleteMultiNoReply => client.delete_multi_no_reply(&["key", "other"]).await,
            Self::Increment => client.increment("key", 1).await.map(|_| ()),
            Self::IncrementNoReply => client.increment_no_reply("key", 1).await,
            Self::Decrement => client.decrement("key", 1).await.map(|_| ()),
            Self::DecrementNoReply => client.decrement_no_reply("key", 1).await,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum ClientOperation {
    Version,
    Stats,
    FlushAll,
    DumpKeys,
}

impl ClientOperation {
    const ALL: [Self; 4] = [Self::Version, Self::Stats, Self::FlushAll, Self::DumpKeys];

    fn request(self) -> &'static [u8] {
        match self {
            Self::Version => b"version\r\n",
            Self::Stats => b"stats\r\n",
            Self::FlushAll => b"flush_all\r\n",
            Self::DumpKeys => b"lru_crawler metadump all\r\n",
        }
    }

    fn success_response(self) -> &'static [u8] {
        match self {
            Self::Version => b"VERSION 1.6.7\r\n",
            Self::Stats => b"STAT pid 1\r\nSTAT uptime 2\r\nEND\r\n",
            Self::FlushAll => b"OK\r\n",
            Self::DumpKeys => b"key=foo exp=-1 la=1 cas=2 fetch=yes cls=1 size=3\nEND\r\n",
        }
    }

    /// Drives the whole operation, including draining the metadump iterator to `END`.
    async fn run(self, client: &mut Client) -> Result<(), Error> {
        match self {
            Self::Version => client.version().await.map(|_| ()),
            Self::Stats => client.stats().await.map(|_| ()),
            Self::FlushAll => client.flush_all().await,
            Self::DumpKeys => {
                let mut keys = client.dump_keys().await?;
                while let Some(entry) = keys.next().await {
                    entry?;
                }
                Ok(())
            }
        }
    }
}

fn replying_ascii_operations() -> impl Iterator<Item = AsciiOperation> {
    AsciiOperation::ALL
        .iter()
        .copied()
        .filter(|op| op.replies())
}

fn no_reply_ascii_operations() -> impl Iterator<Item = AsciiOperation> {
    AsciiOperation::ALL
        .iter()
        .copied()
        .filter(|op| !op.replies())
}

#[tokio::test]
async fn cancellation_closes_each_ascii_entry_point() {
    for operation in replying_ascii_operations() {
        let (mut client, mut peer) = pair().await;
        cancel_after_request(&mut peer, operation.request(), operation.run(&mut client)).await;
        assert!(
            client.is_closed(),
            "{:?} left the connection open",
            operation
        );
        expect_connection_closed(operation.run(&mut client).await);
        expect_closed(&mut peer).await;
    }
}

#[tokio::test]
async fn cancellation_closes_each_client_entry_point() {
    for operation in ClientOperation::ALL {
        let (mut client, mut peer) = pair().await;
        cancel_after_request(&mut peer, operation.request(), operation.run(&mut client)).await;
        assert!(
            client.is_closed(),
            "{:?} left the connection open",
            operation
        );
        expect_connection_closed(operation.run(&mut client).await);
        expect_closed(&mut peer).await;
    }
}

#[tokio::test]
async fn closed_client_refuses_every_entry_point_without_io() {
    let (mut client, mut peer) = pair().await;
    // Close via an already guarded meta operation so this test does not depend on the ASCII guard.
    cancel_after_request(
        &mut peer,
        b"mg key v\r\n",
        client.meta_get("key", false, None, Some(&["v"])),
    )
    .await;
    assert!(client.is_closed());
    timeout(TEST_TIMEOUT, async {
        for operation in AsciiOperation::ALL {
            expect_connection_closed(operation.run(&mut client).await);
        }
        for operation in ClientOperation::ALL {
            expect_connection_closed(operation.run(&mut client).await);
        }
    })
    .await
    .expect("closed client attempted I/O");
    // The peer sees EOF, never stray request bytes.
    expect_closed(&mut peer).await;
}

#[tokio::test]
async fn cancelled_ascii_get_rejects_delayed_value_and_later_reads() {
    let (mut client, mut peer) = pair().await;
    let first = exchange(
        &mut peer,
        b"get prime\r\n",
        b"VALUE prime 0 5\r\nprime\r\nEND\r\n",
        client.get("prime"),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(first.data.as_deref(), Some(&b"prime"[..]));

    cancel_after_request(&mut peer, b"get cancelled\r\n", client.get("cancelled")).await;
    assert!(client.is_closed());

    // The closed socket may reject the delayed response; only the client side matters.
    let _ = timeout(
        TEST_TIMEOUT,
        peer.write_all(b"VALUE cancelled 0 5\r\nstale\r\nEND\r\n"),
    )
    .await
    .expect("delayed response write timed out");
    timeout(TEST_TIMEOUT, async {
        expect_connection_closed(client.get("next").await);
        expect_connection_closed(client.increment("next", 1).await);
        expect_connection_closed(client.meta_get("next", false, None, Some(&["v"])).await);
    })
    .await
    .expect("closed client attempted I/O");
    expect_closed(&mut peer).await;
}

#[tokio::test]
async fn unpolled_ascii_and_client_futures_preserve_connection() {
    let (mut client, mut peer) = pair().await;
    drop(client.get("unused"));
    drop(client.set("unused", "v", None, None));
    drop(client.set_multi(&[("a", "b")], None, None));
    drop(client.delete_no_reply("unused"));
    drop(client.increment("unused", 1));
    drop(client.version());
    drop(client.stats());
    drop(client.flush_all());
    drop(client.dump_keys());
    assert!(!client.is_closed());
    let value = exchange(
        &mut peer,
        b"get next\r\n",
        b"VALUE next 0 4\r\nnext\r\nEND\r\n",
        client.get("next"),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(value.data.as_deref(), Some(&b"next"[..]));
}

#[tokio::test]
async fn ascii_key_too_long_sends_nothing_and_preserves_connection() {
    let (mut client, mut peer) = pair().await;
    let long_key = "x".repeat(251);
    let too_long = Err(Error::Protocol(Status::Error(ErrorKind::KeyTooLong)));
    assert_eq!(client.get(&long_key).await.map(|_| ()), too_long);
    assert_eq!(client.set(&long_key, "value", None, None).await, too_long);
    assert_eq!(client.add(&long_key, "value", None, None).await, too_long);
    assert_eq!(client.delete(&long_key).await, too_long);
    assert_eq!(client.delete_no_reply(&long_key).await, too_long);
    assert_eq!(client.increment(&long_key, 1).await.map(|_| ()), too_long);
    assert_eq!(client.increment_no_reply(&long_key, 1).await, too_long);
    assert_eq!(client.decrement(&long_key, 1).await.map(|_| ()), too_long);
    assert_eq!(client.decrement_no_reply(&long_key, 1).await, too_long);
    assert!(!client.is_closed());
    // `expect_request` would see any stray bytes ahead of `get next`.
    assert_eq!(
        exchange(&mut peer, b"get next\r\n", b"END\r\n", client.get("next"))
            .await
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn completed_ascii_operations_preserve_connection() {
    for operation in replying_ascii_operations() {
        let (mut client, mut peer) = pair().await;
        exchange(
            &mut peer,
            operation.request(),
            operation.success_response(),
            operation.run(&mut client),
        )
        .await
        .unwrap();
        assert!(!client.is_closed(), "{:?} closed the connection", operation);
        assert_eq!(
            exchange(&mut peer, b"get next\r\n", b"END\r\n", client.get("next"))
                .await
                .unwrap(),
            None,
            "{:?} left the connection out of sync",
            operation
        );
    }
}

#[tokio::test]
async fn completed_no_reply_operations_preserve_connection() {
    for operation in no_reply_ascii_operations() {
        let (mut client, mut peer) = pair().await;
        timeout(TEST_TIMEOUT, operation.run(&mut client))
            .await
            .unwrap()
            .unwrap();
        expect_request(&mut peer, operation.request()).await;
        assert!(!client.is_closed(), "{:?} closed the connection", operation);
        assert_eq!(
            exchange(&mut peer, b"get next\r\n", b"END\r\n", client.get("next"))
                .await
                .unwrap(),
            None
        );
    }
}

#[tokio::test]
async fn completed_client_operations_preserve_connection() {
    for operation in ClientOperation::ALL {
        let (mut client, mut peer) = pair().await;
        exchange(
            &mut peer,
            operation.request(),
            operation.success_response(),
            operation.run(&mut client),
        )
        .await
        .unwrap();
        assert!(!client.is_closed(), "{:?} closed the connection", operation);
        assert_eq!(
            exchange(&mut peer, b"get next\r\n", b"END\r\n", client.get("next"))
                .await
                .unwrap(),
            None
        );
    }
}

#[tokio::test]
async fn completed_ascii_multi_operations_preserve_refusals_and_connection() {
    let (mut client, mut peer) = pair().await;
    let values = exchange(
        &mut peer,
        AsciiOperation::GetMulti.request(),
        AsciiOperation::GetMulti.success_response(),
        client.get_multi(["key", "other"]),
    )
    .await
    .unwrap();
    assert_eq!(values.len(), 2);
    assert!(!client.is_closed());

    let kv = [("key", "value"), ("other", "more")];
    let results = exchange(
        &mut peer,
        AsciiOperation::SetMulti.request(),
        b"STORED\r\nNOT_STORED\r\n",
        client.set_multi(&kv, None, None),
    )
    .await
    .unwrap();
    assert_eq!(results[&"key"], Ok(()));
    assert_eq!(results[&"other"], Err(Error::Protocol(Status::NotStored)));
    assert!(!client.is_closed());

    // An oversized key is skipped on the wire and reported in the map without any I/O.
    let long_key = "x".repeat(251);
    let kv = [("ok", "v"), (long_key.as_str(), "v")];
    let results = exchange(
        &mut peer,
        b"set ok 0 0 1\r\nv\r\n",
        b"STORED\r\n",
        client.set_multi(&kv, None, None),
    )
    .await
    .unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[&"ok"], Ok(()));
    assert!(results[&long_key.as_str()].is_err());
    assert!(!client.is_closed());

    let empty: [(&str, &str); 0] = [];
    assert!(client
        .set_multi(&empty, None, None)
        .await
        .unwrap()
        .is_empty());
    assert!(!client.is_closed());

    let value = exchange(
        &mut peer,
        b"get key\r\n",
        b"VALUE key 0 5\r\nvalue\r\nEND\r\n",
        client.get("key"),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(value.data.as_deref(), Some(&b"value"[..]));
}

#[tokio::test]
async fn fully_read_ascii_status_lines_preserve_connection() {
    let (mut client, mut peer) = pair().await;
    assert_eq!(
        exchange(
            &mut peer,
            b"get key\r\n",
            b"NOT_FOUND\r\n",
            client.get("key")
        )
        .await
        .unwrap(),
        None
    );
    assert_eq!(
        exchange(
            &mut peer,
            b"set key 0 0 5\r\nvalue\r\n",
            b"NOT_STORED\r\n",
            client.set("key", "value", None, None),
        )
        .await,
        Err(Error::Protocol(Status::NotStored))
    );
    assert_eq!(
        exchange(
            &mut peer,
            b"add key 0 0 5\r\nvalue\r\n",
            b"NOT_STORED\r\n",
            client.add("key", "value", None, None),
        )
        .await,
        Err(Error::Protocol(Status::NotStored))
    );
    assert_eq!(
        exchange(
            &mut peer,
            b"delete key\r\n",
            b"NOT_FOUND\r\n",
            client.delete("key")
        )
        .await,
        Err(Error::Protocol(Status::NotFound))
    );
    assert_eq!(
        exchange(
            &mut peer,
            b"incr key 1\r\n",
            b"CLIENT_ERROR cannot increment or decrement non-numeric value\r\n",
            client.increment("key", 1),
        )
        .await,
        Err(Error::Protocol(Status::Error(ErrorKind::Client(
            "cannot increment or decrement non-numeric value".into()
        ))))
    );
    assert_eq!(
        exchange(
            &mut peer,
            b"get key\r\n",
            b"SERVER_ERROR out of memory\r\n",
            client.get("key"),
        )
        .await,
        Err(Error::Protocol(Status::Error(ErrorKind::Server(
            "out of memory".into()
        ))))
    );
    assert_eq!(
        exchange(&mut peer, b"get key\r\n", b"ERROR\r\n", client.get("key")).await,
        Err(Error::Protocol(Status::Error(
            ErrorKind::NonexistentCommand
        )))
    );

    // Per-key statuses in a batch are all consumed, including error lines.
    let kv = [("key", "value"), ("other", "more")];
    let results = exchange(
        &mut peer,
        AsciiOperation::SetMulti.request(),
        b"SERVER_ERROR out of memory\r\nSTORED\r\n",
        client.set_multi(&kv, None, None),
    )
    .await
    .unwrap();
    assert!(results[&"key"].is_err());
    assert_eq!(results[&"other"], Ok(()));

    // A complete but unexpected line is fully read by the admin commands too.
    assert!(matches!(
        exchange(&mut peer, b"version\r\n", b"ERROR\r\n", client.version()).await,
        Err(Error::Protocol(_))
    ));
    assert!(matches!(
        exchange(
            &mut peer,
            b"flush_all\r\n",
            b"ERROR\r\n",
            client.flush_all()
        )
        .await,
        Err(Error::Protocol(_))
    ));
    assert!(!client.is_closed());

    let value = exchange(
        &mut peer,
        b"get key\r\n",
        b"VALUE key 0 5\r\nvalue\r\nEND\r\n",
        client.get("key"),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(value.data.as_deref(), Some(&b"value"[..]));
}

#[tokio::test]
async fn ascii_truncated_or_malformed_response_closes_connection() {
    for (response, expect_io_error) in [
        (&b"VALUE key 0 5\r\npar"[..], true),
        (&b"garbage\r\n"[..], false),
    ] {
        let (mut client, mut peer) = pair().await;
        let (result, ()) = timeout(TEST_TIMEOUT, async {
            tokio::join!(client.get("key"), async {
                expect_request(&mut peer, b"get key\r\n").await;
                peer.write_all(response).await.unwrap();
                peer.shutdown().await.unwrap();
            })
        })
        .await
        .unwrap();
        if expect_io_error {
            assert!(
                matches!(result, Err(Error::Io(ref error)) if error.kind() == io::ErrorKind::UnexpectedEof)
            );
        } else {
            assert!(matches!(result, Err(Error::Protocol(_))));
        }
        assert!(client.is_closed());
        expect_connection_closed(client.get("next").await);
        expect_closed(&mut peer).await;
    }

    // Batch: the first status arrives, then EOF aborts the remaining reads.
    let (mut client, mut peer) = pair().await;
    let kv = [("key", "value"), ("other", "more")];
    let (result, ()) = timeout(TEST_TIMEOUT, async {
        tokio::join!(client.set_multi(&kv, None, None), async {
            expect_request(&mut peer, AsciiOperation::SetMulti.request()).await;
            peer.write_all(b"STORED\r\n").await.unwrap();
            peer.shutdown().await.unwrap();
        })
    })
    .await
    .unwrap();
    assert_eq!(
        result.map(|_| ()),
        Err(Error::Io(io::ErrorKind::UnexpectedEof.into()))
    );
    assert!(client.is_closed());
    expect_connection_closed(client.get("next").await);
}

#[tokio::test]
async fn stats_cancelled_mid_stream_closes_connection() {
    let (mut client, mut peer) = pair().await;
    {
        let operation = client.stats();
        tokio::pin!(operation);
        tokio::select! {
            result = &mut operation => panic!("stats completed without END: {:?}", result),
            () = async {
                expect_request(&mut peer, b"stats\r\n").await;
                peer.write_all(b"STAT pid 1\r\nSTAT uptime 2\r\n").await.unwrap();
                // Give the client a chance to consume the partial stream before cancelling.
                tokio::time::sleep(Duration::from_millis(50)).await;
            } => {}
        }
    }
    assert!(client.is_closed());
    let _ = peer.write_all(b"STAT curr_items 3\r\nEND\r\n").await;
    expect_connection_closed(
        timeout(TEST_TIMEOUT, client.stats())
            .await
            .expect("closed client attempted I/O"),
    );
    expect_closed(&mut peer).await;
}

#[tokio::test]
async fn dump_keys_iterator_dropped_before_end_closes_connection() {
    let (mut client, mut peer) = pair().await;
    {
        let mut keys = client.dump_keys().await.unwrap();
        expect_request(&mut peer, b"lru_crawler metadump all\r\n").await;
        peer.write_all(
            b"key=foo exp=-1 la=1 cas=2 fetch=yes cls=1 size=3\nkey=bar exp=-1 la=1 cas=3 fetch=no cls=1 size=3\n",
        )
        .await
        .unwrap();
        let first = timeout(TEST_TIMEOUT, keys.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(first.key, b"foo");
        // `bar` and `END` are still unread when the iterator is dropped.
    }
    assert!(client.is_closed());
    let _ = peer.write_all(b"END\r\n").await;
    expect_connection_closed(
        timeout(TEST_TIMEOUT, client.version())
            .await
            .expect("closed client attempted I/O"),
    );
    expect_closed(&mut peer).await;
}

#[tokio::test]
async fn dump_keys_terminal_responses_preserve_connection() {
    for (response, expect_entry, expect_error) in [
        (
            &b"key=foo exp=-1 la=1 cas=2 fetch=yes cls=1 size=3\nEND\r\n"[..],
            true,
            false,
        ),
        (&b"END\r\n"[..], false, false),
        (&b"BADCLASS 99\r\n"[..], false, true),
        // BUSY is a complete one-line refusal: the iterator ends and the connection stays usable.
        (
            &b"BUSY currently processing crawler request\r\n"[..],
            false,
            true,
        ),
    ] {
        let (mut client, mut peer) = pair().await;
        {
            let mut keys = client.dump_keys().await.unwrap();
            expect_request(&mut peer, b"lru_crawler metadump all\r\n").await;
            peer.write_all(response).await.unwrap();
            let mut saw_entry = false;
            let mut saw_error = false;
            while let Some(item) = timeout(TEST_TIMEOUT, keys.next()).await.unwrap() {
                match item {
                    Ok(_) => saw_entry = true,
                    Err(Error::Protocol(Status::Error(ErrorKind::Generic(_)))) => saw_error = true,
                    other => panic!("unexpected metadump item: {:?}", other),
                }
            }
            assert_eq!(saw_entry, expect_entry);
            assert_eq!(saw_error, expect_error);
            assert!(timeout(TEST_TIMEOUT, keys.next()).await.unwrap().is_none());
        }
        assert!(
            !client.is_closed(),
            "{:?} closed the connection",
            std::str::from_utf8(response).unwrap()
        );
        let version = exchange(
            &mut peer,
            b"version\r\n",
            b"VERSION 1.6.7\r\n",
            client.version(),
        )
        .await
        .unwrap();
        assert_eq!(version.trim_end(), "1.6.7");
    }
}

#[tokio::test]
async fn cancelled_metadump_next_keeps_iterator_usable() {
    let (mut client, mut peer) = pair().await;
    {
        let mut keys = client.dump_keys().await.unwrap();
        expect_request(&mut peer, b"lru_crawler metadump all\r\n").await;
        peer.write_all(b"key=foo exp=-1 la=1 cas=2 fetch=yes cls=1 size=")
            .await
            .unwrap();
        {
            let next = keys.next();
            tokio::pin!(next);
            tokio::select! {
                item = &mut next => panic!("next completed on a partial line: {:?}", item),
                () = tokio::time::sleep(Duration::from_millis(50)) => {}
            }
        }
        // Cancelling `next()` alone keeps the iterator, and its buffered partial line, intact.
        peer.write_all(b"3\nEND\r\n").await.unwrap();
        let first = timeout(TEST_TIMEOUT, keys.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(first.key, b"foo");
        assert!(timeout(TEST_TIMEOUT, keys.next()).await.unwrap().is_none());
    }
    assert!(!client.is_closed());
    assert_eq!(
        exchange(&mut peer, b"get next\r\n", b"END\r\n", client.get("next"))
            .await
            .unwrap(),
        None
    );
}
