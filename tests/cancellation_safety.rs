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
