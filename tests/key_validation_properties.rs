//! Property tests for key validation at every entry point that takes a key.
//!
//! A key goes on the wire only as one token of at most 250 bytes, with no whitespace, control
//! character or DEL byte. Otherwise it could split a command or start a second one. A scripted
//! peer reads exactly the request that each accepted key produces, and the peer must read
//! nothing for a rejected key.

mod support;

use async_memcached::{AsciiProtocol, Client, Error, ErrorKind, MetaProtocol, Status};
use proptest::collection::vec;
use proptest::prelude::*;
use proptest::sample::{select, Index};
use std::collections::HashSet;
use std::future::Future;
use std::time::Duration;
use support::{runtime, within, MAX_KEY_LENGTH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const PEER_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, PartialEq)]
enum Rejection {
    TooLong,
    Unsafe,
}

/// The rejection that the client must return for `key`, if any.
fn rejection(key: &[u8]) -> Option<Rejection> {
    if key.len() > MAX_KEY_LENGTH {
        Some(Rejection::TooLong)
    } else if key.iter().any(|&byte| byte <= b' ' || byte == 0x7f) {
        Some(Rejection::Unsafe)
    } else {
        None
    }
}

fn rejects<T>(result: &Result<T, Error>, rejection: Rejection) -> bool {
    match rejection {
        Rejection::TooLong => matches!(
            result,
            Err(Error::Protocol(Status::Error(ErrorKind::KeyTooLong)))
        ),
        Rejection::Unsafe => matches!(
            result,
            Err(Error::Protocol(Status::Error(ErrorKind::Client(_))))
        ),
    }
}

fn safe_byte() -> impl Strategy<Value = u8> {
    prop_oneof![4 => 0x21u8..=0x7e, 1 => 0x80u8..=0xff]
}

fn unsafe_byte() -> impl Strategy<Value = u8> {
    select(&b" \t\r\n\0\x0b\x0c\x1f\x7f"[..])
}

/// Keys of 1 to 300 bytes, half of them with one whitespace, control or DEL byte. Some keys are
/// padded to a length next to the limit.
fn key() -> impl Strategy<Value = Vec<u8>> {
    let safe = prop_oneof![
        3 => vec(safe_byte(), 1..=16),
        1 => vec(safe_byte(), 1..=300),
    ];
    let padding = prop::option::weighted(0.2, MAX_KEY_LENGTH - 1..=MAX_KEY_LENGTH + 2);
    let replacement = prop::option::weighted(0.5, (any::<Index>(), unsafe_byte()));
    (safe, padding, replacement).prop_map(|(mut key, padding, replacement)| {
        if let Some(length) = padding {
            key.resize(length, b'x');
        }
        if let Some((index, byte)) = replacement {
            let position = index.index(key.len());
            key[position] = byte;
        }
        key
    })
}

/// Batches with at least one accepted key, so that every batch request names a key.
fn batch() -> impl Strategy<Value = Vec<Vec<u8>>> {
    (vec(key(), 0..=3), vec(safe_byte(), 1..=16), any::<Index>()).prop_map(
        |(mut keys, safe, index)| {
            let position = index.index(keys.len() + 1);
            keys.insert(position, safe);
            keys
        },
    )
}

async fn pair() -> (Client, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = Client::new(format!("tcp://{}", listener.local_addr().unwrap()))
        .await
        .unwrap();
    let (peer, _) = listener.accept().await.unwrap();
    (client, peer)
}

/// Runs `call` while the peer reads a request of the expected length and then sends `reply`.
/// Checks that the peer read `request`.
async fn exchange<T>(
    peer: &mut TcpStream,
    request: &[u8],
    reply: &[u8],
    call: impl Future<Output = T>,
) -> Result<T, TestCaseError> {
    let (result, received) = tokio::join!(call, async {
        let mut received = vec![0; request.len()];
        tokio::time::timeout(PEER_TIMEOUT, peer.read_exact(&mut received))
            .await
            .expect("the client did not write a request of the expected length")
            .unwrap();
        peer.write_all(reply).await.unwrap();
        received
    });
    prop_assert_eq!(
        received.escape_ascii().to_string(),
        request.escape_ascii().to_string()
    );
    Ok(result)
}

/// Runs a call that must fail before it writes anything. The test case fails as soon as the
/// peer reads a byte, because the call would then wait for a reply that never comes.
async fn without_writes<T>(
    peer: &mut TcpStream,
    call: impl Future<Output = Result<T, Error>>,
) -> Result<Result<T, Error>, TestCaseError> {
    let mut byte = [0];
    tokio::select! {
        biased;
        result = call => Ok(result),
        _ = peer.read(&mut byte) => Err(TestCaseError::fail(format!(
            "the client wrote {} for a key it must reject",
            byte.escape_ascii()
        ))),
    }
}

/// Checks that the client wrote nothing after the last request the peer read.
async fn assert_no_further_writes(
    client: Client,
    peer: &mut TcpStream,
) -> Result<(), TestCaseError> {
    drop(client);
    let mut rest = Vec::new();
    tokio::time::timeout(PEER_TIMEOUT, peer.read_to_end(&mut rest))
        .await
        .expect("the client did not close its socket")
        .unwrap();
    prop_assert!(rest.is_empty(), "unexpected writes {}", rest.escape_ascii());
    Ok(())
}

fn concat_requests(keys: &[&[u8]], prefix: &[u8], suffix: &[u8]) -> Vec<u8> {
    keys.iter()
        .flat_map(|key| [prefix, *key, suffix].concat())
        .collect()
}

#[derive(Clone, Copy, Debug)]
enum KeyCall {
    Get,
    Set,
    Add,
    Delete,
    DeleteNoReply,
    Increment,
    IncrementNoReply,
    Decrement,
    DecrementNoReply,
    MetaGet,
    MetaSet,
    MetaDelete,
    MetaIncrement,
    MetaDecrement,
}

impl KeyCall {
    const ALL: [Self; 14] = [
        Self::Get,
        Self::Set,
        Self::Add,
        Self::Delete,
        Self::DeleteNoReply,
        Self::Increment,
        Self::IncrementNoReply,
        Self::Decrement,
        Self::DecrementNoReply,
        Self::MetaGet,
        Self::MetaSet,
        Self::MetaDelete,
        Self::MetaIncrement,
        Self::MetaDecrement,
    ];

    /// The request for an accepted `key`, and a reply to it that makes the call return `Ok`.
    fn wire(self, key: &[u8]) -> (Vec<u8>, &'static [u8]) {
        let (prefix, suffix, reply): (&[u8], &[u8], &'static [u8]) = match self {
            Self::Get => (b"get ", b"\r\n", b"END\r\n"),
            Self::Set => (b"set ", b" 0 0 1\r\nv\r\n", b"STORED\r\n"),
            Self::Add => (b"add ", b" 0 0 1\r\nv\r\n", b"STORED\r\n"),
            Self::Delete => (b"delete ", b"\r\n", b"DELETED\r\n"),
            Self::DeleteNoReply => (b"delete ", b" noreply\r\n", b""),
            Self::Increment => (b"incr ", b" 1\r\n", b"1\r\n"),
            Self::IncrementNoReply => (b"incr ", b" 1 noreply\r\n", b""),
            Self::Decrement => (b"decr ", b" 1\r\n", b"0\r\n"),
            Self::DecrementNoReply => (b"decr ", b" 1 noreply\r\n", b""),
            Self::MetaGet => (b"mg ", b"\r\n", b"EN\r\n"),
            Self::MetaSet => (b"ms ", b" 1\r\nv\r\n", b"HD\r\n"),
            Self::MetaDelete => (b"md ", b"\r\n", b"HD\r\n"),
            Self::MetaIncrement => (b"ma ", b"\r\n", b"HD\r\n"),
            Self::MetaDecrement => (b"ma ", b" MD\r\n", b"HD\r\n"),
        };
        ([prefix, key, suffix].concat(), reply)
    }

    async fn call(self, client: &mut Client, key: &[u8]) -> Result<(), Error> {
        match self {
            Self::Get => client.get(key).await.map(|_| ()),
            Self::Set => client.set(key, "v", None, None).await,
            Self::Add => client.add(key, "v", None, None).await,
            Self::Delete => client.delete(key).await,
            Self::DeleteNoReply => client.delete_no_reply(key).await,
            Self::Increment => client.increment(key, 1).await.map(|_| ()),
            Self::IncrementNoReply => client.increment_no_reply(key, 1).await,
            Self::Decrement => client.decrement(key, 1).await.map(|_| ()),
            Self::DecrementNoReply => client.decrement_no_reply(key, 1).await,
            Self::MetaGet => client.meta_get(key, false, None, None).await.map(|_| ()),
            Self::MetaSet => client
                .meta_set(key, "v", false, None, None)
                .await
                .map(|_| ()),
            Self::MetaDelete => client.meta_delete(key, false, None, None).await.map(|_| ()),
            Self::MetaIncrement => client
                .meta_increment(key, false, None, None, None)
                .await
                .map(|_| ()),
            Self::MetaDecrement => client
                .meta_decrement(key, false, None, None, None)
                .await
                .map(|_| ()),
        }
    }
}

proptest! {
    #[test]
    fn single_key_entry_points_write_only_accepted_keys(key in key()) {
        runtime().block_on(within(async {
            let (mut client, mut peer) = pair().await;
            for call in KeyCall::ALL {
                match rejection(&key) {
                    Some(rejection) => {
                        let result = without_writes(&mut peer, call.call(&mut client, &key));
                        let result = result.await?;
                        prop_assert!(rejects(&result, rejection), "{:?}: {:?}", call, result);
                    }
                    None => {
                        let (request, reply) = call.wire(&key);
                        let future = call.call(&mut client, &key);
                        let result = exchange(&mut peer, &request, reply, future).await?;
                        prop_assert_eq!(result, Ok(()), "{:?}", call);
                    }
                }
                prop_assert!(!client.is_closed(), "{:?} closed the client", call);
            }
            assert_no_further_writes(client, &mut peer).await
        }))?;
    }

    #[test]
    fn batch_entry_points_skip_or_refuse_keys_that_are_not_accepted(keys in batch()) {
        runtime().block_on(within(async {
            let (mut client, mut peer) = pair().await;
            let accepted: Vec<&[u8]> = keys
                .iter()
                .filter(|key| rejection(key).is_none())
                .map(Vec::as_slice)
                .collect();
            let first_rejection = keys.iter().find_map(|key| rejection(key));
            let items: Vec<(Vec<u8>, &str)> = keys.iter().map(|key| (key.clone(), "v")).collect();

            // `get_multi` leaves out each key it does not accept.
            let mut request = b"get".to_vec();
            request.extend(concat_requests(&accepted, b" ", b""));
            request.extend_from_slice(b"\r\n");
            let result = exchange(&mut peer, &request, b"END\r\n", client.get_multi(&keys)).await?;
            prop_assert_eq!(result, Err(Error::Protocol(Status::NotFound)));

            // `set_multi` and `add_multi` leave out each key they do not accept, and report it.
            for verb in ["set", "add"] {
                let prefix = format!("{verb} ");
                let request = concat_requests(&accepted, prefix.as_bytes(), b" 0 0 1\r\nv\r\n");
                let reply = b"STORED\r\n".repeat(accepted.len());
                let call = async {
                    if verb == "set" {
                        client.set_multi(&items, None, None).await
                    } else {
                        client.add_multi(&items, None, None).await
                    }
                };
                let results = exchange(&mut peer, &request, &reply, call).await?.unwrap();
                let distinct = items.iter().map(|(key, _)| key).collect::<HashSet<_>>();
                prop_assert_eq!(results.len(), distinct.len());
                for (key, result) in &results {
                    match rejection(key) {
                        Some(rejection) => {
                            prop_assert!(rejects(result, rejection), "{}: {:?}", verb, result);
                        }
                        None => prop_assert_eq!(result, &Ok(()), "{}", verb),
                    }
                }
            }

            // `delete_multi_no_reply` leaves out each key it does not accept.
            let request = concat_requests(&accepted, b"delete ", b" noreply\r\n");
            let call = client.delete_multi_no_reply(&keys);
            prop_assert_eq!(exchange(&mut peer, &request, b"", call).await?, Ok(()));

            // The meta pipelines refuse the whole batch before they write anything.
            match first_rejection {
                Some(rejection) => {
                    let result = without_writes(&mut peer, client.meta_get_multi(&keys, None));
                    let result = result.await?;
                    prop_assert!(rejects(&result, rejection), "{:?}", result);
                    let result = without_writes(&mut peer, client.meta_set_multi(&items, None));
                    let result = result.await?;
                    prop_assert!(rejects(&result, rejection), "{:?}", result);
                }
                None => {
                    let mut request = concat_requests(&accepted, b"mg ", b" k q\r\n");
                    request.extend_from_slice(b"mn\r\n");
                    let call = client.meta_get_multi(&keys, None);
                    let result = exchange(&mut peer, &request, b"MN\r\n", call).await?;
                    prop_assert_eq!(result, Ok(Vec::new()));
                    let mut request = concat_requests(&accepted, b"ms ", b" 1 k q\r\nv\r\n");
                    request.extend_from_slice(b"mn\r\n");
                    let call = client.meta_set_multi(&items, None);
                    let result = exchange(&mut peer, &request, b"MN\r\n", call).await?;
                    prop_assert_eq!(result, Ok(Vec::new()));
                }
            }
            prop_assert!(!client.is_closed());
            assert_no_further_writes(client, &mut peer).await
        }))?;
    }
}
