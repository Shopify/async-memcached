//! Drops each operation after each of its polls while its reply arrives one byte at a time.
//!
//! For each operation and reply, the harness polls the operation `n` times and then drops it,
//! for each `n` from 1 until the operation completes. The peer reads the request after the first
//! poll and sends one more byte of the reply after each later poll. The rest of the reply
//! arrives after the drop. Then the client must be closed, or the next request must receive its
//! own reply.

use async_memcached::{AsciiProtocol, Client, Error, ErrorKind, MetaProtocol, MetaValue, Status};
use std::future::{poll_fn, Future};
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::pin::{pin, Pin};
use std::task::Poll;
use std::time::Duration;
use tokio::time::timeout;

const TIMEOUT: Duration = Duration::from_secs(5);

/// The server side of the connection. It uses blocking sockets, so the only waker registered
/// on the test task is the one for the client's socket.
struct Peer(TcpStream);

impl Peer {
    fn expect_request(&mut self, expected: &[u8]) {
        let mut request = vec![0; expected.len()];
        self.0
            .read_exact(&mut request)
            .expect("the client did not write the whole request");
        assert_eq!(
            request.escape_ascii().to_string(),
            expected.escape_ascii().to_string()
        );
    }

    fn send(&mut self, bytes: &[u8]) {
        self.0.write_all(bytes).unwrap();
    }

    /// Sends bytes that a closed client can refuse.
    fn send_late(&mut self, bytes: &[u8]) {
        let _ = self.0.write_all(bytes);
    }

    fn expect_closed(&mut self, label: &str) {
        let mut byte = [0];
        match self.0.read(&mut byte) {
            Ok(0) => {}
            Err(error) if error.kind() == io::ErrorKind::ConnectionReset => {}
            other => panic!(
                "{}: expected the client to close its socket, got {:?}",
                label, other
            ),
        }
    }
}

async fn connect() -> (Client, Peer) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let client = Client::new(format!("tcp://{}", listener.local_addr().unwrap()))
        .await
        .unwrap();
    let (stream, _) = listener.accept().unwrap();
    stream.set_nodelay(true).unwrap();
    stream.set_read_timeout(Some(TIMEOUT)).unwrap();
    stream.set_write_timeout(Some(TIMEOUT)).unwrap();
    (client, Peer(stream))
}

async fn poll_once<F: Future>(future: &mut Pin<&mut F>) -> Poll<F::Output> {
    poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx))).await
}

/// Returns `Pending` once without a wake-up, so the task continues only when the I/O that the
/// operation waits for wakes it.
async fn until_woken() {
    let mut waited = false;
    poll_fn(|_| {
        if waited {
            Poll::Ready(())
        } else {
            waited = true;
            Poll::Pending
        }
    })
    .await
}

enum Run<T> {
    /// The operation returned after the peer sent `sent` bytes of the reply.
    Completed { output: T, sent: usize },
    /// The operation was dropped after the peer sent `sent` bytes of the reply.
    Cancelled { sent: usize },
}

/// Polls `operation` up to `polls` times, with one more reply byte before each poll after the
/// first, and drops it if it has not completed.
async fn drive<T>(
    operation: impl Future<Output = T>,
    peer: &mut Peer,
    request: &[u8],
    reply: &[u8],
    polls: usize,
) -> Run<T> {
    let mut operation = pin!(operation);
    let mut sent = 0;
    for poll in 1..=polls {
        let result = poll_once(&mut operation).await;
        if poll == 1 {
            peer.expect_request(request);
        }
        if let Poll::Ready(output) = result {
            return Run::Completed { output, sent };
        }
        if poll == polls {
            break;
        }
        assert!(
            sent < reply.len(),
            "the operation waits for more than its reply"
        );
        peer.send(&reply[sent..=sent]);
        sent += 1;
        until_woken().await;
    }
    Run::Cancelled { sent }
}

/// Runs `operation` with the whole reply sent at once.
async fn exchange<T>(
    operation: impl Future<Output = T>,
    peer: &mut Peer,
    request: &[u8],
    reply: &[u8],
) -> T {
    let mut operation = pin!(operation);
    let result = poll_once(&mut operation).await;
    peer.expect_request(request);
    if let Poll::Ready(output) = result {
        return output;
    }
    peer.send(reply);
    timeout(TIMEOUT, operation)
        .await
        .expect("the operation did not complete")
}

/// Checks that the client closed its socket and refuses I/O, or that the next request receives
/// its own reply.
async fn assert_closed_or_aligned(client: &mut Client, peer: &mut Peer, label: &str) {
    if client.is_closed() {
        // The socket closes when the operation ends, without a later call.
        peer.expect_closed(label);
        assert_eq!(
            client.get("next").await.map(|_| ()),
            Err(Error::ConnectionClosed),
            "{label}"
        );
    } else {
        let value = exchange(
            client.get("next"),
            peer,
            b"get next\r\n",
            b"VALUE next 0 4\r\nnext\r\nEND\r\n",
        )
        .await;
        assert_eq!(
            value.map(|value| value.and_then(|value| value.data).map(|data| text(&data))),
            Ok(Some("next".to_string())),
            "{label}: the next request did not receive its own reply"
        );
    }
}

fn ok(_: ()) -> String {
    "ok".into()
}

/// Joins entries that arrive in no particular order.
fn sorted(mut entries: Vec<String>) -> String {
    entries.sort();
    entries.join(" ")
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

fn data(value: Option<MetaValue>) -> String {
    value.map_or_else(
        || "none".into(),
        |value| text(&value.data.unwrap_or_default()),
    )
}

fn outcome(result: &Result<(), Error>) -> String {
    match result {
        Ok(()) => "ok".into(),
        Err(Error::Protocol(status)) => format!("{status:?}"),
        Err(error) => format!("{error:?}"),
    }
}

fn meta_values(values: Vec<MetaValue>) -> String {
    values
        .into_iter()
        .map(|value| {
            let key = text(&value.key.unwrap_or_default());
            match value.data {
                Some(data) => format!("{key}={}", text(&data)),
                None => format!("{key}:{:?}", value.status.unwrap()),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

#[derive(Clone, Copy, Debug)]
enum Op {
    Get,
    GetMulti,
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
    MetaGet,
    MetaGetQuiet,
    MetaSet,
    MetaSetQuiet,
    MetaDelete,
    MetaDeleteQuiet,
    MetaIncrement,
    MetaIncrementQuiet,
    MetaDecrement,
    MetaDecrementQuiet,
    MetaGetMulti,
    MetaSetMulti,
    Stats,
    FlushAll,
    DumpKeys,
}

impl Op {
    fn request(self) -> &'static [u8] {
        match self {
            Self::Get => b"get key\r\n",
            Self::GetMulti => b"get key other\r\n",
            Self::Set => b"set key 0 0 5\r\nvalue\r\n",
            Self::SetMulti => b"set key 0 0 5\r\nvalue\r\nset other 0 0 4\r\nmore\r\n",
            Self::Add => b"add key 0 0 5\r\nvalue\r\n",
            Self::AddMulti => b"add key 0 0 5\r\nvalue\r\nadd other 0 0 4\r\nmore\r\n",
            Self::Delete => b"delete key\r\n",
            Self::DeleteNoReply => b"delete key noreply\r\n",
            Self::DeleteMultiNoReply => b"delete key noreply\r\ndelete other noreply\r\n",
            Self::Increment => b"incr key 2\r\n",
            Self::IncrementNoReply => b"incr key 2 noreply\r\n",
            Self::Decrement => b"decr key 2\r\n",
            Self::DecrementNoReply => b"decr key 2 noreply\r\n",
            Self::MetaGet => b"mg key v\r\n",
            Self::MetaGetQuiet => b"mg key v q\r\nmn\r\n",
            Self::MetaSet => b"ms key 5\r\nvalue\r\n",
            Self::MetaSetQuiet => b"ms key 5 q\r\nvalue\r\nmn\r\n",
            Self::MetaDelete => b"md key\r\n",
            Self::MetaDeleteQuiet => b"md key q\r\nmn\r\n",
            Self::MetaIncrement => b"ma key v\r\n",
            Self::MetaIncrementQuiet => b"ma key q\r\nmn\r\n",
            Self::MetaDecrement => b"ma key MD D2 N0\r\n",
            Self::MetaDecrementQuiet => b"ma key MD q\r\nmn\r\n",
            Self::MetaGetMulti => b"mg key v k q\r\nmg other v k q\r\nmn\r\n",
            Self::MetaSetMulti => b"ms key 5 k q\r\nvalue\r\nms other 4 k q\r\nmore\r\nmn\r\n",
            Self::Stats => b"stats\r\n",
            Self::FlushAll => b"flush_all\r\n",
            Self::DumpKeys => b"lru_crawler metadump all\r\n",
        }
    }

    /// Runs the operation and summarizes what it returned.
    async fn run(self, client: &mut Client) -> Result<String, Error> {
        let items = [("key", "value"), ("other", "more")];
        match self {
            Self::Get => client.get("key").await.map(|value| {
                value.map_or_else(|| "miss".into(), |value| text(&value.data.unwrap()))
            }),
            Self::GetMulti => client.get_multi(["key", "other"]).await.map(|values| {
                values
                    .iter()
                    .map(|value| {
                        format!(
                            "{}={}",
                            text(&value.key),
                            text(value.data.as_deref().unwrap())
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(" ")
            }),
            Self::Set => client.set("key", "value", None, None).await.map(ok),
            Self::SetMulti => client.set_multi(&items, None, None).await.map(|results| {
                sorted(
                    results
                        .iter()
                        .map(|(key, result)| format!("{key}:{}", outcome(result)))
                        .collect(),
                )
            }),
            Self::Add => client.add("key", "value", None, None).await.map(ok),
            Self::AddMulti => client.add_multi(&items, None, None).await.map(|results| {
                sorted(
                    results
                        .iter()
                        .map(|(key, result)| format!("{key}:{}", outcome(result)))
                        .collect(),
                )
            }),
            Self::Delete => client.delete("key").await.map(ok),
            Self::DeleteNoReply => client.delete_no_reply("key").await.map(ok),
            Self::DeleteMultiNoReply => client
                .delete_multi_no_reply(&["key", "other"])
                .await
                .map(ok),
            Self::Increment => client
                .increment("key", 2)
                .await
                .map(|count| count.to_string()),
            Self::IncrementNoReply => client.increment_no_reply("key", 2).await.map(ok),
            Self::Decrement => client
                .decrement("key", 2)
                .await
                .map(|count| count.to_string()),
            Self::DecrementNoReply => client.decrement_no_reply("key", 2).await.map(ok),
            Self::MetaGet => client
                .meta_get("key", false, None, Some(&["v"]))
                .await
                .map(data),
            Self::MetaGetQuiet => client
                .meta_get("key", true, None, Some(&["v"]))
                .await
                .map(data),
            Self::MetaSet => client
                .meta_set("key", "value", false, None, None)
                .await
                .map(data),
            Self::MetaSetQuiet => client
                .meta_set("key", "value", true, None, None)
                .await
                .map(data),
            Self::MetaDelete => client.meta_delete("key", false, None, None).await.map(data),
            Self::MetaDeleteQuiet => client.meta_delete("key", true, None, None).await.map(data),
            // The client does not write a `q` meta flag. Quiet mode comes only from `is_quiet`,
            // which adds the `mn` that ends the reply.
            Self::MetaIncrement => client
                .meta_increment("key", false, None, None, Some(&["q", "v"]))
                .await
                .map(data),
            Self::MetaIncrementQuiet => client
                .meta_increment("key", true, None, None, None)
                .await
                .map(data),
            Self::MetaDecrement => client
                .meta_decrement("key", false, None, Some(2), Some(&["q", "N0"]))
                .await
                .map(data),
            Self::MetaDecrementQuiet => client
                .meta_decrement("key", true, None, None, None)
                .await
                .map(data),
            Self::MetaGetMulti => client
                .meta_get_multi(&["key", "other"], Some(&["v"]))
                .await
                .map(meta_values),
            Self::MetaSetMulti => client.meta_set_multi(&items, None).await.map(meta_values),
            Self::Stats => client.stats().await.map(|stats| {
                sorted(
                    stats
                        .iter()
                        .map(|(name, value)| format!("{name}={value}"))
                        .collect(),
                )
            }),
            Self::FlushAll => client.flush_all().await.map(ok),
            Self::DumpKeys => {
                let mut entries = client.dump_keys().await?;
                let mut keys = Vec::new();
                while let Some(entry) = entries.next().await {
                    keys.push(text(&entry?.key));
                }
                Ok(keys.join(" "))
            }
        }
    }
}

enum Expect {
    /// The operation returns this summary.
    Returns(&'static str),
    /// The operation returns this error.
    Fails(Error),
    /// The reply does not parse.
    Unparsable,
}

impl Expect {
    fn matches(&self, output: &Result<String, Error>) -> bool {
        match (self, output) {
            (Self::Returns(expected), Ok(summary)) => summary == expected,
            (Self::Fails(expected), Err(error)) => error == expected,
            (Self::Unparsable, Err(Error::Protocol(Status::Error(ErrorKind::Protocol(_))))) => true,
            _ => false,
        }
    }
}

/// The state of the client after the operation completes.
#[derive(Clone, Copy)]
enum After {
    Open,
    Closed,
    /// Either state is correct, because the reply is complete but of the wrong kind.
    Unspecified,
}

struct Case {
    op: Op,
    reply: &'static [u8],
    expect: Expect,
    after: After,
}

fn case(op: Op, reply: &'static [u8], expect: Expect, after: After) -> Case {
    Case {
        op,
        reply,
        expect,
        after,
    }
}

fn server_error(message: &str) -> Expect {
    Expect::Fails(Error::Protocol(Status::Error(ErrorKind::Server(
        message.into(),
    ))))
}

fn refused(status: Status) -> Expect {
    Expect::Fails(Error::Protocol(status))
}

/// The error for a complete reply that does not answer the request.
fn wrong_kind() -> Expect {
    Expect::Fails(Error::Protocol(Status::Error(ErrorKind::Protocol(None))))
}

impl Case {
    fn assert_completed(&self, output: &Result<String, Error>, client: &Client, label: &str) {
        assert!(
            self.expect.matches(output),
            "{}: unexpected result {:?}",
            label,
            output
        );
        match self.after {
            After::Open => assert!(!client.is_closed(), "{}: the client closed", label),
            After::Closed => assert!(client.is_closed(), "{}: the client stayed open", label),
            After::Unspecified => {}
        }
    }

    async fn check(&self) {
        let request = self.op.request();
        let reply = self.reply;

        let label = format!(
            "{:?} with the whole reply {}",
            self.op,
            reply.escape_ascii()
        );
        let (mut client, mut peer) = connect().await;
        let output = exchange(self.op.run(&mut client), &mut peer, request, reply).await;
        self.assert_completed(&output, &client, &label);
        assert_closed_or_aligned(&mut client, &mut peer, &label).await;

        for polls in 1.. {
            let label = format!(
                "{:?} dropped after {polls} polls of reply {}",
                self.op,
                reply.escape_ascii()
            );
            let (mut client, mut peer) = connect().await;
            let run = timeout(
                TIMEOUT,
                drive(self.op.run(&mut client), &mut peer, request, reply, polls),
            )
            .await
            .unwrap_or_else(|_| panic!("{}: the operation was not woken", label));
            let completed = match run {
                Run::Completed { output, sent } => {
                    peer.send_late(&reply[sent..]);
                    self.assert_completed(&output, &client, &label);
                    true
                }
                Run::Cancelled { sent } => {
                    peer.send_late(&reply[sent..]);
                    false
                }
            };
            assert_closed_or_aligned(&mut client, &mut peer, &label).await;
            if completed {
                break;
            }
        }
    }
}

#[tokio::test]
async fn ascii_operations_close_or_stay_aligned_when_dropped_after_any_poll() {
    use After::*;
    use Expect::*;
    use Op::*;
    let cases = [
        case(
            Get,
            b"VALUE key 0 5\r\nvalue\r\nEND\r\n",
            Returns("value"),
            Open,
        ),
        case(Get, b"END\r\n", Returns("miss"), Open),
        case(
            Get,
            b"SERVER_ERROR out of memory\r\n",
            server_error("out of memory"),
            Open,
        ),
        case(
            Get,
            b"ERROR\r\n",
            refused(Status::Error(ErrorKind::NonexistentCommand)),
            Open,
        ),
        case(
            Get,
            b"VALUE key 0 1\r\na\r\nVALUE key 0 1\r\nb\r\nEND\r\n",
            wrong_kind(),
            Unspecified,
        ),
        case(Get, b"42\r\n", wrong_kind(), Unspecified),
        // The data block is longer than its declared length.
        case(
            Get,
            b"VALUE key 0 5\r\nvalue!\r\nEND\r\n",
            Unparsable,
            Closed,
        ),
        case(
            GetMulti,
            b"VALUE key 0 5\r\nvalue\r\nVALUE other 0 4\r\nmore\r\nEND\r\n",
            Returns("key=value other=more"),
            Open,
        ),
        case(GetMulti, b"END\r\n", refused(Status::NotFound), Open),
        case(
            GetMulti,
            b"SERVER_ERROR busy\r\n",
            server_error("busy"),
            Open,
        ),
        case(GetMulti, b"7\r\n", wrong_kind(), Unspecified),
        case(Set, b"STORED\r\n", Returns("ok"), Open),
        case(Set, b"NOT_STORED\r\n", refused(Status::NotStored), Open),
        case(Set, b"END\r\n", wrong_kind(), Unspecified),
        case(
            SetMulti,
            b"STORED\r\nSTORED\r\n",
            Returns("key:ok other:ok"),
            Open,
        ),
        case(
            SetMulti,
            b"STORED\r\nNOT_STORED\r\n",
            Returns("key:ok other:NotStored"),
            Open,
        ),
        case(
            SetMulti,
            b"STORED\r\n42\r\n",
            Returns("key:ok other:Error(Protocol(None))"),
            Unspecified,
        ),
        case(Add, b"STORED\r\n", Returns("ok"), Open),
        case(Add, b"NOT_STORED\r\n", refused(Status::NotStored), Open),
        case(Add, b"END\r\n", wrong_kind(), Unspecified),
        case(
            AddMulti,
            b"NOT_STORED\r\nSTORED\r\n",
            Returns("key:NotStored other:ok"),
            Open,
        ),
        case(Delete, b"DELETED\r\n", Returns("ok"), Open),
        case(Delete, b"NOT_FOUND\r\n", refused(Status::NotFound), Open),
        case(Delete, b"END\r\n", wrong_kind(), Unspecified),
        case(DeleteNoReply, b"", Returns("ok"), Open),
        case(DeleteMultiNoReply, b"", Returns("ok"), Open),
        case(Increment, b"42\r\n", Returns("42"), Open),
        case(Increment, b"NOT_FOUND\r\n", refused(Status::NotFound), Open),
        case(Increment, b"END\r\n", wrong_kind(), Unspecified),
        case(IncrementNoReply, b"", Returns("ok"), Open),
        case(Decrement, b"40\r\n", Returns("40"), Open),
        case(Decrement, b"END\r\n", wrong_kind(), Unspecified),
        case(DecrementNoReply, b"", Returns("ok"), Open),
    ];
    for case in &cases {
        case.check().await;
    }
}

#[tokio::test]
async fn meta_operations_close_or_stay_aligned_when_dropped_after_any_poll() {
    use After::*;
    use Expect::*;
    use Op::*;
    let cases = [
        case(MetaGet, b"VA 5\r\nvalue\r\n", Returns("value"), Open),
        case(MetaGet, b"EN\r\n", Returns("none"), Open),
        case(
            MetaGet,
            b"SERVER_ERROR busy\r\n",
            server_error("busy"),
            Open,
        ),
        // The data block is longer than its declared length.
        case(MetaGet, b"VA 5\r\nvalue!\r\n", Unparsable, Closed),
        case(
            MetaGetQuiet,
            b"VA 5\r\nvalue\r\nMN\r\n",
            Returns("value"),
            Open,
        ),
        case(MetaGetQuiet, b"MN\r\n", Returns("none"), Open),
        case(MetaSet, b"HD\r\n", Returns("none"), Open),
        case(MetaSet, b"NS\r\n", refused(Status::NotStored), Open),
        case(MetaSetQuiet, b"MN\r\n", Returns("none"), Open),
        case(
            MetaSetQuiet,
            b"NS\r\nMN\r\n",
            refused(Status::NotStored),
            Open,
        ),
        case(MetaDelete, b"HD\r\n", Returns("none"), Open),
        case(MetaDelete, b"NF\r\n", refused(Status::NotFound), Open),
        case(MetaDeleteQuiet, b"MN\r\n", Returns("none"), Open),
        case(MetaIncrement, b"VA 2\r\n42\r\n", Returns("42"), Open),
        case(MetaIncrement, b"NF\r\n", refused(Status::NotFound), Open),
        case(MetaIncrementQuiet, b"MN\r\n", Returns("none"), Open),
        case(MetaDecrement, b"HD\r\n", Returns("none"), Open),
        case(MetaDecrementQuiet, b"MN\r\n", Returns("none"), Open),
        case(
            MetaGetMulti,
            b"VA 5 kkey\r\nvalue\r\nVA 4 kother\r\nmore\r\nMN\r\n",
            Returns("key=value other=more"),
            Open,
        ),
        case(MetaGetMulti, b"MN\r\n", Returns(""), Open),
        // A hit without flags carries nothing to return.
        case(MetaGetMulti, b"HD\r\nMN\r\n", Returns(""), Open),
        case(MetaSetMulti, b"MN\r\n", Returns(""), Open),
        case(
            MetaSetMulti,
            b"NS kother\r\nMN\r\n",
            Returns("other:NotStored"),
            Open,
        ),
        // A status without flags names no key, so the batch skips it.
        case(MetaSetMulti, b"HD\r\nHD\r\nMN\r\n", Returns(""), Open),
    ];
    for case in &cases {
        case.check().await;
    }
}

#[tokio::test]
async fn admin_operations_close_or_stay_aligned_when_dropped_after_any_poll() {
    use After::*;
    use Expect::*;
    use Op::*;
    let busy = "BUSY currently processing crawler request";
    let cases = [
        case(
            Stats,
            b"STAT pid 1\r\nSTAT uptime 2\r\nEND\r\n",
            Returns("pid=1 uptime=2"),
            Open,
        ),
        case(Stats, b"STAT pid\r\nEND\r\n", Unparsable, Closed),
        case(FlushAll, b"OK\r\n", Returns("ok"), Open),
        case(
            DumpKeys,
            b"key=foo exp=-1 la=1 cas=2 fetch=yes cls=1 size=3\nEND\r\n",
            Returns("foo"),
            Open,
        ),
        case(DumpKeys, b"END\r\n", Returns(""), Open),
        case(
            DumpKeys,
            b"BUSY currently processing crawler request\r\n",
            Fails(Error::Protocol(Status::Error(ErrorKind::Generic(
                busy.into(),
            )))),
            Open,
        ),
        case(
            DumpKeys,
            b"key=foo exp=-1 la=1 cas=2 fetch=yes cls=1 size=3\nkey=bar\n",
            Unparsable,
            Closed,
        ),
    ];
    for case in &cases {
        case.check().await;
    }
}
