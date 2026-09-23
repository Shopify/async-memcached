mod support;

use async_memcached::{AsciiProtocol, Client, Error, ErrorKind, MetaProtocol, Status};
use std::time::Duration;
use support::run;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

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
    fn request(self) -> &'static [u8] {
        match self {
            Self::AsciiGet => b"get key\r\n",
            Self::AsciiMulti => b"get key missing\r\n",
            Self::MetaGet => b"mg key v\r\n",
            Self::MetaMulti => b"mg key v k q\r\nmg missing v k q\r\nmn\r\n",
        }
    }

    fn response(self, value: &[u8]) -> Vec<u8> {
        let (header, tail) = match self {
            Self::AsciiGet | Self::AsciiMulti => (
                format!("VALUE key 0 {}\r\n", value.len()),
                b"\r\nEND\r\n".as_slice(),
            ),
            Self::MetaGet => (format!("VA {}\r\n", value.len()), b"\r\n".as_slice()),
            Self::MetaMulti => (
                format!("VA {} kkey\r\n", value.len()),
                b"\r\nMN\r\n".as_slice(),
            ),
        };
        [header.as_bytes(), value, tail].concat()
    }

    async fn read(self, client: &mut Client) -> Result<Vec<u8>, Error> {
        Ok(match self {
            Self::AsciiGet => client.get("key").await?.unwrap().data.unwrap(),
            Self::AsciiMulti => {
                let values = client.get_multi(&["key", "missing"]).await?;
                assert_eq!(values.len(), 1);
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
                values[0].data.clone().unwrap()
            }
        })
    }
}

async fn expect_request(socket: &mut TcpStream, expected: &[u8]) {
    let mut bytes = vec![0; expected.len()];
    socket.read_exact(&mut bytes).await.unwrap();
    assert_eq!(bytes, expected);
}

async fn reply_once(
    operation: ReadOperation,
    response: Vec<u8>,
    fragmented: bool,
) -> (Client, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        socket.set_nodelay(true).unwrap();
        expect_request(&mut socket, operation.request()).await;
        if fragmented {
            for byte in response {
                socket.write_all(&[byte]).await.unwrap();
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        } else {
            socket.write_all(&response).await.unwrap();
        }
    });
    let client = Client::new(format!("tcp://{address}")).await.unwrap();
    (client, server)
}

#[test]
fn byte_fragmented_headers_bodies_and_terminators_parse_correctly() {
    run(async {
        let payload = b"\0\xff\r\nEND\r\nMN\r\n \t";
        for operation in READS {
            let (mut client, server) =
                reply_once(operation, operation.response(payload), true).await;
            assert_eq!(operation.read(&mut client).await.unwrap(), payload);
            server.await.unwrap();
        }
    });
}

#[test]
fn eof_in_a_header_is_an_io_error() {
    run(async {
        for operation in READS {
            let response = operation.response(b"payload")[..3].to_vec();
            let (mut client, server) = reply_once(operation, response, false).await;
            assert_eq!(
                operation.read(&mut client).await,
                Err(Error::Io(std::io::ErrorKind::UnexpectedEof.into()))
            );
            server.await.unwrap();
        }
    });
}

#[test]
fn eof_in_a_value_is_not_a_successful_short_read() {
    run(async {
        for operation in READS {
            let mut response = operation.response(b"payload");
            let body_start = response
                .windows(2)
                .position(|part| part == b"\r\n")
                .unwrap()
                + 2;
            response.truncate(body_start + 3);
            let (mut client, server) = reply_once(operation, response, false).await;
            assert_eq!(
                operation.read(&mut client).await,
                Err(Error::Io(std::io::ErrorKind::UnexpectedEof.into()))
            );
            server.await.unwrap();
        }
    });
}

#[test]
fn eof_in_a_terminator_does_not_return_a_partial_batch() {
    run(async {
        for operation in READS {
            let mut response = operation.response(b"payload");
            response.pop();
            let (mut client, server) = reply_once(operation, response, false).await;
            assert_eq!(
                operation.read(&mut client).await,
                Err(Error::Io(std::io::ErrorKind::UnexpectedEof.into()))
            );
            server.await.unwrap();
        }
    });
}

#[test]
fn malformed_length_fields_return_protocol_errors() {
    run(async {
        for operation in READS {
            let response: &[u8] = match operation {
                ReadOperation::AsciiGet | ReadOperation::AsciiMulti => b"VALUE key 0 nope\r\n",
                ReadOperation::MetaGet | ReadOperation::MetaMulti => b"VA nope\r\n",
            };
            let (mut client, server) = reply_once(operation, response.to_vec(), false).await;
            let error = operation.read(&mut client).await.unwrap_err();
            assert!(
                matches!(
                    error,
                    Error::Protocol(Status::Error(ErrorKind::Protocol(_)))
                ),
                "{:?}",
                error
            );
            server.await.unwrap();
        }
    });
}

#[test]
fn complete_error_lines_preserve_single_request_alignment() {
    run(async {
        let cases: [(&[u8], ErrorKind); 3] = [
            (b"ERROR\r\n", ErrorKind::NonexistentCommand),
            (
                b"CLIENT_ERROR invalid argument\r\n",
                ErrorKind::Client("invalid argument".into()),
            ),
            (
                b"SERVER_ERROR unavailable\r\n",
                ErrorKind::Server("unavailable".into()),
            ),
        ];
        for operation in [ReadOperation::AsciiGet, ReadOperation::MetaGet] {
            for (response, expected) in &cases {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let address = listener.local_addr().unwrap();
                let response = response.to_vec();
                let server = tokio::spawn(async move {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    expect_request(&mut socket, operation.request()).await;
                    socket.write_all(&response).await.unwrap();
                    expect_request(&mut socket, operation.request()).await;
                    socket
                        .write_all(&operation.response(b"good"))
                        .await
                        .unwrap();
                });
                let mut client = Client::new(format!("tcp://{address}")).await.unwrap();
                assert_eq!(
                    operation.read(&mut client).await,
                    Err(Error::Protocol(Status::Error(expected.clone())))
                );
                assert_eq!(operation.read(&mut client).await.unwrap(), b"good");
                server.await.unwrap();
            }
        }
    });
}

#[test]
fn meta_pipeline_errors_require_discard_of_the_failed_connection() {
    run(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            expect_request(&mut first, ReadOperation::MetaMulti.request()).await;
            first
                .write_all(
                    b"VA 3 kkey\r\nold\r\nSERVER_ERROR failed\r\nVA 5 kmissing\r\nstale\r\nMN\r\n",
                )
                .await
                .unwrap();
            let (mut second, _) = listener.accept().await.unwrap();
            expect_request(&mut second, ReadOperation::MetaGet.request()).await;
            second.write_all(b"VA 3\r\nnew\r\n").await.unwrap();
        });
        let mut failed = Client::new(format!("tcp://{address}")).await.unwrap();
        assert_eq!(
            ReadOperation::MetaMulti.read(&mut failed).await,
            Err(Error::Protocol(Status::Error(ErrorKind::Server(
                "failed".into()
            ))))
        );
        drop(failed);
        let mut fresh = Client::new(format!("tcp://{address}")).await.unwrap();
        assert_eq!(
            ReadOperation::MetaGet.read(&mut fresh).await.unwrap(),
            b"new"
        );
        server.await.unwrap();
    });
}

#[test]
fn abort_of_a_task_that_owns_the_client_closes_its_socket() {
    run(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (request_sent, request_received) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            expect_request(&mut first, ReadOperation::MetaGet.request()).await;
            request_sent.send(()).unwrap();
            let mut byte = [0];
            assert_eq!(first.read(&mut byte).await.unwrap(), 0);
            let (mut second, _) = listener.accept().await.unwrap();
            expect_request(&mut second, ReadOperation::MetaGet.request()).await;
            second.write_all(b"VA 5\r\nfresh\r\n").await.unwrap();
        });
        let mut client = Client::new(format!("tcp://{address}")).await.unwrap();
        let task = tokio::spawn(async move { ReadOperation::MetaGet.read(&mut client).await });
        request_received.await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        let mut fresh = Client::new(format!("tcp://{address}")).await.unwrap();
        assert_eq!(
            ReadOperation::MetaGet.read(&mut fresh).await.unwrap(),
            b"fresh"
        );
        server.await.unwrap();
    });
}
