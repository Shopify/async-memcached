use pin_project::pin_project;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncBufRead, AsyncRead, AsyncWrite, BufReader, BufWriter};
use tokio::net::{lookup_host, TcpSocket, TcpStream, UnixStream};

use crate::Error;

#[pin_project(project = ConnectionProjection)]
#[derive(Debug)]
pub enum Connection {
    Tcp(#[pin] BufReader<BufWriter<TcpStream>>),
    Unix(#[pin] BufReader<BufWriter<UnixStream>>),
}

impl AsyncRead for Connection {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.project() {
            ConnectionProjection::Tcp(s) => s.poll_read(cx, buf),
            ConnectionProjection::Unix(s) => s.poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Connection {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Poll<io::Result<usize>> {
        match self.project() {
            ConnectionProjection::Tcp(s) => s.poll_write(cx, buf),
            ConnectionProjection::Unix(s) => s.poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        match self.project() {
            ConnectionProjection::Tcp(s) => s.poll_flush(cx),
            ConnectionProjection::Unix(s) => s.poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        match self.project() {
            ConnectionProjection::Tcp(s) => s.poll_shutdown(cx),
            ConnectionProjection::Unix(s) => s.poll_shutdown(cx),
        }
    }
}

impl AsyncBufRead for Connection {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<&[u8]>> {
        match self.project() {
            ConnectionProjection::Tcp(s) => s.poll_fill_buf(cx),
            ConnectionProjection::Unix(s) => s.poll_fill_buf(cx),
        }
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        match self.project() {
            ConnectionProjection::Tcp(s) => s.consume(amt),
            ConnectionProjection::Unix(s) => s.consume(amt),
        }
    }
}

#[derive(Debug, PartialEq)]
enum Addr {
    Tcp(String),
    Unix(String),
    Unknown(String),
}

impl Addr {
    const DEFAULT_PORT: u16 = 11211;

    fn parse(dsn: &str) -> Result<Self, Error> {
        let url = url::Url::parse(dsn).map_err(|e| {
            Error::Connect(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("failed to parse DSN: {}", e),
            ))
        })?;

        match url.scheme() {
            "unix" => Ok(Addr::Unix(url.path().to_string())),
            "tcp" => Ok(Addr::Tcp(format!(
                "{}:{}",
                url.host_str().ok_or_else(|| {
                    Error::Connect(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "no host found in DSN",
                    ))
                })?,
                url.port().unwrap_or(Self::DEFAULT_PORT)
            ))),
            _ => Ok(Addr::Unknown(dsn.to_string())),
        }
    }
}

impl Connection {
    pub async fn new<S: AsRef<str>>(dsn: S) -> Result<Self, Error> {
        match Addr::parse(dsn.as_ref())? {
            Addr::Unix(path) => UnixStream::connect(path)
                .await
                .map(|c| Connection::Unix(BufReader::new(BufWriter::new(c))))
                .map_err(Error::Connect),
            Addr::Tcp(url) | Addr::Unknown(url) => {
                let addrs = lookup_host(url)
                    .await
                    .map_err(Error::Connect)?
                    .collect::<Vec<SocketAddr>>();

                let mut last_err = None;

                for addr in addrs {
                    let socket = TcpSocket::new_v4().map_err(Error::Connect)?;
                    socket.set_nodelay(true).map_err(Error::Connect)?;
                    match socket.connect(addr).await {
                        Ok(stream) => return Ok(Connection::Tcp(BufReader::new(BufWriter::new(stream)))),
                        Err(e) => last_err = Some(Error::Connect(e)),
                    }
                }
                Err(last_err.unwrap_or_else(|| {
                    Error::Connect(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "could not resolve to any address",
                    ))
                }))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Addr, Error};

    #[tokio::test]
    async fn test_unknown_scheme() {
        assert_eq!(
            Addr::parse("localhost:11211"),
            Ok(Addr::Unknown("localhost:11211".to_string()))
        )
    }

    #[tokio::test]
    async fn test_tcp_scheme() {
        assert_eq!(
            Addr::parse("tcp://localhost:11211"),
            Ok(Addr::Tcp("localhost:11211".to_string()))
        )
    }

    #[tokio::test]
    async fn test_unix_scheme() {
        assert_eq!(
            Addr::parse("unix:///tmp/memcached.sock"),
            Ok(Addr::Unix("/tmp/memcached.sock".to_string()))
        )
    }

    #[tokio::test]
    async fn test_invalid_url() {
        assert!(matches!(
            Addr::parse("tcp://").unwrap_err(),
            Error::Connect(_)
        ));
    }
}
