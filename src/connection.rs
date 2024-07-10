use pin_project::pin_project;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncBufRead, AsyncRead, AsyncWrite, BufReader, BufWriter};
use tokio::net::{TcpStream, UnixStream};

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

impl Connection {
    pub async fn new<S: AsRef<str>>(dsn: S) -> Result<Connection, Error> {
        let url = url::Url::parse(dsn.as_ref()).map_err(|e| {
            Error::Connect(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("failed to parse DSN: {}", e),
            ))
        })?;

        match url.scheme() {
            "unix" => UnixStream::connect(url.path())
                .await
                .map(|c| Connection::Unix(BufReader::new(BufWriter::new(c))))
                .map_err(Error::Connect),
            "tcp" => {
                Self::connect_tcp(&format!(
                    "{}:{}",
                    url.host_str().ok_or_else(|| {
                        Error::Connect(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "no host found in DSN",
                        ))
                    })?,
                    url.port().unwrap_or(11211)
                ))
                .await
            }
            _ => Self::connect_tcp(dsn.as_ref()).await,
        }
    }

    async fn connect_tcp(dsn: &str) -> Result<Connection, Error> {
        let addrs = tokio::net::lookup_host(dsn).await.map_err(|e| {
            Error::Connect(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("failed to resolve host: {}", e),
            ))
        })?;

        // take the first result; if there are multiple, we just use the first one
        let addr = addrs.into_iter().next().ok_or_else(|| {
            Error::Connect(io::Error::new(
                io::ErrorKind::InvalidInput,
                "no address found in DSN",
            ))
        })?;
        TcpStream::connect(addr)
            .await
            .map(|c| Connection::Tcp(BufReader::new(BufWriter::new(c))))
            .map_err(Error::Connect)
    }
}

mod tests {
    #[tokio::test]
    async fn test_tcp_connection_without_scheme() {
        let _ = super::Connection::new("localhost:11211").await.unwrap();
    }

    #[tokio::test]
    async fn test_tcp_connection_with_scheme() {
        let _ = super::Connection::new("tcp://localhost:11211")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_unix_connection() {
        let _ = super::Connection::new("unix:///tmp/memcached.sock")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_invalid_scheme() {
        let err = super::Connection::new("foo://localhost:11211")
            .await
            .unwrap_err();
        assert!(matches!(err, super::Error::Connect(_)));
    }

    #[tokio::test]
    async fn test_invalid_url() {
        let err = super::Connection::new("tcp://localhost").await.unwrap_err();
        assert!(matches!(err, super::Error::Connect(_)));
    }

    #[tokio::test]
    async fn test_invalid_unix_url() {
        let err = super::Connection::new("unix://localhost")
            .await
            .unwrap_err();
        assert!(matches!(err, super::Error::Connect(_)));
    }

    #[tokio::test]
    async fn test_invalid_unix_path() {
        let err = super::Connection::new("unix://").await.unwrap_err();
        assert!(matches!(err, super::Error::Connect(_)));
    }
}
