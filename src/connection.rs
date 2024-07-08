use pin_project::pin_project;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncBufRead, AsyncRead, AsyncWrite, BufReader, BufWriter};
use tokio::net::{TcpStream, UnixStream};

use crate::Error;

#[pin_project(project = ConnectionProjection)]
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
        let dsn = url::Url::parse(dsn.as_ref()).map_err(|e| {
            Error::Connect(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Failed to parse URL: {}", e),
            ))
        })?;

        println!("Connecting to {:?}", dsn);

        match dsn.scheme() {
            "tcp" | "" => TcpStream::connect(&*dsn.socket_addrs(|| None)?)
                .await
                .map(|c| Connection::Tcp(BufReader::new(BufWriter::new(c))))
                .map_err(Error::Io),
            "unix" => UnixStream::connect(dsn.path())
                .await
                .map(|c| Connection::Unix(BufReader::new(BufWriter::new(c))))
                .map_err(Error::Io),
            _ => Err(Error::Connect(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid scheme",
            ))),
        }
    }
}
