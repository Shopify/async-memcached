use bytes::BytesMut;
use pin_project::pin_project;
use std::fmt;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{
    AsyncBufRead, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter,
};
use tokio::net::TcpStream;

mod parser;
use self::parser::{parse_ascii_response, ErrorKind, Response, Status, Value};

#[derive(Debug)]
pub enum Error {
    InvalidConnectionString(String),
    FailedToConnect(io::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::InvalidConnectionString(s) => write!(f, "invalid connection string: {}", s),
            Self::FailedToConnect(e) => write!(f, "failed to connect: {}", e),
        }
    }
}

impl From<dsn::ParseError> for Error {
    fn from(pe: dsn::ParseError) -> Self {
        Error::InvalidConnectionString(pe.to_string())
    }
}

#[pin_project(project = ConnectionProjection)]
enum Connection {
    Tcp(#[pin] BufReader<BufWriter<TcpStream>>),
}

impl AsyncRead for Connection {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        match self.project() {
            ConnectionProjection::Tcp(s) => s.poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Connection {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Poll<io::Result<usize>> {
        match self.project() {
            ConnectionProjection::Tcp(s) => s.poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        match self.project() {
            ConnectionProjection::Tcp(s) => s.poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        match self.project() {
            ConnectionProjection::Tcp(s) => s.poll_shutdown(cx),
        }
    }
}

impl AsyncBufRead for Connection {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<&[u8]>> {
        match self.project() {
            ConnectionProjection::Tcp(s) => s.poll_fill_buf(cx),
        }
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        match self.project() {
            ConnectionProjection::Tcp(s) => s.consume(amt),
        }
    }
}

pub struct Client {
    buf: BytesMut,
    last_read_n: Option<usize>,
    conn: Connection,
}

impl Client {
    pub async fn new<D: AsRef<str>>(d: D) -> Result<Client, Error> {
        let connection = TcpStream::connect(d.as_ref())
            .await
            .map_err(Error::FailedToConnect)
            .map(|c| Connection::Tcp(BufReader::new(BufWriter::new(c))))?;

        Ok(Client {
            buf: BytesMut::new(),
            last_read_n: None,
            conn: connection,
        })
    }

    pub async fn get_response(&mut self) -> Result<Response, Status> {
        // If we serviced a previous request, advance our buffer forward.
        if let Some(n) = self.last_read_n {
            let _ = self.buf.split_to(n);
        }

        loop {
            match self.conn {
                Connection::Tcp(ref mut s) => {
                    //buf.reserve(1024);
                    let n = s.read_buf(&mut self.buf).await?;
                    if n == 0 {
                        panic!("placeholder for now");
                    }
                }
            }

            // Try and parse out a response.
            match parse_ascii_response(&self.buf) {
                // We got a response.
                Ok(Some((n, response))) => {
                    self.last_read_n = Some(n);
                    return Ok(response);
                }
                // Need more data.
                Ok(None) => continue,
                // Invalid data not matching the protocol.
                Err(kind) => return Err(Status::Error(kind)),
            }
        }
    }

    pub async fn get<K: AsRef<[u8]>>(&mut self, key: K) -> Result<Value, Status> {
        self.conn.write_all(b"get ").await?;
        self.conn.write_all(key.as_ref()).await?;
        self.conn.write_all(b"\r\n").await?;
        self.conn.flush().await?;

        match self.get_response().await? {
            Response::Status(s) => Err(s),
            Response::IncrDecr(_) => Err(Status::Error(ErrorKind::Protocol)),
            Response::Data(d) => d.ok_or(Status::NotFound).and_then(|mut xs| {
                if xs.len() != 1 {
                    Err(Status::Error(ErrorKind::Protocol))
                } else {
                    Ok(xs.remove(0))
                }
            }),
        }
    }

    pub async fn get_many<I, K>(&mut self, keys: I) -> Result<Vec<Value>, Status>
    where
        I: IntoIterator<Item = K>,
        K: AsRef<[u8]>,
    {
        self.conn.write_all(b"get ").await?;
        for key in keys.into_iter() {
            self.conn.write_all(key.as_ref()).await?;
            self.conn.write_all(b" ").await?;
        }
        self.conn.write_all(b"\r\n").await?;
        self.conn.flush().await?;

        match self.get_response().await? {
            Response::Status(s) => Err(s),
            Response::IncrDecr(_) => Err(Status::Error(ErrorKind::Protocol)),
            Response::Data(d) => d.ok_or(Status::NotFound),
        }
    }

    pub async fn set<K, V>(
        &mut self,
        key: K,
        value: V,
        ttl: i64,
        flags: Option<u32>,
    ) -> Result<(), Status>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let kr = key.as_ref();
        let vr = value.as_ref();

        self.conn.write_all(b"set ").await?;
        self.conn.write_all(kr).await?;

        let flags = flags.unwrap_or(0);
        self.conn.write_all(b" ").await?;
        let fs = flags.to_string();
        self.conn.write_all(fs.as_ref()).await?;

        self.conn.write_all(b" ").await?;
        let ts = ttl.to_string();
        self.conn.write_all(ts.as_ref()).await?;

        self.conn.write_all(b" ").await?;
        let vlen = vr.len().to_string();
        self.conn.write_all(vlen.as_ref()).await?;
        self.conn.write_all(b"\r\n").await?;

        self.conn.write_all(vr).await?;
        self.conn.write_all(b"\r\n").await?;
        self.conn.flush().await?;

        match self.get_response().await? {
            Response::Status(Status::Stored) => Ok(()),
            Response::Status(s) => Err(s),
            _ => Err(Status::Error(ErrorKind::Protocol)),
        }
    }
}
