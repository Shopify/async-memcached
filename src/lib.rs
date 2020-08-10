use bytes::BytesMut;
use tokio::net::TcpStream;
use tokio::io::{BufReader, BufWriter, AsyncBufRead, AsyncRead, AsyncWrite, AsyncReadExt};
use pin_project::pin_project;
use std::fmt;
use std::task::{Context, Poll};
use std::pin::Pin;
use std::io;
use std::future::Future;

mod parser;
use self::parser::{Value, Status, Response, ErrorKind, parse_ascii_response};

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
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context, buf: &mut [u8]) -> Poll<io::Result<usize>> {
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
        let dsn = dsn::parse(d.as_ref())?;
        if dsn.driver != "memcached" {
            return Err(Error::InvalidConnectionString("expected 'memcached' driver".to_string()))
        }

        let connection = match dsn.protocol.as_ref() {
            #[cfg(feature = "tcp")]
            "tcp" => {
                let host = dsn.host.as_deref().ok_or_else(|| Error::InvalidConnectionString("'host' must be set".to_string()))?;
                let port = dsn.port.unwrap_or(11211);
                let connection = TcpStream::connect((host.as_ref(), port))
                    .await
                    .map_err(Error::FailedToConnect)?;

                Connection::Tcp(BufReader::new(BufWriter::new(connection)))
            },
            x => return Err(Error::InvalidConnectionString(format!("unknown protocol '{}'", x))),
        };

        Ok(Client {
            buf: BytesMut::new(),
            last_read_n: None,
            conn: connection,
        })
    }

    pub async fn get_response(&mut self) -> Result<Response<'_>, Status<'_>> {
         // If we serviced a previous request, advance our buffer forward.
        if let Some(n) = self.last_read_n {
            let _  = self.buf.split_to(n);
        }
    
        loop {
            self.buf.reserve(1024);
        
            match self.conn {
                Connection::Tcp(ref mut s) => {
                    //let _ = fill_buf(s, &mut self.buf).await?;
                    let n = s.read_buf(&mut self.buf).await?;
                    if n == 0 {
                        panic!("placeholder for now");
                    }
                }
            }

            // Try and parse out a response.
            match parse_ascii_response(&self.buf) {
                // We got a response.
                Ok((rbuf, response)) => {
                    self.last_read_n = Some(self.buf.len() - rbuf.len());
                    return Ok(response)
                },
                // Need more data.
                Err(nom::Err::Incomplete(_)) => {},
                // Invalid data not matching the protocol.
                Err(_) => return Err(Status::Error(ErrorKind::Protocol)),
            }
        }
    }

    /*pub async fn get<K: AsRef<[u8]>>(&mut self, key: K) -> Result<Value<'_>, Status<'_>> {
        self.conn.write_all(b"get ").await?;
        self.conn.write_all(key.as_ref()).await?;
        self.conn.write_all(b"\r\n").await?;
        self.conn.flush().await?;

        match self.get_response().await? {
            Response::Status(s) => Err(s),
            Response::IncrDecr(_) => Err(Status::Error(ErrorKind::Protocol)),
            Response::Data(d) => d.ok_or(Status::NotFound)
                .and_then(|mut xs| {
                    if xs.len() != 1 {
                        Err(Status::Error(ErrorKind::Protocol))
                    } else {
                        Ok(xs.remove(0))
                    }
                })
        }
    }*/
}