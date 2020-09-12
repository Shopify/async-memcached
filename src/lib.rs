#![deny(warnings)]
use bytes::BytesMut;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};

mod connection;
use self::connection::Connection;

mod error;
pub use self::error::Error;

mod parser;
use self::parser::{parse_ascii_metadump_response, parse_ascii_response, Response};
pub use self::parser::{ErrorKind, KeyMetadata, MetadumpResponse, Status, Value};

pub struct Client {
    buf: BytesMut,
    last_read_n: Option<usize>,
    conn: Connection,
}

impl Client {
    pub async fn new<S: AsRef<str>>(dsn: S) -> Result<Client, Error> {
        let connection = Connection::new(dsn).await?;

        Ok(Client {
            buf: BytesMut::new(),
            last_read_n: None,
            conn: connection,
        })
    }

    pub(crate) async fn get_normal_response(&mut self) -> Result<Response, Error> {
        // If we serviced a previous request, advance our buffer forward.
        if let Some(n) = self.last_read_n {
            let _ = self.buf.split_to(n);
        }

        loop {
            match self.conn {
                Connection::Tcp(ref mut s) => {
                    self.buf.reserve(1024);
                    let n = s.read_buf(&mut self.buf).await?;
                    if n == 0 {
                        return Err(Error::Io(std::io::ErrorKind::UnexpectedEof.into()));
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
                Err(kind) => return Err(Status::Error(kind).into()),
            }
        }
    }

    pub(crate) async fn get_metadump_response(&mut self) -> Result<MetadumpResponse, Error> {
        // If we serviced a previous request, advance our buffer forward.
        if let Some(n) = self.last_read_n {
            let _ = self.buf.split_to(n);
        }

        loop {
            // Try and parse out a response.
            match parse_ascii_metadump_response(&self.buf) {
                // We got a response.
                Ok(Some((n, response))) => {
                    self.last_read_n = Some(n);
                    return Ok(response);
                }
                // Need more data.
                Ok(None) => {}
                // Invalid data not matching the protocol.
                Err(kind) => return Err(Status::Error(kind).into()),
            }

            match self.conn {
                Connection::Tcp(ref mut s) => {
                    self.buf.reserve(1024);
                    let n = s.read_buf(&mut self.buf).await?;
                    if n == 0 {
                        return Err(Error::Io(std::io::ErrorKind::UnexpectedEof.into()));
                    }
                }
            }
        }
    }

    pub async fn get<K: AsRef<[u8]>>(&mut self, key: K) -> Result<Value, Error> {
        self.conn.write_all(b"get ").await?;
        self.conn.write_all(key.as_ref()).await?;
        self.conn.write_all(b"\r\n").await?;
        self.conn.flush().await?;

        match self.get_normal_response().await? {
            Response::Status(s) => Err(s.into()),
            Response::IncrDecr(_) => Err(Error::Protocol(Status::Error(ErrorKind::Protocol))),
            Response::Data(d) => d.ok_or(Status::NotFound.into()).and_then(|mut xs| {
                if xs.len() != 1 {
                    Err(Status::Error(ErrorKind::Protocol).into())
                } else {
                    Ok(xs.remove(0))
                }
            }),
        }
    }

    pub async fn get_many<I, K>(&mut self, keys: I) -> Result<Vec<Value>, Error>
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

        match self.get_normal_response().await? {
            Response::Status(s) => Err(s.into()),
            Response::IncrDecr(_) => Err(Status::Error(ErrorKind::Protocol).into()),
            Response::Data(d) => d.ok_or(Status::NotFound.into()),
        }
    }

    pub async fn set<K, V>(
        &mut self,
        key: K,
        value: V,
        ttl: i64,
        flags: Option<u32>,
    ) -> Result<(), Error>
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

        match self.get_normal_response().await? {
            Response::Status(Status::Stored) => Ok(()),
            Response::Status(s) => Err(s.into()),
            _ => Err(Status::Error(ErrorKind::Protocol).into()),
        }
    }

    pub async fn version(&mut self) -> Result<String, Error> {
        self.conn.write_all(b"version\r\n").await?;
        self.conn.flush().await?;

        let mut version = String::new();
        let _ = self.conn.read_line(&mut version).await?;

        // Peel off the leading "VERSION " header.
        Ok(version.split_off(8))
    }

    pub async fn dump_keys(&mut self) -> Result<MetadumpIter<'_>, Error> {
        self.conn.write_all(b"lru_crawler metadump all\r\n").await?;
        self.conn.flush().await?;

        Ok(MetadumpIter {
            client: self,
            done: false,
        })
    }
}

pub struct MetadumpIter<'a> {
    client: &'a mut Client,
    done: bool,
}

impl<'a> MetadumpIter<'a> {
    pub async fn next(&mut self) -> Option<Result<KeyMetadata, Error>> {
        if self.done {
            return None;
        }

        match self.client.get_metadump_response().await {
            Ok(MetadumpResponse::End)
            | Ok(MetadumpResponse::BadClass(_))
            | Ok(MetadumpResponse::Busy(_)) => {
                self.done = true;
                None
            }
            Ok(MetadumpResponse::Entry(km)) => Some(Ok(km)),
            Err(e) => Some(Err(e)),
        }
    }
}
