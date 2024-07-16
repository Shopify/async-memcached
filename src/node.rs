//! A Tokio-based memcached node.
#![deny(warnings, missing_docs)]
use std::collections::HashMap;

use bytes::BytesMut;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};

use crate::connection::Connection;
use crate::error::Error;

use crate::parser::{
    parse_ascii_metadump_response, parse_ascii_response, parse_ascii_stats_response, Response,
};
use crate::parser::{ErrorKind, KeyMetadata, MetadumpResponse, StatsResponse, Status, Value};

/// High-level memcached node client.
///
/// [`Node`] is mapped one-to-one with a given connection to a memcached server, and provides a
/// high-level API for executing commands on that connection.
pub struct Node {
    /// The name of the node, typically the DSN.
    pub name: String,
    buf: BytesMut,
    last_read_n: Option<usize>,
    conn: Connection,
}

impl Node {
    /// Creates a new [`Node`] based on the given data source string.
    ///
    /// Currently only supports TCP connections, and as such, the DSN should be in the format of
    /// `<host of IP>:<port>`.
    pub async fn new<S: AsRef<str>>(dsn: S) -> Result<Node, Error> {
        let connection = Connection::new(dsn.as_ref()).await?;

        Ok(Node {
            name: dsn.as_ref().to_string(),
            buf: BytesMut::new(),
            last_read_n: None,
            conn: connection,
        })
    }

    pub(crate) async fn drive_receive<R, F>(&mut self, op: F) -> Result<R, Error>
    where
        F: Fn(&[u8]) -> Result<Option<(usize, R)>, ErrorKind>,
    {
        // If we serviced a previous request, advance our buffer forward.
        if let Some(n) = self.last_read_n {
            let _ = self.buf.split_to(n);
        }

        let mut needs_more_data = false;
        loop {
            if self.buf.is_empty() || needs_more_data {
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

            // Try and parse out a response.
            match op(&self.buf) {
                // We got a response.
                Ok(Some((n, response))) => {
                    self.last_read_n = Some(n);
                    return Ok(response);
                }
                // We didn't have enough data, so loop around and try again.
                Ok(None) => {
                    needs_more_data = true;
                    continue;
                }
                // Invalid data not matching the protocol.
                Err(kind) => return Err(Status::Error(kind).into()),
            }
        }
    }

    pub(crate) async fn get_read_write_response(&mut self) -> Result<Response, Error> {
        self.drive_receive(parse_ascii_response).await
    }

    pub(crate) async fn get_metadump_response(&mut self) -> Result<MetadumpResponse, Error> {
        self.drive_receive(parse_ascii_metadump_response).await
    }

    pub(crate) async fn get_stats_response(&mut self) -> Result<StatsResponse, Error> {
        self.drive_receive(parse_ascii_stats_response).await
    }

    /// Gets the given key.
    ///
    /// If the key is found, `Some(Value)` is returned, describing the metadata and data of the key.
    ///
    /// Otherwise, [`Error`] is returned.
    pub async fn get<K: AsRef<[u8]>>(&mut self, key: K) -> Result<Option<Value>, Error> {
        self.conn.write_all(b"get ").await?;
        self.conn.write_all(key.as_ref()).await?;
        self.conn.write_all(b"\r\n").await?;
        self.conn.flush().await?;

        match self.get_read_write_response().await? {
            Response::Status(Status::NotFound) => Ok(None),
            Response::Status(s) => Err(s.into()),
            Response::Data(d) => d
                .map(|mut items| {
                    if items.len() != 1 {
                        Err(Status::Error(ErrorKind::Protocol(None)).into())
                    } else {
                        Ok(items.remove(0))
                    }
                })
                .transpose(),
            _ => Err(Error::Protocol(Status::Error(ErrorKind::Protocol(None)))),
        }
    }

    /// Gets the given keys.
    ///
    /// If any of the keys are found, a vector of [`Value`] will be returned, where [`Value`]
    /// describes the metadata and data of the key.
    ///
    /// Otherwise, [`Error`] is returned.
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

        match self.get_read_write_response().await? {
            Response::Status(s) => Err(s.into()),
            Response::Data(d) => d.ok_or(Status::NotFound.into()),
            _ => Err(Status::Error(ErrorKind::Protocol(None)).into()),
        }
    }

    /// Sets the given key.
    ///
    /// If `ttl` or `flags` are not specified, they will default to 0.  If the value is set
    /// successfully, `()` is returned, otherwise [`Error`] is returned.
    pub async fn set<K, V>(
        &mut self,
        key: K,
        value: V,
        ttl: Option<i64>,
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

        let flags = flags.unwrap_or(0).to_string();
        self.conn.write_all(b" ").await?;
        self.conn.write_all(flags.as_ref()).await?;

        let ttl = ttl.unwrap_or(0).to_string();
        self.conn.write_all(b" ").await?;
        self.conn.write_all(ttl.as_ref()).await?;

        self.conn.write_all(b" ").await?;
        let vlen = vr.len().to_string();
        self.conn.write_all(vlen.as_ref()).await?;
        self.conn.write_all(b"\r\n").await?;

        self.conn.write_all(vr).await?;
        self.conn.write_all(b"\r\n").await?;
        self.conn.flush().await?;

        match self.get_read_write_response().await? {
            Response::Status(Status::Stored) => Ok(()),
            Response::Status(s) => Err(s.into()),
            _ => Err(Status::Error(ErrorKind::Protocol(None)).into()),
        }
    }

    /// Add a key. If the value exists, Err(Protocol(NotStored)) is returned.
    pub async fn add<K, V>(
        &mut self,
        key: K,
        value: V,
        ttl: Option<i64>,
        flags: Option<u32>,
    ) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let kr = key.as_ref();
        let vr = value.as_ref();

        self.conn
            .write_all(
                &[
                    b"add ",
                    kr,
                    b" ",
                    flags.unwrap_or(0).to_string().as_ref(),
                    b" ",
                    ttl.unwrap_or(0).to_string().as_ref(),
                    b" ",
                    vr.len().to_string().as_ref(),
                    b"\r\n",
                    vr,
                    b"\r\n",
                ]
                .concat(),
            )
            .await?;
        self.conn.flush().await?;

        match self.get_read_write_response().await? {
            Response::Status(Status::Stored) => Ok(()),
            Response::Status(s) => Err(s.into()),
            _ => Err(Status::Error(ErrorKind::Protocol(None)).into()),
        }
    }

    /// Delete a key but don't wait for a reply.
    pub async fn delete_no_reply<K>(&mut self, key: K) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
    {
        let kr = key.as_ref();

        self.conn
            .write_all(&[b"delete ", kr, b" noreply\r\n"].concat())
            .await?;
        self.conn.flush().await?;
        Ok(())
    }

    /// Delete a key and wait for a reply
    pub async fn delete<K>(&mut self, key: K) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
    {
        let kr = key.as_ref();

        self.conn
            .write_all(&[b"delete ", kr, b"\r\n"].concat())
            .await?;
        self.conn.flush().await?;

        match self.get_read_write_response().await? {
            Response::Status(Status::Deleted) => Ok(()),
            Response::Status(s) => Err(s.into()),
            _ => Err(Status::Error(ErrorKind::Protocol(None)).into()),
        }
    }

    /// Gets the version of the server.
    ///
    /// If the version is retrieved successfully, `String` is returned containing the version
    /// component e.g. `1.6.7`, otherwise [`Error`] is returned.
    ///
    /// For some setups, such as those using Twemproxy, this will return an error as those
    /// intermediate proxies do not support the version command.
    pub async fn version(&mut self) -> Result<String, Error> {
        self.conn.write_all(b"version\r\n").await?;
        self.conn.flush().await?;

        let mut version = String::new();
        let bytes = self.conn.read_line(&mut version).await?;

        // Peel off the leading "VERSION " header.
        if bytes >= 8 && version.is_char_boundary(8) {
            Ok(version.split_off(8))
        } else {
            Err(Error::from(Status::Error(ErrorKind::Protocol(Some(
                format!("Invalid response for `version` command: `{version}`"),
            )))))
        }
    }

    /// Dumps all keys from the server.
    ///
    /// This operation scans all slab classes from tail to head, in a non-blocking fashion.  Thus,
    /// not all items will be found as new items could be inserted or deleted while the crawler is
    /// still running.
    ///
    /// [`MetadumpIter`] must be iterated over to discover whether or not the crawler successfully
    /// started, as this call will only return [`Error`] if the command failed to be written to the
    /// server at all.
    ///
    /// Available as of memcached 1.4.31.
    pub async fn dump_keys(&mut self) -> Result<MetadumpIter<'_>, Error> {
        self.conn.write_all(b"lru_crawler metadump all\r\n").await?;
        self.conn.flush().await?;

        Ok(MetadumpIter {
            node: self,
            done: false,
        })
    }

    /// Collects statistics from the server.
    ///
    /// The statistics that may be returned are detailed in the protocol specification for
    /// memcached, but all values returned by this method are returned as strings and are not
    /// further interpreted or validated for conformity.
    pub async fn stats(&mut self) -> Result<HashMap<String, String>, Error> {
        let mut entries = HashMap::new();

        self.conn.write_all(b"stats\r\n").await?;
        self.conn.flush().await?;

        while let StatsResponse::Entry(key, value) = self.get_stats_response().await? {
            entries.insert(key, value);
        }

        Ok(entries)
    }
}

/// Asynchronous iterator for metadump operations.
pub struct MetadumpIter<'a> {
    node: &'a mut Node,
    done: bool,
}

impl<'a> MetadumpIter<'a> {
    /// Gets the next result for the current operation.
    ///
    /// If there is another key in the dump, `Some(Ok(KeyMetadata))` will be returned.  If there was
    /// an error while attempting to start the metadump operation, or if there was a general
    /// network/protocol-level error, `Some(Err(Error))` will be returned.
    ///
    /// Otherwise, `None` will be returned and signals the end of the iterator.  Subsequent calls
    /// will return `None`.
    pub async fn next(&mut self) -> Option<Result<KeyMetadata, Error>> {
        if self.done {
            return None;
        }

        match self.node.get_metadump_response().await {
            Ok(MetadumpResponse::End) => {
                self.done = true;
                None
            }
            Ok(MetadumpResponse::BadClass(s)) => {
                self.done = true;
                Some(Err(Error::Protocol(MetadumpResponse::BadClass(s).into())))
            }
            Ok(MetadumpResponse::Busy(s)) => {
                Some(Err(Error::Protocol(MetadumpResponse::Busy(s).into())))
            }
            Ok(MetadumpResponse::Entry(km)) => Some(Ok(km)),
            Err(e) => Some(Err(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{TcpListener, TcpStream};
    use std::io::{BufRead, Write};

    const KEY: &str = "async-memcache-test-key";
    const EMPTY_KEY: &str = "no-value-here";
    const SERVER_ADDRESS: &str = "localhost:47386";
    
    #[ctor::ctor]
    fn init() {
        let listener = TcpListener::bind(SERVER_ADDRESS).expect("Failed to bind listener");

        // accept connections and process them serially in a background thread
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let stream = stream.expect("Failed to accept connection");
                std::thread::spawn(move || {
                    handle_connection(stream);
                });
            }
        });
    }

    // mocks out the commands that the memcached server will respond to, but does not mock out
    // the functionality of the server itself.
    fn handle_connection(mut stream: TcpStream) {
        let cloned_stream = stream.try_clone().expect("Failed to clone stream");
        let mut reader = std::io::BufReader::new(&cloned_stream);

        loop {
            let mut buffer = String::new();
            reader.read_line(&mut buffer).expect("Failed to read line");

            if buffer.is_empty() {
                return;
            }

            let parts: Vec<&str> = buffer.split_whitespace().collect();
            let command = parts[0];
            let key = parts[1];

            match command {
                "get" => {
                    if key == EMPTY_KEY {
                        let response = "END\r\n";
                        stream.write_all(response.as_bytes()).expect("Failed to write response");
                        return;
                    }

                    let response = format!("VALUE {} 0 5\r\nvalue\r\nEND\r\n", key);
                    stream.write_all(response.as_bytes()).expect("Failed to write response");
                }
                "set" => {
                    let mut value = String::new();
                    reader.read_line(&mut value).expect("Failed to read line");

                    let response = "STORED\r\n";
                    stream.write_all(response.as_bytes()).expect("Failed to write response");
                }
                "add" => {
                    let mut value = String::new();
                    reader.read_line(&mut value).expect("Failed to read line");

                    let response = "STORED\r\n";
                    stream.write_all(response.as_bytes()).expect("Failed to write response");
                }
                "delete" => {
                    let response = "DELETED\r\n";
                    stream.write_all(response.as_bytes()).expect("Failed to write response");
                }
                "version" => {
                    let response = "VERSION 1.6.7\r\n";
                    stream.write_all(response.as_bytes()).expect("Failed to write response");
                }
                "metadump" => {
                    let response = "END\r\n";
                    stream.write_all(response.as_bytes()).expect("Failed to write response");
                }
                "stats" => {
                    let response = "STAT pid 1234\r\nEND\r\n";
                    stream.write_all(response.as_bytes()).expect("Failed to write response");
                }
                _ => {
                    let response = "ERROR\r\n";
                    stream.write_all(response.as_bytes()).expect("Failed to write response");
                }
            }

            stream.flush().expect("Failed to flush stream");
        }
    }

    #[tokio::test]
    async fn test_add() {
        let mut node = Node::new("localhost:47386")
            .await
            .expect("Failed to connect to server");

        let result = node.delete(KEY).await;
        assert!(result.is_ok(), "failed to delete {}, {:?}", KEY, result);

        let result = node.add(KEY, "value", None, None).await;

        assert!(result.is_ok(), "failed to add {}, {:?}", KEY, result);
    }

    #[tokio::test]
    async fn test_delete() {
        let mut node = Node::new("localhost:47386")
            .await
            .expect("Failed to connect to server");

        let key = "async-memcache-test-key";

        let value = rand::random::<u64>().to_string();
        let result = node.set(key, &value, None, None).await;

        assert!(result.is_ok(), "failed to set {}, {:?}", key, result);

        let result = node.get(key).await;

        assert!(result.is_ok(), "failed to get {}, {:?}", key, result);
        let get_result = result.unwrap();

        match get_result {
            Some(get_value) => assert_eq!(
                String::from_utf8(get_value.data).expect("failed to parse a string"),
                "value" // mocked value instead of random value
            ),
            None => panic!("failed to get {}", key),
        }

        let result = node.delete(key).await;

        assert!(result.is_ok(), "failed to delete {}, {:?}", key, result);
    }
}