//! A Tokio-based memcached client.
#![deny(warnings, missing_docs)]

use bytes::BytesMut;
use fxhash::FxHashMap;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};

mod connection;
use self::connection::Connection;

mod error;
pub use self::error::Error;

mod parser;
use self::parser::{
    parse_ascii_metadump_response, parse_ascii_response, parse_ascii_stats_response, Response,
};
pub use self::parser::{ErrorKind, KeyMetadata, MetadumpResponse, StatsResponse, Status, Value};

mod value_serializer;
pub use self::value_serializer::AsMemcachedValue;

const MAX_KEY_LENGTH: usize = 250; // reference in memcached documentation: https://github.com/memcached/memcached/blob/5609673ed29db98a377749fab469fe80777de8fd/doc/protocol.txt#L46

/// High-level memcached client.
///
/// [`Client`] is mapped one-to-one with a given connection to a memcached server, and provides a
/// high-level API for executing commands on that connection.
pub struct Client {
    buf: BytesMut,
    last_read_n: Option<usize>,
    conn: Connection,
}

impl Client {
    /// Creates a new [`Client`] based on the given data source string.
    ///
    /// Supports UNIX domain sockets and TCP connections.
    /// For TCP: the DSN should be in the format of `tcp://<IP>:<port>` or `<IP>:<port>`.
    /// For UNIX: the DSN should be in the format of `unix://<path>`.
    pub async fn new<S: AsRef<str>>(dsn: S) -> Result<Client, Error> {
        let connection = Connection::new(dsn).await?;

        Ok(Client {
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
                    Connection::Unix(ref mut s) => {
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

    pub(crate) async fn map_set_multi_responses<'a, K, V>(
        &mut self,
        kv: &'a [(K, V)],
    ) -> Result<FxHashMap<&'a K, Result<(), Error>>, Error>
    where
        K: AsRef<[u8]> + Eq + std::hash::Hash,
        V: AsMemcachedValue,
    {
        let mut results = FxHashMap::with_capacity_and_hasher(kv.len(), Default::default());

        for (key, _) in kv {
            let kr = key.as_ref();
            if kr.len() > MAX_KEY_LENGTH {
                results.insert(
                    key,
                    Err(Error::Protocol(Status::Error(ErrorKind::Client(
                        "Key exceeds maximum length of 250 bytes".to_string(),
                    )))),
                );
                continue;
            }

            let result = match self.drive_receive(parse_ascii_response).await {
                Ok(Response::Status(Status::Stored)) => Ok(()),
                Ok(Response::Status(s)) => Err(s.into()),
                Ok(_) => Err(Status::Error(ErrorKind::Protocol(None)).into()),
                Err(e) => return Err(e),
            };

            results.insert(key, result);
        }

        Ok(results)
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
        self.conn
            .write_all(&[b"get ", Self::validate_key_length(key.as_ref())?, b"\r\n"].concat())
            .await?;
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
    pub async fn get_multi<I, K>(&mut self, keys: I) -> Result<Vec<Value>, Error>
    where
        I: IntoIterator<Item = K>,
        K: AsRef<[u8]>,
    {
        self.conn.write_all(b"get").await?;
        for key in keys {
            if key.as_ref().len() > MAX_KEY_LENGTH {
                continue;
            }
            self.conn.write_all(b" ").await?;
            self.conn.write_all(key.as_ref()).await?;
        }
        self.conn.write_all(b"\r\n").await?;
        self.conn.flush().await?;

        match self.get_read_write_response().await? {
            Response::Status(s) => Err(s.into()),
            Response::Data(d) => d.ok_or(Status::NotFound.into()),
            _ => Err(Status::Error(ErrorKind::Protocol(None)).into()),
        }
    }

    /// Gets the given keys.
    ///
    /// Deprecated: This is now an alias for `get_multi`, and  will be removed in the future.
    #[deprecated(
        since = "0.4.0",
        note = "This is now an alias for `get_multi`, and will be removed in the future."
    )]
    pub async fn get_many<I, K>(&mut self, keys: I) -> Result<Vec<Value>, Error>
    where
        I: IntoIterator<Item = K>,
        K: AsRef<[u8]>,
    {
        self.get_multi(keys).await
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
        V: AsMemcachedValue,
    {
        let vr = value.as_bytes();

        self.conn.write_all(b"set ").await?;
        self.conn
            .write_all(Self::validate_key_length(key.as_ref())?)
            .await?;

        let flags = flags.unwrap_or(0).to_string();
        self.conn.write_all(b" ").await?;
        self.conn.write_all(flags.as_ref()).await?;

        let ttl = ttl.unwrap_or(0).to_string();
        self.conn.write_all(b" ").await?;
        self.conn.write_all(ttl.as_ref()).await?;

        let vlen = vr.len().to_string();
        self.conn.write_all(b" ").await?;
        self.conn.write_all(vlen.as_ref()).await?;
        self.conn.write_all(b"\r\n").await?;

        self.conn.write_all(vr.as_ref()).await?;
        self.conn.write_all(b"\r\n").await?;

        self.conn.flush().await?;

        match self.get_read_write_response().await? {
            Response::Status(Status::Stored) => Ok(()),
            Response::Status(s) => Err(s.into()),
            _ => Err(Status::Error(ErrorKind::Protocol(None)).into()),
        }
    }

    /// Sets multiple keys and values through pipelined commands.
    ///
    /// If `ttl` or `flags` are not specified, they will default to 0. The same values for `ttl` and `flags` will be applied to each key.
    /// Returns a result with a HashMap of keys mapped to the result of the set operation, or an error.
    pub async fn set_multi<'a, K, V>(
        &mut self,
        kv: &'a [(K, V)],
        ttl: Option<i64>,
        flags: Option<u32>,
    ) -> Result<FxHashMap<&'a K, Result<(), Error>>, Error>
    where
        K: AsRef<[u8]> + Eq + std::hash::Hash + std::fmt::Debug,
        V: AsMemcachedValue,
    {
        for (key, value) in kv {
            let kr = key.as_ref();
            if kr.len() > MAX_KEY_LENGTH {
                continue;
            }

            let vr = value.as_bytes();

            self.conn.write_all(b"set ").await?;
            self.conn.write_all(kr).await?;

            let flags = flags.unwrap_or(0).to_string();
            self.conn.write_all(b" ").await?;
            self.conn.write_all(flags.as_ref()).await?;

            let ttl = ttl.unwrap_or(0).to_string();
            self.conn.write_all(b" ").await?;
            self.conn.write_all(ttl.as_ref()).await?;

            let vlen = vr.len().to_string();
            self.conn.write_all(b" ").await?;
            self.conn.write_all(vlen.as_ref()).await?;
            self.conn.write_all(b"\r\n").await?;

            self.conn.write_all(vr.as_ref()).await?;
            self.conn.write_all(b"\r\n").await?;
        }
        self.conn.flush().await?;

        let results = self.map_set_multi_responses(kv).await?;

        Ok(results)
    }

    /// Sets multiple keys.
    ///
    /// If `ttl` or `flags` are not specified, they will default to 0. The same values for `ttl` and `flags` will be applied to each key.
    /// Returns a result with a HashMap of keys to the results of the set operation, or an error.
    pub async fn set_multi_loop_no_reply<I, K, V>(
        &mut self,
        kv: impl AsRef<[(K, V)]>,
        ttl: Option<i64>,
        flags: Option<u32>,
    ) -> Result<HashMap<K, Result<Response, Error>>, Error>
    where
        I: IntoIterator<Item = (K, V)> + Clone,
        K: AsRef<[u8]> + Eq + std::hash::Hash,
        V: AsMemcachedValue,
    {
        let mut kv_iter = kv.into_iter().peekable();

        if kv_iter.peek().is_none() {
            return Ok(());
        }

        // Write commands and collect key-error pairs
        for (key, value) in kv_iter {
            self.set_no_reply(&key, value, ttl, flags).await?;
        }

        Ok(())
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
        V: AsMemcachedValue,
    {
        let vr = value.as_bytes();

        self.conn.write_all(b"add ").await?;
        self.conn
            .write_all(Self::validate_key_length(key.as_ref())?)
            .await?;

        let flags = flags.unwrap_or(0).to_string();
        self.conn.write_all(b" ").await?;
        self.conn.write_all(flags.as_ref()).await?;

        let ttl = ttl.unwrap_or(0).to_string();
        self.conn.write_all(b" ").await?;
        self.conn.write_all(ttl.as_ref()).await?;

        let vlen = vr.len().to_string();
        self.conn.write_all(b" ").await?;
        self.conn.write_all(vlen.as_ref()).await?;
        self.conn.write_all(b"\r\n").await?;

        self.conn.write_all(vr.as_ref()).await?;
        self.conn.write_all(b"\r\n").await?;

        self.conn.flush().await?;

        match self.get_read_write_response().await? {
            Response::Status(Status::Stored) => Ok(()),
            Response::Status(s) => Err(s.into()),
            _ => Err(Status::Error(ErrorKind::Protocol(None)).into()),
        }
    }

    /// Attempts to add multiple keys and values through pipelined commands.
    ///
    /// If `ttl` or `flags` are not specified, they will default to 0. The same values for `ttl` and `flags` will be applied to each key.
    /// Returns a result with a HashMap of keys mapped to the result of the add operation, or an error.
    pub async fn add_multi<'a, K, V>(
        &mut self,
        kv: &'a [(K, V)],
        ttl: Option<i64>,
        flags: Option<u32>,
    ) -> Result<FxHashMap<&'a K, Result<(), Error>>, Error>
    where
        K: AsRef<[u8]> + Eq + std::hash::Hash + std::fmt::Debug,
        V: AsMemcachedValue,
    {
        for (key, value) in kv {
            let kr = key.as_ref();
            if kr.len() > MAX_KEY_LENGTH {
                continue;
            }

            let vr = value.as_bytes();

            self.conn.write_all(b"add ").await?;
            self.conn.write_all(kr).await?;

            let flags = flags.unwrap_or(0).to_string();
            self.conn.write_all(b" ").await?;
            self.conn.write_all(flags.as_ref()).await?;

            let ttl = ttl.unwrap_or(0).to_string();
            self.conn.write_all(b" ").await?;
            self.conn.write_all(ttl.as_ref()).await?;

            let vlen = vr.len().to_string();
            self.conn.write_all(b" ").await?;
            self.conn.write_all(vlen.as_ref()).await?;
            self.conn.write_all(b"\r\n").await?;

            self.conn.write_all(vr.as_ref()).await?;
            self.conn.write_all(b"\r\n").await?;
        }
        self.conn.flush().await?;

        let results = self.map_set_multi_responses(kv).await?;

        Ok(results)
    }

    /// Delete a key but don't wait for a reply.
    pub async fn delete_no_reply<K>(&mut self, key: K) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
    {
        self.conn
            .write_all(
                &[
                    b"delete ",
                    Self::validate_key_length(key.as_ref())?,
                    b" noreply\r\n",
                ]
                .concat(),
            )
            .await?;
        self.conn.flush().await?;
        Ok(())
    }

    /// Delete a key and wait for a reply
    pub async fn delete<K>(&mut self, key: K) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
    {
        self.conn
            .write_all(
                &[
                    b"delete ",
                    Self::validate_key_length(key.as_ref())?,
                    b"\r\n",
                ]
                .concat(),
            )
            .await?;
        self.conn.flush().await?;

        match self.get_read_write_response().await? {
            Response::Status(Status::Deleted) => Ok(()),
            Response::Status(s) => Err(s.into()),
            _ => Err(Status::Error(ErrorKind::Protocol(None)).into()),
        }
    }

    /// Delete multiple keys
    pub async fn delete_multi_no_reply<K>(&mut self, keys: &[K]) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
    {
        for key in keys {
            self.conn.write_all(b"delete ").await?;
            self.conn
                .write_all(Self::validate_key_length(key.as_ref())?)
                .await?;
            self.conn.write_all(b" noreply\r\n").await?;
        }
        self.conn.flush().await?;

        Ok(())
    }

    /// Increments the given key by the specified amount.
    /// Can overflow from the max value of u64 (18446744073709551615) -> 0.
    /// If the key does not exist, the server will return a KeyNotFound error.
    /// If the key exists but the value is non-numeric, the server will return a ClientError.
    pub async fn increment<K>(&mut self, key: K, amount: u64) -> Result<u64, Error>
    where
        K: AsRef<[u8]>,
    {
        self.conn
            .write_all(
                &[
                    b"incr ",
                    Self::validate_key_length(key.as_ref())?,
                    b" ",
                    amount.to_string().as_bytes(),
                    b"\r\n",
                ]
                .concat(),
            )
            .await?;
        self.conn.flush().await?;

        match self.get_read_write_response().await? {
            Response::Status(s) => Err(s.into()),
            Response::IncrDecr(amount) => Ok(amount),
            _ => Err(Status::Error(ErrorKind::Protocol(None)).into()),
        }
    }

    /// Increments the given key by the specified amount with no reply from the server.
    /// Can overflow from the max value of u64 (18446744073709551615) -> 0.
    /// Always returns () for a complete request, will not return any indication of success or failure.
    pub async fn increment_no_reply<K>(&mut self, key: K, amount: u64) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
    {
        self.conn
            .write_all(
                &[
                    b"incr ",
                    Self::validate_key_length(key.as_ref())?,
                    b" ",
                    amount.to_string().as_bytes(),
                    b" noreply\r\n",
                ]
                .concat(),
            )
            .await?;
        self.conn.flush().await?;

        Ok(())
    }

    /// Decrements the given key by the specified amount.
    /// Will not decrement the counter below 0.
    /// If the key does not exist, the server will return a KeyNotFound error.
    /// If the key exists but the value is non-numeric, the server will return a ClientError.
    pub async fn decrement<K>(&mut self, key: K, amount: u64) -> Result<u64, Error>
    where
        K: AsRef<[u8]>,
    {
        self.conn
            .write_all(
                &[
                    b"decr ",
                    Self::validate_key_length(key.as_ref())?,
                    b" ",
                    amount.to_string().as_bytes(),
                    b"\r\n",
                ]
                .concat(),
            )
            .await?;
        self.conn.flush().await?;

        match self.get_read_write_response().await? {
            Response::Status(s) => Err(s.into()),
            Response::IncrDecr(amount) => Ok(amount),
            _ => Err(Status::Error(ErrorKind::Protocol(None)).into()),
        }
    }

    /// Decrements the given key by the specified amount with no reply from the server.
    /// Will not decrement the counter below 0.
    /// Always returns () for a complete request, will not return any indication of success or failure.
    pub async fn decrement_no_reply<K>(&mut self, key: K, amount: u64) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
    {
        self.conn
            .write_all(
                &[
                    b"decr ",
                    Self::validate_key_length(key.as_ref())?,
                    b" ",
                    amount.to_string().as_bytes(),
                    b" noreply\r\n",
                ]
                .concat(),
            )
            .await?;
        self.conn.flush().await?;

        Ok(())
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
            client: self,
            done: false,
        })
    }

    /// Collects statistics from the server.
    ///
    /// The statistics that may be returned are detailed in the protocol specification for
    /// memcached, but all values returned by this method are returned as strings and are not
    /// further interpreted or validated for conformity.
    pub async fn stats(&mut self) -> Result<FxHashMap<String, String>, Error> {
        let mut entries = FxHashMap::default();

        self.conn.write_all(b"stats\r\n").await?;
        self.conn.flush().await?;

        while let StatsResponse::Entry(key, value) = self.get_stats_response().await? {
            entries.insert(key, value);
        }

        Ok(entries)
    }

    /// Flushes all existing items on the server
    ///
    /// This operation invalidates all existing items immediately. Any items with an update time
    /// older than the time of the flush_all operation will be ignored for retrieval purposes.
    /// This operation does not free up memory taken up by the existing items.

    pub async fn flush_all(&mut self) -> Result<(), Error> {
        self.conn.write_all(b"flush_all\r\n").await?;
        self.conn.flush().await?;

        let mut response = String::new();
        self.conn.read_line(&mut response).await?;
        // check if response is ok
        if response.trim() == "OK" {
            Ok(())
        } else {
            Err(Error::from(Status::Error(ErrorKind::Protocol(Some(
                format!("Invalid response for `flush_all` command: `{response}`"),
            )))))
        }
    }

    fn validate_key_length(kr: &[u8]) -> Result<&[u8], Error> {
        if kr.len() > MAX_KEY_LENGTH {
            return Err(Error::from(Status::Error(ErrorKind::KeyTooLong)));
        }
        Ok(kr)
    }
}

/// Asynchronous iterator for metadump operations.
pub struct MetadumpIter<'a> {
    client: &'a mut Client,
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

        match self.client.get_metadump_response().await {
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
