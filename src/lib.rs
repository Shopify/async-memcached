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
    parse_ascii_metadump_response, parse_ascii_response, parse_ascii_stats_response,
    parse_meta_response, Response,
};
pub use self::parser::{ErrorKind, KeyMetadata, MetadumpResponse, StatsResponse, Status, Value};

mod value_serializer;
pub use self::value_serializer::AsMemcachedValue;

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
            .write_all(&[b"get ", key.as_ref(), b"\r\n"].concat())
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

    /// Gets the given key with additional metadata.
    ///
    /// If the key is found, `Some(Value)` is returned, describing the metadata and data of the key.
    ///
    /// Otherwise, `None` is returned.
    ///
    /// Supported meta flags:
    /// - h: return whether item has been hit before as a 0 or 1
    /// - l: return time since item was last accessed in seconds
    /// - t: return item TTL remaining in seconds (-1 for unlimited)
    pub async fn meta_get<K: AsRef<[u8]>>(
        &mut self,
        key: K,
        flags: &[char],
    ) -> Result<Option<Value>, Error> {
        let mut command = Vec::with_capacity(64);
        command.extend_from_slice(b"mg ");
        command.extend_from_slice(key.as_ref());
        // TODO: Do we want to always add the v flag? so all gets return the value? or we want clients to know to request it?
        command.extend_from_slice(b" v");
        if !flags.is_empty() {
            for flag in flags {
                command.push(b' ');
                command.push(*flag as u8);
            }
        }
        command.extend_from_slice(b"\r\n");

        self.conn.write_all(&command).await?;
        self.conn.flush().await?;

        match self.drive_receive(parse_meta_response).await? {
            Response::Status(Status::NotFound) => Ok(None),
            Response::Status(s) => Err(s.into()),
            Response::Data(d) => d
                .map(|mut items| {
                    if items.len() != 1 {
                        Err(Status::Error(ErrorKind::Protocol(None)).into())
                    } else {
                        let mut item = items.remove(0);
                        item.key = key.as_ref().to_vec();
                        Ok(item)
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
        self.conn.write_all(b"get ").await?;
        for key in keys {
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
        let kr = key.as_ref();
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
        let kr = key.as_ref();
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

    /// Delete multiple keys
    pub async fn delete_multi_no_reply<K>(&mut self, keys: &[K]) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
    {
        for key in keys {
            self.conn.write_all(b"delete ").await?;
            self.conn.write_all(key.as_ref()).await?;
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
                    key.as_ref(),
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
                    key.as_ref(),
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
                    key.as_ref(),
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
                    key.as_ref(),
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

#[cfg(test)]
mod tests {
    use super::*;

    async fn setup_client() -> Client {
        Client::new("tcp://127.0.0.1:11211")
            .await
            .expect("Failed to connect to server")
    }

    #[ignore = "Relies on a running memcached server"]
    #[tokio::test]
    async fn test_add_with_string_value() {
        let mut client = setup_client().await;

        let key = "async-memcache-test-key-add";

        let result = client.delete_no_reply(key).await;
        assert!(result.is_ok(), "failed to delete {}, {:?}", key, result);

        let result = client.add(key, "value", None, None).await;

        assert!(result.is_ok());
    }

    #[ignore = "Relies on a running memcached server"]
    #[tokio::test]
    async fn test_add_with_u64_value() {
        let mut client = setup_client().await;

        let key = "async-memcache-test-key-add-u64";
        let value: u64 = 10;

        let result = client.delete_no_reply(key).await;
        assert!(result.is_ok(), "failed to delete {}, {:?}", key, result);

        let result = client.add(key, value, None, None).await;

        assert!(result.is_ok());
    }

    #[ignore = "Relies on a running memcached server"]
    #[tokio::test]
    async fn test_set_with_str_ref_value() {
        let mut client = setup_client().await;

        let key = "set-key-with-string-value";
        let value = "value";
        let result = client.set(key, value, None, None).await;

        assert!(result.is_ok());

        let result = client.get(key).await;

        assert!(result.is_ok());

        let get_result = result.unwrap();

        assert_eq!(String::from_utf8(get_result.unwrap().data).unwrap(), value);
    }

    #[ignore = "Relies on a running memcached server"]
    #[tokio::test]
    async fn test_set_with_string_value() {
        let mut client = setup_client().await;

        let key = "set-key-with-string-value";
        let value = String::from("value");
        let result = client.set(key, &value, None, None).await;

        assert!(result.is_ok());

        let result = client.get(key).await;

        assert!(result.is_ok());

        let get_result = result.unwrap();

        assert_eq!(String::from_utf8(get_result.unwrap().data).unwrap(), value);
    }

    #[ignore = "Relies on a running memcached server"]
    #[tokio::test]
    async fn test_set_with_string_ref_value() {
        let mut client = setup_client().await;

        let key = "set-key-with-string-reference-value";
        let value = String::from("value");
        let result = client.set(key, &value, None, None).await;

        assert!(result.is_ok());

        let result = client.get(key).await;

        assert!(result.is_ok());

        let get_result = result.unwrap();

        assert_eq!(String::from_utf8(get_result.unwrap().data).unwrap(), value);
    }

    #[ignore = "Relies on a running memcached server"]
    #[tokio::test]
    async fn test_set_with_u64_value() {
        let mut client = setup_client().await;

        let key = "set-key-with-u64-value";
        let value: u64 = 20;

        let result = client.set(key, value, None, None).await;

        assert!(result.is_ok());

        let result = client.get(key).await;

        assert!(result.is_ok());
    }

    #[ignore = "Relies on a running memcached server"]
    #[tokio::test]
    async fn test_delete() {
        let mut client = setup_client().await;

        let key = "async-memcache-test-key-delete";

        let value = rand::random::<u64>();
        let result = client.set(key, value, None, None).await;

        assert!(result.is_ok(), "failed to set {}, {:?}", key, result);

        let result = client.get(key).await;

        assert!(result.is_ok(), "failed to get {}, {:?}", key, result);

        let get_result = result.unwrap();

        match get_result {
            Some(get_value) => assert_eq!(
                String::from_utf8(get_value.data).expect("failed to parse a string"),
                value.to_string()
            ),
            None => panic!("failed to get {}", key),
        }

        let result = client.delete(key).await;

        assert!(result.is_ok(), "failed to delete {}, {:?}", key, result);
    }

    #[ignore = "Relies on a running memcached server"]
    #[tokio::test]
    async fn test_delete_no_reply() {
        let mut client = setup_client().await;

        let key = "async-memcache-test-key-delete-no-reply";

        let value = format!("{}", rand::random::<u64>());
        let result = client.set(key, value.as_str(), None, None).await;

        assert!(result.is_ok(), "failed to set {}, {:?}", key, result);

        let result = client.get(key).await;

        assert!(result.is_ok(), "failed to get {}, {:?}", key, result);

        let get_result = result.unwrap();

        match get_result {
            Some(get_value) => assert_eq!(
                String::from_utf8(get_value.data).expect("failed to parse a string"),
                value
            ),
            None => panic!("failed to get {}", key),
        }

        let result = client.delete_no_reply(key).await;

        assert!(result.is_ok(), "failed to delete {}, {:?}", key, result);
    }

    #[ignore = "Relies on a running memcached server"]
    #[tokio::test]
    async fn test_increment_raises_error_when_key_doesnt_exist() {
        let mut client = setup_client().await;

        let key = "key-does-not-exist";
        let amount = 1;

        let result = client.increment(key, amount).await;

        assert!(matches!(result, Err(Error::Protocol(Status::NotFound))));
    }

    #[ignore = "Relies on a running memcached server"]
    #[tokio::test]
    async fn test_increments_existing_key() {
        let mut client = setup_client().await;

        let key = "key-to-increment";
        let value: u64 = 1;

        let _ = client.set(key, value, None, None).await;

        let amount = 1;

        let result = client.increment(key, amount).await;

        assert_eq!(Ok(2), result);
    }

    #[ignore = "Relies on a running memcached server"]
    #[tokio::test]
    async fn test_increment_can_overflow() {
        let mut client = setup_client().await;

        let key = "key-to-increment-overflow";
        let value = u64::MAX; // max value for u64

        let _ = client.set(key, value, None, None).await;

        let amount = 1;

        // First increment should overflow
        let result = client.increment(key, amount).await;

        assert_eq!(Ok(0), result);

        // Subsequent increments should work as normal
        let result = client.increment(key, amount).await;

        assert_eq!(Ok(1), result);
    }

    #[ignore = "Relies on a running memcached server"]
    #[tokio::test]
    async fn test_increments_existing_key_with_no_reply() {
        let mut client = setup_client().await;

        let key = "key-to-increment-no-reply";
        let value: u64 = 1;

        let _ = client.set(key, value, None, None).await;

        let amount = 1;

        let result = client.increment_no_reply(key, amount).await;

        assert_eq!(Ok(()), result);
    }

    #[ignore = "Relies on a running memcached server"]
    #[tokio::test]
    async fn test_decrement_raises_error_when_key_doesnt_exist() {
        let mut client = setup_client().await;

        let key = "fails-to-decrement";
        let amount = 1;

        let result = client.decrement(key, amount).await;

        assert!(matches!(result, Err(Error::Protocol(Status::NotFound))));
    }

    #[ignore = "Relies on a running memcached server"]
    #[tokio::test]
    async fn test_decrements_existing_key() {
        let mut client = setup_client().await;

        let key = "key-to-decrement";
        let value: u64 = 10;

        let _ = client.set(key, value, None, None).await;

        let amount = 1;

        let result = client.decrement(key, amount).await;

        assert_eq!(Ok(9), result);
    }

    #[ignore = "Relies on a running memcached server"]
    #[tokio::test]
    async fn test_decrement_does_not_reduce_value_below_zero() {
        let mut client = setup_client().await;

        let key = "key-to-decrement-past-zero";
        let value: u64 = 0;

        let _ = client.set(key, value, None, None).await;

        let amount = 1;

        let result = client.decrement(key, amount).await;

        assert_eq!(Ok(0), result);
    }

    #[ignore = "Relies on a running memcached server"]
    #[tokio::test]
    async fn test_decrements_existing_key_with_no_reply() {
        let mut client = setup_client().await;

        let key = "key-to-decrement-no-reply";
        let value: u64 = 1;

        let _ = client.set(key, value, None, None).await;

        let amount = 1;

        let result = client.decrement_no_reply(key, amount).await;

        assert_eq!(Ok(()), result);
    }

    #[ignore = "Relies on a running memcached server"]
    #[tokio::test]
    async fn test_get() {
        let mut client = setup_client().await;

        let key = "meta-get-test-key";
        let value = "test-value";
        let ttl = 3600; // 1 hour

        // Set the key with a TTL
        client.set(key, value, Some(ttl), None).await.unwrap();

        // Perform a get to ensure the item has been hit
        let result = client.get(key).await.unwrap();
        let result_value = result.unwrap();
        assert_eq!(String::from_utf8(result_value.data).unwrap(), value);
    }

    #[ignore = "Relies on a running memcached server"]
    #[tokio::test]
    async fn test_get_not_found() {
        let mut client = setup_client().await;

        let key = "not-found-get-test-key";
        let result = client.get(key).await.unwrap();
        assert_eq!(result, None);
    }

    #[ignore = "Relies on a running memcached server"]
    #[tokio::test]
    async fn test_meta_get_with_all_flags() {
        let mut client = setup_client().await;

        let key = "a-get-test-key";
        let value = "test-value";
        let ttl = 3600; // 1 hour

        // Set the key with a TTL
        client.set(key, value, Some(ttl), None).await.unwrap();

        // Perform a get to ensure the item has been hit
        let result = client.get(key).await.unwrap();
        let result_value = result.unwrap();
        assert_eq!(String::from_utf8(result_value.data).unwrap(), value);

        let flags = ['h', 'l', 't'];
        let result = client.meta_get(key, &flags).await.unwrap();

        assert!(result.is_some());
        let result_meta_value = result.unwrap();

        assert_eq!(String::from_utf8(result_meta_value.data).unwrap(), value);

        let meta_flag_values = result_meta_value.meta_values.unwrap();
        assert!(meta_flag_values.hit_before.unwrap());
        assert_eq!(meta_flag_values.last_accessed.unwrap(), 0);
        assert!(meta_flag_values.ttl_remaining.unwrap() > 0);
    }

    #[ignore = "Relies on a running memcached server"]
    #[tokio::test]
    async fn test_meta_get_not_found() {
        let mut client = setup_client().await;

        let key = "not-found-meta-get-test-key";
        let flags = ['h', 'l', 't'];
        let result = client.meta_get(key, &flags).await.unwrap();
        assert_eq!(result, None);
    }
}
