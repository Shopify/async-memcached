//! A Tokio-based memcached client.
#![deny(warnings, missing_docs)]
use std::collections::HashMap;

use bytes::BytesMut;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};

mod connection;
use self::connection::Connection;

mod error;
pub use self::error::Error;

mod parser;
use self::parser::{
    parse_ascii_metadump_response, parse_ascii_response, parse_ascii_stats_response, Response,
};
pub use self::parser::{
    ErrorKind, KeyMetadata, MetadumpResponse, StatsResponse, Status, ToMemcachedValue, Value,
};

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
        V: ToMemcachedValue,
    {
        let kr = key.as_ref();

        self.conn.write_all(b"set ").await?;
        self.conn.write_all(kr).await?;

        let flags = flags.unwrap_or(0).to_string();
        self.conn.write_all(b" ").await?;
        self.conn.write_all(flags.as_ref()).await?;

        let ttl = ttl.unwrap_or(0).to_string();
        self.conn.write_all(b" ").await?;
        self.conn.write_all(ttl.as_ref()).await?;

        self.conn.write_all(b" ").await?;
        self.conn
            .write_all(value.length().to_string().as_bytes())
            .await?;
        self.conn.write_all(b"\r\n").await?;

        value.write_to(&mut self.conn).await?;
        self.conn.write_all(b"\r\n").await?;
        self.conn.flush().await?;

        match self.get_read_write_response().await? {
            Response::Status(Status::Stored) => Ok(()),
            Response::Status(s) => Err(s.into()),
            _ => Err(Status::Error(ErrorKind::Protocol(None)).into()),
        }
    }

    /// Sets the given key.
    ///
    /// If `ttl` or `flags` are not specified, they will default to 0.  If the value is set
    /// successfully, `()` is returned, otherwise [`Error`] is returned.
    pub async fn original_set<K, V>(
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
        V: ToMemcachedValue,
    {
        let kr = key.as_ref();

        self.conn.write_all(b"add ").await?;
        self.conn.write_all(kr).await?;

        let flags = flags.unwrap_or(0).to_string();
        self.conn.write_all(b" ").await?;
        self.conn.write_all(flags.as_ref()).await?;

        let ttl = ttl.unwrap_or(0).to_string();
        self.conn.write_all(b" ").await?;
        self.conn.write_all(ttl.as_ref()).await?;

        self.conn.write_all(b" ").await?;
        self.conn
            .write_all(value.length().to_string().as_bytes())
            .await?;
        self.conn.write_all(b"\r\n").await?;

        value.write_to(&mut self.conn).await?;
        self.conn.write_all(b"\r\n").await?;
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

    /// Increments the given key by the specified amount.
    /// Can overflow from the max value of u64 (18446744073709551615) -> 0.
    /// Returns the new value of the key if key exists, otherwise returns KeyNotFound error.
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
    /// Returns the new value of the key if key exists, otherwise returns KeyNotFound error.
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
    async fn test_add() {
        let mut client = setup_client().await;

        let key = "async-memcache-test-key-add";

        let result = client.delete_no_reply(key).await;
        assert!(result.is_ok(), "failed to delete {}, {:?}", key, result);

        let result = client.add(key, "value", None, None).await;

        assert!(result.is_ok());
    }

    #[ignore = "Relies on a running memcached server"]
    #[tokio::test]
    async fn test_original_set() {
        let mut client = setup_client().await;

        let key = "original-set-key";
        let value = "value";
        let result = client.original_set(key, value, None, None).await;

        assert!(result.is_ok());

        let result = client.get(key).await;

        assert!(result.is_ok());

        let get_result = result.unwrap();

        assert_eq!(String::from_utf8(get_result.unwrap().data).unwrap(), value);
    }

    #[ignore = "Relies on a running memcached server"]
    #[tokio::test]
    async fn test_new_set_with_string_value() {
        let mut client = setup_client().await;

        let key = "new-set-key";
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
    async fn test_new_set_with_u64_value() {
        let mut client = setup_client().await;

        let key = "new-set-key-with-u64-value";
        let value: u64 = 20;

        println!("value: {}", value);
        let result = client.set(key, value, None, None).await;

        println!("result: {:?}", result);
        assert!(result.is_ok());

        let result = client.get(key).await;

        assert!(result.is_ok());

        let get_result = result.unwrap();

        println!("get_result: {:?}", get_result);

        println!(
            "get_result.unwrap().data: {:?}",
            String::from_utf8(get_result.unwrap().data).unwrap()
        );
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
}
