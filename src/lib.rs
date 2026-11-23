//! A Tokio-based memcached client.
#![deny(warnings, missing_docs)]

use bytes::BytesMut;
use rustc_hash::FxHashMap;
use std::ops::{Deref, DerefMut};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};

mod connection;
use self::connection::Connection;

mod error;
pub use self::error::Error;

mod parser;
use self::parser::{
    parse_ascii_metadump_response, parse_ascii_response, parse_ascii_stats_response,
};
pub use self::parser::{
    ErrorKind, KeyMetadata, MetaValue, MetadumpResponse, Response, StatsResponse, Status, Value,
};

/// Ascii & Meta protocol implementations
pub mod proto;
pub use self::proto::{AsciiProtocol, MetaProtocol};

mod value_serializer;
pub use self::value_serializer::AsMemcachedValue;

pub(crate) const MAX_KEY_LENGTH: usize = 250; // reference in memcached documentation: https://github.com/memcached/memcached/blob/5609673ed29db98a377749fab469fe80777de8fd/doc/protocol.txt#L46

/// High-level memcached client.
///
/// [`Client`] is mapped one-to-one with a given connection to a memcached server, and provides a
/// high-level API for executing commands on that connection.
///
/// Cancelling an operation or failing before its response is complete closes the connection,
/// because the unread bytes would otherwise be mistaken for the next operation's response.
/// This applies to every ASCII, meta, and administrative command, and to dropping a
/// [`MetadumpIter`] before it has been drained. An `ERROR` or `CLIENT_ERROR` reply to a command
/// that sends a value (`set`, `add`, `set_multi`, `add_multi`, `meta_set`, `meta_set_multi`) also
/// closes it: the server may have rejected the command line and then read the value as commands.
/// Later I/O returns [`Error::ConnectionClosed`]; operations are not retried.
pub struct Client {
    buf: BytesMut,
    conn: Connection,
    /// Set while an operation holds the connection. Still set when the next operation starts
    /// means the previous guard was leaked (e.g. `mem::forget`) and its reply may be unread.
    in_flight: bool,
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
            conn: connection,
            in_flight: false,
        })
    }

    /// Returns whether this client's connection is closed or will be closed by the next call.
    ///
    /// That is the case after an incomplete operation closed it, and also when an earlier
    /// operation or [`MetadumpIter`] was leaked (e.g. with `mem::forget`) before it completed:
    /// its reply may still be unread, so the next call closes the connection and returns
    /// [`Error::ConnectionClosed`].
    ///
    /// This checks local state only; it does not check whether the server is reachable.
    pub fn is_closed(&self) -> bool {
        self.in_flight || matches!(self.conn, Connection::Closed)
    }

    fn start_operation(&mut self) -> Result<OperationGuard<'_>, Error> {
        if self.in_flight {
            // The previous operation was leaked, so its guard never ran.
            self.close();
        }
        if self.is_closed() {
            return Err(Error::ConnectionClosed);
        }

        self.in_flight = true;
        Ok(OperationGuard {
            client: self,
            completed: false,
        })
    }

    fn close(&mut self) {
        // Dropping the transport discards buffered writes without flushing them.
        self.conn = Connection::Closed;
        self.buf = BytesMut::new();
        self.in_flight = false;
    }

    pub(crate) async fn drive_receive<R, F>(&mut self, op: F) -> Result<R, Error>
    where
        F: Fn(&[u8]) -> Result<Option<(usize, R)>, ErrorKind>,
    {
        let mut needs_more_data = false;
        loop {
            if self.buf.is_empty() || needs_more_data {
                self.buf.reserve(1024);
                let n = self.conn.read_buf(&mut self.buf).await?;
                if n == 0 {
                    return Err(Error::Io(std::io::ErrorKind::UnexpectedEof.into()));
                }
            }

            // Try and parse out a response.
            match op(&self.buf) {
                // We got a response.
                Ok(Some((n, response))) => {
                    let _ = self.buf.split_to(n);
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

    /// Reads the replies to a `set_multi` or `add_multi` pipeline. `keys` holds the result of
    /// `validate_key` for each item of `kv`, as the pipeline used it, so the call reads one reply
    /// for each key that the pipeline wrote and reports the error for each other key.
    pub(crate) async fn map_set_multi_responses<'a, K, V>(
        &mut self,
        kv: &'a [(K, V)],
        keys: Vec<Result<&[u8], Error>>,
    ) -> Result<FxHashMap<&'a K, Result<(), Error>>, Error>
    where
        K: Eq + std::hash::Hash,
        V: AsMemcachedValue,
    {
        let mut results = FxHashMap::with_capacity_and_hasher(kv.len(), Default::default());

        for ((key, _), checked) in kv.iter().zip(keys) {
            if let Err(error) = checked {
                results.insert(key, Err(error));
                continue;
            }

            let result = match self.drive_receive(parse_ascii_response).await {
                Ok(Response::Status(Status::Stored)) => Ok(()),
                Ok(Response::Status(s)) => {
                    Self::fail_on_rejected_line(&s)?;
                    Err(s.into())
                }
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

    /// Gets the version of the server.
    ///
    /// If the version is retrieved successfully, `String` is returned containing the version
    /// component e.g. `1.6.7`, otherwise [`Error`] is returned.
    ///
    /// The reply must start with `VERSION `. Any other line closes the connection and returns an
    /// error, because it most likely answers an earlier request, so the reply to `version` would
    /// answer the next one. If the client still holds unread bytes of an earlier reply, `version`
    /// closes the connection and returns an error without sending the command.
    ///
    /// For some setups, such as those using Twemproxy, this will return an error as those
    /// intermediate proxies do not support the version command.
    pub async fn version(&mut self) -> Result<String, Error> {
        let mut operation = self.start_operation()?;
        if !operation.buf.is_empty() {
            // Returning before `complete` makes the guard close the connection.
            return Err(Error::from(Status::Error(ErrorKind::Protocol(Some(
                "Unread bytes of an earlier reply before the `version` command".to_string(),
            )))));
        }

        operation.conn.write_all(b"version\r\n").await?;
        operation.conn.flush().await?;

        let mut version = String::new();
        operation.conn.read_line(&mut version).await?;
        Self::expect_complete_line(&version)?;

        // Peel off the leading "VERSION " header.
        if version.starts_with("VERSION ") {
            operation.complete();
            Ok(version.split_off(8))
        } else {
            // Returning before `complete` makes the guard close the connection.
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
    /// The dump occupies the connection until the iterator yields `None`. Dropping the iterator
    /// before then closes the connection, since the remaining entries would otherwise be read as
    /// the next operation's response.
    ///
    /// The iterator mutably borrows the client until it is dropped, even after it yields `None`.
    /// Drop it, or let it go out of scope, before sending more commands on the client.
    ///
    /// Available as of memcached 1.4.31.
    pub async fn dump_keys(&mut self) -> Result<MetadumpIter<'_>, Error> {
        let mut operation = self.start_operation()?;

        operation
            .conn
            .write_all(b"lru_crawler metadump all\r\n")
            .await?;
        operation.conn.flush().await?;

        Ok(MetadumpIter {
            operation: Some(operation),
            saw_end: false,
        })
    }

    /// Collects statistics from the server.
    ///
    /// The statistics that may be returned are detailed in the protocol specification for
    /// memcached, but all values returned by this method are returned as strings and are not
    /// further interpreted or validated for conformity.
    pub async fn stats(&mut self) -> Result<FxHashMap<String, String>, Error> {
        let mut entries = FxHashMap::default();

        let mut operation = self.start_operation()?;

        operation.conn.write_all(b"stats\r\n").await?;
        operation.conn.flush().await?;

        while let StatsResponse::Entry(key, value) = operation.get_stats_response().await? {
            entries.insert(key, value);
        }
        operation.complete();

        Ok(entries)
    }

    /// Flushes all existing items on the server
    ///
    /// This operation invalidates all existing items immediately. Any items with an update time
    /// older than the time of the flush_all operation will be ignored for retrieval purposes.
    /// This operation does not free up memory taken up by the existing items.
    pub async fn flush_all(&mut self) -> Result<(), Error> {
        let mut operation = self.start_operation()?;

        operation.conn.write_all(b"flush_all\r\n").await?;
        operation.conn.flush().await?;

        let mut response = String::new();
        operation.conn.read_line(&mut response).await?;
        Self::expect_complete_line(&response)?;
        operation.complete();

        // check if response is ok
        if response.trim() == "OK" {
            Ok(())
        } else {
            Err(Error::from(Status::Error(ErrorKind::Protocol(Some(
                format!("Invalid response for `flush_all` command: `{response}`"),
            )))))
        }
    }

    /// `read_line` stops early only at EOF, so a line without `\n` means the server closed the
    /// connection mid-response. Report it like `drive_receive` does, before completing the
    /// operation, so the guard closes the client.
    fn expect_complete_line(line: &str) -> Result<(), Error> {
        if line.ends_with('\n') {
            Ok(())
        } else {
            Err(Error::Io(std::io::ErrorKind::UnexpectedEof.into()))
        }
    }

    /// Fails with `status` when it says the server rejected the command line (`ERROR` or
    /// `CLIENT_ERROR`). Call it for every command that sends a value, and return its error before
    /// completing the operation: the guard then closes the connection, because the server may
    /// have read the value as commands whose replies would answer later requests.
    fn fail_on_rejected_line(status: &Status) -> Result<(), Error> {
        match status {
            Status::Error(ErrorKind::Client(_) | ErrorKind::NonexistentCommand) => {
                Err(status.clone().into())
            }
            _ => Ok(()),
        }
    }

    fn is_valid_token_byte(byte: u8) -> bool {
        // Text-protocol tokens must not contain whitespace, controls, or DEL. Keep bytes above
        // ASCII for compatibility with existing writers; the server can reject unsupported bytes.
        byte > 0x20 && byte != 0x7f
    }

    fn empty_key_error() -> Error {
        Error::Protocol(Status::Error(ErrorKind::Client("Key is empty".to_string())))
    }

    fn invalid_key_error() -> Error {
        Error::Protocol(Status::Error(ErrorKind::Client(
            "Key contains whitespace or control characters".to_string(),
        )))
    }

    fn invalid_opaque_error() -> Error {
        Error::Protocol(Status::Error(ErrorKind::Client(
            "Opaque token is empty or contains whitespace or control characters".to_string(),
        )))
    }

    fn invalid_meta_flag_error() -> Error {
        Error::Protocol(Status::Error(ErrorKind::Client(
            "Meta flag is empty or contains whitespace or control characters".to_string(),
        )))
    }

    fn meta_flag_without_letter_error() -> Error {
        Error::Protocol(Status::Error(ErrorKind::Client(
            "Meta flag does not start with an ASCII letter".to_string(),
        )))
    }

    /// Whether `kr` is safe to write as one key in a text-protocol command line: 1 to
    /// `MAX_KEY_LENGTH` bytes with no whitespace, control characters, or DEL byte. An empty key
    /// would leave the key token out of the line, so the server would read the next argument in
    /// its place. Other bytes could split the key into two tokens or end the line and start
    /// another command.
    fn is_valid_key(kr: &[u8]) -> bool {
        !kr.is_empty()
            && kr.len() <= MAX_KEY_LENGTH
            && kr.iter().all(|&b| Self::is_valid_token_byte(b))
    }

    fn validate_key(kr: &[u8]) -> Result<&[u8], Error> {
        if kr.is_empty() {
            return Err(Self::empty_key_error());
        }
        if kr.len() > MAX_KEY_LENGTH {
            return Err(Error::from(Status::Error(ErrorKind::KeyTooLong)));
        }
        if !kr.iter().all(|&b| Self::is_valid_token_byte(b)) {
            return Err(Self::invalid_key_error());
        }
        Ok(kr)
    }

    /// The opaque is written as one `O<opaque>` token. memcached limits that token, `O` included,
    /// to 32 bytes, so the opaque must be 1 to 31 bytes with no whitespace, control characters,
    /// or DEL byte.
    fn validate_opaque(opaque: &[u8]) -> Result<&[u8], Error> {
        if opaque.len() > 31 {
            return Err(Error::from(Status::Error(ErrorKind::OpaqueTooLong)));
        }
        if opaque.is_empty() || !opaque.iter().all(|&b| Self::is_valid_token_byte(b)) {
            return Err(Self::invalid_opaque_error());
        }
        Ok(opaque)
    }

    /// Each meta flag is written as one token, so it must not be empty or contain whitespace,
    /// control characters, or the DEL byte. Otherwise it could add an empty token or more tokens
    /// to the command, or end the line. It must also start with a letter, as every meta flag does:
    /// memcached 1.6.40 and newer reject an `ms` line whose flag starts with a byte that is
    /// neither a digit nor in `b'A'..=b'z'`, such as `!`, before they read the length of the
    /// value, and then read the value as commands.
    fn validate_meta_flags(meta_flags: Option<&[&str]>) -> Result<(), Error> {
        if let Some(meta_flags) = meta_flags {
            for flag in meta_flags {
                if flag.is_empty() || !flag.bytes().all(Self::is_valid_token_byte) {
                    return Err(Self::invalid_meta_flag_error());
                }
                if !flag.starts_with(|c: char| c.is_ascii_alphabetic()) {
                    return Err(Self::meta_flag_without_letter_error());
                }
            }
        }
        Ok(())
    }

    async fn check_and_write_opaque(&mut self, opaque: Option<&[u8]>) -> Result<(), Error> {
        if let Some(opaque) = &opaque {
            self.conn.write_all(b" O").await?;
            self.conn.write_all(opaque.as_ref()).await?;
        }
        Ok(())
    }

    /// Whether `check_and_write_meta_flags` writes `flag`. It drops a `q` flag, because quiet mode
    /// comes from the `is_quiet` parameter, and an `O` flag when an `opaque` is given.
    fn writes_meta_flag(flag: &str, opaque: Option<&[u8]>) -> bool {
        !(flag.starts_with('q') || (flag.starts_with('O') && opaque.is_some()))
    }

    async fn check_and_write_meta_flags(
        &mut self,
        meta_flags: Option<&[&str]>,
        opaque: Option<&[u8]>,
    ) -> Result<(), Error> {
        Self::validate_meta_flags(meta_flags)?;
        if let Some(meta_flags) = meta_flags {
            for flag in meta_flags {
                // Ignore q flag and require use of param, prefer explicit opaque param over O meta flag
                if !Self::writes_meta_flag(flag, opaque) {
                    continue;
                } else {
                    self.conn.write_all(b" ").await?;
                    self.conn.write_all(flag.as_bytes()).await?;
                }
            }
        }
        Ok(())
    }

    async fn check_and_write_quiet_mode(&mut self, is_quiet: bool) -> Result<(), Error> {
        if is_quiet {
            self.conn.write_all(b" q\r\nmn\r\n").await?;
        } else {
            self.conn.write_all(b"\r\n").await?;
        }
        Ok(())
    }
}

struct OperationGuard<'a> {
    client: &'a mut Client,
    completed: bool,
}

impl OperationGuard<'_> {
    fn complete(mut self) {
        self.completed = true;
        self.client.in_flight = false;
    }
}

impl Deref for OperationGuard<'_> {
    type Target = Client;

    fn deref(&self) -> &Self::Target {
        self.client
    }
}

impl DerefMut for OperationGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.client
    }
}

impl Drop for OperationGuard<'_> {
    fn drop(&mut self) {
        if !self.completed {
            self.client.close();
        }
    }
}

/// Asynchronous iterator for metadump operations.
///
/// The iterator holds the connection until it yields `None`. Dropping it earlier closes the
/// connection; see [`Client::dump_keys`].
pub struct MetadumpIter<'a> {
    /// `None` once the dump has ended, whether by `END`, a refusal, or an error.
    operation: Option<OperationGuard<'a>>,
    /// Set only when `END` was read.
    saw_end: bool,
}

impl MetadumpIter<'_> {
    /// Gets the next result for the current operation.
    ///
    /// If there is another key in the dump, `Some(Ok(KeyMetadata))` will be returned.  If there was
    /// an error while attempting to start the metadump operation, or if there was a general
    /// network/protocol-level error, `Some(Err(Error))` will be returned.
    ///
    /// Otherwise, `None` will be returned and signals the end of the iterator.  Subsequent calls
    /// will return `None`.
    ///
    /// Every `Some(Err(_))` is terminal: the next call returns `None`. A `BUSY` or `BADCLASS`
    /// refusal leaves the connection usable, since the whole refusal line has been read. A
    /// network or protocol error closes the connection. The dump is complete only if `None`
    /// arrives without a preceding `Some(Err(_))`; `None` after an error means the dump was cut
    /// short, not that the server has no more keys. Use [`is_complete`](Self::is_complete) to
    /// tell the two apart. Cancelling a call to `next` is safe: any partially read entry stays
    /// buffered and the following call resumes from it.
    pub async fn next(&mut self) -> Option<Result<KeyMetadata, Error>> {
        let operation = self.operation.as_mut()?;

        match operation.get_metadump_response().await {
            Ok(MetadumpResponse::End) => {
                self.saw_end = true;
                self.finish();
                None
            }
            Ok(MetadumpResponse::BadClass(s)) => {
                self.finish();
                Some(Err(Error::Protocol(MetadumpResponse::BadClass(s).into())))
            }
            Ok(MetadumpResponse::Busy(s)) => {
                self.finish();
                Some(Err(Error::Protocol(MetadumpResponse::Busy(s).into())))
            }
            Ok(MetadumpResponse::Entry(km)) => Some(Ok(km)),
            Err(e) => {
                // The stream is in an unknown state; dropping the guard uncompleted closes it.
                self.operation = None;
                Some(Err(e))
            }
        }
    }

    /// Returns whether the dump finished: `true` only after [`next`](Self::next) has read the
    /// server's `END` line. Before that, and after a `BUSY` or `BADCLASS` refusal or an error,
    /// it returns `false`.
    pub fn is_complete(&self) -> bool {
        self.saw_end
    }

    /// Marks the dump as fully read so the connection stays open.
    fn finish(&mut self) {
        if let Some(operation) = self.operation.take() {
            operation.complete();
        }
    }
}
