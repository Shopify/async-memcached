use crate::{AsMemcachedValue, Client, Error, ErrorKind, Status};

use crate::parser::{
    parse_meta_arithmetic_response, parse_meta_delete_response, parse_meta_get_response,
    parse_meta_set_response,
};
use crate::parser::{MetaResponse, MetaValue};

use std::future::Future;

use tokio::io::AsyncWriteExt;

type MetaResponseParser = fn(&[u8]) -> Result<Option<(usize, MetaResponse)>, ErrorKind>;

async fn drain_quiet_noop_response(
    client: &mut Client,
    parser: MetaResponseParser,
) -> Result<(), Error> {
    match client.drive_receive(parser).await? {
        MetaResponse::Status(Status::NoOp) => Ok(()),
        MetaResponse::Status(status) => Err(Status::Error(ErrorKind::Protocol(Some(format!(
            "Expected quiet-mode no-op response, got status {status}"
        ))))
        .into()),
        MetaResponse::Data(_) => Err(Status::Error(ErrorKind::Protocol(Some(
            "Expected quiet-mode no-op response, got data response".to_string(),
        )))
        .into()),
    }
}

/// Trait defining Meta protocol-specific methods for the Client.
pub trait MetaProtocol {
    /// Gets the given key with additional metadata.
    ///
    /// If the key is found, `Some(MetaValue)` is returned, describing the metadata and data of the key.
    ///
    /// Otherwise, `None` is returned.
    //
    // Command format:
    // mg <key> <meta_flags>*\r\n
    //
    // - <key> is the key string, with a maximum length of 250 bytes.
    //
    // - <meta_flags> is an optional slice of string references for meta flags.
    // Meta flags may have associated tokens after the initial character, e.g. "O123" for opaque.
    // Using the "q" flag for quiet mode will append a no-op command to the request ("mn\r\n") so that the client
    // can proceed properly in the event of a cache miss.
    fn meta_get<K: AsRef<[u8]>>(
        &mut self,
        key: K,
        is_quiet: bool,
        opaque: Option<&[u8]>,
        meta_flags: Option<&[&str]>,
    ) -> impl Future<Output = Result<Option<MetaValue>, Error>>;

    /// Sets the given key with additional metadata.
    ///
    /// If the value is set successfully, `Some(MetaValue)` is returned, otherwise [`Error`] is returned.
    /// NOTE: That the data in this MetaValue is sparsely populated, containing only requested data by meta_flags
    /// The meta set command is a generic command for storing data to memcached. Based on the flags supplied,
    /// it can replace all storage commands (see token M) as well as adds new options.
    //
    // Command format:
    // ms <key> <datalen> <meta_flags>*\r\n<data_block>\r\n
    //
    // - <key> is the key string, with a maximum length of 250 bytes.
    // - <datalen> is the length of the payload data.
    //
    // - <meta_flags> is an optional slice of string references for meta flags.
    // Meta flags may have associated tokens after the initial character, e.g. "O123" for opaque.
    //
    // - <data_block> is the payload data to be stored, with a maximum size of ~1MB.
    fn meta_set<K, V>(
        &mut self,
        key: K,
        value: V,
        is_quiet: bool,
        opaque: Option<&[u8]>,
        meta_flags: Option<&[&str]>,
    ) -> impl Future<Output = Result<Option<MetaValue>, Error>>
    where
        K: AsRef<[u8]>,
        V: AsMemcachedValue;

    /// Deletes the given key with additional metadata.
    ///
    /// If the key is found, it will be deleted, invalidated or tombstoned depending on the meta flags provided.
    /// If data is requested back via meta flags then a `MetaValue` is returned, otherwise `None`.
    //
    // Command format:
    // md <key> <meta_flags>*\r\n
    //
    // - <key> is the key string, with a maximum length of 250 bytes.
    //
    // - <meta_flags> is an optional slice of string references for meta flags.
    // Meta flags may have associated tokens after the initial character, e.g. "O123" for opaque.
    fn meta_delete<K: AsRef<[u8]>>(
        &mut self,
        key: K,
        is_quiet: bool,
        opaque: Option<&[u8]>,
        meta_flags: Option<&[&str]>,
    ) -> impl Future<Output = Result<Option<MetaValue>, Error>>;

    /// Performs an increment (arithmetic) operation on the given key.
    ///
    /// If the key is found, the increment operation is performed.
    /// If data is requested back via meta flags then a `MetaValue` is returned, otherwise `None`.
    ///
    /// Command format:
    ///   ma <key> <meta_flags>*\r\n
    ///
    /// - <key> is the key string, with a maximum length of 250 bytes.
    ///
    /// - <opaque> is an optional slice of string references with a maximum length of 32 bytes.
    ///
    /// - <delta> is an optional u64 value for the decrement delta.
    ///   The default behaviour is to decrement with a delta of 1.
    ///
    /// - <is_quiet> is a boolean value indicating whether to use quiet mode.
    ///   quiet mode will append a no-op command to the request ("mn\r\n") so that the client
    ///   can proceed properly in the event of a cache miss.
    ///
    /// - <meta_flags> is an optional slice of string references for additional meta flags.
    ///   Meta flags may have associated tokens after the initial character, e.g "N123"
    ///   Do not include "M", "D", "O" or "q" flags as additional meta flags, they will be ignored.
    ///   Instead, use the specified parameters.
    fn meta_increment<K: AsRef<[u8]>>(
        &mut self,
        key: K,
        is_quiet: bool,
        opaque: Option<&[u8]>,
        delta: Option<u64>,
        meta_flags: Option<&[&str]>,
    ) -> impl Future<Output = Result<Option<MetaValue>, Error>>;

    /// Performs a decrement (arithmetic) operation on the given key.
    ///
    /// If the key is found, the decrement operation is performed.
    /// If data is requested back via meta flags then a `MetaValue` is returned, otherwise `None`.
    ///
    /// Command format:
    ///   ma <key> MD <meta_flags>*\r\n
    ///
    /// - <key> is the key string, with a maximum length of 250 bytes.
    ///
    /// - <opaque> is an optional slice of string references with a maximum length of 32 bytes.
    ///
    /// - <delta> is an optional u64 value for the decrement delta.
    ///   The default behaviour is to decrement with a delta of 1.
    ///
    /// - <is_quiet> is a boolean value indicating whether to use quiet mode.
    ///   quiet mode will append a no-op command to the request ("mn\r\n") so that the client
    ///   can proceed properly in the event of a cache miss.
    ///
    /// - <meta_flags> is an optional slice of string references for additional meta flags.
    ///   Meta flags may have associated tokens after the initial character, e.g "N123"
    ///   Do not include "M", "D", "O" or "q" flags as additional meta flags, they will be ignored.
    ///   Instead, use the specified parameters.
    fn meta_decrement<K: AsRef<[u8]>>(
        &mut self,
        key: K,
        is_quiet: bool,
        opaque: Option<&[u8]>,
        delta: Option<u64>,
        meta_flags: Option<&[&str]>,
    ) -> impl Future<Output = Result<Option<MetaValue>, Error>>;

    /// Gets many keys in one round trip.
    ///
    /// Every key is sent as a quiet `mg` carrying the `k` flag, followed by a single `mn`. The
    /// server suppresses misses in quiet mode, so only hits come back; each returned `MetaValue`
    /// has `key` populated so the caller can match hits to the requested keys. Keys that were
    /// not found are simply absent from the result.
    ///
    /// Every key is validated before anything is written, so a key that is too long fails the
    /// whole call without leaving a partial pipeline on the connection.
    ///
    /// An error line (`CLIENT_ERROR`, `SERVER_ERROR`, `ERROR`) for any key fails the call and
    /// leaves later responses of the same pipeline unread; the connection should be discarded
    /// rather than reused.
    //
    // Command format:
    // mg <key> <meta_flags>* k q\r\n  (once per key)
    // mn\r\n
    //
    // - <meta_flags> is an optional slice of string references applied to every key, for example
    //   `v` to return values. `k` and `q` are always added and are ignored if supplied.
    fn meta_get_multi<K: AsRef<[u8]>>(
        &mut self,
        keys: &[K],
        meta_flags: Option<&[&str]>,
    ) -> impl Future<Output = Result<Vec<MetaValue>, Error>>;

    /// Sets many keys in one round trip.
    ///
    /// Every item is sent as a quiet `ms` carrying the `k` flag, followed by a single `mn`. The
    /// server suppresses `HD` in quiet mode, so a fully successful pipeline returns an empty
    /// `Vec`. Items the server refused (`NS`, `EX`, `NF`) are returned with `key` and `status`
    /// populated.
    ///
    /// Every key is validated before anything is written, so a key that is too long fails the
    /// whole call without leaving a partial pipeline on the connection.
    ///
    /// An error line (`CLIENT_ERROR`, `SERVER_ERROR`, `ERROR`) for any item fails the call and
    /// leaves later responses of the same pipeline unread; the connection should be discarded
    /// rather than reused.
    //
    // Command format:
    // ms <key> <datalen> <meta_flags>* k q\r\n<data_block>\r\n  (once per item)
    // mn\r\n
    //
    // - <meta_flags> is an optional slice of string references applied to every item, for example
    //   `T300` for a TTL. `k` and `q` are always added and are ignored if supplied.
    fn meta_set_multi<K, V>(
        &mut self,
        items: &[(K, V)],
        meta_flags: Option<&[&str]>,
    ) -> impl Future<Output = Result<Vec<MetaValue>, Error>>
    where
        K: AsRef<[u8]>,
        V: AsMemcachedValue;
}

/// Writes the caller's meta flags for a pipelined command, skipping the flags the pipeline
/// itself owns (`k` and `q`).
async fn write_pipeline_meta_flags(
    client: &mut Client,
    meta_flags: Option<&[&str]>,
) -> Result<(), Error> {
    if let Some(meta_flags) = meta_flags {
        for flag in meta_flags {
            if flag.starts_with('k') || flag.starts_with('q') {
                continue;
            }
            client.conn.write_all(b" ").await?;
            client.conn.write_all(flag.as_bytes()).await?;
        }
    }
    Ok(())
}

/// Reads pipelined responses until the terminating `MN`, collecting every response that carries
/// per-key metadata. Bare success and miss statuses are skipped; error lines fail the read.
async fn collect_pipeline_responses(
    client: &mut Client,
    parser: MetaResponseParser,
) -> Result<Vec<MetaValue>, Error> {
    let mut collected = Vec::new();

    loop {
        match client.drive_receive(parser).await? {
            MetaResponse::Status(Status::NoOp) => return Ok(collected),
            MetaResponse::Status(Status::Error(kind)) => {
                return Err(Error::Protocol(Status::Error(kind)))
            }
            MetaResponse::Status(_) => {}
            MetaResponse::Data(Some(values)) => collected.extend(values),
            MetaResponse::Data(None) => {}
        }
    }
}

impl MetaProtocol for Client {
    async fn meta_get<K: AsRef<[u8]>>(
        &mut self,
        key: K,
        is_quiet: bool,
        opaque: Option<&[u8]>,
        meta_flags: Option<&[&str]>,
    ) -> Result<Option<MetaValue>, Error> {
        let kr = Self::validate_key_length(key.as_ref())?;

        if let Some(opaque) = &opaque {
            Self::validate_opaque_length(opaque)?;
        }

        self.conn.write_all(b"mg ").await?;
        self.conn.write_all(kr).await?;

        Self::check_and_write_opaque(self, opaque).await?;

        Self::check_and_write_meta_flags(self, meta_flags, opaque).await?;

        Self::check_and_write_quiet_mode(self, is_quiet).await?;

        self.conn.flush().await?;

        let response = self.drive_receive(parse_meta_get_response).await?;
        if is_quiet && !matches!(response, MetaResponse::Status(Status::NoOp)) {
            drain_quiet_noop_response(self, parse_meta_get_response).await?;
        }

        match response {
            MetaResponse::Status(Status::NotFound) => Ok(None),
            MetaResponse::Status(Status::NoOp) => Ok(None),
            MetaResponse::Status(s) => Err(s.into()),
            MetaResponse::Data(d) => d
                .map(|mut items| {
                    let item = items.remove(0);
                    Ok(item)
                })
                .transpose(),
        }
    }

    async fn meta_set<K, V>(
        &mut self,
        key: K,
        value: V,
        is_quiet: bool,
        opaque: Option<&[u8]>,
        meta_flags: Option<&[&str]>,
    ) -> Result<Option<MetaValue>, Error>
    where
        K: AsRef<[u8]>,
        V: AsMemcachedValue,
    {
        let kr = Self::validate_key_length(key.as_ref())?;

        if let Some(opaque) = &opaque {
            Self::validate_opaque_length(opaque)?;
        }

        let vr = value.as_bytes();

        self.conn.write_all(b"ms ").await?;
        self.conn.write_all(kr).await?;

        let vlen = vr.len().to_string();
        self.conn.write_all(b" ").await?;
        self.conn.write_all(vlen.as_ref()).await?;

        Self::check_and_write_opaque(self, opaque).await?;

        Self::check_and_write_meta_flags(self, meta_flags, opaque).await?;

        if is_quiet {
            self.conn.write_all(b" q").await?;
        }

        self.conn.write_all(b"\r\n").await?;
        self.conn.write_all(vr.as_ref()).await?;
        self.conn.write_all(b"\r\n").await?;

        if is_quiet {
            self.conn.write_all(b"mn\r\n").await?;
        }

        self.conn.flush().await?;

        let response = self.drive_receive(parse_meta_set_response).await?;
        if is_quiet && !matches!(response, MetaResponse::Status(Status::NoOp)) {
            drain_quiet_noop_response(self, parse_meta_set_response).await?;
        }

        match response {
            MetaResponse::Status(Status::Stored) => Ok(None),
            MetaResponse::Status(Status::NoOp) => Ok(None),
            MetaResponse::Status(s) => Err(s.into()),
            MetaResponse::Data(d) => d
                .map(|mut items| {
                    let item = items.remove(0);
                    Ok(item)
                })
                .transpose(),
        }
    }

    async fn meta_delete<K: AsRef<[u8]>>(
        &mut self,
        key: K,
        is_quiet: bool,
        opaque: Option<&[u8]>,
        meta_flags: Option<&[&str]>,
    ) -> Result<Option<MetaValue>, Error> {
        let kr = Self::validate_key_length(key.as_ref())?;

        if let Some(opaque) = &opaque {
            Self::validate_opaque_length(opaque)?;
        }

        self.conn.write_all(b"md ").await?;
        self.conn.write_all(kr).await?;

        Self::check_and_write_opaque(self, opaque).await?;

        Self::check_and_write_meta_flags(self, meta_flags, opaque).await?;

        Self::check_and_write_quiet_mode(self, is_quiet).await?;

        self.conn.flush().await?;

        let response = self.drive_receive(parse_meta_delete_response).await?;
        if is_quiet && !matches!(response, MetaResponse::Status(Status::NoOp)) {
            drain_quiet_noop_response(self, parse_meta_delete_response).await?;
        }

        match response {
            MetaResponse::Status(Status::Deleted) => Ok(None),
            MetaResponse::Status(Status::Exists) => Err(Error::Protocol(Status::Exists)),
            MetaResponse::Status(Status::NoOp) => Ok(None),
            MetaResponse::Status(s) => Err(s.into()),
            MetaResponse::Data(d) => d
                .map(|mut items| {
                    let item = items.remove(0);
                    Ok(item)
                })
                .transpose(),
        }
    }

    async fn meta_increment<K: AsRef<[u8]>>(
        &mut self,
        key: K,
        is_quiet: bool,
        opaque: Option<&[u8]>,
        delta: Option<u64>,
        meta_flags: Option<&[&str]>,
    ) -> Result<Option<MetaValue>, Error> {
        let kr = Self::validate_key_length(key.as_ref())?;

        if let Some(opaque) = &opaque {
            Self::validate_opaque_length(opaque)?;
        }

        self.conn.write_all(b"ma ").await?;
        self.conn.write_all(kr).await?;

        Self::check_and_write_opaque(self, opaque).await?;

        // skip writing "MI" because it's default behaviour and we can save the bytes.
        if let Some(delta) = delta {
            if delta != 1 {
                self.conn.write_all(b" D").await?;
                self.conn.write_all(delta.to_string().as_bytes()).await?;
            }
        }

        if let Some(meta_flags) = meta_flags {
            for flag in meta_flags {
                // ignore M flag because it's specific to the method called, ignore q and require param to be used
                // prefer explicit D and O params over meta flags
                if flag.starts_with('M')
                    || flag.starts_with('q')
                    || (flag.starts_with('D') && delta.is_some())
                    || (flag.starts_with('O') && opaque.is_some())
                {
                    continue;
                } else {
                    self.conn.write_all(b" ").await?;
                    self.conn.write_all(flag.as_bytes()).await?;
                }
            }
        }

        Self::check_and_write_quiet_mode(self, is_quiet).await?;

        self.conn.flush().await?;

        let response = self.drive_receive(parse_meta_arithmetic_response).await?;
        if is_quiet && !matches!(response, MetaResponse::Status(Status::NoOp)) {
            drain_quiet_noop_response(self, parse_meta_arithmetic_response).await?;
        }

        match response {
            MetaResponse::Status(Status::Stored) => Ok(None),
            MetaResponse::Status(Status::NoOp) => Ok(None),
            MetaResponse::Status(s) => Err(s.into()),
            MetaResponse::Data(d) => d
                .map(|mut items| {
                    let item = items.remove(0);
                    Ok(item)
                })
                .transpose(),
        }
    }

    async fn meta_decrement<K: AsRef<[u8]>>(
        &mut self,
        key: K,
        is_quiet: bool,
        opaque: Option<&[u8]>,
        delta: Option<u64>,
        meta_flags: Option<&[&str]>,
    ) -> Result<Option<MetaValue>, Error> {
        let kr = Self::validate_key_length(key.as_ref())?;

        if let Some(opaque) = &opaque {
            Self::validate_opaque_length(opaque)?;
        }

        self.conn.write_all(b"ma ").await?;
        self.conn.write_all(kr).await?;
        self.conn.write_all(b" MD").await?;

        Self::check_and_write_opaque(self, opaque).await?;

        if let Some(delta) = delta {
            if delta != 1 {
                self.conn.write_all(b" D").await?;
                self.conn.write_all(delta.to_string().as_bytes()).await?;
            }
        }

        if let Some(meta_flags) = meta_flags {
            for flag in meta_flags {
                // ignore M flag because it's specific to the method called, ignore q and require param to be used
                // prefer explicit D and O params over meta flags
                if flag.starts_with('M')
                    || flag.starts_with('q')
                    || (flag.starts_with('D') && delta.is_some())
                    || (flag.starts_with('O') && opaque.is_some())
                {
                    continue;
                } else {
                    self.conn.write_all(b" ").await?;
                    self.conn.write_all(flag.as_bytes()).await?;
                }
            }
        }

        Self::check_and_write_quiet_mode(self, is_quiet).await?;

        self.conn.flush().await?;

        let response = self.drive_receive(parse_meta_arithmetic_response).await?;
        if is_quiet && !matches!(response, MetaResponse::Status(Status::NoOp)) {
            drain_quiet_noop_response(self, parse_meta_arithmetic_response).await?;
        }

        match response {
            MetaResponse::Status(Status::Stored) => Ok(None),
            MetaResponse::Status(Status::NoOp) => Ok(None),
            MetaResponse::Status(s) => Err(s.into()),
            MetaResponse::Data(d) => d
                .map(|mut items| {
                    let item = items.remove(0);
                    Ok(item)
                })
                .transpose(),
        }
    }

    async fn meta_get_multi<K: AsRef<[u8]>>(
        &mut self,
        keys: &[K],
        meta_flags: Option<&[&str]>,
    ) -> Result<Vec<MetaValue>, Error> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        for key in keys {
            Self::validate_key_length(key.as_ref())?;
        }

        for key in keys {
            self.conn.write_all(b"mg ").await?;
            self.conn.write_all(key.as_ref()).await?;
            write_pipeline_meta_flags(self, meta_flags).await?;
            self.conn.write_all(b" k q\r\n").await?;
        }

        self.conn.write_all(b"mn\r\n").await?;
        self.conn.flush().await?;

        collect_pipeline_responses(self, parse_meta_get_response).await
    }

    async fn meta_set_multi<K, V>(
        &mut self,
        items: &[(K, V)],
        meta_flags: Option<&[&str]>,
    ) -> Result<Vec<MetaValue>, Error>
    where
        K: AsRef<[u8]>,
        V: AsMemcachedValue,
    {
        if items.is_empty() {
            return Ok(Vec::new());
        }

        for (key, _) in items {
            Self::validate_key_length(key.as_ref())?;
        }

        for (key, value) in items {
            let vr = value.as_bytes();

            self.conn.write_all(b"ms ").await?;
            self.conn.write_all(key.as_ref()).await?;
            self.conn.write_all(b" ").await?;
            self.conn.write_all(vr.len().to_string().as_bytes()).await?;
            write_pipeline_meta_flags(self, meta_flags).await?;
            self.conn.write_all(b" k q\r\n").await?;
            self.conn.write_all(vr.as_ref()).await?;
            self.conn.write_all(b"\r\n").await?;
        }

        self.conn.write_all(b"mn\r\n").await?;
        self.conn.flush().await?;

        collect_pipeline_responses(self, parse_meta_set_response).await
    }
}
