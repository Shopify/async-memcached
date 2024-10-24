use crate::{AsMemcachedValue, Client, Error, Status};

use crate::parser::{parse_meta_delete_response, parse_meta_get_response, parse_meta_set_response};
use crate::parser::{MetaResponse, MetaValue};

use std::future::Future;

use tokio::io::AsyncWriteExt;

/// Trait defining Meta protocol-specific methods for the Client.
pub trait MetaProtocol {
    /// Gets the given key with additional metadata.
    ///
    /// If the key is found, `Some(Value)` is returned, describing the metadata and data of the key.
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
        meta_flags: Option<&[&str]>,
    ) -> impl Future<Output = Result<Option<MetaValue>, Error>>;

    /// Sets the given key.
    ///
    /// If the value is set successfully, `Some(Value)` is returned, otherwise [`Error`] is returned.
    /// NOTE: That the data in this Value is sparsely populated, containing only requested data by meta_flags
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
        meta_flags: Option<&[&str]>,
    ) -> impl Future<Output = Result<Option<MetaValue>, Error>>
    where
        K: AsRef<[u8]>,
        V: AsMemcachedValue;

    /// Deletes the given key with additional metadata.
    ///
    /// If the key is found ...
    ///
    /// Otherwise, `None` is returned.
    ///
    /// Supported meta flags:
    /// - b: return whether item has been hit before as a 0 or 1
    /// - C: return time since item was last accessed in seconds
    /// - E: return item TTL remaining in seconds (-1 for unlimited)
    fn meta_delete<K: AsRef<[u8]>>(
        &mut self,
        key: K,
        meta_flags: Option<&[&str]>,
    ) -> impl Future<Output = Result<Option<MetaValue>, Error>>;
}

impl MetaProtocol for Client {
    async fn meta_get<K: AsRef<[u8]>>(
        &mut self,
        key: K,
        meta_flags: Option<&[&str]>,
    ) -> Result<Option<MetaValue>, Error> {
        let kr = Self::validate_key_length(key.as_ref())?;

        self.conn.write_all(b"mg ").await?;
        self.conn.write_all(kr).await?;
        self.conn.write_all(b" ").await?;
        if let Some(meta_flags) = meta_flags {
            self.conn.write_all(meta_flags.join(" ").as_bytes()).await?;
            self.conn.write_all(b"\r\n").await?;
            if meta_flags.contains(&"q") {
                // Write a no-op command if quiet mode is used so reliably detect cache misses.
                self.conn.write_all(b"mn\r\n").await?;
            }
        } else {
            self.conn.write_all(b"\r\n").await?;
        }

        self.conn.flush().await?;

        match self.drive_receive(parse_meta_get_response).await? {
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
        meta_flags: Option<&[&str]>,
    ) -> Result<Option<MetaValue>, Error>
    where
        K: AsRef<[u8]>,
        V: AsMemcachedValue,
    {
        let kr = Self::validate_key_length(key.as_ref())?;
        let vr = value.as_bytes();
        let mut quiet_mode = false;

        self.conn.write_all(b"ms ").await?;
        self.conn.write_all(kr).await?;

        let vlen = vr.len().to_string();
        self.conn.write_all(b" ").await?;
        self.conn.write_all(vlen.as_ref()).await?;

        if let Some(meta_flags) = meta_flags {
            self.conn.write_all(b" ").await?;
            self.conn.write_all(meta_flags.join(" ").as_bytes()).await?;
            if meta_flags.contains(&"q") {
                quiet_mode = true;
            }
        }

        self.conn.write_all(b"\r\n").await?;
        self.conn.write_all(vr.as_ref()).await?;
        self.conn.write_all(b"\r\n").await?;

        if quiet_mode {
            self.conn.write_all(b"mn\r\n").await?;
        }

        self.conn.flush().await?;

        match self.drive_receive(parse_meta_set_response).await? {
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
        meta_flags: Option<&[&str]>,
    ) -> Result<Option<MetaValue>, Error> {
        let kr = Self::validate_key_length(key.as_ref())?;

        self.conn.write_all(b"md ").await?;
        self.conn.write_all(kr).await?;
        self.conn.write_all(b" ").await?;
        if let Some(meta_flags) = meta_flags {
            self.conn.write_all(meta_flags.join(" ").as_bytes()).await?;
            self.conn.write_all(b"\r\n").await?;
            if meta_flags.contains(&"q") {
                self.conn.write_all(b"mn\r\n").await?;
            }
        } else {
            self.conn.write_all(b"\r\n").await?;
        }

        self.conn.flush().await?;

        match self.drive_receive(parse_meta_delete_response).await? {
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
}
