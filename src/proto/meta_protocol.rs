use crate::{Client, Error, Status, ErrorKind, AsMemcachedValue};

use crate::parser::{MetaResponse, MetaValue};
use crate::parser::{parse_meta_get_response, parse_meta_set_response};

use std::future::Future;

use tokio::io::AsyncWriteExt;

/// Trait defining Meta protocol-specific methods for the Client.
pub trait MetaProtocol {
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
    fn meta_get<K: AsRef<[u8]>>(
        &mut self,
        key: K,
        meta_flags: Option<&[&str]>,
    ) -> impl Future<Output = Result<Option<MetaValue>, Error>>;

    /// Sets the given key.
    ///
    /// If `ttl` they will default to 0.  If the value is set
    /// successfully, `Some(Value)` is returned, otherwise [`Error`] is returned.
    /// NOTE: That the data is some Value is sparsely populated, containing only requested data by meta_flags
    /// The meta set command a generic command for storing data to memcached. Based
    /// on the flags supplied, it can replace all storage commands (see token M) as
    /// well as adds new options.
    //
    // ms <key> <datalen> <flags>*\r\n
    //
    // - <key> means one key string.
    // - <datalen> is the length of the payload data.
    //
    // - <flags> are a set of single character codes ended with a space or newline.
    //   flags may have strings after the initial character.
    fn meta_set<K, V>(
        &mut self,
        key: K,
        value: V,
        meta_flags: Option<&[&str]>,
    ) -> impl Future<Output = Result<Option<MetaValue>, Error>>
    where
        K: AsRef<[u8]>,
        V: AsMemcachedValue;
}

impl MetaProtocol for Client {
    async fn meta_get<K: AsRef<[u8]>>(
        &mut self,
        key: K,
        meta_flags: Option<&[&str]>,
    ) -> Result<Option<MetaValue>, Error> {
        self.conn.write_all(b"mg ").await?;
        self.conn
            .write_all(Self::validate_key_length(key.as_ref())?)
            .await?;
        self.conn.write_all(b" ").await?;
        if let Some(flags) = meta_flags {
            self.conn.write_all(flags.join(" ").as_bytes()).await?;
        }
        self.conn.write_all(b"\r\n").await?;
        self.conn.flush().await?;

        match self.drive_receive(parse_meta_get_response).await? {
            MetaResponse::Status(Status::NotFound) => Ok(None),
            MetaResponse::Status(s) => Err(s.into()),
            MetaResponse::Data(d) => d
                .map(|mut items| {
                    if items.len() != 1 {
                        Err(Status::Error(ErrorKind::Protocol(None)).into())
                    } else {
                        let mut item = items.remove(0);
                        item.key = Some(key.as_ref().to_vec());
                        Ok(item)
                    }
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
        let kr = key.as_ref();
        let vr = value.as_bytes();

        self.conn.write_all(b"ms ").await?;
        self.conn.write_all(kr).await?;

        let vlen = vr.len().to_string();
        self.conn.write_all(b" ").await?;
        self.conn.write_all(vlen.as_ref()).await?;

        if let Some(meta_flags) = meta_flags {
            self.conn.write_all(b" ").await?;
            self.conn.write_all(meta_flags.join(" ").as_bytes()).await?;
        }

        self.conn.write_all(b"\r\n").await?;
        self.conn.write_all(vr.as_ref()).await?;
        self.conn.write_all(b"\r\n").await?;
        self.conn.flush().await?;

        match self.drive_receive(parse_meta_set_response).await? {
            MetaResponse::Status(Status::Stored) => Ok(None),
            MetaResponse::Status(s) => Err(s.into()),
            MetaResponse::Data(d) => d
                .map(|mut items| {
                    if items.len() != 1 {
                        Err(Status::Error(ErrorKind::Protocol(None)).into())
                    } else {
                        let mut item = items.remove(0);
                        item.key = Some(key.as_ref().to_vec());
                        Ok(item)
                    }
                })
                .transpose(),
        }
    }
}
