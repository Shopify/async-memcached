use super::{Client, Error, MetaValue, Status};
use super::{ErrorKind, AsMemcachedValue};

use crate::parser::MetaResponse;
// use async_trait;
use fxhash::FxHashMap;
use tokio::io::AsyncWriteExt;

use crate::parser::meta::parse_meta_response;
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
    async fn meta_get<K: AsRef<[u8]>>(
        &mut self,
        key: K,
        meta_flags: &[char],
    ) -> Result<Option<MetaValue>, Error>;

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
    async fn meta_set<K, V>(
        &mut self,
        key: K,
        value: V,
        ttl: Option<i64>,
        meta_flags: FxHashMap<&[char], String>,
    ) -> Result<Option<MetaValue>, Error>
    where
        K: AsRef<[u8]>,
        V: AsMemcachedValue;
}

impl MetaProtocol for Client {
    async fn meta_get<K: AsRef<[u8]>>(
        &mut self,
        key: K,
        meta_flags: &[char],
    ) -> Result<Option<MetaValue>, Error> {
        let mut command = Vec::with_capacity(64);
        command.extend_from_slice(b"mg ");
        command.extend_from_slice(key.as_ref());
        // TODO: Do we want to always add the v flag? so all gets return the value? or we want clients to know to request it?
        command.extend_from_slice(b" v");
        if !meta_flags.is_empty() {
            for flag in meta_flags {
                command.push(b' ');
                command.push(*flag as u8);
            }
        }
        command.extend_from_slice(b"\r\n");
        //println!("command: {:?}", String::from_utf8_lossy(&command));
        self.conn.write_all(&command).await?;
        self.conn.flush().await?;

        match self.drive_receive(parse_meta_response).await? {
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
            // _ => Err(Error::Protocol(Status::Error(ErrorKind::Protocol(None)))),
        }
    }

    async fn meta_set<K, V>(
        &mut self,
        key: K,
        value: V,
        ttl: Option<i64>,
        meta_flags: FxHashMap<&[char], String>,
    ) -> Result<Option<MetaValue>, Error>
    where
        K: AsRef<[u8]>,
        V: AsMemcachedValue,
    {
        let mut command = Vec::with_capacity(64);
        let kr = key.as_ref();
        let vr = value.as_bytes();

        command.extend_from_slice(b"ms ");
        command.extend_from_slice(kr);
        command.extend_from_slice(b" ");
        let vlen = vr.len().to_string();
        command.extend_from_slice(vlen.as_ref());

        if !meta_flags.is_empty() {
            for (flag, flag_value) in meta_flags {
                command.push(b' ');
                command.extend(flag.iter().map(|&c| c as u8));
                if !flag_value.is_empty() {
                    command.extend_from_slice(flag_value.as_bytes());
                }
            }
        }

        // TTL is just another flag, but we encourage passing it to the function for clarity
        let ttl = ttl.unwrap_or(0).to_string();
        command.extend_from_slice(b" ");
        command.extend_from_slice(b"T");
        command.extend_from_slice(ttl.as_ref());
        command.extend_from_slice(b"\r\n");
        self.conn.write_all(&command).await?;
        println!("command: {:?}", String::from_utf8_lossy(&command));
        self.conn.write_all(vr.as_ref()).await?;
        self.conn.write_all(b"\r\n").await?;
        self.conn.flush().await?;

        match self.drive_receive(parse_meta_response).await? {
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
            // _ => Err(Error::Protocol(Status::Error(ErrorKind::Protocol(None)))),
        }
    }
}
