use crate::{AsMemcachedValue, ErrorKind};
use crate::{Client, Error, Response, Status, Value};

use fxhash::FxHashMap;
use std::future::Future;
use tokio::io::AsyncWriteExt;

const MAX_KEY_LENGTH: usize = 250; // reference in memcached documentation: https://github.com/memcached/memcached/blob/5609673ed29db98a377749fab469fe80777de8fd/doc/protocol.txt#L46

/// Trait defining ASCII protocol-specific methods for the Client.
pub trait AsciiProtocol {
    /// Gets the given key.
    ///
    /// If the key is found, `Some(Value)` is returned, describing the metadata and data of the key.
    ///
    /// Otherwise, [`Error`] is returned.
    fn get<K: AsRef<[u8]>>(&mut self, key: K)
        -> impl Future<Output = Result<Option<Value>, Error>>;

    /// Gets multiple keys.
    ///
    /// If any of the keys are found, a vector of [`Value`] will be returned.
    ///
    /// Otherwise, [`Error`] is returned.
    fn get_multi<I, K>(&mut self, keys: I) -> impl Future<Output = Result<Vec<Value>, Error>>
    where
        I: IntoIterator<Item = K>,
        K: AsRef<[u8]>;

    /// Gets the given keys.
    ///
    /// Deprecated: This is now an alias for `get_multi`, and  will be removed in the future.
    #[deprecated(
        since = "0.4.0",
        note = "This is now an alias for `get_multi`, and will be removed in the future."
    )]
    fn get_many<I, K>(&mut self, keys: I) -> impl Future<Output = Result<Vec<Value>, Error>>
    where
        I: IntoIterator<Item = K>,
        K: AsRef<[u8]>;

    /// Sets the given key.
    ///
    /// If `ttl` or `flags` are not specified, they will default to 0. If the value is set
    /// successfully, `()` is returned, otherwise [`Error`] is returned.
    fn set<K, V>(
        &mut self,
        key: K,
        value: V,
        ttl: Option<i64>,
        flags: Option<u32>,
    ) -> impl Future<Output = Result<(), Error>>
    where
        K: AsRef<[u8]>,
        V: AsMemcachedValue;

    /// Sets multiple keys and values through pipelined commands.
    ///
    /// If `ttl` or `flags` are not specified, they will default to 0. The same values for `ttl` and `flags` will be applied to each key.
    /// Returns a result with a HashMap of keys mapped to the result of the set operation, or an error.
    fn set_multi<'a, K, V>(
        &mut self,
        kv: &'a [(K, V)],
        ttl: Option<i64>,
        flags: Option<u32>,
    ) -> impl Future<Output = Result<FxHashMap<&'a K, Result<(), Error>>, Error>>
    where
        K: AsRef<[u8]> + Eq + std::hash::Hash + std::fmt::Debug,
        V: AsMemcachedValue;

    /// Add a key. If the value exists, Err(Protocol(NotStored)) is returned.
    fn add<K, V>(
        &mut self,
        key: K,
        value: V,
        ttl: Option<i64>,
        flags: Option<u32>,
    ) -> impl Future<Output = Result<(), Error>>
    where
        K: AsRef<[u8]>,
        V: AsMemcachedValue;

    /// Attempts to add multiple keys and values through pipelined commands.
    ///
    /// If `ttl` or `flags` are not specified, they will default to 0. The same values for `ttl` and `flags` will be applied to each key.
    /// Returns a result with a HashMap of keys mapped to the result of the add operation, or an error.
    fn add_multi<'a, K, V>(
        &mut self,
        kv: &'a [(K, V)],
        ttl: Option<i64>,
        flags: Option<u32>,
    ) -> impl Future<Output = Result<FxHashMap<&'a K, Result<(), Error>>, Error>>
    where
        K: AsRef<[u8]> + Eq + std::hash::Hash + std::fmt::Debug,
        V: AsMemcachedValue;

    /// Delete multiple keys
    fn delete_multi_no_reply<K>(&mut self, keys: &[K]) -> impl Future<Output = Result<(), Error>>
    where
        K: AsRef<[u8]>;

    /// Delete a key but don't wait for a reply.
    fn delete_no_reply<K>(&mut self, key: K) -> impl Future<Output = Result<(), Error>>
    where
        K: AsRef<[u8]>;

    /// Delete a key and wait for a reply.
    fn delete<K>(&mut self, key: K) -> impl Future<Output = Result<(), Error>>
    where
        K: AsRef<[u8]>;

    /// Increments the given key by the specified amount.
    /// Can overflow from the max value of u64 (18446744073709551615) -> 0.
    /// If the key does not exist, the server will return a KeyNotFound error.
    /// If the key exists but the value is non-numeric, the server will return a ClientError.
    fn increment<K>(&mut self, key: K, amount: u64) -> impl Future<Output = Result<u64, Error>>
    where
        K: AsRef<[u8]>;

    /// Increments the given key by the specified amount with no reply from the server.
    /// Can overflow from the max value of u64 (18446744073709551615) -> 0.
    /// Always returns () for a complete request, will not return any indication of success or failure.
    fn increment_no_reply<K>(
        &mut self,
        key: K,
        amount: u64,
    ) -> impl Future<Output = Result<(), Error>>
    where
        K: AsRef<[u8]>;

    /// Decrements the given key by the specified amount.
    /// Will not decrement the counter below 0.
    /// If the key does not exist, the server will return a KeyNotFound error.
    /// If the key exists but the value is non-numeric, the server will return a ClientError.
    fn decrement<K>(&mut self, key: K, amount: u64) -> impl Future<Output = Result<u64, Error>>
    where
        K: AsRef<[u8]>;

    /// Decrements the given key by the specified amount with no reply from the server.
    /// Will not decrement the counter below 0.
    /// Always returns () for a complete request, will not return any indication of success or failure.
    fn decrement_no_reply<K>(
        &mut self,
        key: K,
        amount: u64,
    ) -> impl Future<Output = Result<(), Error>>
    where
        K: AsRef<[u8]>;
}

impl AsciiProtocol for Client {
    async fn get<K: AsRef<[u8]>>(&mut self, key: K) -> Result<Option<Value>, Error> {
        let kr = Self::validate_key_length(key.as_ref())?;

        self.conn
            .write_all(&[b"get ", kr, b"\r\n"].concat())
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

    async fn get_multi<I, K>(&mut self, keys: I) -> Result<Vec<Value>, Error>
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

    async fn get_many<I, K>(&mut self, keys: I) -> Result<Vec<Value>, Error>
    where
        I: IntoIterator<Item = K>,
        K: AsRef<[u8]>,
    {
        self.get_multi(keys).await
    }

    async fn set<K, V>(
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
        let kr = Self::validate_key_length(key.as_ref())?;
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

    async fn set_multi<'a, K, V>(
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

    async fn add<K, V>(
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
        let kr = Self::validate_key_length(key.as_ref())?;
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

    async fn add_multi<'a, K, V>(
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
    async fn delete_no_reply<K>(&mut self, key: K) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
    {
        let kr = Self::validate_key_length(key.as_ref())?;

        self.conn
            .write_all(&[b"delete ", kr, b" noreply\r\n"].concat())
            .await?;
        self.conn.flush().await?;
        Ok(())
    }

    /// Delete a key and wait for a reply
    async fn delete<K>(&mut self, key: K) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
    {
        let kr = Self::validate_key_length(key.as_ref())?;

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

    async fn delete_multi_no_reply<K>(&mut self, keys: &[K]) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
    {
        for key in keys {
            let kr = key.as_ref();
            if kr.len() > MAX_KEY_LENGTH {
                continue;
            }

            self.conn.write_all(b"delete ").await?;
            self.conn.write_all(kr).await?;
            self.conn.write_all(b" noreply\r\n").await?;
        }
        self.conn.flush().await?;

        Ok(())
    }

    async fn increment<K>(&mut self, key: K, amount: u64) -> Result<u64, Error>
    where
        K: AsRef<[u8]>,
    {
        let kr = Self::validate_key_length(key.as_ref())?;

        self.conn
            .write_all(&[b"incr ", kr, b" ", amount.to_string().as_bytes(), b"\r\n"].concat())
            .await?;
        self.conn.flush().await?;

        match self.get_read_write_response().await? {
            Response::Status(s) => Err(s.into()),
            Response::IncrDecr(amount) => Ok(amount),
            _ => Err(Status::Error(ErrorKind::Protocol(None)).into()),
        }
    }

    async fn increment_no_reply<K>(&mut self, key: K, amount: u64) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
    {
        let kr = Self::validate_key_length(key.as_ref())?;

        self.conn
            .write_all(
                &[
                    b"incr ",
                    kr,
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

    async fn decrement<K>(&mut self, key: K, amount: u64) -> Result<u64, Error>
    where
        K: AsRef<[u8]>,
    {
        let kr = Self::validate_key_length(key.as_ref())?;

        self.conn
            .write_all(&[b"decr ", kr, b" ", amount.to_string().as_bytes(), b"\r\n"].concat())
            .await?;
        self.conn.flush().await?;

        match self.get_read_write_response().await? {
            Response::Status(s) => Err(s.into()),
            Response::IncrDecr(amount) => Ok(amount),
            _ => Err(Status::Error(ErrorKind::Protocol(None)).into()),
        }
    }

    async fn decrement_no_reply<K>(&mut self, key: K, amount: u64) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
    {
        let kr = Self::validate_key_length(key.as_ref())?;

        self.conn
            .write_all(
                &[
                    b"decr ",
                    kr,
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
}
