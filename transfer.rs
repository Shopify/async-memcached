/// Sets multiple keys and values through pipelined commands.
    ///
    /// If `ttl` or `flags` are not specified, they will default to 0. The same values for `ttl` and `flags` will be applied to each key.
    /// Returns a result with a HashMap of keys mapped to the result of the set operation, or an error.
    pub async fn set_multi<'a, K, V>(
        &mut self,
        kv: impl AsRef<[(K, V)]>,
        ttl: Option<i64>,
        flags: Option<u32>,
    ) -> Result<HashMap<K, Result<Response, Error>>, Error>
    where
        K: AsRef<[u8]> + Eq + std::hash::Hash + Clone,
        V: AsMemcachedValue,
    {
        for (key, value) in kv.as_ref() {
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

    #[inline]
    pub(crate) async fn map_set_multi_responses<'a, K, V>(
        &mut self,
        kv: impl AsRef<[(K, V)]> + 'a,
    ) -> Result<HashMap<K, Result<Response, Error>>, Error>
    where
        K: AsRef<[u8]> + Eq + std::hash::Hash + Clone,
        V: AsMemcachedValue,
    {
        let mut results = HashMap::new();

        for (key, _) in kv.as_ref() {
            let result = match self.drive_receive(parse_ascii_response).await {
                Ok(Response::Status(Status::Stored)) => Ok(Response::Status(Status::Stored)),
                Ok(Response::Status(s)) => Err(s.into()),
                Ok(_) => Err(Status::Error(ErrorKind::Protocol(None)).into()),
                Err(e) => return Err(e),
            };

            results.insert(key.clone(), result);
        }

        Ok(results)
    }
