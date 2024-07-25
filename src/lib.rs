//! A Tokio-based memcached node.
#![deny(warnings, missing_docs)]
use futures::stream::{self, StreamExt};
use itertools::Itertools;
use std::collections::HashMap;

mod connection;
mod error;
pub use self::error::Error;

pub mod node;
pub use self::node::{MetadumpIter, Node};

mod ring;
use self::ring::Ring;

mod parser;
pub use self::parser::{ErrorKind, KeyMetadata, MetadumpResponse, StatsResponse, Status, Value};

mod collectible_result;
use collectible_result::CollectibleResult;


/// High-level memcached client.
///
/// [`Client`] is mapped to a set of memcached server nodes, and provides a
/// high-level API for executing commands on that cluster.
pub struct Client {
    ring: Ring,
}

impl Client {
    /// Creates a new [`Client`] based on the given list of data source strings.
    ///
    /// Supports UNIX domain sockets and TCP connections.
    /// For TCP: the DSN should be in the format of `tcp://<IP>:<port>` or `<IP>:<port>`.
    /// For UNIX: the DSN should be in the format of `unix://<path>`.
    pub async fn new<S: AsRef<str>>(node_dsns: Vec<S>) -> Result<Client, Error> {
        let ring = Ring::new(node_dsns).await?;
        Ok(Client { ring })
    }

    /// Gets the given key.
    ///
    /// If the key is found, `Some(Value)` is returned, describing the metadata and data of the key.
    ///
    /// Otherwise, [`Error`] is returned.
    pub async fn get<K: AsRef<[u8]>>(&mut self, key: K) -> Result<Option<Value>, Error> {
        self.ring.server_for(&key).get(key).await
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
        K: AsRef<[u8]> + Clone,
    {
        let mut keys_for_server = vec![Vec::new(); self.ring.servers.len()];
        keys.into_iter().for_each(|key| {
            let server_index = self.ring.server_index_for(&key);
            keys_for_server[server_index].push(key);
        });

        let result = stream::iter(self.ring.servers.iter_mut().zip_eq(keys_for_server))
            .then(|(node, keys)| node.get_many(keys))
            .collect::<CollectibleResult<_>>()
            .await;

        // Return the result from the CollectibleResult new type struct
        result.inner
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
        V: AsRef<[u8]>,
    {
        self.ring.server_for(&key).set(key, value, ttl, flags).await
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
        V: AsRef<[u8]>,
    {
        self.ring.server_for(&key).add(key, value, ttl, flags).await
    }

    /// Delete a key but don't wait for a reply.
    pub async fn delete_no_reply<K>(&mut self, key: K) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
    {
        self.ring.server_for(&key).delete_no_reply(key).await
    }

    /// Delete a key and wait for a reply
    pub async fn delete<K>(&mut self, key: K) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
    {
        self.ring.server_for(&key).delete(key).await
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
    pub async fn dump_keys(&mut self) -> Result<Vec<MetadumpIter<'_>>, Error> {
        let result = stream::iter(&mut self.ring.servers)
            .then(|node| node.dump_keys())
            .collect::<CollectibleResult<_>>()
            .await;

        // Return the result from the CollectibleResult new type struct
        result.inner
    }

    /// Collects statistics from the server.
    ///
    /// The statistics that may be returned are detailed in the protocol specification for
    /// memcached, but all values returned by this method are returned as strings and are not
    /// further interpreted or validated for conformity.
    pub async fn stats(&mut self) -> Result<Vec<HashMap<String, String>>, Error> {
        let result = stream::iter(&mut self.ring.servers)
            .then(|node| node.stats())
            .collect::<CollectibleResult<_>>()
            .await;

        // Return the result from the CollectibleResult new type struct
        result.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, Write};
    use std::net::{TcpListener, TcpStream};

    const KEY: &str = "async-memcache-test-key";
    const EMPTY_KEY: &str = "no-value-here";
    const SERVER_ADDRESSES: [&str; 2] = ["localhost:1234", "localhost:1235"];

    #[ctor::ctor]
    fn init() {
        SERVER_ADDRESSES.iter().for_each(|dsn| {
            let listener = TcpListener::bind(dsn).expect("Failed to bind listener");

            // accept connections and process them serially in a background thread
            std::thread::spawn(move || {
                for stream in listener.incoming() {
                    let stream = stream.expect("Failed to accept connection");
                    std::thread::spawn(move || {
                        handle_connection(stream);
                    });
                }
            });
        });
    }

    // Used to mock out the memcached server responses to test the client connecting to multiple
    // nodes without needing to run a number of real memcached servers in the background.
    fn handle_connection(mut stream: TcpStream) {
        let cloned_stream = stream.try_clone().expect("Failed to clone stream");
        let mut reader = std::io::BufReader::new(&cloned_stream);

        loop {
            let mut buffer = String::new();
            reader.read_line(&mut buffer).expect("Failed to read line");

            if buffer.is_empty() {
                return;
            }

            let parts: Vec<&str> = buffer.split_whitespace().collect();
            let command = parts[0];
            let key = parts[1];

            match command {
                "get" => {
                    if key == EMPTY_KEY {
                        let response = "END\r\n";
                        stream
                            .write_all(response.as_bytes())
                            .expect("Failed to write response");
                        return;
                    }

                    let response = format!("VALUE {} 0 5\r\nvalue\r\nEND\r\n", key);
                    stream
                        .write_all(response.as_bytes())
                        .expect("Failed to write response");
                }
                "set" => {
                    let mut value = String::new();
                    reader.read_line(&mut value).expect("Failed to read line");

                    let response = "STORED\r\n";
                    stream
                        .write_all(response.as_bytes())
                        .expect("Failed to write response");
                }
                "add" => {
                    let mut value = String::new();
                    reader.read_line(&mut value).expect("Failed to read line");

                    let response = "STORED\r\n";
                    stream
                        .write_all(response.as_bytes())
                        .expect("Failed to write response");
                }
                "delete" => {
                    let response = "DELETED\r\n";
                    stream
                        .write_all(response.as_bytes())
                        .expect("Failed to write response");
                }
                "version" => {
                    let response = "VERSION 1.6.7\r\n";
                    stream
                        .write_all(response.as_bytes())
                        .expect("Failed to write response");
                }
                "metadump" => {
                    let response = "END\r\n";
                    stream
                        .write_all(response.as_bytes())
                        .expect("Failed to write response");
                }
                "stats" => {
                    let response = "STAT pid 1234\r\nEND\r\n";
                    stream
                        .write_all(response.as_bytes())
                        .expect("Failed to write response");
                }
                _ => {
                    let response = "ERROR\r\n";
                    stream
                        .write_all(response.as_bytes())
                        .expect("Failed to write response");
                }
            }
        }
    }

    async fn setup_client() -> Client {
        Client::new(SERVER_ADDRESSES.to_vec())
            .await
            .expect("Failed to create client")
    }

    #[tokio::test]
    async fn test_set() {
        let mut client = setup_client().await;

        let result = client.set(KEY, "value", None, None).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_get_non_existent_key() {
        let mut client = setup_client().await;

        let result = client.get(EMPTY_KEY).await;

        assert_eq!(result, Ok(None), "Expected None, got {:?}", result);
    }

    #[tokio::test]
    async fn test_get_existing_key() {
        let mut client = setup_client().await;

        let result = client.get(KEY).await;

        let expected = Value {
            key: KEY.as_bytes().to_vec(),
            flags: 0,
            cas: None,
            data: b"value".to_vec(),
        };

        assert_eq!(
            result,
            Ok(Some(expected.clone())),
            "Expected Ok(Some({:?})), got {:?}",
            expected,
            result
        );
    }
}
