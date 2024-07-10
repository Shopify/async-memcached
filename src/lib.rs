//! A Tokio-based memcached node.
#![deny(warnings, missing_docs)]
use std::collections::HashMap;
use futures::stream::{self, StreamExt};
use std::iter::Extend;

mod connection;
mod error;
pub use self::error::Error;

mod node;
pub use self::node::{Node, MetadumpIter};

mod ring;
use self::ring::Ring;

mod parser;
pub use self::parser::{ErrorKind, KeyMetadata, MetadumpResponse, StatsResponse, Status, Value};

struct KeyDumpResult<'a>(Result<Vec<MetadumpIter<'a>>, Error>);
struct StatsResult(Result<Vec<HashMap<String, String>>, Error>);

impl<'a> Default for KeyDumpResult<'a> {
    fn default() -> Self {
        KeyDumpResult(Ok(Vec::new()))
    }
}

impl<'a> Extend<Result<MetadumpIter<'a>, Error>> for KeyDumpResult<'a> {
    fn extend<T: IntoIterator<Item = Result<MetadumpIter<'a>, Error>>>(&mut self, iter: T) {
        for item in iter {
            match item {
                Ok(node) => self.0.as_mut().unwrap().push(node),
                Err(e) => {
                    self.0 = Err(e);
                    return;
                }
            }
        }
    }
}

impl Default for StatsResult {
    fn default() -> Self {
        StatsResult(Ok(Vec::new()))
    }
}

impl Extend<Result<HashMap<String, String>, Error>> for StatsResult {
    fn extend<T: IntoIterator<Item = Result<HashMap<String, String>, Error>>>(&mut self, iter: T) {
        for item in iter {
            match item {
                Ok(stats) => self.0.as_mut().unwrap().push(stats),
                Err(e) => {
                    self.0 = Err(e);
                    return;
                }
            }
        }
    }
}

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
    /// Currently only supports TCP connections, and as such, the DSN should be in the format of
    /// `<host of IP>:<port>`.
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
        K: AsRef<[u8]>,
    {
        // TODO: handle consistent hashing for multiple keys and servers when doing multi-get
        // operations
        todo!()
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

    /// Gets the version of the server.
    ///
    /// If the version is retrieved successfully, `String` is returned containing the version
    /// component e.g. `1.6.7`, otherwise [`Error`] is returned.
    ///
    /// For some setups, such as those using Twemproxy, this will return an error as those
    /// intermediate proxies do not support the version command.
    pub async fn version(&mut self) -> Result<String, Error> {
        self.ring.servers[0].version().await
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
        let result = stream::iter(&mut self.ring.servers).then(|node| node.dump_keys()).collect::<KeyDumpResult>().await;

        // Unwrap the result from the KeyDumpResult new type struct
        result.0
    }

    /// Collects statistics from the server.
    ///
    /// The statistics that may be returned are detailed in the protocol specification for
    /// memcached, but all values returned by this method are returned as strings and are not
    /// further interpreted or validated for conformity.
    pub async fn stats(&mut self) -> Result<Vec<HashMap<String, String>>, Error> {
        let result = stream::iter(&mut self.ring.servers).then(|node| node.stats()).collect::<StatsResult>().await;

        // Unwrap the result from the StatsResult new type struct
        result.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &str = "async-memcache-test-key";

    // #[tokio::test]
    // async fn test_add() {
    //     let mut node = Node::new("localhost:47386")
    //         .await
    //         .expect("Failed to connect to server");

    //     let result = node.delete_no_reply(KEY).await;
    //     assert!(result.is_ok(), "failed to delete {}, {:?}", KEY, result);

    //     let result = node.add(KEY, "value", None, None).await;

    //     assert!(result.is_ok());
    // }

    // #[tokio::test]
    // async fn test_delete() {
    //     let mut node = Node::new("localhost:47386")
    //         .await
    //         .expect("Failed to connect to server");

    //     let key = "async-memcache-test-key";

    //     let value = rand::random::<u64>().to_string();
    //     let result = node.set(key, &value, None, None).await;

    //     assert!(result.is_ok(), "failed to set {}, {:?}", key, result);

    //     let result = node.get(key).await;

    //     assert!(result.is_ok(), "failed to get {}, {:?}", key, result);
    //     let get_result = result.unwrap();

    //     match get_result {
    //         Some(get_value) => assert_eq!(
    //             String::from_utf8(get_value.data).expect("failed to parse a string"),
    //             value
    //         ),
    //         None => panic!("failed to get {}", key),
    //     }

    //     let result = node.delete(key).await;

    //     assert!(result.is_ok(), "failed to delete {}, {:?}", key, result);
    // }
}
