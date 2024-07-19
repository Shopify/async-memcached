//! A consistent hash ring implementation on a continuum of server nodes.
//!
//! The ring is a continuum of nodes, each with a hash value. When a key is
//! passed to the ring, the ring will return the node with the closest hash
//! value to the key. This design is aimed to mimimize the cache miss impact
//! of adding or removing servers from the ring. That is, adding or removing
//! a server from the ring should impact the key -> server mapping of ~ 1/N
//! of the stored keys where N is the number of servers in the ring.  
//! This is done by creating a large number of "points" per server, distributed
//! over the space 0x00000000 - 0xFFFFFFFF. For a given key, we calculate the CRC32
//! hash, and find the nearest "point" that is less than or equal to the
//! the key's hash.

use crate::collectible_result::CollectibleResult;
use crate::error::Error;
use crate::node::Node;

use futures::stream::{self, StreamExt};

const POINTS_PER_SERVER: usize = 160;

struct Entry {
    value: u32,
    node_index: usize,
}

pub struct Ring {
    pub servers: Vec<Node>,
    continuum: Vec<Entry>,
}

impl Ring {
    pub async fn new<S: AsRef<str>>(server_addrs: Vec<S>) -> Result<Ring, Error> {
        let servers = stream::iter(&server_addrs)
            .then(Node::new)
            .collect::<CollectibleResult<_>>()
            .await;
        let servers = match servers.inner {
            Ok(servers) => servers,
            Err(e) => return Err(e),
        };

        let continuum = build_continuum(&servers).await;

        Ok(Ring { servers, continuum })
    }

    pub fn server_index_for<K: AsRef<[u8]>>(&self, key: K) -> usize {
        let hash = hash_for(key);
        let entry = self.continuum.binary_search_by(|n| n.value.cmp(&hash));
        match entry {
            Ok(index) => self.continuum[index].node_index,
            Err(index) => {
                if index == self.continuum.len() {
                    self.continuum[0].node_index
                } else {
                    self.continuum[index].node_index
                }
            }
        }
    }

    pub fn server_for<K: AsRef<[u8]>>(&mut self, key: K) -> &mut Node {
        if self.servers.len() == 1 {
            return &mut self.servers[0];
        }

        let node_index = self.server_index_for(key);

        &mut self.servers[node_index]
    }
}

fn entry_count_for(_server: &Node) -> usize {
    POINTS_PER_SERVER // left as a constant for now, but could be extended to support
                      // different weights for different servers
}

fn hash_for<K: AsRef<[u8]>>(key: K) -> u32 {
    crc32fast::hash(key.as_ref())
}

async fn build_continuum(servers: &[Node]) -> Vec<Entry> {
    let mut continuum = Vec::new();

    for (i, server) in servers.iter().enumerate() {
        // create a continuum of points for each server to allow for a more even distribution
        // across the ring and to reduce the impact of a single server failing or nodes scaling
        for cont_idx in 0..entry_count_for(server) {
            // blake3 chosen here because it is faster than sha1 and sha256, and has a low collision rate
            let hash = blake3::hash(format!("{}:{}", server.name, cont_idx).as_ref()).to_string();
            let value = u32::from_str_radix(&hash[0..7], 16).unwrap();
            continuum.push(Entry {
                value,
                node_index: i,
            });
        }
    }

    continuum
}
