use std::fmt;
mod ascii;
mod meta;
pub use ascii::{parse_ascii_metadump_response, parse_ascii_response, parse_ascii_stats_response};
pub use meta::{parse_meta_response};

/// A value from memcached.
#[derive(Clone, Debug, PartialEq)]
pub struct Value {
    /// The key.
    pub key: Vec<u8>,
    /// CAS identifier.
    pub cas: Option<u64>,
    /// Flags for this key.
    ///
    /// Defaults to 0.
    pub flags: u32,
    /// Data for this key.
    pub data: Vec<u8>,
}

/// Status of a memcached operation.
#[derive(Clone, Debug, PartialEq)]
pub enum Status {
    /// The value was stored.
    Stored,
    /// The value was not stored.
    NotStored,
    /// The key was deleted.
    Deleted,
    /// The key was touched.
    Touched,
    /// The key already exists.
    Exists,
    /// The key was not found.
    NotFound,
    /// An error occurred for the given operation.
    Error(ErrorKind),
}

/// Errors related to a memcached operation.
#[derive(Clone, Debug, PartialEq)]
pub enum ErrorKind {
    /// General error that may or may not have come from either the server or this crate.
    Generic(String),
    /// The command sent by the client does not exist.
    NonexistentCommand,
    /// Protocol-level error i.e. an invalid response from memcached for the given operation.
    Protocol(Option<String>),
    /// An error from memcached related to CLIENT_ERROR.
    Client(String),
    /// An error from memcached related to SERVER_ERROR.
    Server(String),
}

/// Response to a memcached operation.
#[derive(Clone, Debug, PartialEq)]
pub enum Response {
    /// The status of a given operation, which may or may not have succeeded.
    Status(Status),
    /// Data response, which is only returned for reads.
    Data(Option<Vec<Value>>),
    /// Resulting value of a key after an increment/decrement operation.
    IncrDecr(u64),
}

/// Metadump response.
#[derive(Clone, Debug, PartialEq)]
pub enum MetadumpResponse {
    /// The server is busy running another LRU crawler operation.
    Busy(String),
    /// An invalid class ID was specified for the metadump.
    BadClass(String),
    /// A single key entry within the overall metadump operation.
    Entry(KeyMetadata),
    /// End of the metadump.
    End,
}

/// Stats response.
#[derive(Clone, Debug, PartialEq)]
pub enum StatsResponse {
    /// A stats entry, represented by a key and value.
    Entry(String, String),
    /// End of stats output.
    End,
}

/// Metadata for a given key in a metadump operation.
#[derive(Clone, Debug, PartialEq)]
pub struct KeyMetadata {
    /// The key.
    pub key: Vec<u8>,
    /// Expiration time of this key, as a Unix timestamp.
    pub expiration: i64,
    /// Last time this key was accessed, in seconds.
    pub last_accessed: u64,
    /// CAS identifier.
    pub cas: u64,
    /// Whether or not this key has ever been fetched.
    pub fetched: bool,
    /// Slab class ID.
    pub class_id: u32,
    /// Size, in bytes.
    pub size: u32,
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Stored => write!(f, "stored"),
            Self::NotStored => write!(f, "not stored"),
            Self::Deleted => write!(f, "deleted"),
            Self::Touched => write!(f, "touched"),
            Self::Exists => write!(f, "exists"),
            Self::NotFound => write!(f, "not found"),
            Self::Error(ek) => write!(f, "error: {}", ek),
        }
    }
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Generic(s) => write!(f, "generic: {}", s),
            Self::NonexistentCommand => write!(f, "command does not exist"),
            Self::Protocol(s) => match s {
                Some(s) => write!(f, "protocol: {}", s),
                None => write!(f, "protocol"),
            },
            Self::Client(s) => write!(f, "client: {}", s),
            Self::Server(s) => write!(f, "server: {}", s),
        }
    }
}

impl From<MetadumpResponse> for Status {
    fn from(resp: MetadumpResponse) -> Self {
        match resp {
            MetadumpResponse::BadClass(s) => {
                Status::Error(ErrorKind::Generic(format!("BADCLASS {}", s)))
            }
            MetadumpResponse::Busy(s) => Status::Error(ErrorKind::Generic(format!("BUSY {}", s))),
            _ => unreachable!("Metadump Entry/End states should never be used as a Status!"),
        }
    }
}
