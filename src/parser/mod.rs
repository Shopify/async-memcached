use btoi::{btoi, btou};
use nom::{
    branch::alt,
    bytes::streaming::{tag, take_while_m_n},
    character::{is_digit, streaming::crlf},
    combinator::{map, map_res, value},
    sequence::terminated,
    IResult,
};
use std::fmt;

mod ascii;
pub use ascii::{parse_ascii_metadump_response, parse_ascii_response, parse_ascii_stats_response};

mod meta;
#[allow(unused_imports)]
pub use meta::{parse_meta_get_response, parse_meta_set_response};

/// A value from memcached.
#[derive(Clone, Debug, PartialEq)]
pub struct Value {
    /// The key.
    pub key: Vec<u8>,
    /// CAS identifier.
    pub cas: Option<u64>,
    /// Flags for this key.
    /// Defaults to 0.
    /// NOTE: This is the client bitflags, not meta flags
    /// which is an opaque number passed by the client
    pub flags: Option<u32>,
    /// Data for this key.
    pub data: Option<Vec<u8>>,
    /// optional extra Meta Flags Response Data
    /// Meta flags are not to be confused with client bitflags, which is an opaque
    /// number passed by the client. Meta flags change how the command operates, but
    /// they are not stored in cache.
    pub meta_values: Option<MetaValue>,
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct MetaValue {
    /// Status of the operation for set/add/replace commands
    pub status: Option<Status>,
    /// CAS value (c flag)
    pub cas: Option<u64>,
    /// Client flags (f flag)
    pub flags: Option<u32>,
    /// Whether the item has been accessed before (h flag)
    pub hit_before: Option<bool>,
    /// Last access time in seconds since the epoch (l flag)
    pub last_accessed: Option<u64>,
    /// Remaining TTL in seconds, or -1 for unlimited (t flag)
    pub ttl_remaining: Option<i64>,
    /// Value size (s flag)
    pub size: Option<u64>,
    /// opaque value, consumes a token and copies back with response (O flag)
    pub opaque_token: Option<Vec<u8>>,
    /// Staleness indicator (X flag)
    pub is_stale: Option<bool>,
    /// Win/lose for recache (W/Z flag)
    pub is_recache_winner: Option<bool>,
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
    /// Quiet mode no-op.
    NoOp,
    /// The key already exists.
    Exists,
    /// The key already exists and value was requested in the meta protocol.
    Value,
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
    /// An error from memcached related to a key that exceeds maximum allowed length.
    KeyTooLong,
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
            Self::NoOp => write!(f, "no-op"),
            Self::Exists => write!(f, "exists"),
            Self::Value => write!(f, "value"),
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
            Self::KeyTooLong => write!(f, "Key exceeds maximum allowed length of 250 characters"),
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

// shared parsing functions
pub(crate) fn parse_u64(buf: &[u8]) -> IResult<&[u8], u64> {
    map_res(take_while_m_n(1, 20, is_digit), btou)(buf)
}

pub(crate) fn parse_i64(buf: &[u8]) -> IResult<&[u8], i64> {
    map_res(take_while_m_n(1, 20, is_signed_digit), btoi)(buf)
}

pub(crate) fn parse_bool(buf: &[u8]) -> IResult<&[u8], bool> {
    alt((value(true, tag(b"yes")), value(false, tag(b"no"))))(buf)
}

pub(crate) fn parse_incrdecr(buf: &[u8]) -> IResult<&[u8], Response> {
    terminated(map(parse_u64, Response::IncrDecr), crlf)(buf)
}

pub(crate) fn is_key_char(chr: u8) -> bool {
    chr > 32 && chr < 127
}

pub(crate) fn is_signed_digit(chr: u8) -> bool {
    chr == 45 || (48..=57).contains(&chr)
}

pub(crate) fn parse_u32(buf: &[u8]) -> IResult<&[u8], u32> {
    map_res(take_while_m_n(1, 10, is_digit), btou)(buf)
}
