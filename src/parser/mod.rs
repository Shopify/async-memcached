use std::fmt;
mod ascii;
pub use ascii::parse_ascii_response;

#[derive(Clone, Debug, PartialEq)]
pub struct Value {
    pub key: Vec<u8>,
    pub cas: Option<u64>,
    pub flags: u32,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Status {
    Stored,
    NotStored,
    Deleted,
    Touched,
    Exists,
    NotFound,
    Error(ErrorKind),
}

#[derive(Clone, Debug, PartialEq)]
pub enum ErrorKind {
    Generic,
    Protocol,
    Client(String),
    Server(String),
}

#[derive(Clone, Debug, PartialEq)]
pub enum Response {
    Status(Status),
    Data(Option<Vec<Value>>),
    IncrDecr(u64),
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
            Self::Generic => write!(f, "generic"),
            Self::Protocol => write!(f, "protocol"),
            Self::Client(s) => write!(f, "client: {}", s),
            Self::Server(s) => write!(f, "server: {}", s),
        }
    }
}
