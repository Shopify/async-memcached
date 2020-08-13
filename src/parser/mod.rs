use std::io;
mod ascii;
pub use ascii::parse_ascii_response;

#[derive(Clone, Debug, PartialEq)]
pub struct Value {
    key: Vec<u8>,
    cas: Option<u64>,
    flags: u32,
    data: Vec<u8>,
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
    Io(io::ErrorKind),
    Client(String),
    Server(String),
}

#[derive(Clone, Debug, PartialEq)]
pub enum Response {
    Status(Status),
    Data(Option<Vec<Value>>),
    IncrDecr(u64),
}

impl From<io::Error> for Status {
    fn from(e: io::Error) -> Self {
        Status::Error(ErrorKind::Io(e.kind()))
    }
}

impl From<io::Error> for ErrorKind {
    fn from(e: io::Error) -> Self {
        ErrorKind::Io(e.kind())
    }
}
