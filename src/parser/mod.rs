use std::io;
mod ascii;
pub use ascii::parse_ascii_response;

#[derive(Clone, Debug, PartialEq)]
pub struct Value<'a> {
    key: &'a [u8],
    cas: Option<u64>,
    flags: u32,
    data: &'a [u8],
}

#[derive(Clone, Debug, PartialEq)]
pub enum Status<'a> {
    Stored,
    NotStored,
    Deleted,
    Touched,
    Exists,
    NotFound,
    Error(ErrorKind<'a>),
}

#[derive(Clone, Debug, PartialEq)]
pub enum ErrorKind<'a> {
    Generic,
    Protocol,
    Io(io::ErrorKind),
    Client(&'a str),
    Server(&'a str),
}

#[derive(Clone, Debug, PartialEq)]
pub enum Response<'a> {
    Status(Status<'a>),
    Data(Option<Vec<Value<'a>>>),
    IncrDecr(u64),
}

impl<'a> From<io::Error> for Status<'a> {
    fn from(e: io::Error) -> Self {
        Status::Error(ErrorKind::Io(e.kind()))
    }
}
