use crate::parser::Status;
use std::{fmt, io};

/// Error type for [`Client`](crate::Client) operations.
#[derive(Debug)]
pub enum Error {
    /// I/O-related error.
    Io(io::Error),
    /// A protocol-level error i.e. a failed operation or message that
    /// does not match the protocol specification.
    Protocol(Status),
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(ref e) => Some(e),
            _ => None,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "io: {}", e),
            Self::Protocol(e) => write!(f, "protocol: {}", e),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

impl From<Status> for Error {
    fn from(s: Status) -> Self {
        Error::Protocol(s)
    }
}
