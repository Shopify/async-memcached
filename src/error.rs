use crate::parser::Status;
use std::{fmt, io};

/// Error type for [`Client`](crate::Client) operations.
#[derive(Debug)]
pub enum Error {
    /// Connect error.
    /// Useful for distinguishing between transitive I/O errors and connection errors.
    Connect(io::Error),
    /// I/O-related error.
    Io(io::Error),
    /// A protocol-level error i.e. a failed operation or message that
    /// does not match the protocol specification.
    Protocol(Status),
    /// Key not found for incrdecr
    NotFound,
}

impl PartialEq for Error {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Connect(e1), Self::Connect(e2)) => e1.kind() == e2.kind(),
            (Self::Io(e1), Self::Io(e2)) => e1.kind() == e2.kind(),
            (Self::Protocol(s1), Self::Protocol(s2)) => s1 == s2,
            _ => false,
        }
    }
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
            Self::Connect(e) => write!(f, "connect: {}", e),
            Self::Io(e) => write!(f, "io: {}", e),
            Self::Protocol(e) => write!(f, "protocol: {}", e),
            Self::NotFound => write!(f, "key not found"),
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
