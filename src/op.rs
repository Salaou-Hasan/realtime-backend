use std::fmt;
use std::io;

pub const MAX_KEY: usize = 4096;
pub const MAX_VALUE: usize = 65536;
pub const MAX_CONNS: usize = 100_000;
pub const MAX_SUBS: usize = 10_000;
/// Alias matching the name used in the implementation plan.
pub const MAX_SUBS_PER_TOPIC: usize = MAX_SUBS;
pub const OUT_CAP: usize = 256 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ConnId(pub u64);

#[derive(Debug, Clone)]
pub enum Op {
    Put {
        key: String,
        value: Vec<u8>,
        sync: bool,
    },
    Get {
        key: String,
    },
    Pub {
        topic: String,
        value: Vec<u8>,
        sync: bool,
    },
    Sub {
        topic: String,
    },
    Unsub {
        topic: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    OpTooLarge,
    WalFull,
    BadFrame,
    Io,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::OpTooLarge => write!(f, "op too large"),
            Error::WalFull => write!(f, "wal full"),
            Error::BadFrame => write!(f, "bad frame"),
            Error::Io => write!(f, "io error"),
        }
    }
}

impl std::error::Error for Error {}

impl From<io::Error> for Error {
    fn from(_: io::Error) -> Self {
        Error::Io
    }
}

pub fn validate_value_len(n: usize) -> Result<(), Error> {
    if n > MAX_VALUE {
        Err(Error::OpTooLarge)
    } else {
        Ok(())
    }
}

pub fn validate_key_len(n: usize) -> Result<(), Error> {
    if n > MAX_KEY {
        Err(Error::OpTooLarge)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_oversized_value() {
        let big = vec![0u8; 65537];
        assert_eq!(validate_value_len(big.len()), Err(Error::OpTooLarge));
    }

    #[test]
    fn accepts_max_value() {
        let max = vec![0u8; 65536];
        assert_eq!(validate_value_len(max.len()), Ok(()));
    }
}
