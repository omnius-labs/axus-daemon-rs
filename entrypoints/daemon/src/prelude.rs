pub use omnius_core_rocketpack::{
    Error as RocketPackError, ErrorKind as RocketPackErrorKind, Result as RocketPackResult, RocketMessage, RocketMessageReader, RocketMessageWriter,
};

pub use crate::error::{Error, ErrorKind};

pub type Result<T> = std::result::Result<T, Error>;
