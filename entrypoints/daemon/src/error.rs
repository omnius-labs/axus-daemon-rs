use backtrace::Backtrace;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorKind {
    IoError,
    TimeError,
    SerdeError,
    DatabaseError,
    HttpClientError,
    CryptoError,
    UpnpError,
    UnexpectedError,
    NetworkError,

    InvalidFormat,
    EndOfStream,
    UnsupportedVersion,
    UnsupportedType,
    Reject,
    NotFound,
    AlreadyConnected,
    RateLimitExceeded,
}

impl std::fmt::Display for ErrorKind {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorKind::IoError => write!(fmt, "I/O error"),
            ErrorKind::TimeError => write!(fmt, "time conversion error"),
            ErrorKind::SerdeError => write!(fmt, "serde error"),
            ErrorKind::DatabaseError => write!(fmt, "database error"),
            ErrorKind::HttpClientError => write!(fmt, "http client error"),
            ErrorKind::CryptoError => write!(fmt, "crypto error"),
            ErrorKind::UpnpError => write!(fmt, "upnp error"),
            ErrorKind::NetworkError => write!(fmt, "network error"),
            ErrorKind::UnexpectedError => write!(fmt, "unexpected error"),

            ErrorKind::InvalidFormat => write!(fmt, "invalid format"),
            ErrorKind::EndOfStream => write!(fmt, "end of stream"),
            ErrorKind::UnsupportedVersion => write!(fmt, "unsupported version"),
            ErrorKind::UnsupportedType => write!(fmt, "unsupported type"),
            ErrorKind::Reject => write!(fmt, "reject"),
            ErrorKind::NotFound => write!(fmt, "not found"),
            ErrorKind::AlreadyConnected => write!(fmt, "already connected"),
            ErrorKind::RateLimitExceeded => write!(fmt, "rate limit exceeded"),
        }
    }
}

pub struct Error {
    kind: ErrorKind,
    message: Option<String>,
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
    backtrace: Backtrace,
}

impl Error {
    pub fn new(kind: ErrorKind) -> Self {
        Self {
            kind,
            message: None,
            source: None,
            backtrace: Backtrace::new(),
        }
    }

    pub fn message<S: AsRef<str>>(mut self, message: S) -> Self {
        self.message = Some(message.as_ref().to_string());
        self
    }

    pub fn source<E: Into<Box<dyn std::error::Error + Send + Sync>>>(mut self, source: E) -> Self {
        self.source = Some(source.into());
        self
    }

    pub fn kind(&self) -> &ErrorKind {
        &self.kind
    }
}

impl std::fmt::Debug for Error {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = fmt.debug_struct("Error");

        debug.field("kind", &self.kind);

        if let Some(message) = &self.message {
            debug.field("message", message);
        }

        if let Some(source) = &self.source {
            debug.field("source", source);
        }

        debug.field("backtrace", &format_args!("{:?}", self.backtrace));

        debug.finish()
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(message) = &self.message {
            write!(fmt, "{}: {}", self.kind, message)
        } else {
            write!(fmt, "{}", self.kind)
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_ref().map(|s| &**s as &(dyn std::error::Error + 'static))
    }
}

impl From<std::convert::Infallible> for Error {
    fn from(e: std::convert::Infallible) -> Self {
        Error::new(ErrorKind::InvalidFormat).message("convert failed").source(e)
    }
}

impl From<std::array::TryFromSliceError> for Error {
    fn from(e: std::array::TryFromSliceError) -> Self {
        Error::new(ErrorKind::InvalidFormat).message("failed to convert slice to array").source(e)
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Error {
        Error::new(ErrorKind::IoError).message("io error").source(e)
    }
}

impl From<sqlx::Error> for Error {
    fn from(e: sqlx::Error) -> Self {
        Error::new(ErrorKind::DatabaseError).message("Database operation failed").source(e)
    }
}

impl From<std::num::ParseIntError> for Error {
    fn from(e: std::num::ParseIntError) -> Error {
        Error::new(ErrorKind::InvalidFormat).message("int parse error").source(e)
    }
}

impl From<std::net::AddrParseError> for Error {
    fn from(e: std::net::AddrParseError) -> Error {
        Error::new(ErrorKind::InvalidFormat).message("addr parse error").source(e)
    }
}

impl From<hex::FromHexError> for Error {
    fn from(e: hex::FromHexError) -> Self {
        Error::new(ErrorKind::InvalidFormat).message("hex decode error").source(e)
    }
}

impl From<base64::DecodeError> for Error {
    fn from(e: base64::DecodeError) -> Self {
        Error::new(ErrorKind::InvalidFormat).message("base64 decode error").source(e)
    }
}

impl From<omnius_core_rocketpack::Error> for Error {
    fn from(e: omnius_core_rocketpack::Error) -> Error {
        Error::new(ErrorKind::SerdeError).message("rocket pack error").source(e)
    }
}

impl From<omnius_core_omnikit::Error> for Error {
    fn from(e: omnius_core_omnikit::Error) -> Self {
        match e.kind() {
            omnius_core_omnikit::ErrorKind::SerdeError => Error::new(ErrorKind::SerdeError).message("Omnikit serde error").source(e),
            omnius_core_omnikit::ErrorKind::IoError => Error::new(ErrorKind::IoError).message("Omnikit IO error").source(e),
            omnius_core_omnikit::ErrorKind::UnexpectedError => Error::new(ErrorKind::UnexpectedError).message("Omnikit unexpected error").source(e),

            omnius_core_omnikit::ErrorKind::InvalidFormat => Error::new(ErrorKind::InvalidFormat).message("Omnikit invalid format").source(e),
            omnius_core_omnikit::ErrorKind::EndOfStream => Error::new(ErrorKind::EndOfStream).message("Omnikit end of stream").source(e),
            omnius_core_omnikit::ErrorKind::UnsupportedVersion => {
                Error::new(ErrorKind::UnsupportedVersion).message("Omnikit unsupported version").source(e)
            }
            omnius_core_omnikit::ErrorKind::UnsupportedType => Error::new(ErrorKind::UnsupportedType).message("Omnikit unsupported type").source(e),
        }
    }
}

impl From<std::num::TryFromIntError> for Error {
    fn from(e: std::num::TryFromIntError) -> Self {
        Error::new(ErrorKind::InvalidFormat).message("Integer conversion error").source(e)
    }
}

impl From<reqwest::Error> for Error {
    fn from(e: reqwest::Error) -> Self {
        Error::new(ErrorKind::HttpClientError).message("HTTP request failed").source(e)
    }
}

impl From<toml::de::Error> for Error {
    fn from(e: toml::de::Error) -> Error {
        Error::new(ErrorKind::SerdeError).message("failed to parse toml file").source(e)
    }
}
