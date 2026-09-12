use std::{fmt, io};

/// Adapter failures are converted to the public worker error at the port boundary.
#[derive(Debug)]
pub(crate) enum AdapterError {
    Public(ledgence_worker_api::Error),
    Invalid(String),
    Limit(String),
    Pressure,
    Io(io::Error),
    Http(reqwest::Error),
}

impl fmt::Display for AdapterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Public(error) => fmt::Display::fmt(error, f),
            Self::Invalid(message) => write!(f, "invalid artifact: {message}"),
            Self::Limit(message) => write!(f, "artifact limit exceeded: {message}"),
            Self::Pressure => f.write_str("artifact cache is full; remaining entries are pinned"),
            Self::Io(error) => write!(f, "artifact filesystem operation failed: {error}"),
            Self::Http(error) => write!(f, "program store request failed: {error}"),
        }
    }
}

impl std::error::Error for AdapterError {}
impl From<io::Error> for AdapterError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}
impl From<reqwest::Error> for AdapterError {
    fn from(value: reqwest::Error) -> Self {
        Self::Http(value)
    }
}
impl From<zip::result::ZipError> for AdapterError {
    fn from(value: zip::result::ZipError) -> Self {
        Self::Invalid(value.to_string())
    }
}
impl From<serde_json::Error> for AdapterError {
    fn from(value: serde_json::Error) -> Self {
        Self::Invalid(value.to_string())
    }
}
pub(crate) type Result<T> = std::result::Result<T, AdapterError>;

impl From<ledgence_worker_api::Error> for AdapterError {
    fn from(value: ledgence_worker_api::Error) -> Self {
        Self::Public(value)
    }
}
impl From<AdapterError> for ledgence_worker_api::Error {
    fn from(value: AdapterError) -> Self {
        use ledgence_worker_api::{Error, ErrorKind};
        let kind = match &value {
            AdapterError::Public(error) => return error.clone(),
            AdapterError::Invalid(_) => ErrorKind::Integrity,
            AdapterError::Limit(_) => ErrorKind::InvalidInput,
            AdapterError::Pressure => ErrorKind::Capacity,
            AdapterError::Io(error) if error.kind() == io::ErrorKind::NotFound => {
                ErrorKind::NotFound
            }
            AdapterError::Io(_) => ErrorKind::Io,
            AdapterError::Http(error) if error.is_timeout() => ErrorKind::TimedOut,
            AdapterError::Http(error) if error.status() == Some(reqwest::StatusCode::NOT_FOUND) => {
                ErrorKind::NotFound
            }
            AdapterError::Http(_) => ErrorKind::Unavailable,
        };
        Error::new(kind, value.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn only_recoverable_cache_pressure_reports_capacity() {
        let hard_limit: ledgence_worker_api::Error =
            AdapterError::Limit("ZIP expansion".into()).into();
        let pressure: ledgence_worker_api::Error = AdapterError::Pressure.into();
        assert_eq!(
            hard_limit.kind,
            ledgence_worker_api::ErrorKind::InvalidInput
        );
        assert_eq!(pressure.kind, ledgence_worker_api::ErrorKind::Capacity);
    }
}
