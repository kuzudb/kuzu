use crate::logical_type::LogicalType;
use std::fmt;

pub enum Error {
    /// Exception raised by C++ kuzu library
    CxxException(cxx::Exception),
    /// Message produced by kuzu when a query fails
    FailedQuery(String),
    /// Message produced by kuzu when a query fails to prepare
    FailedPreparedStatement(String),
    /// Message produced when you attempt to pass read-only types over the FFI boundary
    ReadOnlyType(LogicalType),
    #[cfg(feature = "arrow")]
    ArrowError(arrow::error::ArrowError),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use Error::*;
        match self {
            CxxException(cxx) => write!(f, "{cxx}"),
            FailedQuery(message) => write!(f, "Query execution failed: {message}"),
            FailedPreparedStatement(message) => write!(f, "Query execution failed: {message}"),
            ReadOnlyType(typ) => write!(f, "Attempted to pass read only type {:?} over ffi!", typ),
            #[cfg(feature = "arrow")]
            ArrowError(err) => write!(f, "{}", err),
        }
    }
}

impl std::fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        use Error::*;
        match self {
            CxxException(cxx) => Some(cxx),
            _ => None,
        }
    }
}

impl From<cxx::Exception> for Error {
    fn from(item: cxx::Exception) -> Self {
        Error::CxxException(item)
    }
}

#[cfg(feature = "arrow")]
impl From<arrow::error::ArrowError> for Error {
    fn from(item: arrow::error::ArrowError) -> Self {
        Error::ArrowError(item)
    }
}
