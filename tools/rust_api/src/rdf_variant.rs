use crate::ffi::ffi;
use std::fmt;

#[derive(Clone, Debug, PartialEq)]
pub enum RDFVariant {
    Bool(bool),
    Int64(i64),
    Int32(i32),
    Int16(i16),
    Int8(i8),
    UInt64(u64),
    UInt32(u32),
    UInt16(u16),
    UInt8(u8),
    Double(f64),
    Float(f32),
    Date(time::Date),
    Interval(time::Duration),
    Timestamp(time::OffsetDateTime),
    String(String),
    Blob(Vec<u8>),
}

impl fmt::Display for RDFVariant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RDFVariant::Bool(true) => write!(f, "True"),
            RDFVariant::Bool(false) => write!(f, "False"),
            RDFVariant::Int8(x) => write!(f, "{x}"),
            RDFVariant::Int16(x) => write!(f, "{x}"),
            RDFVariant::Int32(x) => write!(f, "{x}"),
            RDFVariant::Int64(x) => write!(f, "{x}"),
            RDFVariant::UInt8(x) => write!(f, "{x}"),
            RDFVariant::UInt16(x) => write!(f, "{x}"),
            RDFVariant::UInt32(x) => write!(f, "{x}"),
            RDFVariant::UInt64(x) => write!(f, "{x}"),
            RDFVariant::Double(x) => write!(f, "{x}"),
            RDFVariant::Float(x) => write!(f, "{x}"),
            RDFVariant::Date(x) => write!(f, "{x}"),
            RDFVariant::Interval(x) => write!(f, "{x}"),
            RDFVariant::Timestamp(x) => write!(f, "{x}"),
            RDFVariant::String(x) => write!(f, "{x}"),
            RDFVariant::Blob(x) => write!(f, "{x:x?}"),
        }
    }
}

impl From<&RDFVariant> for ffi::LogicalTypeID {
    fn from(item: &RDFVariant) -> Self {
        match item {
            RDFVariant::Bool(_) => ffi::LogicalTypeID::BOOL,
            RDFVariant::Int8(_) => ffi::LogicalTypeID::INT8,
            RDFVariant::Int16(_) => ffi::LogicalTypeID::INT16,
            RDFVariant::Int32(_) => ffi::LogicalTypeID::INT32,
            RDFVariant::Int64(_) => ffi::LogicalTypeID::INT64,
            RDFVariant::UInt8(_) => ffi::LogicalTypeID::UINT8,
            RDFVariant::UInt16(_) => ffi::LogicalTypeID::UINT16,
            RDFVariant::UInt32(_) => ffi::LogicalTypeID::UINT32,
            RDFVariant::UInt64(_) => ffi::LogicalTypeID::UINT64,
            RDFVariant::Double(_) => ffi::LogicalTypeID::DOUBLE,
            RDFVariant::Float(_) => ffi::LogicalTypeID::FLOAT,
            RDFVariant::Date(_) => ffi::LogicalTypeID::DATE,
            RDFVariant::Timestamp(_) => ffi::LogicalTypeID::TIMESTAMP,
            RDFVariant::Interval(_) => ffi::LogicalTypeID::INTERVAL,
            RDFVariant::String(_) => ffi::LogicalTypeID::STRING,
            RDFVariant::Blob(_) => ffi::LogicalTypeID::BLOB,
        }
    }
}
