//! Bindings to Kùzu: an in-process property graph database management system built for query speed and scalability.
//!
//! ## Example Usage
//! ```
//! use kuzu::{Database, SystemConfig, Connection};
//! # use anyhow::Error;
//!
//! # fn main() -> Result<(), Error> {
//! # let temp_dir = tempfile::tempdir()?;
//! # let path = temp_dir.path();
//! let db = Database::new(path, SystemConfig::default())?;
//! let conn = Connection::new(&db)?;
//! conn.query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name));")?;
//! conn.query("CREATE (:Person {name: 'Alice', age: 25});")?;
//! conn.query("CREATE (:Person {name: 'Bob', age: 30});")?;
//!
//! let mut result = conn.query("MATCH (a:Person) RETURN a.name AS NAME, a.age AS AGE;")?;
//! println!("{}", result.display());
//! # temp_dir.close()?;
//! # Ok(())
//! # }
//! ```
//! ## Building
//!
//! By default, the kuzu C++ library will be compiled from source and statically linked.
//!
//! If you want to instead link against a pre-built version of the library, the following environment
//! variables can be used to configure the build process:
//!
//! - `KUZU_SHARED`: If set, link dynamically instead of statically
//! - `KUZU_INCLUDE_DIR`: Directory of kuzu's headers
//! - `KUZU_LIBRARY_DIR`: Directory containing kuzu's pre-built libraries.

pub use connection::{Connection, PreparedStatement};
pub use database::{Database, SystemConfig};
pub use error::Error;
pub use logical_type::LogicalType;
#[cfg(feature = "arrow")]
pub use query_result::ArrowIterator;
pub use query_result::{CSVOptions, QueryResult};
pub use rdf_variant::RDFVariant;
pub use value::{InternalID, NodeVal, RelVal, Value};

mod connection;
mod database;
mod error;
mod ffi;
mod logical_type;
mod query_result;
mod rdf_variant;
mod value;

/// The version of the Kùzu crate as reported by Cargo's CARGO_PKG_VERSION environment variable
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
/// Returns the storage version of the Kùzu library
pub fn get_storage_version() -> u64 {
    crate::ffi::ffi::get_storage_version()
}
