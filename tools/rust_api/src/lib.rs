//! Bindings to Kuzu: an in-process property graph database management system built for query speed and scalability.
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
//! println!("{}", result);
//! # temp_dir.close()?;
//! # Ok(())
//! # }
//! ```
//! ## Building
//!
//! By default, the Kuzu C++ library will be compiled from source and statically linked.
//!
//! If you want to instead link against a pre-built version of the library, the following environment
//! variables can be used to configure the build process:
//!
//! - `KUZU_SHARED`: If set, link dynamically instead of statically
//! - `KUZU_INCLUDE_DIR`: Directory of Kuzu's headers
//! - `KUZU_LIBRARY_DIR`: Directory containing Kuzu's pre-built libraries.
//!
//! Example:
//! ```bash
//! kuzu_prebuilt_dir=/tmp/kuzu # pre-built Kuzu from https://docs.kuzudb.com/installation/#cc
//! kuzu_prebuilt_dir=/path_to_kuzu_source/build/release/src # Kuzu built from source
//! export KUZU_LIBRARY_DIR="kuzu_prebuilt_dir"
//! export KUZU_INCLUDE_DIR="kuzu_prebuilt_dir"
//! export KUZU_SHARED=1
//! ```
//! On macOS:
//! ```bash
//! brew install kuzu
//! export KUZU_LIBRARY_DIR=/opt/homebrew/lib
//! export KUZU_INCLUDE_DIR=/opt/homebrew/include
//! export KUZU_SHARED=1
//! ```
//!
//! ## Using Extensions
//! By default, binaries created using this library will not work with Kuzu's
//! [extensions](https://docs.kuzudb.com/extensions/) (except on Windows/MSVC, where the linker works differently).
//!
//! If you want to use extensions in binaries (binary crates or tests) using this
//! library, you will need to add the following (or a similar command; see
//! [build-scripts](https://doc.rust-lang.org/cargo/reference/build-scripts.html#rustc-link-arg))
//! to your build.rs (or create one) so that the binary
//! produced acts like a library that the extension can link with. Not doing this will produce
//! undefined symbol errors when the extension is loaded:
//!
//! ```ignore
//! println!("cargo:rustc-link-arg=-rdynamic");
//! ```

pub use connection::{Connection, PreparedStatement};
pub use database::{Database, SystemConfig};
pub use error::Error;
pub use logical_type::LogicalType;
#[cfg(feature = "arrow")]
pub use query_result::ArrowIterator;
pub use query_result::{CSVOptions, QueryResult};
pub use value::{InternalID, NodeVal, RelVal, Value};

mod connection;
mod database;
mod error;
mod ffi;
mod logical_type;
mod query_result;
mod value;

/// The version of the Kuzu crate as reported by Cargo's `CARGO_PKG_VERSION` environment variable
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
/// Returns the storage version of the Kuzu library
pub fn get_storage_version() -> u64 {
    crate::ffi::ffi::get_storage_version()
}
