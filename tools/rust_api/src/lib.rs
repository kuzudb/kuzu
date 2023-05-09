//! Bindings to Kùzu: an in-process property graph database management system built for query speed and scalability.
//!
//! ## Example Usage
//! ```
//! use kuzu::{Database, Connection};
//! # use anyhow::Error;
//!
//! # fn main() -> Result<(), Error> {
//! # let temp_dir = tempdir::TempDir::new("example")?;
//! # let path = temp_dir.path();
//! let db = Database::new(path, 0)?;
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
//!
//! ## Safety
//!
//! Generally, use of of this API is safe, however creating multiple databases in the same
//! scope is not safe.
//! If you need to access multiple databases you will need to do so in separate processes.

mod connection;
mod database;
mod error;
mod ffi;
mod logical_type;
mod query_result;
mod value;

pub use connection::{Connection, PreparedStatement};
pub use database::{Database, LoggingLevel};
pub use error::Error;
pub use logical_type::LogicalType;
pub use query_result::{CSVOptions, QueryResult};
pub use value::{InternalID, NodeVal, RelVal, Value};
