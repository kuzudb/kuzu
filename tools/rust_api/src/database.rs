use crate::error::Error;
use crate::ffi::ffi;
use cxx::{let_cxx_string, UniquePtr};
use std::cell::UnsafeCell;
use std::fmt;
use std::path::Path;

/// The Database class is the main class of KuzuDB. It manages all database components.
pub struct Database {
    pub(crate) db: UnsafeCell<UniquePtr<ffi::Database>>,
}

unsafe impl Send for Database {}
unsafe impl Sync for Database {}

/// Logging level of the database instance
pub enum LoggingLevel {
    Debug,
    Info,
    Error,
}

impl Database {
    /// Creates a database object
    ///
    /// # Arguments:
    /// * `path`: Path of the database. If the database does not already exist, it will be created.
    /// * `buffer_pool_size`: Max size of the buffer pool in bytes
    pub fn new<P: AsRef<Path>>(path: P, buffer_pool_size: u64) -> Result<Self, Error> {
        let_cxx_string!(path = path.as_ref().display().to_string());
        Ok(Database {
            db: UnsafeCell::new(ffi::new_database(&path, buffer_pool_size)?),
        })
    }

    /// Sets the logging level of the database instance
    ///
    /// # Arguments
    /// * `logging_level`: New logging level.
    pub fn set_logging_level(&mut self, logging_level: LoggingLevel) {
        let_cxx_string!(
            level = match logging_level {
                LoggingLevel::Debug => "debug",
                LoggingLevel::Info => "info",
                LoggingLevel::Error => "err",
            }
        );
        ffi::database_set_logging_level(self.db.get_mut().pin_mut(), &level);
    }
}

impl fmt::Debug for Database {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Database")
            .field("db", &"Opaque C++ data".to_string())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use crate::database::{Database, LoggingLevel};
    use anyhow::{Error, Result};
    // Note: Cargo runs tests in parallel by default, however kuzu does not support
    // working with multiple databases in parallel.
    // Tests can be run serially with `cargo test -- --test-threads=1` to work around this.

    #[test]
    fn create_database() -> Result<()> {
        let temp_dir = tempdir::TempDir::new("example")?;
        let mut db = Database::new(temp_dir.path(), 0)?;
        db.set_logging_level(LoggingLevel::Debug);
        db.set_logging_level(LoggingLevel::Info);
        db.set_logging_level(LoggingLevel::Error);
        temp_dir.close()?;
        Ok(())
    }

    #[test]
    fn create_database_failure() {
        let result: Error = Database::new("", 0)
            .expect_err("An empty string should not be a valid database path!")
            .into();
        assert_eq!(
            result.to_string(),
            "Failed to create directory  due to: filesystem error: cannot create directory: No such file or directory []"
        );
    }
}
