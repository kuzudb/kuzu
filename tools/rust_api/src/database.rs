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

#[derive(Clone, Debug)]
/// Configuration options for the database.
pub struct SystemConfig {
    /// Max size of the buffer pool in bytes
    ///
    /// The larger the buffer pool, the more data from the database files is kept in memory,
    /// reducing the amount of File I/O.
    ///
    /// Defaults to an auto-detected size
    buffer_pool_size: u64,
    /// The maximum number of threads to use when executing queries
    /// Defaults to auto-detect from the number of processors
    max_num_threads: u64,
    /// When true, new columns will be compressed if possible
    /// Defaults to true
    enable_compression: bool,
    read_only: bool,
}

impl Default for SystemConfig {
    fn default() -> SystemConfig {
        SystemConfig {
            buffer_pool_size: 0,
            max_num_threads: 0,
            enable_compression: true,
            read_only: false,
        }
    }
}

impl SystemConfig {
    pub fn buffer_pool_size(mut self, buffer_pool_size: u64) -> Self {
        self.buffer_pool_size = buffer_pool_size;
        self
    }
    pub fn max_num_threads(mut self, max_num_threads: u64) -> Self {
        self.max_num_threads = max_num_threads;
        self
    }
    pub fn enable_compression(mut self, enable_compression: bool) -> Self {
        self.enable_compression = enable_compression;
        self
    }
    pub fn read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;
        self
    }
}

impl Database {
    /// Creates a database object
    ///
    /// # Arguments:
    /// * `path`: Path of the database. If the database does not already exist, it will be created.
    /// * `buffer_pool_size`: Max size of the buffer pool in bytes
    /// * `max_num_threads`: The maximum number of threads to use in the system
    /// * `enable_compression`: When true, table data will be compressed if possible
    /// * `read_only`: When true, the database will be opened in READ_ONLY mode, otherwise in READ_WRITE mode.
    pub fn new<P: AsRef<Path>>(path: P, config: SystemConfig) -> Result<Self, Error> {
        let_cxx_string!(path = path.as_ref().display().to_string());
        Ok(Database {
            db: UnsafeCell::new(ffi::new_database(
                &path,
                config.buffer_pool_size,
                config.max_num_threads,
                config.enable_compression,
                config.read_only,
            )?),
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
    use crate::connection::Connection;
    use crate::database::{Database, LoggingLevel, SystemConfig};
    use anyhow::{Error, Result};
    // Note: Cargo runs tests in parallel by default, however kuzu does not support
    // working with multiple databases in parallel.
    // Tests can be run serially with `cargo test -- --test-threads=1` to work around this.

    #[test]
    fn create_database() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let mut db = Database::new(temp_dir.path(), SystemConfig::default())?;
        db.set_logging_level(LoggingLevel::Debug);
        db.set_logging_level(LoggingLevel::Info);
        db.set_logging_level(LoggingLevel::Error);
        temp_dir.close()?;
        Ok(())
    }

    #[test]
    fn create_database_no_compression() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let _ = Database::new(
            temp_dir.path(),
            SystemConfig::default().enable_compression(false),
        )?;
        temp_dir.close()?;
        Ok(())
    }

    #[test]
    fn create_database_failure() {
        let result: Error = Database::new("", SystemConfig::default())
            .expect_err("An empty string should not be a valid database path!")
            .into();
        if cfg!(windows) {
            assert_eq!(
                result.to_string(),
                "Failed to create directory  due to: create_directory: The system cannot find the path specified.: \"\""
            );
        } else if cfg!(linux) {
            assert_eq!(
                result.to_string(),
                "Failed to create directory  due to: filesystem error: cannot create directory: No such file or directory []"
            );
        } else {
            assert!(result
                .to_string()
                .starts_with("Failed to create directory  due to"));
        }
    }

    #[test]
    fn test_database_read_only() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        // Create database first so that it can be opened read-only
        {
            Database::new(temp_dir.path(), SystemConfig::default())?;
        }
        let db = Database::new(temp_dir.path(), SystemConfig::default().read_only(true))?;
        let conn = Connection::new(&db)?;
        let result: Error = conn
            .query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name));")
            .expect_err("Invalid syntax in query should produce an error")
            .into();

        assert_eq!(
            result.to_string(),
            "Cannot execute write operations in a read-only database!"
        );
        Ok(())
    }
}
