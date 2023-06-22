use crate::database::Database;
use crate::error::Error;
use crate::ffi::ffi;
use crate::query_result::QueryResult;
use crate::value::Value;
use cxx::{let_cxx_string, UniquePtr};
use std::cell::UnsafeCell;
use std::convert::TryInto;

/// A prepared stattement is a parameterized query which can avoid planning the same query for
/// repeated execution
pub struct PreparedStatement {
    statement: UniquePtr<ffi::PreparedStatement>,
}

/// Connections are used to interact with a Database instance.
///
/// ## Concurrency
///
/// Each connection is thread-safe, and multiple connections can connect to the same Database
/// instance in a multithreaded environment.
///
/// Note that since connections require a reference to the Database, creating or using connections
/// in multiple threads cannot be done from a regular std::thread since the threads (and
/// connections) could outlive the database. This can be worked around by using a
/// [scoped thread](std::thread::scope) (Note: Introduced in rust 1.63. For compatibility with
/// older versions of rust, [crosssbeam_utils::thread::scope](https://docs.rs/crossbeam-utils/latest/crossbeam_utils/thread/index.html) can be used instead).
///
/// Also note that write queries can only be done one at a time; the query command will return an
/// [error](Error::FailedQuery) if another write query is in progress.
///
/// ```
/// # use kuzu::{Connection, Database, Value, Error};
/// # fn main() -> anyhow::Result<()> {
/// # let temp_dir = tempdir::TempDir::new("example3")?;
/// # let db = Database::new(temp_dir.path(), 0)?;
/// let conn = Connection::new(&db)?;
/// conn.query("CREATE NODE TABLE Person(name STRING, age INT32, PRIMARY KEY(name));")?;
/// // Write queries must be done sequentially
/// conn.query("CREATE (:Person {name: 'Alice', age: 25});")?;
/// conn.query("CREATE (:Person {name: 'Bob', age: 30});")?;
/// let (alice, bob) = std::thread::scope(|s| -> Result<(Vec<Value>, Vec<Value>), Error> {
///     let alice_thread = s.spawn(|| -> Result<Vec<Value>, Error> {
///         let conn = Connection::new(&db)?;
///         let mut result = conn.query("MATCH (a:Person) WHERE a.name = \"Alice\" RETURN a.name AS NAME, a.age AS AGE;")?;
///         Ok(result.next().unwrap())
///     });
///     let bob_thread = s.spawn(|| -> Result<Vec<Value>, Error> {
///         let conn = Connection::new(&db)?;
///         let mut result = conn.query(
///             "MATCH (a:Person) WHERE a.name = \"Bob\" RETURN a.name AS NAME, a.age AS AGE;",
///         )?;
///         Ok(result.next().unwrap())
///     });
///     Ok((alice_thread.join().unwrap()?, bob_thread.join().unwrap()?))
///  })?;
///
///  assert_eq!(alice, vec!["Alice".into(), 25.into()]);
///  assert_eq!(bob, vec!["Bob".into(), 30.into()]);
///  temp_dir.close()?;
///  Ok(())
/// # }
/// ```
///
/// ## Committing
/// If the connection is in AUTO_COMMIT mode any query over the connection will be wrapped around
/// a transaction and committed (even if the query is READ_ONLY).
/// If the connection is in MANUAL transaction mode, which happens only if an application
/// manually begins a transaction (see below), then an application has to manually commit or
/// rollback the transaction by calling commit() or rollback().
///
/// AUTO_COMMIT is the default mode when a Connection is created. If an application calls
/// begin[ReadOnly/Write]Transaction at any point, the mode switches to MANUAL. This creates
/// an "active transaction" in the connection. When a connection is in MANUAL mode and the
/// active transaction is rolled back or committed, then the active transaction is removed (so
/// the connection no longer has an active transaction) and the mode automatically switches
/// back to AUTO_COMMIT.
/// Note: When a Connection object is deconstructed, if the connection has an active (manual)
/// transaction, then the active transaction is rolled back.
///
/// ```
/// use kuzu::{Database, Connection};
/// # use anyhow::Error;
/// # fn main() -> Result<(), Error> {
/// # let temp_dir = tempdir::TempDir::new("example")?;
/// # let path = temp_dir.path();
/// let db = Database::new(path, 0)?;
/// let conn = Connection::new(&db)?;
/// // AUTO_COMMIT mode
/// conn.query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name));")?;
/// conn.begin_write_transaction()?;
/// // MANUAL mode (write)
/// conn.query("CREATE (:Person {name: 'Alice', age: 25});")?;
/// conn.query("CREATE (:Person {name: 'Bob', age: 30});")?;
/// // Queries committed and mode goes back to AUTO_COMMIT
/// conn.commit()?;
/// let result = conn.query("MATCH (a:Person) RETURN a.name AS NAME, a.age AS AGE;")?;
/// assert!(result.count() == 2);
/// # temp_dir.close()?;
/// # Ok(())
/// # }
/// ```
///
/// ```
/// use kuzu::{Database, Connection};
/// # use anyhow::Error;
/// # fn main() -> Result<(), Error> {
/// # let temp_dir = tempdir::TempDir::new("example")?;
/// # let path = temp_dir.path();
/// let db = Database::new(path, 0)?;
/// let conn = Connection::new(&db)?;
/// // AUTO_COMMIT mode
/// conn.query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name));")?;
/// conn.begin_write_transaction()?;
/// // MANUAL mode (write)
/// conn.query("CREATE (:Person {name: 'Alice', age: 25});")?;
/// conn.query("CREATE (:Person {name: 'Bob', age: 30});")?;
/// // Queries rolled back and mode goes back to AUTO_COMMIT
/// conn.rollback()?;
/// let result = conn.query("MATCH (a:Person) RETURN a.name AS NAME, a.age AS AGE;")?;
/// assert!(result.count() == 0);
/// # temp_dir.close()?;
/// # Ok(())
/// # }
/// ```
pub struct Connection<'a> {
    // bmwinger: Access to the underlying value for synchronized functions can be done
    // with (*self.conn.get()).pin_mut()
    // Turning this into a function just causes lifetime issues.
    conn: UnsafeCell<UniquePtr<ffi::Connection<'a>>>,
}

// Connections are synchronized on the C++ side and should be safe to move and access across
// threads
unsafe impl<'a> Send for Connection<'a> {}
unsafe impl<'a> Sync for Connection<'a> {}

impl<'a> Connection<'a> {
    /// Creates a connection to the database.
    ///
    /// # Arguments
    /// * `database`: A reference to the database instance to which this connection will be connected.
    pub fn new(database: &'a Database) -> Result<Self, Error> {
        let db = unsafe { (*database.db.get()).pin_mut() };
        Ok(Connection {
            conn: UnsafeCell::new(ffi::database_connect(db)?),
        })
    }

    /// Sets the maximum number of threads to use for execution in the current connection
    ///
    /// # Arguments
    /// * `num_threads`: The maximum number of threads to use for execution in the current connection
    pub fn set_max_num_threads_for_exec(&mut self, num_threads: u64) {
        self.conn
            .get_mut()
            .pin_mut()
            .setMaxNumThreadForExec(num_threads);
    }

    /// Returns the maximum number of threads used for execution in the current connection
    pub fn get_max_num_threads_for_exec(&self) -> u64 {
        unsafe { (*self.conn.get()).pin_mut().getMaxNumThreadForExec() }
    }

    /// Prepares the given query and returns the prepared statement.
    ///
    /// # Arguments
    /// * `query`: The query to prepare.
    ///            See <https://kuzudb.com/docs/cypher> for details on the query format
    pub fn prepare(&self, query: &str) -> Result<PreparedStatement, Error> {
        let_cxx_string!(query = query);
        let statement = unsafe { (*self.conn.get()).pin_mut() }.prepare(&query)?;
        if statement.isSuccess() {
            Ok(PreparedStatement { statement })
        } else {
            Err(Error::FailedPreparedStatement(
                ffi::prepared_statement_error_message(&statement),
            ))
        }
    }

    /// Executes the given query and returns the result.
    ///
    /// # Arguments
    /// * `query`: The query to execute.
    ///            See <https://kuzudb.com/docs/cypher> for details on the query format
    // TODO(bmwinger): Instead of having a Value enum in the results, perhaps QueryResult, and thus query
    // should be generic.
    //
    // E.g.
    // let result: QueryResult<kuzu::value::VarList<kuzu::value::String>> = conn.query("...")?;
    // let result: QueryResult<kuzu::value::Int64> = conn.query("...")?;
    //
    // But this would really just be syntactic sugar wrapping the current system
    pub fn query(&self, query: &str) -> Result<QueryResult, Error> {
        let mut statement = self.prepare(query)?;
        self.execute(&mut statement, vec![])
    }

    /// Executes the given prepared statement with args and returns the result.
    ///
    /// # Arguments
    /// * `prepared_statement`: The prepared statement to execute
    pub fn execute(
        &self,
        prepared_statement: &mut PreparedStatement,
        params: Vec<(&str, Value)>,
    ) -> Result<QueryResult, Error> {
        // Passing and converting Values in a collection across the ffi boundary is difficult
        // (std::vector cannot be constructed from rust, Vec cannot contain opaque C++ types)
        // So we create an opaque parameter pack and copy the parameters into it one by one
        let mut cxx_params = ffi::new_params();
        for (key, value) in params {
            let ffi_value: cxx::UniquePtr<ffi::Value> = value.try_into()?;
            cxx_params.pin_mut().insert(key, ffi_value);
        }
        let conn = unsafe { (*self.conn.get()).pin_mut() };
        let result =
            ffi::connection_execute(conn, prepared_statement.statement.pin_mut(), cxx_params)?;
        if !result.isSuccess() {
            Err(Error::FailedQuery(ffi::query_result_get_error_message(
                &result,
            )))
        } else {
            Ok(QueryResult { result })
        }
    }

    /// Manually starts a new read-only transaction in the current connection
    pub fn begin_read_only_transaction(&self) -> Result<(), Error> {
        let conn = unsafe { (*self.conn.get()).pin_mut() };
        Ok(conn.beginReadOnlyTransaction()?)
    }

    /// Manually starts a new write transaction in the current connection
    pub fn begin_write_transaction(&self) -> Result<(), Error> {
        let conn = unsafe { (*self.conn.get()).pin_mut() };
        Ok(conn.beginWriteTransaction()?)
    }

    /// Manually commits the current transaction
    pub fn commit(&self) -> Result<(), Error> {
        let conn = unsafe { (*self.conn.get()).pin_mut() };
        Ok(conn.commit()?)
    }

    /// Manually rolls back the current transaction
    pub fn rollback(&self) -> Result<(), Error> {
        let conn = unsafe { (*self.conn.get()).pin_mut() };
        Ok(conn.rollback()?)
    }

    /// Interrupts all queries currently executing within this connection
    pub fn interrupt(&self) -> Result<(), Error> {
        let conn = unsafe { (*self.conn.get()).pin_mut() };
        Ok(conn.interrupt()?)
    }

    /// Returns all node table names in string format
    pub fn get_node_table_names(&self) -> String {
        let conn = unsafe { (*self.conn.get()).pin_mut() };
        ffi::get_node_table_names(conn)
    }

    /// Returns all rel table names in string format
    pub fn get_rel_table_names(&self) -> String {
        let conn = unsafe { (*self.conn.get()).pin_mut() };
        ffi::get_rel_table_names(conn)
    }

    /// Returns all property names of the given table
    pub fn get_node_property_names(&self, table_name: &str) -> String {
        let conn = unsafe { (*self.conn.get()).pin_mut() };
        ffi::get_node_property_names(conn, table_name)
    }

    /// Returns all property names of the given table
    pub fn get_rel_property_names(&self, rel_table_name: &str) -> String {
        let conn = unsafe { (*self.conn.get()).pin_mut() };
        ffi::get_rel_property_names(conn, rel_table_name)
    }

    /// Sets the query timeout value of the current connection
    ///
    /// A value of zero (the default) disables the timeout.
    pub fn set_query_timeout(&self, timeout_ms: u64) {
        let conn = unsafe { (*self.conn.get()).pin_mut() };
        conn.setQueryTimeOut(timeout_ms);
    }
}

#[cfg(test)]
mod tests {
    use crate::{connection::Connection, database::Database, value::Value};
    use anyhow::{Error, Result};
    // Note: Cargo runs tests in parallel by default, however kuzu does not support
    // working with multiple databases in parallel.
    // Tests can be run serially with `cargo test -- --test-threads=1` to work around this.

    #[test]
    fn test_connection_threads() -> Result<()> {
        let temp_dir = tempdir::TempDir::new("example1")?;
        let db = Database::new(temp_dir.path(), 0)?;
        let mut conn = Connection::new(&db)?;
        conn.set_max_num_threads_for_exec(5);
        assert_eq!(conn.get_max_num_threads_for_exec(), 5);
        temp_dir.close()?;
        Ok(())
    }

    #[test]
    fn test_invalid_query() -> Result<()> {
        let temp_dir = tempdir::TempDir::new("example2")?;
        let db = Database::new(temp_dir.path(), 0)?;
        let conn = Connection::new(&db)?;
        conn.query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name));")?;
        conn.query("CREATE (:Person {name: 'Alice', age: 25});")?;
        conn.query("CREATE (:Person {name: 'Bob', age: 30});")?;

        let result: Error = conn
            .query("MATCH (a:Person RETURN a.name AS NAME, a.age AS AGE;")
            .expect_err("Invalid syntax in query should produce an error")
            .into();
        assert_eq!(
            result.to_string(),
            "Query execution failed: Parser exception: \
Invalid input <MATCH (a:Person RETURN>: expected rule oC_SingleQuery (line: 1, offset: 16)
\"MATCH (a:Person RETURN a.name AS NAME, a.age AS AGE;\"
                 ^^^^^^"
        );
        Ok(())
    }

    #[test]
    fn test_query_result() -> Result<()> {
        let temp_dir = tempdir::TempDir::new("example3")?;
        let db = Database::new(temp_dir.path(), 0)?;
        let conn = Connection::new(&db)?;
        conn.query("CREATE NODE TABLE Person(name STRING, age INT16, PRIMARY KEY(name));")?;
        conn.query("CREATE (:Person {name: 'Alice', age: 25});")?;

        for result in conn.query("MATCH (a:Person) RETURN a.name AS NAME, a.age AS AGE;")? {
            assert_eq!(result.len(), 2);
            assert_eq!(result[0], Value::String("Alice".to_string()));
            assert_eq!(result[1], Value::Int16(25));
        }
        temp_dir.close()?;
        Ok(())
    }

    #[test]
    fn test_params() -> Result<()> {
        let temp_dir = tempdir::TempDir::new("example3")?;
        let db = Database::new(temp_dir.path(), 0)?;
        let conn = Connection::new(&db)?;
        conn.query("CREATE NODE TABLE Person(name STRING, age INT16, PRIMARY KEY(name));")?;
        conn.query("CREATE (:Person {name: 'Alice', age: 25});")?;
        conn.query("CREATE (:Person {name: 'Bob', age: 30});")?;

        let mut statement = conn.prepare("MATCH (a:Person) WHERE a.age = $age RETURN a.name;")?;
        for result in conn.execute(&mut statement, vec![("age", Value::Int16(25))])? {
            assert_eq!(result.len(), 1);
            assert_eq!(result[0], Value::String("Alice".to_string()));
        }
        temp_dir.close()?;
        Ok(())
    }

    #[test]
    fn test_params_invalid_type() -> Result<()> {
        let temp_dir = tempdir::TempDir::new("example3")?;
        let db = Database::new(temp_dir.path(), 0)?;
        let conn = Connection::new(&db)?;
        conn.query("CREATE NODE TABLE Person(name STRING, age INT16, PRIMARY KEY(name));")?;
        conn.query("CREATE (:Person {name: 'Alice', age: 25});")?;
        conn.query("CREATE (:Person {name: 'Bob', age: 30});")?;

        let mut statement = conn.prepare("MATCH (a:Person) WHERE a.age = $age RETURN a.name;")?;
        let result: Error = conn
            .execute(
                &mut statement,
                vec![("age", Value::String("25".to_string()))],
            )
            .expect_err("Age should be an int16!")
            .into();
        assert_eq!(
            result.to_string(),
            "Query execution failed: Parameter age has data type STRING but expects INT16."
        );
        temp_dir.close()?;
        Ok(())
    }

    #[test]
    fn test_multithreaded_single_conn() -> Result<()> {
        let temp_dir = tempdir::TempDir::new("example3")?;
        let db = Database::new(temp_dir.path(), 0)?;

        let conn = Connection::new(&db)?;
        conn.query("CREATE NODE TABLE Person(name STRING, age INT32, PRIMARY KEY(name));")?;
        // Write queries must be done sequentially
        conn.query("CREATE (:Person {name: 'Alice', age: 25});")?;
        conn.query("CREATE (:Person {name: 'Bob', age: 30});")?;

        let (alice, bob) = std::thread::scope(|s| -> Result<(Vec<Value>, Vec<Value>)> {
            let alice_thread = s.spawn(|| -> Result<Vec<Value>> {
                let mut result = conn.query("MATCH (a:Person) WHERE a.name = \"Alice\" RETURN a.name AS NAME, a.age AS AGE;")?;
                Ok(result.next().unwrap())
            });
            let bob_thread = s.spawn(|| -> Result<Vec<Value>> {
                let mut result = conn.query(
                    "MATCH (a:Person) WHERE a.name = \"Bob\" RETURN a.name AS NAME, a.age AS AGE;",
                )?;
                Ok(result.next().unwrap())
            });

            Ok((alice_thread.join().unwrap()?, bob_thread.join().unwrap()?))
        })?;

        assert_eq!(alice, vec!["Alice".into(), 25.into()]);
        assert_eq!(bob, vec!["Bob".into(), 30.into()]);
        temp_dir.close()?;
        Ok(())
    }

    #[test]
    fn test_multithreaded_multiple_conn() -> Result<()> {
        let temp_dir = tempdir::TempDir::new("example3")?;
        let db = Database::new(temp_dir.path(), 0)?;

        let conn = Connection::new(&db)?;
        conn.query("CREATE NODE TABLE Person(name STRING, age INT32, PRIMARY KEY(name));")?;
        // Write queries must be done sequentially
        conn.query("CREATE (:Person {name: 'Alice', age: 25});")?;
        conn.query("CREATE (:Person {name: 'Bob', age: 30});")?;

        let (alice, bob) = std::thread::scope(|s| -> Result<(Vec<Value>, Vec<Value>)> {
            let alice_thread = s.spawn(|| -> Result<Vec<Value>> {
                let conn = Connection::new(&db)?;
                let mut result = conn.query("MATCH (a:Person) WHERE a.name = \"Alice\" RETURN a.name AS NAME, a.age AS AGE;")?;
                Ok(result.next().unwrap())
            });
            let bob_thread = s.spawn(|| -> Result<Vec<Value>> {
                let conn = Connection::new(&db)?;
                let mut result = conn.query(
                    "MATCH (a:Person) WHERE a.name = \"Bob\" RETURN a.name AS NAME, a.age AS AGE;",
                )?;
                Ok(result.next().unwrap())
            });

            Ok((alice_thread.join().unwrap()?, bob_thread.join().unwrap()?))
        })?;

        assert_eq!(alice, vec!["Alice".into(), 25.into()]);
        assert_eq!(bob, vec!["Bob".into(), 30.into()]);
        temp_dir.close()?;
        Ok(())
    }

    #[test]
    fn test_table_names() -> Result<()> {
        let temp_dir = tempdir::TempDir::new("example3")?;
        let db = Database::new(temp_dir.path(), 0)?;
        let conn = Connection::new(&db)?;
        conn.query("CREATE NODE TABLE Person(name STRING, age INT16, PRIMARY KEY(name));")?;
        conn.query("CREATE REL TABLE Follows(FROM Person TO Person, since DATE);")?;
        assert_eq!(
            conn.get_node_table_names(),
            "Node tables: \n\tPerson\n".to_string()
        );
        assert_eq!(
            conn.get_rel_table_names(),
            "Rel tables: \n\tFollows\n".to_string()
        );
        assert_eq!(
            conn.get_node_property_names("Person"),
            "Person properties: \n\tname STRING(PRIMARY KEY)\n\tage INT16\n".to_string()
        );
        assert_eq!(
            conn.get_rel_property_names("Follows"),
            "Follows src node: Person\n\
            Follows dst node: Person\nFollows properties: \n\tsince DATE\n"
                .to_string()
        );

        temp_dir.close()?;
        Ok(())
    }
}
