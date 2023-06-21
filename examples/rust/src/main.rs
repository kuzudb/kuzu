use kuzu::{Connection, Database, Error};

fn main() -> Result<(), Error> {
    let db = Database::new(
        std::env::args()
            .nth(1)
            .expect("The first CLI argument should be the database path"),
        0,
    )?;
    let connection = Connection::new(&db)?;

    // Create schema.
    connection.query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name));")?;
    // Create nodes.
    connection.query("CREATE (:Person {name: 'Alice', age: 25});")?;
    connection.query("CREATE (:Person {name: 'Bob', age: 30});")?;

    // Execute a simple query.
    let mut result = connection.query("MATCH (a:Person) RETURN a.name AS NAME, a.age AS AGE;")?;

    // Print query result.
    println!("{}", result.display());
    Ok(())
}
