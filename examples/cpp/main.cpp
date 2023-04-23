#include <iostream>

#include "main/kuzu.h"
using namespace kuzu::main;

int main() {
    auto database = std::make_unique<Database>("" /* fill db path */);
    auto connection = std::make_unique<Connection>(database.get());

    // Create schema.
    connection->query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name));");
    // Create nodes.
    connection->query("CREATE (:Person {name: 'Alice', age: 25});");
    connection->query("CREATE (:Person {name: 'Bob', age: 30});");

    // Execute a simple query.
    auto result = connection->query("MATCH (a:Person) RETURN a.name AS NAME, a.age AS AGE;");
    // Print query result.
    std::cout << result->toString();
}
