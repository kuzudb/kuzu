#include <iostream>

#include "kuzu.hpp"
#include <filesystem>
using namespace kuzu::main;

int main() {
    std::filesystem::remove_all("testdb");
    auto database = std::make_unique<Database>("testdb" /* fill db path */);
    auto connection = std::make_unique<Connection>(database.get());

    // Create schema.
    connection->query("CREATE NODE TABLE doc (ID UINT64, content STRING, PRIMARY KEY (ID));");
    // Create nodes.
    connection->query("CREATE (:Person {name: 'Alice', age: 25});");
    connection->query("CREATE (:Person {name: 'Bob', age: 30});");

    // Execute a simple query.
    auto result = connection->query("MATCH (a:Person) RETURN a.name AS NAME, a.age AS AGE;");
    // Print query result.
    std::cout << result->toString();
}
