#include <iostream>

#include "kuzu.hpp"
using namespace kuzu::main;

int main() {
    auto database = std::make_unique<Database>("test" /* fill db path */);
    auto connection = std::make_unique<Connection>(database.get());

    // Create schema.
    connection->query("CREATE NODE TABLE Person(name STRING, age INT64[5], PRIMARY KEY(name));");
    // Create nodes.
//    auto result=connection->query("CREATE (:Person {name: 'Alice', age: [25,20]});");
//    connection->query("CREATE (:Person {name: 'Bob', age: 30});");

    // Execute a simple query.
    auto result = connection->query("COPY Person FROM \"/Users/monkey/Desktop/博士/code/KUZU_PROJECT/kuzu/dataset/tinysnb2/person.csv\" (HeaDER=true, deLim=',');");
//    auto result = connection->query("MATCH (a:Person) RETURN a.name AS NAME, a.age AS AGE;");
//    auto result = connection->query("COPY Person FROM \"/Users/monkey/Desktop/博士/code/KUZU_PROJECT/kuzu/dataset/tinysnb2/person.csv\" (HeaDER=true, deLim=',');");

    // Print query result.
    std::cout << result->toString();
}
