#include <iostream>

#include "kuzu.hpp"
using namespace kuzu::main;

int main() {
    auto database = std::make_unique<Database>("/Users/xiyangfeng/Desktop/ku_play/rdfdb" /* fill db path */);
    auto connection = std::make_unique<Connection>(database.get());

    // Create schema.
    connection->query("CREATE RDFGraph T;");
    // Create nodes.
//    connection->query("CREATE (:T_l {val: \"Long string that doesn't fit in ku_string_t\"});");

    // Execute a simple query.
    auto result = connection->query("MATCH (a:T_l) RETURN a.val;");
    // Print query result.
    std::cout << result->toString();
}
