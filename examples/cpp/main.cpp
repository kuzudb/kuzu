#include <iostream>

#include "kuzu.hpp"
using namespace kuzu::main;

int main() {
    auto database = std::make_unique<Database>("" /* fill db path */);
    auto connection = std::make_unique<Connection>(database.get());

    connection->query("load vector;");
    auto result = connection->query("return create_embedding();");
    // Print query result.
    std::cout << result->toString();
}
