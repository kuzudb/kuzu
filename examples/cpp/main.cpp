#include <iostream>

#include "kuzu.hpp"
using namespace kuzu::main;

int main() {
    auto database = std::make_unique<Database>("" /* fill db path */);
    auto connection = std::make_unique<Connection>(database.get());

    // Create schema.
    connection->query("load llm;");

    auto result = connection->query("return create_embedding('hello', 'amazon-bedrock', 'amazon.titan-embed-text-v1')");
    // Print query result.
    std::cout << result->toString();
} 
