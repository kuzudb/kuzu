#include <iostream>

#include "kuzu.hpp"
using namespace kuzu::main;

int main() {
    auto database = std::make_unique<Database>("" /* fill db path */);
    auto connection = std::make_unique<Connection>(database.get());

    // Create schema.
    auto result = connection->query("load llm;");
    std::cout << result->toString();
    result = connection->query("return create_embedding('hello world','amazon-bedrock','amazon.titan-embed-text-v1')");
    std::cout << result->toString();

}
