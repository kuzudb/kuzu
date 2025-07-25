#include <iostream>

#include "kuzu.hpp"
using namespace kuzu::main;

int main() {
    auto database = std::make_unique<Database>("" /* fill db path */);
    auto connection = std::make_unique<Connection>(database.get());
    auto result = connection->query("return -170141183460469231731687303715884105728;");
    std::cout << result->toString() << std::endl;
}
