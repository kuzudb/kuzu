#include <iostream>

#include "kuzu.hpp"
using namespace kuzu::main;

int main() {
    auto database = std::make_unique<Database>("dasda" /* fill db path */);
    auto connection = std::make_unique<Connection>(database.get());
    printf("%s", connection->query("call query_fts_index('Person','personidx', 'age') return *;")
                     ->toString()
                     .c_str());
    printf("%s", connection->query("call query_fts_index('Person','personidx', 'age') return *;")
                     ->toString()
                     .c_str());
}
