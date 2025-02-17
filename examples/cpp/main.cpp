#include <iostream>

#include "kuzu.hpp"
using namespace kuzu::main;

int main() {
    auto database = std::make_unique<Database>("/Users/guodong/Downloads/debug" /* fill db path */);
    auto connection = std::make_unique<Connection>(database.get());

    // Create schema.
    // connection->query("create node table tbl(id int64 primary key, vector float[96]);");
    // Create nodes.
    // connection->query("copy tbl from '/Users/guodong/Downloads/deep-10M/0.parquet';");

    // Execute a simple query.
    auto result = connection->query("call create_hnsw_index('tbl_hnsw', 'tbl', 'vector');");
    // Print query result.
    std::cout << result->toString() << std::endl;
    std::cout << "Execution time: " << result->getQuerySummary()->getExecutionTime() << std::endl;
}
