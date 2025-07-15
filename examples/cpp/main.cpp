#include <iostream>

#include "kuzu.hpp"
using namespace kuzu::main;

int main() {
    auto database = std::make_unique<Database>("/Users/z473chen/Desktop/code/kuzu/dasxaddd");
    auto connection = std::make_unique<Connection>(database.get());
    connection->query(
        "MATCH (b:`0_emailidx_appears_info`) RETURN b.term, b.docID, CAST(count(*) as UINT64);");
}
