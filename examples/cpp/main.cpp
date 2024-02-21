#include <iostream>

#include "kuzu.hpp"

using namespace kuzu::main;
using namespace kuzu::common;

static void pathUDF(const std::vector<std::shared_ptr<ValueVector>>& parameters,
    ValueVector& result, void* /*dataPtr*/ = nullptr) {
    auto nodesVec = parameters[0].get();
    auto nodesDataVec = ListVector::getDataVector(nodesVec);
    auto nameDataVecIdx = StructType::getFieldIdx(&nodesDataVec->dataType, "name");
    auto nameDataVec = StructVector::getFieldVector(nodesDataVec, nameDataVecIdx);
    for (auto i = 0u; i < nodesVec->state->selVector->selectedSize; ++i) {
        auto selectedPos = nodesVec->state->selVector->selectedPositions[i];
        auto entry = nodesVec->getValue<list_entry_t>(selectedPos);
        auto first = nameDataVec->getValue<ku_string_t>(entry.offset).getAsString();
        auto last = nameDataVec->getValue<ku_string_t>(entry.offset + entry.size - 1).getAsString();
        StringVector::addString(&result, selectedPos, first + "," + last);
    }
}

int main() {
    auto database = std::make_unique<Database>("" /* fill db path */);
    auto connection = std::make_unique<Connection>(database.get());

    connection->query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name));");
    connection->query("CREATE (:Person {name: 'Alice', age: 25});");
    connection->query("CREATE (:Person {name: 'Bob', age: 30});");
    connection->query("CREATE (:Person {name: 'Carol', age: 45})");

    connection->query("CREATE REL TABLE Knows (FROM Person TO Person);");
    connection->query("MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:Knows]->(b)");
    connection->query("MATCH (a:Person {name: 'Bob'}), (b:Person {name: 'Carol'}) CREATE (a)-[:Knows]->(b)");

    // Execute a simple query.
    connection->createVectorizedFunction<ku_string_t, ku_list_t>("test", &pathUDF);
    auto result = connection->query("MATCH p = (a:Person)-[:Knows*]->(b:Person) "
                                    "RETURN properties(nodes(p), 'name') AS names, "
                                    "test(nodes(p)) AS test");
    // Print query result.
    std::cout << result->toString();
    // names|test
    // [Bob,Carol]|Bob,Carol
    // [Alice,Bob]|Alice,Bob
    // [Alice,Bob,Carol]|Alice,Carol
}
