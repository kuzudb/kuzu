#include <iostream>

#include "kuzu.hpp"
using namespace kuzu::main;

int main() {
    auto database = std::make_unique<Database>("" /* fill db path */);
    auto connection = std::make_unique<Connection>(database.get());

    connection->query("create node table Node(id int128 primary key);");
    connection->query("create rel table Edge(FROM Node to Node, weight int64);");
    connection->query("load algo;");
    connection->query("call project_graph('Graph', ['Node'], ['Edge']);");
    auto result = connection->query("CALL MINIMUM_SPANNING_FOREST('Graph', weight_property:='weight') return *;");
    // Print query result.
    std::cout << result->toString();
}
