#include <iostream>

#include "kuzu.hpp"
using namespace kuzu::main;

int main() {
    auto database = std::make_unique<Database>("" /* fill db path */);
    auto connection = std::make_unique<Connection>(database.get());

    connection->query("load algo;");
    connection->query("CREATE NODE TABLE Node(id INT64 PRIMARY KEY);");
    connection->query("CREATE REL TABLE Edge(FROM Node to Node, weight INT64);");
    connection->query("CREATE (u0:Node {id: 0}), (u1:Node {id: 1}), (u2:Node {id: 2}), (u3:Node {id: 3}), (u4:Node {id: 4}), (u5:Node {id: 5}), (u6:Node {id: 6}), (u7:Node {id: 7}), (u8:Node {id: 8}), (u9:Node {id: 9}), (u0)-[:Edge {weight: 5}]->(u1), (u0)-[:Edge {weight: 6}]->(u2), (u1)-[:Edge {weight: 8}]->(u2), (u2)-[:Edge {weight: 3}]->(u3), (u3)-[:Edge {weight: 3}]->(u4), (u5)-[:Edge {weight: 10}]->(u6), (u5)-[:Edge {weight: 10}]->(u7), (u6)-[:Edge {weight: 3}]->(u7), (u7)-[:Edge {weight: 10}]->(u8), (u8)-[:Edge {weight: 3}]->(u9), (u2)-[:Edge {weight: 15}]->(u5), (u4)-[:Edge {weight: 2}]->(u9);");
    connection->query("CALL PROJECT_GRAPH('Graph', ['Node'], ['Edge']);");
    auto result = connection->query("CALL MINIMUM_SPANNING_FOREST('Graph', weight_property:='weight') return src");
    // Print query result.
    std::cout << result->toString();
}
