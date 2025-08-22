#include <iostream>

#include "kuzu.hpp"
using namespace kuzu::main;

int main() {
    auto database = std::make_unique<Database>("" /* fill db path */);
    auto connection = std::make_unique<Connection>(database.get());

    // Create schema.
    connection->query("load algo;");

    connection->query("CREATE NODE TABLE Node(id INT64 PRIMARY KEY);");
    connection->query("CREATE REL TABLE Edge(FROM Node to Node, weight INT64);");
    connection->query("CREATE (u0:Node {id: 0}), (u1:Node {id: 1}), (u2:Node {id: 2}), (u3:Node {id: 3}), (u4:Node {id: 4}), (u5:Node {id: 5}), (u6:Node {id: 6}), (u7:Node {id: 7}), (u8:Node {id: 8}), (u9:Node {id: 9}), (u0)-[:Edge {weight: 1}]->(u1), (u1)-[:Edge {weight: 2}]->(u2), (u2)-[:Edge {weight: 3}]->(u3), (u3)-[:Edge {weight: 4}]->(u4), (u4)-[:Edge {weight: 5}]->(u5), (u5)-[:Edge {weight: 6}]->(u6), (u7)-[:Edge {weight: 10}]->(u8);");
    connection->query("CALL PROJECT_GRAPH('Graph', ['Node'], ['Edge']);");
    auto result = connection->query("CALL SPANNING_FOREST('Graph', weight_property:='weight') RETURN src.id AS src, dst.id AS dst, rel.weight as weight, forest_id ORDER BY forest_id;");
    std::cout << result->toString();
}
