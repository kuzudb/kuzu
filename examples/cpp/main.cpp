#include <fstream>
#include <iostream>

#include "kuzu.hpp"
using namespace kuzu::main;

int main() {
    SystemConfig systemConfig;
    // systemConfig.maxNumThreads = 1;
    auto database = std::make_unique<Database>(
        "/Users/guodong/Downloads/debug060" /* fill db path */, systemConfig);
    auto connection = std::make_unique<Connection>(database.get());
    auto r = connection->query("match (c:Concept) where lower(c.name)='alcohol' return c;");
    std::cout << r->toString() << std::endl;
    r = connection->query("CHECKPOINT;");
    std::cout << r->toString() << std::endl;
    r = connection->query("match (c:Concept)-[e:G]->(m:Concept) where lower(c.name)='alcohol' hint "
                          "m join (e JOIN c) return m.name order by m.name;");
    std::cout << r->toString() << std::endl;

    // // Execute a simple query.
    // std::vector<std::vector<int64_t>> rows;
    // std::ifstream stream("/Users/guodong/Downloads/soma/relation_g.csv");
    // std::string line;
    // std::string field;
    // std::getline(stream, line); // skip header
    // while (std::getline(stream, line)) {
    //     std::istringstream s(line);
    //     std::vector<int64_t> row;
    //     row.reserve(2);
    //     while (getline(s, field, ',')) {
    //         row.push_back(std::stoi(field));
    //     }
    //     rows.push_back(row);
    // }
    // std::cout << "Read " << rows.size() << " rows" << std::endl;
    // auto numRows = 0u;
    // for (auto& row : rows) {
    //     if (numRows % 1000 == 0) {
    //         std::cout << "Processed " << numRows << " rows" << std::endl;
    //     }
    //     numRows++;
    //     auto q = "match (a:Concept), (b:Concept) where a.id=" + std::to_string(row[0]) +
    //              " and b.id=" + std::to_string(row[1]) + " merge (a)-[e:G]->(b)";
    //     auto result = connection->query(q);
    //     if (!result->isSuccess()) {
    //         std::cout << "Query " << q << " failed: " << result->getErrorMessage() << std::endl;
    //     }
    // }
}
