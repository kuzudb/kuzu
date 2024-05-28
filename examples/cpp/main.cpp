#include <iostream>

#include "kuzu.hpp"

using namespace kuzu::common;
using namespace kuzu::main;

int main() {
    auto database = std::make_unique<Database>("/Users/guodong/Downloads/d" /* fill db path */);
    auto connection = std::make_unique<Connection>(database.get());

    auto r = connection->query("BEGIN TRANSACTION;");
    std::cout << r->toString() << std::endl;
    // Create schema.
    r = connection->query(
        "CREATE NODE TABLE Person(id INT64, name STRING, age INT64, PRIMARY KEY(id));");
    std::cout << r->toString() << std::endl;
    auto preparedStmt = connection->prepare("CREATE (:Person {id: $id, name: $name, age: $age});");
    for (auto i = 0u; i < 200000; i++) {
        std::unordered_map<std::string, std::unique_ptr<Value>> params;
        params["id"] = std::make_unique<Value>(i);
        params["name"] = std::make_unique<Value>("Alice" + std::to_string(i));
        params["age"] = std::make_unique<Value>(20 + i);
        connection->executeWithParams(preparedStmt.get(), std::move(params));
    }
    // Execute a simple query.
    auto result = connection->query("MATCH (a:Person) RETURN COUNT(*);");
    std::cout << result->toString();
    // COMMIT.
    r = connection->query("COMMIT;");
    std::cout << r->toString() << std::endl;
}
