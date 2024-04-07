#include <filesystem>
#include <iostream>
#include <fstream>

#include "main/kuzu.h"
using namespace kuzu::main;

int main() {
    try {
        std::cout << "Hello, World!" << std::endl;
        std::cout << "Printed from C++." << std::endl;
        std::cout << "Listing files in demo-db:" << std::endl;

        for (const auto& entry : std::filesystem::directory_iterator("./demo-db")) {
            std::cout << entry.path() << std::endl;
        }
        std::cout << std::endl;

        std::cout << "Reading and printing the content of user.csv:" << std::endl;
        // Read and print the content of user.csv.
        std::ifstream file("./demo-db/csv/user.csv");
        std::string line;
        while (std::getline(file, line)) {
            std::cout << line << std::endl;
        }
        std::cout << std::endl;

        auto systemConfig = SystemConfig();
        systemConfig.bufferPoolSize = 1024 * 1024 * 50; // 50 MB
        systemConfig.maxNumThreads = 4;
        auto database = std::make_unique<Database>("tmp", systemConfig);
        std::cout << "Database created." << std::endl;
        auto connection = std::make_unique<Connection>(database.get());
        std::cout << "Connection created." << std::endl;

        // Run a CALL
        auto callResult = connection->query("CALL db_version() RETURN *;");
        std::cout << callResult->toString();

        // Create schema.
        connection->query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name));");
        std::cout << "Schema created." << std::endl;
        // Create nodes.
        // connection->query("CREATE (:Person {name: 'Alice', age: 25});");
        // connection->query("CREATE (:Person {name: 'Bob', age: 30});");
        // std::cout << "Nodes created." << std::endl;

        // Copy nodes from CSV.
        auto res = connection->query("COPY Person FROM \"./demo-db/csv/user.csv\";");
        std::cout << "Nodes copied:" << res->getErrorMessage() << std::endl;

        // Execute a simple query.
        auto result = connection->query("MATCH (a:Person) RETURN a.name AS NAME, a.age AS AGE;");
        std::cout << "Query executed:" << result->getErrorMessage() << std::endl;
        // Print query result.
        std::cout << result->toString();
    } catch (const std::exception& e) {
        std::cout << e.what() << std::endl;
    }
}
