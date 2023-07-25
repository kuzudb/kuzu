#include <iostream>
#include <filesystem>
#include <fstream>


#include "main/kuzu.h"
using namespace kuzu::main;

int main() {
    int length = 30;
    std::cout << length << ": ";
    std::string dbPath = "testdb/" + std::to_string(length);
    SystemConfig config = SystemConfig();
    config.maxNumThreads = 1;
    auto database = std::make_unique<Database>(dbPath, config);
    auto connection = std::make_unique<Connection>(database.get());

    connection->query("create node table person (ID STRING, PRIMARY KEY (ID));");
    auto result = connection->query("COPY person FROM \"csv/vPersonStringLength" + std::to_string(length) + ".csv\"");
    std::cout << result->getQuerySummary()->getExecutionTime() << std::endl;
    std::filesystem::remove_all(dbPath);

}
