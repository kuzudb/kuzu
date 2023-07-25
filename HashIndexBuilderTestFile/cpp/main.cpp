#include <iostream>
#include <filesystem>
#include <fstream>


#include "main/kuzu.h"
using namespace kuzu::main;

int main() {

    std::ofstream output("benchmark_result");

    int sizes[] = {5000, 10000, 20000, 50000, 100000, 150000, 200000, 300000, 500000, 700000, 
                    1000000, 1500000, 2000000, 3000000, 4000000, 5000000, 6000000, 
                    7000000, 8000000, 9000000, 10000000};

    output << "size,trial 1,trial 2,trial 3,trial 4,trial 5,trial 6,trial 7,trial 8,trial 9,trial 10,avg\n";
    for (int size: sizes) {
        output << size << ",";
        for (int i = 0; i < 10; i++) {
            std::string dbPath = "testdb/" + std::to_string(size);
            auto database = std::make_unique<Database>(dbPath);
            auto connection = std::make_unique<Connection>(database.get());

            connection->query("create node table person (ID INT64, PRIMARY KEY (ID));");
            auto result = connection->query("COPY person FROM \"csv/vPerson" + std::to_string(size) + ".csv\"");
            output << result->getQuerySummary()->getExecutionTime() << ",";
            std::filesystem::remove_all(dbPath);
        }
        output << "\n";
    }
    
    output << "\n\n\n";
/*
    for (int size: sizes) {
        output << size << ",";
        for (int i = 0; i < 3; i++) {
            std::string dbPath = "testdb/" + std::to_string(size);
            auto database = std::make_unique<Database>(dbPath);
            auto connection = std::make_unique<Connection>(database.get());

            connection->query("create node table person (ID INT64, PRIMARY KEY (ID));");
            auto result = connection->query("COPY person FROM \"csv/vPersonRandom" + std::to_string(size) + ".csv\"");
            output << result->getQuerySummary()->getExecutionTime() << ",";
            std::filesystem::remove_all(dbPath);
        }
        output << "\n";
    }
*/
}
