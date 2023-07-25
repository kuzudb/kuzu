#include <iostream>
#include <filesystem>
#include <fstream>


#include "main/kuzu.h"
using namespace kuzu::main;

int main() {

    std::ofstream output("string_benchmark_result_mix");

    // config maxthread=1, is the seg fault still there?
    // try directly append, don't go through copy

    int lengths[] = {10, 12, 14, 16, 20, 24, 30};
    //int lengths[] = {30};
    //int percents[] = {0, 20, 40, 60, 80, 100};
    output << "size,trial 1,trial 2,trial 3,trial 4,trial 5,trial 6,trial 7,trial 8,trial 9,trial 10,avg\n";
    for (int length: lengths) {
        output << length << ",";
        std::cout << length << std::endl;
        for (int i = 0; i < 10; i++) {
            std::string dbPath = "testdb/" + std::to_string(length);
            auto database = std::make_unique<Database>(dbPath);
            auto connection = std::make_unique<Connection>(database.get());

            connection->query("create node table person (ID STRING, PRIMARY KEY (ID));");
            auto result = connection->query("COPY person FROM \"csv/vPersonStringLength" + std::to_string(length) + ".csv\"");
            output << result->getQuerySummary()->getExecutionTime() << ",";
            std::filesystem::remove_all(dbPath);
        }
        output << "\n";
    }
    output.close();
}
