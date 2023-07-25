#include <iostream>
#include <filesystem>
#include <fstream>
#include <memory>
#include <thread>
#include <vector>
#include <chrono>
#include <fstream>



#include "main/kuzu.h"
#include "storage/index/hash_index_builder.h"

using namespace kuzu::main;

int main() {

    std::vector<std::string> strs;
    std::string line;
    std::ifstream myfile;
    myfile.open("csv/vPersonStringLengthPartialTest" + std::to_string(30) + ".csv");

    while(getline(myfile, line)) {
        strs.push_back(line);
    }

    auto pkIndex = new kuzu::storage::PrimaryKeyIndexBuilder("test",
            kuzu::common::LogicalType(kuzu::common::LogicalTypeID::STRING));
    pkIndex->bulkReserve(10000000);

    for (int64_t i = 0; i < 10000000; i++) {
        pkIndex->append(strs[i].c_str(), i);
    }

    kuzu::common::offset_t result;

    for (int i = 0; i < 1; i++) {
        auto timerS = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < 10000000; i++) {
            pkIndex->lookup(reinterpret_cast<int64_t>(strs[i].c_str()), result);
        }
        auto timerE = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(timerE - timerS);

        std::cout << duration.count() << std::endl;
    }
 
}