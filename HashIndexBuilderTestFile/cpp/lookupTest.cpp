#include <iostream>
#include <filesystem>
#include <fstream>
#include <memory>
#include <thread>
#include <vector>
#include <chrono>



#include "main/kuzu.h"
#include "storage/index/hash_index_builder.h"

using namespace kuzu::main;

int main() {

    auto pkIndex = new kuzu::storage::PrimaryKeyIndexBuilder("test",
            kuzu::common::LogicalType(kuzu::common::LogicalTypeID::INT64));
    pkIndex->bulkReserve(10000000);
    
    for (int64_t i = 0; i < 10000000; i++) {
        pkIndex->append(i+1, i);
    }
    
    kuzu::common::offset_t result;
    for (int i = 1; i <= 10000000; i++) {
        
        bool r = pkIndex->lookup(i, result);
        if(!r) std::cout << "wrong" << std::endl;
    }

    // string case: random access the array of string 

    /*
    const int maxThreads = 1; // Set the maximum number of threads

    std::vector<std::thread> threads;
    for (int i = 0; i < maxThreads; ++i)
    {
        threads.emplace_back(lookupTest, pkIndex);
    }

    // Wait for all threads to finish
    for (auto& thread : threads)
    {
        thread.join();
    }
    */
    
 
}