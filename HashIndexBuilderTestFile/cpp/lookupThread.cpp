#include <iostream>
#include <thread>
#include <vector>
#include <filesystem>
#include <fstream>
#include <memory>
#include <chrono>


#include "main/kuzu.h"
#include "storage/index/hash_index_builder.h"

using namespace kuzu::main;

constexpr int kNumIterations = 10000000;

void lookupTest(int s, int e, kuzu::storage::PrimaryKeyIndexBuilder* pkIndex) {
    kuzu::common::offset_t result;
    for (int i = s; i <= e; i++)
        pkIndex->lookup(i, result);
}

int main(int argc, char* argv[])
{
    int kNumThreads = std::atoi(argv[1]);

    std::vector<kuzu::storage::PrimaryKeyIndexBuilder*> builders;

    for (int i = 0; i < kNumThreads; i++) {
        builders.push_back(new kuzu::storage::PrimaryKeyIndexBuilder("test",
            kuzu::common::LogicalType(kuzu::common::LogicalTypeID::INT64)));
        builders[i]->bulkReserve(kNumIterations);
        for (int64_t j = 0; j < kNumIterations; j++) {
            builders[i]->append(j+1, j);
        }
    }

    std::vector<std::thread> threads;
    const int iterationsPerThread = kNumIterations / kNumThreads;
    int start = 1;

    auto timerS = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < kNumThreads - 1; ++i)
    {
        int end = start + iterationsPerThread;
        threads.emplace_back(lookupTest, start, end, builders[i]);
        start = end;
    }

    // The last thread may need to handle the remaining iterations
    threads.emplace_back(lookupTest, start, kNumIterations, builders[kNumThreads - 1]);

    // Wait for all threads to finish
    for (auto& thread : threads)
    {
        thread.join();
    }
    auto timerE = std::chrono::high_resolution_clock::now();

    // Calculate the duration
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(timerE - timerS);

    std::cout << duration.count() << std::endl;

    return 0;
}
