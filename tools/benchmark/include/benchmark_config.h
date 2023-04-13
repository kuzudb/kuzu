#pragma once

#include <cstdint>
#include <string>

namespace kuzu {
namespace benchmark {

/**
 * Benchmark config should be shared for a suite of benchmarks
 */
struct BenchmarkConfig {
    bool enableProfile = false;
    // number of warm up runs
    uint32_t numWarmups = 1;
    // number of actual runs
    uint32_t numRuns = 5;
    // number of threads to execute benchmark
    uint32_t numThreads = 1;
    // output benchmark log to file
    std::string outputPath;
    uint64_t bufferPoolSize = 1 << 23;
};

} // namespace benchmark
} // namespace kuzu
