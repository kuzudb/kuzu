#pragma once

#include "tools/benchmark/include/benchmark.h"

namespace graphflow {
namespace benchmark {

// Class stores information of a benchmark
class BenchmarkRunner {

public:
    BenchmarkRunner(const string& datasetPath, unique_ptr<BenchmarkConfig> config);

    void registerBenchmarks(const string& path);

    void runAllBenchmarks();

private:
    void registerBenchmark(const string& path);

    void runBenchmark(Benchmark* benchmark);

public:
    unique_ptr<BenchmarkConfig> config;
    unique_ptr<System> system;
    vector<unique_ptr<Benchmark>> benchmarks;
};

} // namespace benchmark
} // namespace graphflow
