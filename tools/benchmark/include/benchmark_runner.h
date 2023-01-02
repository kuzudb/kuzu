#pragma once

#include "benchmark.h"

namespace kuzu {
namespace benchmark {

// Class stores information of a benchmark
class BenchmarkRunner {

public:
    BenchmarkRunner(const string& datasetPath, unique_ptr<BenchmarkConfig> config);

    void registerBenchmarks(const string& path);

    void runAllBenchmarks();
    static double computeAverageOfLastRuns(
        const double* runTimes, const int& len, const int& lastRunsToAverage);

private:
    void registerBenchmark(const string& path);

    void runBenchmark(Benchmark* benchmark) const;

public:
    unique_ptr<BenchmarkConfig> config;
    unique_ptr<Database> database;
    vector<unique_ptr<Benchmark>> benchmarks;
};

} // namespace benchmark
} // namespace kuzu
