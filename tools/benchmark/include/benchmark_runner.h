#pragma once

#include "benchmark.h"

namespace kuzu {
namespace benchmark {

// Class stores information of a benchmark
class BenchmarkRunner {

public:
    BenchmarkRunner(const std::string& datasetPath, std::unique_ptr<BenchmarkConfig> config);

    void registerBenchmarks(const std::string& path);

    void runAllBenchmarks();
    static double computeAverageOfLastRuns(const double* runTimes, const int& len,
        const int& lastRunsToAverage);

private:
    void registerBenchmark(const std::string& path);

    void runBenchmark(Benchmark* benchmark) const;

    void profileQueryIfEnabled(Benchmark* benchmark) const;

public:
    std::unique_ptr<BenchmarkConfig> config;
    std::unique_ptr<main::Database> database;
    std::vector<std::unique_ptr<Benchmark>> benchmarks;
};

} // namespace benchmark
} // namespace kuzu
