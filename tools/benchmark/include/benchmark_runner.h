#pragma once

#include "benchmark.h"

namespace kuzu {
namespace benchmark {

// Class stores information of a benchmark
class BenchmarkRunner {

public:
    BenchmarkRunner(std::string datasetPath, std::string serializedPath,
        std::unique_ptr<BenchmarkConfig> config);

    void registerBenchmarks(const std::string& path);

    void runAllBenchmarks();
    static double computeAverageOfLastRuns(const double* runTimes, const int& len,
        const int& lastRunsToAverage);

private:
    void registerBenchmark(const std::string& path);

    void runBenchmark(Benchmark* benchmark, main::Connection* conn) const;

    void profileQueryIfEnabled(const Benchmark* benchmark, main::Connection* conn) const;

    void reinitializeInMemDatabase();

public:
    std::unique_ptr<BenchmarkConfig> config;
    std::string datasetPath;
    std::string serializedPath;
    std::unique_ptr<main::Database> database;
    std::vector<std::unique_ptr<Benchmark>> benchmarks;
};

} // namespace benchmark
} // namespace kuzu
