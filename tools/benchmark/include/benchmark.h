#pragma once

#include "tools/benchmark/include/benchmark_config.h"

#include "src/main/include/system.h"

using namespace std;
using namespace graphflow::main;

namespace graphflow {
namespace benchmark {

/**
 * Each benchmark represents a query to be executed against the system
 */
class Benchmark {

public:
    Benchmark(const string& benchmarkPath, System& system, BenchmarkConfig& config);

    void run();
    void log();

private:
    void loadBenchmark(const string& benchmarkPath);
    void verify();

public:
    System& system;
    BenchmarkConfig& config;

    string name;
    uint64_t expectedNumOutput;

    unique_ptr<SessionContext> context;
};

} // namespace benchmark
} // namespace graphflow
