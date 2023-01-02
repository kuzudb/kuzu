#pragma once

#include "benchmark_config.h"
#include "jo_connection.h"
#include "main/kuzu.h"

using namespace kuzu::main;

namespace kuzu {
namespace benchmark {

/**
 * Each benchmark represents a query to be executed against the system
 */
class Benchmark {

public:
    Benchmark(const string& benchmarkPath, Database* database, BenchmarkConfig& config);

    unique_ptr<QueryResult> run();
    void log(uint32_t runNum, QueryResult& queryResult) const;

private:
    void loadBenchmark(const string& benchmarkPath);
    void logQueryInfo(ofstream& log, uint32_t runNum, vector<string>& actualOutput) const;
    void verify(vector<string>& actualOutput) const;

public:
    BenchmarkConfig& config;
    unique_ptr<JOConnection> conn;
    string name;
    string query;
    vector<string> expectedOutput;
    string encodedJoin;
};

} // namespace benchmark
} // namespace kuzu
