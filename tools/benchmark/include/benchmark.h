#pragma once

#include "tools/benchmark/include/benchmark_config.h"

#include "src/main/include/graphflowdb.h"

using namespace std;
using namespace graphflow::main;

namespace graphflow {
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
    void logQueryInfo(ofstream& log, uint32_t runNum, QueryResult& queryResult) const;
    void verify(QueryResult& queryResult) const;

public:
    BenchmarkConfig& config;
    unique_ptr<Connection> conn;
    string name;
    string query;
    vector<string> expectedOutput;
};

} // namespace benchmark
} // namespace graphflow
