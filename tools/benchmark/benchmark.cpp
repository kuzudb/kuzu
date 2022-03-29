#include "tools/benchmark/include/benchmark.h"

#include <filesystem>
#include <fstream>

#include "spdlog/spdlog.h"
#include "test/test_utility/include/test_helper.h"

using namespace graphflow::common;

namespace graphflow {
namespace benchmark {

Benchmark::Benchmark(const string& benchmarkPath, Database* database, BenchmarkConfig& config)
    : config{config} {
    conn = make_unique<Connection>(database);
    conn->setMaxNumThreadForExec(config.numThreads);
    loadBenchmark(benchmarkPath);
}

void Benchmark::loadBenchmark(const string& benchmarkPath) {
    vector<testing::TestQueryConfig> queryConfigs =
        testing::TestHelper::parseTestFile(benchmarkPath);
    assert(queryConfigs.size() == 1);
    testing::TestQueryConfig queryConfig = queryConfigs[0];
    query = config.enableProfile ? "PROFILE " : "";
    query += queryConfig.query;
    name = queryConfig.name;
    expectedOutput = queryConfig.expectedTuples;
}

unique_ptr<QueryResult> Benchmark::run() {
    return conn->query(query);
}

void Benchmark::logQueryInfo(ofstream& log, uint32_t runNum, QueryResult& queryResult) const {
    vector<string> actualOutput = testing::TestHelper::getOutput(queryResult);
    log << "Run Num: " << runNum << endl;
    log << "Status: " << (actualOutput == expectedOutput ? "pass" : "error") << endl;
    log << "Query: " << query << endl;
    log << "Expected output: " << endl;
    for (auto& result : expectedOutput) {
        log << result << endl;
    }
    log << "Actual output: " << endl;
    for (auto& result : actualOutput) {
        log << result << endl;
    }
}

void Benchmark::log(uint32_t runNum, QueryResult& queryResult) const {
    auto querySummary = queryResult.getQuerySummary();
    string plan = "Plan: \n" + querySummary->printPlanToJson().dump(4);
    spdlog::info("Run number: {}", runNum);
    spdlog::info("Compiling time {}", querySummary->getCompilingTime());
    spdlog::info("Execution time {}", querySummary->getExecutionTime());
    verify(queryResult);
    spdlog::info("");
    if (config.enableProfile) {
        spdlog::info("{}", plan);
    }
    if (!config.outputPath.empty()) {
        ofstream logFile(config.outputPath + "/" + name + "_log.txt", ios_base::app);
        logQueryInfo(logFile, runNum, queryResult);
        logFile << "Compiling time: " << querySummary->getCompilingTime() << endl;
        logFile << "Execution time: " << querySummary->getExecutionTime() << endl << endl;
        logFile.flush();
        logFile.close();
        if (config.enableProfile) {
            ofstream profileFile(config.outputPath + "/" + name + "_profile.txt", ios_base::app);
            logQueryInfo(profileFile, runNum, queryResult);
            profileFile << plan << endl << endl;
            profileFile.flush();
            profileFile.close();
        }
    }
}

void Benchmark::verify(QueryResult& queryResult) const {
    vector<string> actualOutput = testing::TestHelper::getOutput(queryResult);
    if (actualOutput != expectedOutput) {
        spdlog::error("Query: {}", query);
        spdlog::error("Result tuples are not matched");
        spdlog::error("RESULT:");
        for (auto& tuple : actualOutput) {
            spdlog::error(tuple);
        }
        spdlog::error("EXPECTED:");
        for (auto& tuple : expectedOutput) {
            spdlog::error(tuple);
        }
    }
}

} // namespace benchmark
} // namespace graphflow
