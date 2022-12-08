#include "benchmark.h"

#include <filesystem>

#include "spdlog/spdlog.h"
#include "test_helper.h"

using namespace kuzu::common;

namespace kuzu {
namespace benchmark {

Benchmark::Benchmark(const string& benchmarkPath, Database* database, BenchmarkConfig& config)
    : config{config} {
    conn = make_unique<JOConnection>(database);
    conn->setMaxNumThreadForExec(config.numThreads);
    loadBenchmark(benchmarkPath);
}

void Benchmark::loadBenchmark(const string& benchmarkPath) {
    auto queryConfigs = testing::TestHelper::parseTestFile(benchmarkPath);
    assert(queryConfigs.size() == 1);
    auto queryConfig = queryConfigs[0].get();
    query = config.enableProfile ? "PROFILE " : "";
    query += queryConfig->query;
    name = queryConfig->name;
    expectedOutput = queryConfig->expectedTuples;
    encodedJoin = queryConfig->encodedJoin;
}

unique_ptr<QueryResult> Benchmark::run() {
    return conn->query(query, encodedJoin);
}

void Benchmark::logQueryInfo(ofstream& log, uint32_t runNum, vector<string>& actualOutput) const {
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
    auto actualOutput = testing::TestHelper::convertResultToString(queryResult);
    string plan = "Plan: \n" + querySummary->printPlanToJson().dump(4);
    spdlog::info("Run number: {}", runNum);
    spdlog::info("Compiling time {}", querySummary->getCompilingTime());
    spdlog::info("Execution time {}", querySummary->getExecutionTime());
    verify(actualOutput);
    spdlog::info("");
    if (config.enableProfile) {
        spdlog::info("{}", plan);
    }
    if (!config.outputPath.empty()) {
        ofstream logFile(config.outputPath + "/" + name + "_log.txt", ios_base::app);
        logQueryInfo(logFile, runNum, actualOutput);
        logFile << "Compiling time: " << querySummary->getCompilingTime() << endl;
        logFile << "Execution time: " << querySummary->getExecutionTime() << endl << endl;
        logFile.flush();
        logFile.close();
        if (config.enableProfile) {
            ofstream profileFile(config.outputPath + "/" + name + "_profile.txt", ios_base::app);
            logQueryInfo(profileFile, runNum, actualOutput);
            profileFile << plan << endl << endl;
            profileFile.flush();
            profileFile.close();
        }
    }
}

void Benchmark::verify(vector<string>& actualOutput) const {
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
} // namespace kuzu
