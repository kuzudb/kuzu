#include "benchmark.h"

#include <filesystem>

#include "json.hpp"
#include "spdlog/spdlog.h"
#include "test_helper.h"

using namespace kuzu::common;

namespace kuzu {
namespace benchmark {

Benchmark::Benchmark(const std::string& benchmarkPath, Database* database, BenchmarkConfig& config)
    : config{config} {
    conn = std::make_unique<Connection>(database);
    conn->setMaxNumThreadForExec(config.numThreads);
    loadBenchmark(benchmarkPath);
}

void Benchmark::loadBenchmark(const std::string& benchmarkPath) {
    auto queryConfigs = testing::TestHelper::parseTestFile(benchmarkPath);
    assert(queryConfigs.size() == 1);
    auto queryConfig = queryConfigs[0].get();
    query = config.enableProfile ? "PROFILE " : "";
    query += queryConfig->query;
    name = queryConfig->name;
    expectedOutput = queryConfig->expectedTuples;
    encodedJoin = queryConfig->encodedJoin;
}

std::unique_ptr<QueryResult> Benchmark::run() const {
    return conn->query(query, encodedJoin);
}

void Benchmark::logQueryInfo(
    std::ofstream& log, uint32_t runNum, std::vector<std::string>& actualOutput) const {
    log << "Run Num: " << runNum << std::endl;
    log << "Status: " << (actualOutput == expectedOutput ? "pass" : "error") << std::endl;
    log << "Query: " << query << std::endl;
    log << "Expected output: " << std::endl;
    for (auto& result : expectedOutput) {
        log << result << std::endl;
    }
    log << "Actual output: " << std::endl;
    for (auto& result : actualOutput) {
        log << result << std::endl;
    }
}

void Benchmark::log(uint32_t runNum, QueryResult& queryResult) const {
    auto querySummary = queryResult.getQuerySummary();
    auto actualOutput = testing::TestHelper::convertResultToString(queryResult);
    std::string plan = "Plan: \n" + querySummary->printPlanToJson().dump(4);
    spdlog::info("Run number: {}", runNum);
    spdlog::info("Compiling time {}", querySummary->getCompilingTime());
    spdlog::info("Execution time {}", querySummary->getExecutionTime());
    verify(actualOutput);
    spdlog::info("");
    if (config.enableProfile) {
        spdlog::info("{}", plan);
    }
    if (!config.outputPath.empty()) {
        std::ofstream logFile(config.outputPath + "/" + name + "_log.txt", std::ios_base::app);
        logQueryInfo(logFile, runNum, actualOutput);
        logFile << "Compiling time: " << querySummary->getCompilingTime() << std::endl;
        logFile << "Execution time: " << querySummary->getExecutionTime() << std::endl << std::endl;
        logFile.flush();
        logFile.close();
        if (config.enableProfile) {
            std::ofstream profileFile(
                config.outputPath + "/" + name + "_profile.txt", std::ios_base::app);
            logQueryInfo(profileFile, runNum, actualOutput);
            profileFile << plan << std::endl << std::endl;
            profileFile.flush();
            profileFile.close();
        }
    }
}

void Benchmark::verify(std::vector<std::string>& actualOutput) const {
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
