#include "benchmark.h"

#include <fstream>

#include "spdlog/spdlog.h"
#include "test_helper.h"

using namespace kuzu::common;
using namespace kuzu::main;

namespace kuzu {
namespace benchmark {

Benchmark::Benchmark(const std::string& benchmarkPath, Database* database, BenchmarkConfig& config)
    : config{config}, compareResult{true}, expectedNumTuples{0} {
    conn = std::make_unique<Connection>(database);
    conn->setMaxNumThreadForExec(config.numThreads);
    loadBenchmark(benchmarkPath);
}

void Benchmark::loadBenchmark(const std::string& benchmarkPath) {
    const auto queryConfigs = testing::TestHelper::parseTestFile(benchmarkPath);
    KU_ASSERT(queryConfigs.size() == 1);
    const auto queryConfig = queryConfigs[0].get();
    query = queryConfig->query;
    name = queryConfig->name;
    expectedOutput = queryConfig->expectedTuples;
    compareResult = queryConfig->compareResult;
    expectedNumTuples = queryConfig->expectedNumTuples;
}

std::unique_ptr<QueryResult> Benchmark::run() const {
    return conn->query(query);
}

std::unique_ptr<QueryResult> Benchmark::runWithProfile() const {
    return conn->query("PROFILE " + query);
}

void Benchmark::logQueryInfo(std::ofstream& log, uint32_t runNum,
    const std::vector<std::string>& actualOutput) const {
    log << "Run Num: " << runNum << '\n';
    log << "Status: " << (actualOutput == expectedOutput ? "pass" : "error") << '\n';
    log << "Query: " << query << '\n';
    log << "Expected output: " << '\n';
    for (auto& result : expectedOutput) {
        log << result << '\n';
    }
    log << "Actual output: " << '\n';
    for (auto& result : actualOutput) {
        log << result << '\n';
    }
    log << std::flush;
}

void Benchmark::log(uint32_t runNum, QueryResult& queryResult) const {
    auto querySummary = queryResult.getQuerySummary();
    auto actualOutput = testing::TestHelper::convertResultToString(queryResult);
    spdlog::info("Run number: {}", // NOLINT(clang-analyzer-optin.cplusplus.UninitializedObject):
                                   // spdlog has an unitialized object.
        runNum);
    spdlog::info("Compiling time {}", querySummary->getCompilingTime());
    spdlog::info("Execution time {}", querySummary->getExecutionTime());
    verify(actualOutput);
    spdlog::info("");
    if (!config.outputPath.empty()) {
        std::ofstream logFile(config.outputPath + "/" + name + "_log.txt", std::ios_base::app);
        logQueryInfo(logFile, runNum, actualOutput);
        logFile << "Compiling time: " << querySummary->getCompilingTime() << '\n';
        logFile << "Execution time: " << querySummary->getExecutionTime() << "\n\n";
        logFile.flush();
        logFile.close();
    }
}

void Benchmark::verify(const std::vector<std::string>& actualOutput) const {
    bool matched = expectedNumTuples == actualOutput.size();
    if (matched && compareResult) {
        matched = actualOutput == expectedOutput;
    }
    if (!matched) {
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
