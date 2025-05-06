#include "benchmark.h"

#include <fstream>

#include "benchmark_parser.h"
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
    BenchmarkParser parser;
    const auto parsedBenchmarks = parser.parseBenchmarkFile(benchmarkPath);
    KU_ASSERT(parsedBenchmarks.size() == 1);
    const auto queryConfig = parsedBenchmarks[0].get();
    preRun = queryConfig->preRun;
    query = queryConfig->query;
    postRun = queryConfig->postRun;
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

void Benchmark::writeLogFile(std::ofstream& log, uint32_t runNum, const QuerySummary& querySummary,
    const std::vector<std::string>& actualOutput) const {
    log << "Run Num: " << runNum << '\n';
    log << "Status: " << (compareResult && actualOutput != expectedOutput ? "error" : "pass")
        << '\n';
    log << "Query: " << query << '\n';
    log << "Expected output: " << '\n';
    for (auto& result : expectedOutput) {
        log << result << '\n';
    }
    log << "Actual output: " << '\n';
    for (auto& result : actualOutput) {
        log << result << '\n';
    }
    log << "Compiling time: " << querySummary.getCompilingTime() << '\n';
    log << "Execution time: " << querySummary.getExecutionTime() << "\n\n";
    log.flush();
    log.close();
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
        writeLogFile(logFile, runNum, *querySummary, actualOutput);
    }
}

void Benchmark::verify(const std::vector<std::string>& actualOutput) const {
    if (actualOutput.size() == expectedNumTuples) {
        if (compareResult && actualOutput != expectedOutput) {
            spdlog::info("Output:");
            for (auto& tuple : actualOutput) {
                spdlog::info(tuple);
            }
            for (auto i = 0u; i < actualOutput.size(); i++) {
                if (actualOutput[i] != expectedOutput[i]) {
                    spdlog::error("Result tuple at index {} did not match the expected value", i);
                    spdlog::error("Actual  :", actualOutput[i]);
                    spdlog::error("Expected:", expectedOutput[i]);
                }
            }
        }
    } else {
        spdlog::error("Expected {} tuples but found {} tuples", expectedNumTuples,
            actualOutput.size());
        if (compareResult && actualOutput != expectedOutput) {
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
}

} // namespace benchmark
} // namespace kuzu
