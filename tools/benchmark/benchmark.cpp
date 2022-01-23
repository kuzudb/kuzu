#include "tools/benchmark/include/benchmark.h"

#include <filesystem>
#include <fstream>

#include "spdlog/spdlog.h"
#include "test/test_utility/include/test_helper.h"

using namespace graphflow::common;

namespace graphflow {
namespace benchmark {

Benchmark::Benchmark(const string& benchmarkPath, System& system, BenchmarkConfig& config)
    : system{system}, config{config} {
    context = make_unique<SessionContext>();
    context->numThreads = config.numThreads;
    loadBenchmark(benchmarkPath);
}

void Benchmark::loadBenchmark(const string& benchmarkPath) {
    vector<testing::TestQueryConfig> queryConfigs =
        testing::TestHelper::parseTestFile(benchmarkPath);
    assert(queryConfigs.size() == 1);
    testing::TestQueryConfig queryConfig = queryConfigs[0];
    if (config.enableProfile) {
        context->query += "PROFILE ";
    }
    context->query = queryConfig.query;
    name = queryConfig.name;
    expectedOutput = queryConfig.expectedTuples;
}

void Benchmark::run() {
    system.executeQuery(*context);
}

void Benchmark::logQueryInfo(ofstream& log, uint32_t runNum) const {
    vector<string> actualOutput = testing::TestHelper::getActualOutput(*context->queryResult);
    log << "Run Num: " << runNum << endl;
    log << "Status: " << (actualOutput == expectedOutput ? "pass" : "error") << endl;
    log << "Query: " << context->query << endl;
    log << "Expected output: " << endl;
    for (auto result : expectedOutput) {
        log << result << endl;
    }
    log << "Actual output: " << endl;
    for (auto result : actualOutput) {
        log << result << endl;
    }
}

void Benchmark::log(uint32_t runNum) const {
    string plan = "Plan: \n" + context->planPrinter->printPlanToJson(*context->profiler).dump(4);
    spdlog::info("Run number: {}", runNum);
    spdlog::info("Compiling time {}", context->compilingTime);
    spdlog::info("Execution time {}", context->executingTime);
    verify();
    spdlog::info("");
    if (config.enableProfile) {
        spdlog::info("{}", plan);
    }
    if (!config.outputPath.empty()) {
        ofstream logFile(config.outputPath + "/" + name + "_log.txt", ios_base::app);
        logQueryInfo(logFile, runNum);
        logFile << "Compiling time: " << context->compilingTime << endl;
        logFile << "Execution time: " << context->executingTime << endl << endl;
        logFile.flush();
        logFile.close();
        if (config.enableProfile) {
            ofstream profileFile(config.outputPath + "/" + name + "_profile.txt", ios_base::app);
            logQueryInfo(profileFile, runNum);
            profileFile << plan << endl << endl;
            profileFile.flush();
            profileFile.close();
        }
    }
}

void Benchmark::verify() const {
    vector<string> actualOutput = testing::TestHelper::getActualOutput(*context->queryResult);
    if (actualOutput != expectedOutput) {
        spdlog::error("Query: {}", context->query);
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
