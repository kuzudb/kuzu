#include "benchmark_runner.h"

#include <filesystem>
#include <fstream>

#include "common/exception/runtime.h"
#include "spdlog/spdlog.h"

using namespace kuzu::main;

namespace kuzu {
namespace benchmark {

const char* BENCHMARK_SUFFIX = ".benchmark";

BenchmarkRunner::BenchmarkRunner(std::string datasetPath, std::string serializedPath,
    std::unique_ptr<BenchmarkConfig> config)
    : config{std::move(config)}, datasetPath{std::move(datasetPath)},
      serializedPath{std::move(serializedPath)} {
    database = std::make_unique<Database>(this->serializedPath,
        SystemConfig(this->config->bufferPoolSize, this->config->numThreads));
    spdlog::set_level(spdlog::level::debug);
}

void BenchmarkRunner::registerBenchmarks(const std::string& path) {
    if (std::filesystem::is_regular_file(path)) {
        registerBenchmark(path);
    } else if (std::filesystem::is_directory(path)) {
        for (auto& f : std::filesystem::directory_iterator(path)) {
            registerBenchmark(f.path().string());
        }
    }
}

void BenchmarkRunner::runAllBenchmarks() {
    for (auto& benchmark : benchmarks) {
        try {
            reinitializeInMemDatabase();
            auto connection = std::make_unique<Connection>(database.get());
            runBenchmark( // NOLINT(clang-analyzer-optin.cplusplus.UninitializedObject): spdlog has
                          // an unitialized object.
                benchmark.get(), connection.get());
        } catch (std::exception& e) {
            spdlog::error("Error encountered while running benchmark {}: {}.", benchmark->name,
                e.what());
        }
    }
}

void BenchmarkRunner::reinitializeInMemDatabase() {
    if (this->serializedPath != ":memory:") {
        return;
    }
    database = std::make_unique<Database>(this->serializedPath,
        SystemConfig(this->config->bufferPoolSize, this->config->numThreads));
    std::string importQuery = common::stringFormat("IMPORT DATABASE '{}'", datasetPath);
    spdlog::info("Run benchmarks in an in memory database. Start loading dataset {}.", importQuery);
    auto conn = std::make_unique<Connection>(database.get());
    auto res = conn->query(importQuery);
    if (res->isSuccess()) {
        spdlog::info("Done loading dataset.");
    } else {
        throw common::RuntimeException(
            common::stringFormat("Failed to load dataset {}.", importQuery));
    }
}

void BenchmarkRunner::registerBenchmark(const std::string& path) {
    if (path.ends_with(BENCHMARK_SUFFIX)) {
        auto benchmark = std::make_unique<Benchmark>(path, *config);
        spdlog::info("Register benchmark {}", benchmark->name);
        benchmarks.push_back(std::move(benchmark));
    }
}

double BenchmarkRunner::computeAverageOfLastRuns(const double* runTimes, const int& len,
    const int& lastRunsToAverage) {
    double sum = 0;
    for (int i = len - lastRunsToAverage; i < len; ++i) {
        sum += runTimes[i];
    }
    return sum / lastRunsToAverage;
}

void BenchmarkRunner::runBenchmark(Benchmark* benchmark, Connection* conn) const {
    spdlog::info(
        "Running benchmark {} with {} thread", // NOLINT(clang-analyzer-optin.cplusplus.UninitializedObject):
                                               // spdlog has an unitialized object.
        benchmark->name, config->numThreads);
    if (!benchmark->preRun.empty()) {
        spdlog::info("Prerun. {}", benchmark->preRun);
        conn->query(benchmark->preRun);
    }
    for (auto i = 0u; i < config->numWarmups; ++i) {
        spdlog::info("Warm up");
        benchmark->run(conn);
    }
    profileQueryIfEnabled(benchmark, conn);
    std::vector<double> runTimes(config->numRuns);
    for (auto i = 0u; i < config->numRuns; ++i) {
        auto queryResult = benchmark->run(conn);
        benchmark->log(i + 1, *queryResult);
        runTimes[i] = queryResult->getQuerySummary()->getExecutionTime();
    }
    spdlog::info("Time Taken (Average of Last {} runs) (ms): {}", config->numRuns,
        computeAverageOfLastRuns(&runTimes[0], config->numRuns,
            config->numRuns /* numRunsToAverage */));
    if (!benchmark->postRun.empty()) {
        spdlog::info("PostRun. {}", benchmark->postRun);
        conn->query(benchmark->postRun);
    }
}

void BenchmarkRunner::profileQueryIfEnabled(const Benchmark* benchmark, Connection* conn) const {
    if (config->enableProfile && !config->outputPath.empty()) {
        auto profileInfo = benchmark->runWithProfile(conn);
        std::ofstream profileFile(config->outputPath + "/" + benchmark->name + "_profile.txt",
            std::ios_base::app);
        profileFile << profileInfo->getNext()->toString() << '\n';
        profileFile.flush();
        profileFile.close();
    }
}

} // namespace benchmark
} // namespace kuzu
