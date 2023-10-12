#include "benchmark_runner.h"

#include <filesystem>
#include <fstream>

#include "spdlog/spdlog.h"

using namespace kuzu::main;

namespace kuzu {
namespace benchmark {

const std::string BENCHMARK_SUFFIX = ".benchmark";

BenchmarkRunner::BenchmarkRunner(
    const std::string& datasetPath, std::unique_ptr<BenchmarkConfig> config)
    : config{std::move(config)} {
    database = std::make_unique<Database>(datasetPath, SystemConfig(this->config->bufferPoolSize));
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
            runBenchmark( // NOLINT(clang-analyzer-optin.cplusplus.UninitializedObject): spdlog has
                          // an unitialized object.
                benchmark.get());
        } catch (std::exception& e) {
            spdlog::error(
                "Error encountered while running benchmark {}: {}.", benchmark->name, e.what());
        }
    }
}

void BenchmarkRunner::registerBenchmark(const std::string& path) {
    if (path.ends_with(BENCHMARK_SUFFIX)) {
        auto benchmark = std::make_unique<Benchmark>(path, database.get(), *config);
        spdlog::info("Register benchmark {}", benchmark->name);
        benchmarks.push_back(std::move(benchmark));
    }
}

double BenchmarkRunner::computeAverageOfLastRuns(
    const double* runTimes, const int& len, const int& lastRunsToAverage) {
    double sum = 0;
    for (int i = len - lastRunsToAverage; i < len; ++i) {
        sum += runTimes[i];
    }
    return sum / lastRunsToAverage;
}

void BenchmarkRunner::runBenchmark(Benchmark* benchmark) const {
    spdlog::info("Running benchmark {} with {} thread", // NOLINT(clang-analyzer-optin.cplusplus.UninitializedObject):
                                                        // spdlog has an unitialized object.
        benchmark->name, config->numThreads);
    for (auto i = 0u; i < config->numWarmups; ++i) {
        spdlog::info("Warm up");
        benchmark->run();
    }
    profileQueryIfEnabled(benchmark);
    std::vector<double> runTimes(config->numRuns);
    for (auto i = 0u; i < config->numRuns; ++i) {
        auto queryResult = benchmark->run();
        benchmark->log(i + 1, *queryResult);
        runTimes[i] = queryResult->getQuerySummary()->getExecutionTime();
    }
    spdlog::info("Time Taken (Average of Last {} runs) (ms): {}", config->numRuns,
        computeAverageOfLastRuns(
            &runTimes[0], config->numRuns, config->numRuns /* numRunsToAverage */));
}

void BenchmarkRunner::profileQueryIfEnabled(Benchmark* benchmark) const {
    if (config->enableProfile && !config->outputPath.empty()) {
        auto profileInfo = benchmark->runWithProfile();
        std::ofstream profileFile(
            config->outputPath + "/" + benchmark->name + "_profile.txt", std::ios_base::app);
        profileFile << profileInfo->getNext()->toString() << std::endl;
        profileFile.flush();
        profileFile.close();
    }
}

} // namespace benchmark
} // namespace kuzu
