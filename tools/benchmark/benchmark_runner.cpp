#include "benchmark_runner.h"

#include <filesystem>

#include "spdlog/spdlog.h"

namespace kuzu {
namespace benchmark {

const string BENCHMARK_SUFFIX = ".benchmark";

BenchmarkRunner::BenchmarkRunner(const string& datasetPath, unique_ptr<BenchmarkConfig> config)
    : config{move(config)} {
    database = make_unique<Database>(DatabaseConfig(datasetPath, this->config->isInMemoryMode),
        SystemConfig(this->config->bufferPoolSize));
    spdlog::set_level(spdlog::level::debug);
}

void BenchmarkRunner::registerBenchmarks(const string& path) {
    if (filesystem::is_regular_file(path)) {
        registerBenchmark(path);
    } else if (filesystem::is_directory(path)) {
        for (auto& f : filesystem::directory_iterator(path)) {
            registerBenchmark(f.path());
        }
    }
}

void BenchmarkRunner::runAllBenchmarks() {
    for (auto& benchmark : benchmarks) {
        runBenchmark(benchmark.get());
    }
}

void BenchmarkRunner::registerBenchmark(const string& path) {
    if (path.ends_with(BENCHMARK_SUFFIX)) {
        auto benchmark = make_unique<Benchmark>(path, database.get(), *config);
        spdlog::info("Register benchmark {}", benchmark->name);
        benchmarks.push_back(move(benchmark));
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
    spdlog::info("Running benchmark {} with {} thread", benchmark->name, config->numThreads);
    for (auto i = 0u; i < config->numWarmups; ++i) {
        spdlog::info("Warm up");
        benchmark->run();
    }
    double runTimes[config->numRuns];
    for (auto i = 0u; i < config->numRuns; ++i) {
        auto queryResult = benchmark->run();
        benchmark->log(i + 1, *queryResult);
        runTimes[i] = queryResult->getQuerySummary()->getExecutionTime();
    }
    spdlog::info("Time Taken (Average of Last " + to_string(config->numRuns) + " runs) (ms): " +
                 to_string(computeAverageOfLastRuns(
                     runTimes, config->numRuns, config->numRuns /* numRunsToAverage */)));
}

} // namespace benchmark
} // namespace kuzu
