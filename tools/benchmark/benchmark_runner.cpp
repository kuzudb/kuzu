#include "tools/benchmark/include/benchmark_runner.h"

#include <filesystem>

#include "spdlog/spdlog.h"

namespace graphflow {
namespace benchmark {

const string BENCHMARK_SUFFIX = ".benchmark";

BenchmarkRunner::BenchmarkRunner(const string& datasetPath, unique_ptr<BenchmarkConfig> config)
    : config{move(config)} {
    system = make_unique<System>(datasetPath);
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
        auto benchmark = make_unique<Benchmark>(path, *system, *config);
        spdlog::info("Register benchmark {}", benchmark->name);
        benchmarks.push_back(move(benchmark));
    }
}

double BenchmarkRunner::computeAverageOfLastRuns(
    double* runTimes, const int& len, const int& lastRunsToAverage) {
    double sum = 0;
    for (int i = len - lastRunsToAverage; i < len; ++i) {
        sum += runTimes[i];
    }
    return sum / lastRunsToAverage;
}

void BenchmarkRunner::runBenchmark(Benchmark* benchmark) {
    spdlog::info("Running benchmark {} with {} thread", benchmark->name, config->numThreads);
    for (auto i = 0u; i < config->numWarmups; ++i) {
        spdlog::info("Warm up");
        benchmark->run();
    }
    double runTimes[config->numRuns];
    for (auto i = 0u; i < config->numRuns; ++i) {
        benchmark->run();
        benchmark->log();
        runTimes[i] = benchmark->context->executingTime;
    }
    spdlog::info("Time Taken (Average of Last 3 runs) (ms): " +
                 to_string(computeAverageOfLastRuns(
                     runTimes, config->numRuns, (int)3 /* numRunsToAverage */)));
}

} // namespace benchmark
} // namespace graphflow
