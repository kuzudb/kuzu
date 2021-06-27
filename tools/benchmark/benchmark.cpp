#include "tools/benchmark/include/benchmark.h"

#include <filesystem>
#include <fstream>

#include "spdlog/spdlog.h"

#include "src/common/include/utils.h"

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
    if (!filesystem::exists(benchmarkPath)) {
        throw invalid_argument(benchmarkPath + " does not exist.");
    }
    string line;
    ifstream ifs(benchmarkPath);
    while (getline(ifs, line)) {
        if (line.empty() || line[0] == '#') {
            continue;
        }
        auto splits = StringUtils::split(line, " ");
        if (splits[0] == "name") { // load benchmark name
            if (splits.size() != 2) {
                throw invalid_argument("Empty benchmark name.");
            }
            name = splits[1];
        } else if (splits[0] == "query") { // load benchmark query
            if (config.enableProfile) {
                context->query += "PROFILE ";
            }
            while (getline(ifs, line)) {
                if (line.empty()) {
                    break;
                }
                context->query += line + " ";
            }
        } else if (splits[0] == "expectedNumOutput") { // load benchmark number of output tuples
            if (splits.size() != 2) {
                throw invalid_argument("Empty number of output tuples.");
            }
            expectedNumOutput = stoul(splits[1]);
        } else {
            throw invalid_argument("Unrecognized line " + line);
        }
    }
}

void Benchmark::run() {
    auto timeMetric = TimeMetric(true);
    timeMetric.start();
    system.executeQuery(*context);
    timeMetric.stop();
    executionTime = timeMetric.getElapsedTimeMS();

    verify();
}

void Benchmark::log() {
    string time = "Execution time: " + to_string(executionTime);
    string plan = "Plan: \n" + context->planPrinter->printPlanToJson(*context->profiler).dump(4);
    spdlog::info("{}", time);
    if (config.enableProfile) {
        spdlog::info("{}", plan);
    }
    if (!config.outputPath.empty()) {
        ofstream f(config.outputPath + "/" + name + ".result", ios_base::app);
        f << time << endl;
        if (config.enableProfile) {
            f << plan << endl;
        }
        f.flush();
        f.close();
    }
}

void Benchmark::verify() {
    if (context->queryResult->numTuples != expectedNumOutput) {
        spdlog::error("Query: {}", context->query);
        spdlog::error("Expected number of output equals {} but get {}", expectedNumOutput,
            context->queryResult->numTuples);
    }
}

} // namespace benchmark
} // namespace graphflow
