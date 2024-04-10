#include "benchmark_runner.h"
#include "common/string_utils.h"
#include "spdlog/spdlog.h"

using namespace kuzu::benchmark;
using namespace kuzu::common;

static std::string getArgumentValue(const std::string& arg) {
    auto splits = StringUtils::split(arg, "=");
    if (splits.size() != 2) {
        throw std::invalid_argument("Expect value associate with " + splits[0]);
    }
    return splits[1];
}

int main(int argc, char** argv) {
    std::string datasetPath;
    std::string benchmarkPath;
    auto config = std::make_unique<BenchmarkConfig>();
    // parse arguments
    for (auto i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg.starts_with("--dataset")) {
            datasetPath = getArgumentValue(arg);
        } else if (arg.starts_with("--benchmark")) {
            benchmarkPath = getArgumentValue(arg);
        } else if (arg.starts_with("--warmup")) {
            config->numWarmups = stoul(getArgumentValue(arg));
        } else if (arg.starts_with("--run")) {
            config->numRuns = stoul(getArgumentValue(arg));
        } else if (arg.starts_with("--thread")) {
            config->numThreads = stoul(getArgumentValue(arg));
        } else if (arg.starts_with("--out")) { // save benchmark result to file
            config->outputPath = getArgumentValue(arg);
        } else if (arg.starts_with("--profile")) {
            config->enableProfile = true;
        } else if (arg.starts_with("--bm-size")) {
            config->bufferPoolSize = (uint64_t)stoull(getArgumentValue(arg)) << 20;
        } else {
            printf("Unrecognized option %s", arg.c_str());
            return 1;
        }
    }
    if (datasetPath.empty()) {
        printf("Missing --dataset input.");
        return 1;
    }
    if (benchmarkPath.empty()) {
        printf("Missing --benchmark input");
        return 1;
    }
    auto runner = BenchmarkRunner(datasetPath, std::move(config));
    try {
        runner.registerBenchmarks(benchmarkPath);
    } catch (std::exception& e) {
        spdlog::error("Error encountered while registering benchmark in {}: {}.", benchmarkPath,
            e.what());
    }
    runner.runAllBenchmarks();
    return 0;
}
