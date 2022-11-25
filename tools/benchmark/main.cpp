#include "benchmark_runner.h"
#include "common/utils.h"

using namespace kuzu::benchmark;

static string getArgumentValue(const string& arg) {
    auto splits = StringUtils::split(arg, "=");
    if (splits.size() != 2) {
        throw invalid_argument("Expect value associate with " + splits[0]);
    }
    return splits[1];
}

int main(int argc, char** argv) {
    string datasetPath;
    string benchmarkPath;
    auto config = make_unique<BenchmarkConfig>();
    // parse arguments
    for (auto i = 1; i < argc; ++i) {
        string arg = argv[i];
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
        } else if (arg.starts_with("--in-memory")) {
            config->isInMemoryMode = true;
        } else if (arg.starts_with("--profile")) {
            config->enableProfile = true;
        } else if (arg.starts_with("--bm-size")) {
            config->bufferPoolSize = (uint64_t)stoull(getArgumentValue(arg)) << 20;
        } else {
            printf("Unrecognized option %s", arg.c_str());
            exit(1);
        }
    }
    if (datasetPath.empty()) {
        printf("Missing --dataset input.");
        exit(1);
    }
    if (benchmarkPath.empty()) {
        printf("Missing --benchmark input");
        exit(1);
    }
    auto runner = BenchmarkRunner(datasetPath, move(config));
    try {
        runner.registerBenchmarks(benchmarkPath);
        runner.runAllBenchmarks();
    } catch (exception& e) { printf("Error encountered during benchmarking. %s", e.what()); }
    return 0;
}
