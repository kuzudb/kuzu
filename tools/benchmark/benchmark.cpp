#include "tools/benchmark/include/benchmark.h"

#include <filesystem>
#include <fstream>

#include "spdlog/spdlog.h"

#include "src/common/include/utils.h"
#include "src/processor/include/physical_plan/operator/result/result_set_iterator.h"

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
    system.executeQuery(*context);
    verify();
}

void Benchmark::log() const {
    auto numOutput = context->queryResult->numTuples;
    string plan = "Plan: \n" + context->planPrinter->printPlanToJson(*context->profiler).dump(4);
    spdlog::info("Number of output {}", numOutput);
    spdlog::info("Compiling time {}", context->compilingTime);
    spdlog::info("Execution time {}", context->executingTime);
    if (config.enableProfile) {
        spdlog::info("{}", plan);
    }
    if (!config.outputPath.empty()) {
        ofstream f(config.outputPath + "/" + name + ".result", ios_base::app);
        f << "Number of output " << numOutput << endl;
        f << "Compiling time " << context->compilingTime << endl;
        f << "Execution time " << context->executingTime << endl;
        if (config.enableProfile) {
            f << plan << endl;
        }
        f.flush();
        f.close();
    }
}

void Benchmark::verify() const {
    if (context->queryResult->numTuples == 1) {
        uint64_t numTuples = 0;
        ResultSetIterator resultSetIterator(context->queryResult->resultSetCollection[0].get());
        Tuple tuple(resultSetIterator.dataTypes);
        resultSetIterator.setResultSet(context->queryResult->resultSetCollection[0].get());
        resultSetIterator.getNextTuple(tuple);
        numTuples = tuple.getValue(0)->val.int64Val;
        if (numTuples != expectedNumOutput) {
            spdlog::error("Query: {}", context->query);
            spdlog::error(
                "Expected number of output equals {} but get {}", expectedNumOutput, numTuples);
        }
    } else {
        spdlog::error("Expected number of tuples is 1.");
    }
}

} // namespace benchmark
} // namespace graphflow
