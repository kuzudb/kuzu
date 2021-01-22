#include "src/runner/include/server/session.h"

#include "src/processor/include/plan/logical/logical_plan.h"

namespace graphflow {
namespace runner {

void Session::loadGraph(const string& path) {
    if (nullptr == graph || 0 != path.compare(graph->getPath())) {
        throwErrorIfPathIsEmpty(path);
        graph.reset();
        graph = make_unique<Graph>(path, bufferPoolSize);
        return;
    }
    throw invalid_argument("The same graph at path: " + path + " is already loaded.");
}

uint64_t Session::setBufferPoolSize(const uint64_t& size) {
    if (size < (1 << 20) || size > (1ul << 40u)) {
        throw invalid_argument(
            "Buffer pool size argument should be graeter than 1MB or less than 1TB.");
    }
    if (0 != size % PAGE_SIZE) {
        throw invalid_argument(
            "Buffer pool size should be aligned to the system PAGE SIZE (=4096B)");
    }
    if (bufferPoolSize == size) {
        throw invalid_argument("No change in size.");
    }
    bufferPoolSize = size;
    if (nullptr != graph) {
        string path = graph->getPath();
        graph.reset();
        graph = make_unique<Graph>(path, bufferPoolSize);
    }
    return bufferPoolSize;
}

uint32_t Session::setNumProcessorThreads(const uint32_t& num) {
    if (num < 1 || num > thread::hardware_concurrency()) {
        throw invalid_argument(
            "Number should be greater than 0 and less than the number of physical threads "
            "available in the system.");
    }
    if (numProcessorThreads == num) {
        throw invalid_argument("No change in num.");
    }
    numProcessorThreads = num;
    processor.reset(new QueryProcessor(numProcessorThreads));
    return numProcessorThreads;
}

unique_ptr<nlohmann::json> Session::getSession() {
    auto json = make_unique<nlohmann::json>();
    (*json)["session"]["bufferPoolSize"] = bufferPoolSize;
    (*json)["session"]["numProcessorThreads"] = numProcessorThreads;
    if (nullptr != graph.get()) {
        (*json)["session"]["graph"] = graph->getPath();
    } else {
        (*json)["session"]["graph"] = "";
    }
    return json;
}

unique_ptr<nlohmann::json> Session::getPrettyPlan(const string& path) {
    auto json = make_unique<nlohmann::json>();
    (*json)["plan"] = "not implemented";
    return json;
}

unique_ptr<nlohmann::json> Session::execute(const string& path, const uint32_t& numThreads) {
    throwErrorIfGraphNotInitialized();
    throwErrorIfPathIsEmpty(path);
    auto physicalPlan = LogicalPlan(path).mapToPhysical(*graph);
    auto result = processor->execute(physicalPlan, *graph, numThreads);
    auto json = make_unique<nlohmann::json>();
    (*json)["number of tuples"] = result->first.getNumOutputTuples();
    (*json)["time"] = to_string(result->second.count()) + "ms";
    return json;
}

unique_ptr<nlohmann::json> Session::getGraphDebugInfo() {
    throwErrorIfGraphNotInitialized();
    return graph->debugInfo();
}

void Session::throwErrorIfGraphNotInitialized() {
    if (nullptr == graph) {
        throw invalid_argument("Graph is not loaded.");
    }
}

void Session::throwErrorIfPathIsEmpty(const string& path) {
    if (path.empty()) {
        throw invalid_argument("Given path is empty.");
    }
}

} // namespace runner
} // namespace graphflow
