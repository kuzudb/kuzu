#pragma once

#include "thread"

#include "nlohmann/json.hpp"

#include "src/processor/include/processor.h"
#include "src/storage/include/graph.h"

using namespace std;
using namespace graphflow::storage;
using namespace graphflow::processor;

namespace graphflow {
namespace main {

// Holds the state of a single connection to the Graphflowdb server.
class Session {

public:
    void loadGraph(const string& path);

    uint64_t setBufferPoolSize(const uint64_t& size);
    uint32_t setNumProcessorThreads(const uint32_t& num);

    unique_ptr<nlohmann::json> getSession();

    unique_ptr<nlohmann::json> getPrettyPlan(const string& path);

    unique_ptr<nlohmann::json> execute(const string& path, const uint32_t& numThreads);

    unique_ptr<nlohmann::json> getGraphDebugInfo();

    void throwErrorIfGraphNotInitialized();
    void throwErrorIfPathIsEmpty(const string& path);

public:
    uint64_t bufferPoolSize = DEFAULT_BUFFER_POOL_SIZE;
    uint64_t numProcessorThreads = thread::hardware_concurrency();
    unique_ptr<Graph> graph;
    unique_ptr<QueryProcessor> processor = make_unique<QueryProcessor>(1);
};

} // namespace main
} // namespace graphflow
