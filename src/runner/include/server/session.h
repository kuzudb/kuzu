#pragma once

#include "thread"

#include "nlohmann/json.hpp"

#include "src/processor/include/processor.h"
#include "src/storage/include/graph.h"

using namespace std;
using namespace graphflow::storage;
using namespace graphflow::processor;

namespace graphflow {
namespace runner {

// Persists the state of Graphflowdb server and interfaces Graph Storage and Query Processor with
// the exposed API.
class Session {

public:
    void loadGraph(const string& path);

    uint64_t setBufferPoolSize(const uint64_t& size);
    uint32_t setNumProcessorThreads(const uint32_t& num);

    unique_ptr<nlohmann::json> getSession();

    unique_ptr<nlohmann::json> getPrettyPlan(const string& path);

    unique_ptr<nlohmann::json> execute(const string& path, const uint32_t& numThreads);

public:
    uint64_t bufferPoolSize = DEFAULT_BUFFER_POOL_SIZE;
    uint64_t numProcessorThreads = thread::hardware_concurrency();
    unique_ptr<Graph> graph;
    unique_ptr<QueryProcessor> processor =
        make_unique<QueryProcessor>(thread::hardware_concurrency());
};

} // namespace runner
} // namespace graphflow
