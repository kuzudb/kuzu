#pragma once

#include "nlohmann/json.hpp"

#include "src/storage/include/graph.h"

using namespace std;
using namespace graphflow::storage;

namespace graphflow {
namespace runner {

// Persists the state of Graphflowdb server and interfaces Graph Storage and Query Processor with
// the exposed API.
class Session {

public:
    // loads the serialized graph at path.
    void loadGraph(const string& path);

    void setBufferPoolSize(uint64_t bufferPoolSize);
    uint64_t getBufferPoolSize() { return bufferPoolSize; }

    unique_ptr<nlohmann::json> getPrettyPlan(const string& path) {
        auto json = make_unique<nlohmann::json>();
        (*json)["plan"] = "not implemented";
        return json;
    }

    unique_ptr<nlohmann::json> getExecutionStats(const string& path) {
        auto json = make_unique<nlohmann::json>();
        (*json)["stats"] = "not implemented";
        return json;
    }

public:
    uint64_t bufferPoolSize = DEFAULT_BUFFER_POOL_SIZE;
    unique_ptr<Graph> graph{nullptr};
};

} // namespace runner
} // namespace graphflow
