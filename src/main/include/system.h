#pragma once

#include "src/main/include/session_context.h"
#include "src/planner/logical_plan/include/logical_plan.h"
#include "src/processor/include/processor.h"
#include "src/storage/include/graph.h"

using namespace graphflow::storage;
using namespace graphflow::processor;
using namespace graphflow::planner;

namespace graphflow {
namespace main {

struct SystemConfig {

    SystemConfig(bool isInMemoryMode,
        uint64_t defaultPageBufferPoolSize = StorageConfig::DEFAULT_BUFFER_POOL_SIZE,
        uint64_t largePageBufferPoolSize = StorageConfig::DEFAULT_BUFFER_POOL_SIZE)
        : isInMemoryMode{isInMemoryMode}, defaultPageBufferPoolSize{defaultPageBufferPoolSize},
          largePageBufferPoolSize{largePageBufferPoolSize} {}

public:
    bool isInMemoryMode;
    uint64_t defaultPageBufferPoolSize;
    uint64_t largePageBufferPoolSize;
};

class System {

public:
    explicit System(const string& path, const SystemConfig& systemConfig);

    void executeQuery(SessionContext& context) const;

    // currently used in testing framework
    vector<unique_ptr<LogicalPlan>> enumerateAllPlans(SessionContext& sessionContext) const;
    shared_ptr<FactorizedTable> executePlan(
        unique_ptr<LogicalPlan> logicalPlan, SessionContext& sessionContext) const;

    unique_ptr<nlohmann::json> debugInfo() const;

public:
    unique_ptr<MemoryManager> memManager;
    unique_ptr<BufferManager> bufferManager;
    shared_ptr<spdlog::logger> logger;
    unique_ptr<Graph> graph;
    unique_ptr<QueryProcessor> processor;
    bool initialized = false;
};

} // namespace main
} // namespace graphflow
