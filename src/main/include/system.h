#pragma once

#include "src/main/include/session_context.h"
#include "src/planner/include/logical_plan/logical_plan.h"
#include "src/processor/include/processor.h"
#include "src/storage/include/graph.h"
#include "src/transaction/include/transaction_manager.h"

using namespace graphflow::storage;
using namespace graphflow::transaction;
using namespace graphflow::processor;
using namespace graphflow::planner;

namespace graphflow {
namespace main {

class System {

public:
    explicit System(const string& path, bool isInMemoryMode);

    void executeQuery(SessionContext& context) const;

    // currently used in testing framework
    vector<unique_ptr<LogicalPlan>> enumerateAllPlans(SessionContext& sessionContext) const;
    unique_ptr<QueryResult> executePlan(
        unique_ptr<LogicalPlan> logicalPlan, SessionContext& sessionContext) const;

    unique_ptr<nlohmann::json> debugInfo() const;

public:
    unique_ptr<Graph> graph;
    unique_ptr<QueryProcessor> processor;
    unique_ptr<TransactionManager> transactionManager;
    unique_ptr<MemoryManager> memManager;
    unique_ptr<BufferManager> bufferManager;
    uint64_t BUFFER_POOL_SIZE = StorageConfig::DEFAULT_BUFFER_POOL_SIZE;
    bool initialized = false;

    void resizeBuffer(uint64_t bufferSize);
    string getDataPath();
    void setDataPath(const string path);

private:
    string dataPath;
};

} // namespace main
} // namespace graphflow
