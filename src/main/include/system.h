#pragma once

#include "src/main/include/system_config.h"
#include "src/planner/include/logical_plan/logical_plan.h"
#include "src/processor/include/processor.h"
#include "src/storage/include/graph.h"
#include "src/transaction/include/transaction.h"
#include "src/transaction/include/transaction_manager.h"

using namespace graphflow::storage;
using namespace graphflow::transaction;
using namespace graphflow::processor;
using namespace graphflow::planner;

namespace graphflow {
namespace main {

class System {

public:
    System(const SystemConfig& config, const string path);

    void restart(const SystemConfig& config);

    bool isInitialized() {
        return nullptr != graph.get() && nullptr != transactionManager.get() &&
               nullptr != processor.get();
    }

    vector<unique_ptr<LogicalPlan>> enumerateLogicalPlans(const string& query);

    unique_ptr<QueryResult> execute(
        unique_ptr<LogicalPlan> plan, Transaction* transactionPtr, uint32_t numThreads);

    unique_ptr<nlohmann::json> debugInfo();

public:
    SystemConfig config;
    unique_ptr<TransactionManager> transactionManager;

private:
    void initialize(string path);

private:
    unique_ptr<Graph> graph;
    unique_ptr<QueryProcessor> processor;
};

} // namespace main
} // namespace graphflow
