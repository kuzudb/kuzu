#pragma once

#include "src/main/include/session_context.h"
#include "src/planner/include/logical_plan/logical_plan.h"
#include "src/processor/include/processor.h"
#include "src/transaction/include/transaction_manager.h"

using namespace graphflow::storage;
using namespace graphflow::transaction;
using namespace graphflow::processor;
using namespace graphflow::planner;

namespace graphflow {
namespace main {

class System {

public:
    explicit System(const string& path);

    unique_ptr<QueryResult> executeQuery(SessionContext& sessionContext) const;

    // currently used in testing framework
    vector<unique_ptr<LogicalPlan>> enumerateAllPlans(SessionContext& sessionContext) const;
    unique_ptr<QueryResult> executePlan(
        unique_ptr<LogicalPlan> logicalPlan, SessionContext& sessionContext) const;

    unique_ptr<nlohmann::json> debugInfo() const;

public:
    unique_ptr<Graph> graph;
    unique_ptr<QueryProcessor> processor;
    unique_ptr<TransactionManager> transactionManager;
    bool initialized = false;
};

} // namespace main
} // namespace graphflow
