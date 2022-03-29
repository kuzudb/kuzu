#pragma once

#include "client_context.h"
#include "query_result.h"

#include "src/planner/logical_plan/include/logical_plan.h"

using namespace graphflow::planner;

namespace graphflow {
namespace main {

class Database;
class Connection {

public:
    explicit Connection(Database* database);

    std::unique_ptr<QueryResult> query(const std::string& query);

    inline void setMaxNumThreadForExec(uint64_t numThreads) {
        clientContext->numThreadsForExecution = numThreads;
    }

    /**
     * TODO: APIs that need to be added
     * * prepare (parameter related)
     * * catalog related
     * * streaming
     */

    // used in test helper
    std::vector<unique_ptr<planner::LogicalPlan>> enumeratePlans(const std::string& query);
    std::unique_ptr<QueryResult> executePlan(unique_ptr<planner::LogicalPlan> logicalPlan);
    // used in API test
    inline uint64_t getMaxNumThreadForExec() const { return clientContext->numThreadsForExecution; }

private:
    std::unique_lock<mutex> acquireLock() { return std::unique_lock<std::mutex>{mtx}; }

    void configProfiler(Profiler& profiler, bool isEnabled);

private:
    Database* database;
    std::unique_ptr<ClientContext> clientContext;
    std::mutex mtx;
};

} // namespace main
} // namespace graphflow
