#pragma once

#include "src/processor/include/memory_manager.h"
#include "src/processor/include/physical_plan/plan_mapper.h"
#include "src/processor/include/physical_plan/query_result.h"
#include "src/processor/include/task_system/task_queue.h"
#include "src/storage/include/graph.h"
#include "src/transaction/include/transaction.h"

using namespace graphflow::storage;
using namespace graphflow::transaction;

namespace graphflow {
namespace processor {

class QueryProcessor {

public:
    QueryProcessor(uint64_t numThreads);
    ~QueryProcessor();

    unique_ptr<QueryResult> execute(unique_ptr<LogicalPlan> plan, uint64_t maxNumThreads,
        Transaction* transactionPtr, const Graph& graph);

    // Only used in testing.
    unique_ptr<QueryResult> execute(unique_ptr<PhysicalPlan> plan, uint64_t maxNumThreads);

private:
    void run();
    void decomposePlanIntoTasks(PhysicalOperator* op, uint64_t maxNumThreads, Task* parentTask);
    void scheduleTask(Task* task);

private:
    shared_ptr<spdlog::logger> logger;
    TaskQueue queue;
    bool stopThreads{false};
    vector<thread> threads;
    unique_ptr<MemoryManager> memManager;
};

} // namespace processor
} // namespace graphflow
