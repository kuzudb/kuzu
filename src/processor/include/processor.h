#pragma once

#include <thread>

#include "src/common/include/memory_manager.h"
#include "src/processor/include/physical_plan/physical_plan.h"
#include "src/processor/include/physical_plan/query_result.h"
#include "src/processor/include/task_system/task_queue.h"

using namespace graphflow::storage;
using namespace graphflow::transaction;

namespace graphflow {
namespace processor {

class QueryProcessor {

public:
    explicit QueryProcessor(uint64_t numThreads);
    ~QueryProcessor();

    unique_ptr<QueryResult> execute(PhysicalPlan* physicalPlan, uint64_t numThreads);

private:
    void run();
    void decomposePlanIntoTasks(PhysicalOperator* op, Task* parentTask, uint64_t numThreads);
    void scheduleTask(Task* task);

public:
    unique_ptr<MemoryManager> memManager;

private:
    TaskQueue queue;
    bool stopThreads{false};
    vector<thread> threads;
};

} // namespace processor
} // namespace graphflow
