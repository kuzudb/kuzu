#pragma once

#include <thread>

#include "src/common/include/memory_manager.h"
#include "src/common/include/task_system/task_scheduler.h"
#include "src/processor/include/physical_plan/physical_plan.h"
#include "src/processor/include/physical_plan/result/query_result.h"

using namespace graphflow::storage;

namespace graphflow {
namespace processor {

class QueryProcessor {

public:
    explicit QueryProcessor(uint64_t numThreads);

    unique_ptr<QueryResult> execute(PhysicalPlan* physicalPlan, uint64_t numThreads);

private:
    void run();
    void decomposePlanIntoTasks(PhysicalOperator* op, Task* parentTask, uint64_t numThreads);
    void scheduleTaskAndWaitUntilExecution(const shared_ptr<Task>& task);

private:
    unique_ptr<TaskScheduler> taskScheduler;
};

} // namespace processor
} // namespace graphflow
