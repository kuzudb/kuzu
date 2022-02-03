#pragma once

#include <thread>

#include "src/common/include/memory_manager.h"
#include "src/common/include/task_system/task_scheduler.h"
#include "src/processor/include/physical_plan/physical_plan.h"
#include "src/processor/include/physical_plan/result/factorized_table.h"

using namespace graphflow::storage;

namespace graphflow {
namespace processor {

class QueryProcessor {

public:
    explicit QueryProcessor(uint64_t numThreads);

    shared_ptr<FactorizedTable> execute(PhysicalPlan* physicalPlan, uint64_t numThreads);

private:
    void run();
    void decomposePlanIntoTasks(
        PhysicalOperator* op, PhysicalOperator* parent, Task* parentTask, uint64_t numThreads);
    void scheduleTaskAndWaitUntilExecution(const shared_ptr<Task>& task);

private:
    unique_ptr<TaskScheduler> taskScheduler;
};

} // namespace processor
} // namespace graphflow
