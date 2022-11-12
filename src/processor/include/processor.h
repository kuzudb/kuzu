#pragma once

#include <thread>

#include "src/common/include/task_system/task_scheduler.h"
#include "src/processor/include/physical_plan.h"
#include "src/processor/result/include/factorized_table.h"
#include "src/storage/buffer_manager/include/memory_manager.h"

using namespace kuzu::storage;

namespace kuzu {
namespace processor {

class QueryProcessor {

public:
    explicit QueryProcessor(uint64_t numThreads);

    shared_ptr<FactorizedTable> execute(PhysicalPlan* physicalPlan, ExecutionContext* context);

private:
    void decomposePlanIntoTasks(PhysicalOperator* op, PhysicalOperator* parent, Task* parentTask,
        ExecutionContext* context);

    static shared_ptr<FactorizedTable> getFactorizedTableForOutputMsg(
        string& outputMsg, MemoryManager* memoryManager);

private:
    unique_ptr<TaskScheduler> taskScheduler;
};

} // namespace processor
} // namespace kuzu
