#pragma once

#include <thread>

#include "common/task_system/task_scheduler.h"
#include "processor/physical_plan.h"
#include "processor/result/factorized_table.h"
#include "storage/buffer_manager/memory_manager.h"

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
