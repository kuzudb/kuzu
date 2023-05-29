#pragma once

#include <thread>

#include "common/task_system/task_scheduler.h"
#include "processor/physical_plan.h"
#include "processor/result/factorized_table.h"
#include "storage/buffer_manager/memory_manager.h"

namespace kuzu {
namespace processor {

class QueryProcessor {

public:
    explicit QueryProcessor(uint64_t numThreads);

    std::shared_ptr<FactorizedTable> execute(PhysicalPlan* physicalPlan, ExecutionContext* context);

private:
    void decomposePlanIntoTasks(PhysicalOperator* op, PhysicalOperator* parent,
        common::Task* parentTask, ExecutionContext* context);

private:
    std::unique_ptr<common::TaskScheduler> taskScheduler;
};

} // namespace processor
} // namespace kuzu
