#pragma once

#include "common/task_system/task_scheduler.h"
#include "processor/operator/result_collector.h"
#include "processor/physical_plan.h"

namespace kuzu {
namespace processor {

class QueryProcessor {

public:
    explicit QueryProcessor(uint64_t numThreads);

    inline common::TaskScheduler* getTaskScheduler() { return taskScheduler.get(); }

    CollectedQueryResult execute(PhysicalPlan* physicalPlan, ExecutionContext* context);

private:
    void decomposePlanIntoTask(PhysicalOperator* op, common::Task* task, ExecutionContext* context);

    void initTask(common::Task* task);

private:
    std::unique_ptr<common::TaskScheduler> taskScheduler;
};

} // namespace processor
} // namespace kuzu
