#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>

#include "common/task_system/task_scheduler.h"
#include "function/table_functions.h"
#include "ife_morsel.h"
#include "processor/operator/parallel_utils_operator.h"

using namespace kuzu::processor;

namespace kuzu {
namespace function {

struct ParallelUtilsJob {
    ExecutionContext *executionContext;
    std::unique_ptr<GDSLocalState> gdsLocalState;
    GDSCallSharedState *gdsCallSharedState;
    gds_algofunc_t gdsAlgoFunc;
    bool isParallel;
};

class ParallelUtils {
public:
    explicit ParallelUtils(uint32_t operatorID, common::TaskScheduler* taskScheduler);

    void submitParallelTaskAndWait(ParallelUtilsJob &parallelUtilsJob);

    std::shared_ptr<common::ScheduledTask> submitTaskAndReturn(ParallelUtilsJob &job);

    std::vector<std::shared_ptr<common::ScheduledTask>> submitTasksAndReturn(std::vector<ParallelUtilsJob> &jobs);

    bool taskCompletedNoError(std::shared_ptr<common::ScheduledTask> & scheduledTask);

    bool taskHasExceptionOrTimedOut(std::shared_ptr<common::ScheduledTask> &scheduledTask,
        ExecutionContext *executionContext);

private:
    common::TaskScheduler *taskScheduler;
    uint32_t operatorID;
};

} // namespace graph
} // namespace kuzu
