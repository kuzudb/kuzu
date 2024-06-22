#include "function/gds/parallel_utils.h"

#include "processor/processor_task.h"
#include "processor/result/factorized_table.h"

using namespace kuzu::common;
using namespace kuzu::function;

namespace kuzu {
namespace function {

ParallelUtils::ParallelUtils(uint32_t operatorID_, common::TaskScheduler* scheduler) {
    taskScheduler = scheduler;
    operatorID = operatorID_;
}

void ParallelUtils::doParallelBlocking(ExecutionContext* executionContext,
    GDSAlgorithm* gdsAlgorithm, GDSCallSharedState* gdsCallSharedState,
    gds_algofunc_t gdsAlgoFunc) {
    auto parallelUtilsOp = std::make_unique<ParallelUtilsOperator>(gdsAlgorithm->copy(),
        gdsAlgoFunc, gdsCallSharedState, operatorID, "");
    auto task = std::make_shared<ProcessorTask>(parallelUtilsOp.get(), executionContext);
    task->setSharedStateInitialized();
    taskScheduler->scheduleTaskAndWaitOrError(task, executionContext);
}

std::shared_ptr<ScheduledTask> ParallelUtils::doSequentialNonBlocking(
    ExecutionContext* executionContext, GDSAlgorithm* gdsAlgorithm,
    GDSCallSharedState* gdsCallSharedState, gds_algofunc_t gdsAlgoFunc) {
    auto parallelUtilsOp = std::make_unique<ParallelUtilsOperator>(gdsAlgorithm->copy(),
        gdsAlgoFunc, gdsCallSharedState, operatorID, "");
    auto task = std::make_shared<ProcessorTask>(parallelUtilsOp.get(), executionContext);
    task->setSingleThreadedTask();
    task->setSharedStateInitialized();
    return taskScheduler->scheduleTaskAndReturn(task);
}

bool ParallelUtils::taskCompletedNoError(std::shared_ptr<common::ScheduledTask>& scheduledTask) {
    scheduledTask->task->mtx.lock();
    if (scheduledTask->task->isCompletedSuccessfully()) {
        scheduledTask->task->mtx.unlock();
        return true;
    }
    scheduledTask->task->mtx.unlock();
    return false;
}

bool ParallelUtils::taskHasExceptionOrTimedOut(std::shared_ptr<common::ScheduledTask>& scheduledTask,
    ExecutionContext* executionContext) {
    scheduledTask->task->mtx.lock();
    if (scheduledTask->task->hasExceptionNoLock()) {
        scheduledTask->task->mtx.unlock();
        executionContext->clientContext->interrupt();
        taskScheduler->removeErroringTask(scheduledTask->ID);
        return true;
    }
    if (executionContext->clientContext->hasTimeout()) {
        auto timeout = executionContext->clientContext->getTimeoutRemainingInMS();
        if (timeout == 0) {
            executionContext->clientContext->interrupt();
            return true;
        }
    }
    return false;
}

} // namespace function
} // namespace kuzu
