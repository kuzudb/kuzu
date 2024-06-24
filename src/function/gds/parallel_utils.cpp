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

void ParallelUtils::submitParallelTaskAndWait(ParallelUtilsJob& job) {
    auto parallelUtilsOp = std::make_unique<ParallelUtilsOperator>(std::move(job.gdsLocalState),
        job.gdsAlgoFunc, job.gdsCallSharedState, operatorID, "");
    auto task = std::make_shared<ProcessorTask>(parallelUtilsOp.get(), job.executionContext);
    task->setSharedStateInitialized();
    taskScheduler->scheduleTaskAndWaitOrError(task, job.executionContext);
}

std::shared_ptr<ScheduledTask> ParallelUtils::submitTaskAndReturn(ParallelUtilsJob& job) {
    auto parallelUtilsOp = std::make_unique<ParallelUtilsOperator>(std::move(job.gdsLocalState),
        job.gdsAlgoFunc, job.gdsCallSharedState, operatorID, "");
    auto task = std::make_shared<ProcessorTask>(parallelUtilsOp.get(), job.executionContext);
    if (!job.isParallel) {
        task->setSingleThreadedTask();
    }
    task->setSharedStateInitialized();
    return taskScheduler->scheduleTaskAndReturn(task);
}

std::vector<std::shared_ptr<ScheduledTask>> ParallelUtils::submitTasksAndReturn(
    std::vector<ParallelUtilsJob>& jobs) {
    std::vector<std::shared_ptr<Task>> tasks;
    tasks.reserve(jobs.size());
    auto jobIdx = 0u;
    for (auto& job : jobs) {
        auto parallelUtilsOp = std::make_unique<ParallelUtilsOperator>(std::move(job.gdsLocalState),
            job.gdsAlgoFunc, job.gdsCallSharedState, operatorID, "");
        auto task = std::make_shared<ProcessorTask>(parallelUtilsOp.get(), job.executionContext);
        if (!job.isParallel) {
            task->setSingleThreadedTask();
        }
        task->setSharedStateInitialized();
        tasks[jobIdx++] = task;
    }
    return taskScheduler->scheduleTasksAndReturn(tasks);
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

bool ParallelUtils::taskHasExceptionOrTimedOut(
    std::shared_ptr<common::ScheduledTask>& scheduledTask, ExecutionContext* executionContext) {
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
