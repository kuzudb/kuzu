#include "common/task_system/task_scheduler.h"

#include "common/constants.h"

using namespace kuzu::common;

namespace kuzu {
namespace common {

TaskScheduler::TaskScheduler(uint64_t numThreads) : nextScheduledTaskID{0} {
    for (auto n = 0u; n < numThreads; ++n) {
        threads.emplace_back([&] { runWorkerThread(); });
    }
}

TaskScheduler::~TaskScheduler() {
    stopThreads.store(true);
    for (auto& thread : threads) {
        thread.join();
    }
}

std::shared_ptr<ScheduledTask> TaskScheduler::scheduleTask(const std::shared_ptr<Task>& task) {
    lock_t lck{mtx};
    auto scheduledTask = std::make_shared<ScheduledTask>(task, nextScheduledTaskID++);
    taskQueue.push_back(scheduledTask);
    return scheduledTask;
}

void TaskScheduler::scheduleTaskAndWaitOrError(
    const std::shared_ptr<Task>& task, processor::ExecutionContext* context) {
    for (auto& dependency : task->children) {
        scheduleTaskAndWaitOrError(dependency, context);
    }
    auto scheduledTask = scheduleTask(task);
    while (!task->isCompleted()) {
        if (context->clientContext->isTimeOutEnabled()) {
            interruptTaskIfTimeOutNoLock(context);
        } else if (task->hasException()) {
            // Interrupt tasks that errored, so other threads can stop working on them early.
            context->clientContext->interrupt();
        }
        std::this_thread::sleep_for(
            std::chrono::microseconds(THREAD_SLEEP_TIME_WHEN_WAITING_IN_MICROS));
    }
    if (task->hasException()) {
        removeErroringTask(scheduledTask->ID);
        std::rethrow_exception(task->getExceptionPtr());
    }
}

std::shared_ptr<ScheduledTask> TaskScheduler::getTaskAndRegister() {
    lock_t lck{mtx};
    if (taskQueue.empty()) {
        return nullptr;
    }
    auto it = taskQueue.begin();
    while (it != taskQueue.end()) {
        auto task = (*it)->task;
        if (!task->registerThread()) {
            // If we cannot register for a thread it is because of three possibilities:
            // (i) maximum number of threads have registered for task and the task is completed
            // without an exception; or (ii) same as (i) but the task has not yet successfully
            // completed; or (iii) task has an exception; Only in (i) we remove the task from the
            // queue. For (ii) and (iii) we keep the task in queue. Recall erroring tasks need to be
            // manually removed.
            if (task->isCompletedSuccessfully()) { // option (i)
                it = taskQueue.erase(it);
            } else { // option (ii) or (iii): keep the task in the queue.
                ++it;
            }
        } else {
            return *it;
        }
    }
    return nullptr;
}

void TaskScheduler::removeErroringTask(uint64_t scheduledTaskID) {
    lock_t lck{mtx};
    for (auto it = taskQueue.begin(); it != taskQueue.end(); ++it) {
        if (scheduledTaskID == (*it)->ID) {
            taskQueue.erase(it);
            return;
        }
    }
}

void TaskScheduler::runWorkerThread() {
    while (true) {
        if (stopThreads.load()) {
            break;
        }
        auto scheduledTask = getTaskAndRegister();
        if (!scheduledTask) {
            std::this_thread::sleep_for(
                std::chrono::microseconds(THREAD_SLEEP_TIME_WHEN_WAITING_IN_MICROS));
            continue;
        }
        try {
            scheduledTask->task->run();
            scheduledTask->task->deRegisterThreadAndFinalizeTaskIfNecessary();
        } catch (std::exception& e) {
            scheduledTask->task->setException(std::current_exception());
            scheduledTask->task->deRegisterThreadAndFinalizeTaskIfNecessary();
            continue;
        }
    }
}

void TaskScheduler::interruptTaskIfTimeOutNoLock(processor::ExecutionContext* context) {
    if (context->clientContext->isTimeOut()) {
        context->clientContext->interrupt();
    }
}

} // namespace common
} // namespace kuzu
