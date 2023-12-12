#include "common/task_system/task_scheduler.h"

using namespace kuzu::common;

namespace kuzu {
namespace common {

TaskScheduler::TaskScheduler(uint64_t numThreads) : stopThreads{false}, nextScheduledTaskID{0} {
    for (auto n = 0u; n < numThreads; ++n) {
        threads.emplace_back([&] { runWorkerThread(); });
    }
}

TaskScheduler::~TaskScheduler() {
    lock_t lck{mtx};
    stopThreads = true;
    lck.unlock();
    cv.notify_all();
    for (auto& thread : threads) {
        thread.join();
    }
}

void TaskScheduler::scheduleTaskAndWaitOrError(
    const std::shared_ptr<Task>& task, processor::ExecutionContext* context) {
    for (auto& dependency : task->children) {
        scheduleTaskAndWaitOrError(dependency, context);
    }
    auto scheduledTask = pushTaskIntoQueue(task);
    cv.notify_all();
    std::unique_lock<std::mutex> taskLck{task->mtx, std::defer_lock};
    while (true) {
        taskLck.lock();
        bool timedWait = false;
        auto timeout = 0u;
        if (task->isCompletedNoLock()) {
            taskLck.unlock();
            break;
        }
        if (context->clientContext->isTimeOutEnabled()) {
            timeout = context->clientContext->getTimeoutRemainingInMS();
            if (timeout == 0) {
                interruptTaskIfTimeOutNoLock(context);
            } else {
                timedWait = true;
            }
        } else if (task->hasExceptionNoLock()) {
            // Interrupt tasks that errored, so other threads can stop working on them early.
            context->clientContext->interrupt();
        }
        if (timedWait) {
            task->cv.wait_for(taskLck, std::chrono::milliseconds(timeout));
        } else {
            task->cv.wait(taskLck);
        }
        taskLck.unlock();
    }
    if (task->hasException()) {
        removeErroringTask(scheduledTask->ID);
        std::rethrow_exception(task->getExceptionPtr());
    }
}

std::shared_ptr<ScheduledTask> TaskScheduler::pushTaskIntoQueue(const std::shared_ptr<Task>& task) {
    lock_t lck{mtx};
    auto scheduledTask = std::make_shared<ScheduledTask>(task, nextScheduledTaskID++);
    taskQueue.push_back(scheduledTask);
    return scheduledTask;
}

std::shared_ptr<ScheduledTask> TaskScheduler::getTaskAndRegister() {
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
    std::unique_lock<std::mutex> lck{mtx, std::defer_lock};
    while (true) {
        lck.lock();
        cv.wait(lck, [&] { return !taskQueue.empty() || stopThreads; });
        if (stopThreads) {
            return;
        }
        auto scheduledTask = getTaskAndRegister();
        lck.unlock();
        if (!scheduledTask) {
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
