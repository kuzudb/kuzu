#include "common/task_system/task_scheduler.h"

using namespace kuzu::common;

namespace kuzu {
namespace common {

TaskScheduler::TaskScheduler(uint64_t numWorkerThreads)
    : stopWorkerThreads{false}, nextScheduledTaskID{0} {
    for (auto n = 0u; n < numWorkerThreads; ++n) {
        workerThreads.emplace_back([&] { runWorkerThread(); });
    }
}

TaskScheduler::~TaskScheduler() {
    lock_t lck{mtx};
    stopWorkerThreads = true;
    lck.unlock();
    cv.notify_all();
    for (auto& thread : workerThreads) {
        thread.join();
    }
}

void TaskScheduler::scheduleTaskAndWaitOrError(const std::shared_ptr<Task>& task,
    processor::ExecutionContext* context) {
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
            // Note: we do not remove completed tasks from the queue in this function. They will be
            // removed by the worker threads when they traverse down the queue for a task to work on
            // (see getTaskAndRegister()).
            taskLck.unlock();
            break;
        }
        if (context->clientContext->hasTimeout()) {
            timeout = context->clientContext->getTimeoutRemainingInMS();
            if (timeout == 0) {
                context->clientContext->interrupt();
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

// For now assume that there are NO DEPENDENCIES for the task being submitted
std::shared_ptr<ScheduledTask> TaskScheduler::scheduleTaskAndReturn(
    const std::shared_ptr<Task>& task) {
    auto scheduledTask = pushTaskIntoQueue(task);
    cv.notify_all();
    return scheduledTask;
}

std::vector<std::shared_ptr<ScheduledTask>> TaskScheduler::scheduleTasksAndReturn(
    const std::vector<std::shared_ptr<Task>>& tasks) {
    lock_t lck{mtx};
    std::vector<std::shared_ptr<ScheduledTask>> scheduledTasks;
    for (auto& task : tasks) {
        scheduledTasks.emplace_back(std::make_shared<ScheduledTask>(task, nextScheduledTaskID++));
        taskQueue.push_back(scheduledTasks.back());
    }
    cv.notify_all();
    return scheduledTasks;
}

std::shared_ptr<ScheduledTask> TaskScheduler::pushTaskIntoQueue(const std::shared_ptr<Task>& task) {
    lock_t lck{mtx};
    auto scheduledTask = std::make_shared<ScheduledTask>(task, nextScheduledTaskID++);
    taskQueue.push_back(scheduledTask);
    return scheduledTask;
}

/*
 * Changing the logic of binding threads to tasks here, below is the logic:
 *
 * (1) We are going to scan the task queue and calculate the "amount of work" for each task
 *
 * (2) The amount of work will be calculated as: pipelineSink->getWork() / numThreadsRegistered
 *
 * (3) If we encounter a task that does not have even 1 thread executing on it, that task gets the
 *     highest priority, the thread will pick that task over all others. It exits the process
 *     immediately after registering to it.
 *
 * (4) Else, after reaching the end of the task queue, the threads will pick the task that has the
 *     most amount of work.
 *
 * There is an edge case here, where the task that got picked (with the most amount of work) can
 * either fail or already got finished. In that case we have to return nullptr, ignoring a more
 * graceful way to handle this for now.
 *
 */
std::shared_ptr<ScheduledTask> TaskScheduler::getTaskAndRegister() {
    if (taskQueue.empty()) {
        return nullptr;
    }
    auto it = taskQueue.begin();
    auto schedTask = std::make_shared<ScheduledTask>(nullptr, 0u);
    uint64_t maxTaskWork = 0u;
    while (it != taskQueue.end()) {
        auto task = (*it)->task;
        task->mtx.lock();
        if (task->numThreadsRegistered == 0) {
            task->numThreadsRegistered++;
            task->mtx.unlock();
            return *it;
        }
        if (task->isCompletedNoLock() && !task->hasExceptionNoLock()) {
            task->mtx.unlock();
            it = taskQueue.erase(it);
            continue;
        }
        // don't remove task if failed, done by thread which submitted the task
        // if task is single threaded (and already has >1 thread executing it), ignore it
        if ((task->exceptionsPtr != nullptr)  || (task->maxNumThreads == 1)) {
            task->mtx.unlock();
            it++;
            continue;
        }
        auto taskWork = task->getWorkNoLock();
        if (taskWork > maxTaskWork) {
            maxTaskWork = taskWork;
            schedTask = *it;
        }
        task->mtx.unlock();
        it++;
    }
    if (schedTask->task && schedTask->task->registerThread()) {
        return schedTask;
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
        std::shared_ptr<ScheduledTask> scheduledTask = nullptr;
        lck.lock();
        cv.wait(lck, [&] {
            scheduledTask = getTaskAndRegister();
            return scheduledTask != nullptr || stopWorkerThreads;
        });
        lck.unlock();
        if (stopWorkerThreads) {
            return;
        }
        try {
            scheduledTask->task->run();
            scheduledTask->task->deRegisterThreadAndFinalizeTask();
        } catch (std::exception& e) {
            scheduledTask->task->setException(std::current_exception());
            scheduledTask->task->deRegisterThreadAndFinalizeTask();
            continue;
        }
    }
}
} // namespace common
} // namespace kuzu
