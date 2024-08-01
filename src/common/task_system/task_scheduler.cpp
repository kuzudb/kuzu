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
    lock_t lck{taskSchedulerMtx};
    stopWorkerThreads = true;
    lck.unlock();
    cv.notify_all();
    for (auto& thread : workerThreads) {
        thread.join();
    }
}

void TaskScheduler::scheduleTaskAndWaitOrError(const std::shared_ptr<Task>& task,
    processor::ExecutionContext* context, bool launchNewWorkerThread) {
    for (auto& dependency : task->children) {
        scheduleTaskAndWaitOrError(dependency, context);
    }
    std::thread newWorkerThread;
    if (launchNewWorkerThread) {
        // Note that newWorkerThread is not executing yet. However, we still call
        // task->registerThread() function because the call in the next line will guarantee
        // that the thread starts working on it. registerThread() function only increases the
        // numThreadsRegistered field of the task, tt does not keep track of the thread ids or
        // anything specific to the thread.
        task->registerThread();
        newWorkerThread = std::thread(runTask, task.get());
    }
    auto scheduledTask = pushTaskIntoQueue(task);
    cv.notify_all();
    std::unique_lock<std::mutex> taskLck{task->taskMtx, std::defer_lock};
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
    if (launchNewWorkerThread) {
        newWorkerThread.join();
    }
    if (task->hasException()) {
        removeErroringTask(scheduledTask->ID);
        std::rethrow_exception(task->getExceptionPtr());
    }
}

std::shared_ptr<ScheduledTask> TaskScheduler::pushTaskIntoQueue(const std::shared_ptr<Task>& task) {
    lock_t lck{taskSchedulerMtx};
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
    lock_t lck{taskSchedulerMtx};
    for (auto it = taskQueue.begin(); it != taskQueue.end(); ++it) {
        if (scheduledTaskID == (*it)->ID) {
            taskQueue.erase(it);
            return;
        }
    }
}

void TaskScheduler::runWorkerThread() {
    std::unique_lock<std::mutex> lck{taskSchedulerMtx, std::defer_lock};
    std::exception_ptr exceptionPtr = nullptr;
    std::shared_ptr<ScheduledTask> scheduledTask = nullptr;
    while (true) {
        lck.lock();
        if (scheduledTask != nullptr) {
            if (exceptionPtr != nullptr) {
                scheduledTask->task->setException(exceptionPtr);
                exceptionPtr = nullptr;
            }
            scheduledTask->task->deRegisterThreadAndFinalizeTask();
            scheduledTask = nullptr;
        }
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
        } catch (std::exception& e) {
            exceptionPtr = std::current_exception();
        }
//        TaskScheduler::runTask(scheduledTask->task.get());
//        // Warning: We do this additional lock acquisition to ensure that we put a memory fence
//        // after this thread (T1) has finished working on this task (Task1). This ensures that all
//        // the writes that T1 has done becomes visible to any other worker thread T2 that might
//        // start working on a task Task2 that depends on Task1. The reason this is ensured is that
//        // T2 will also grab the same lock in the above lck.lock() call (above the cv.wait).
//        // This safety measure was added after discussions with Trevor Brown.
//        lck.lock();
//        lck.unlock();
    }
}

void TaskScheduler::runTask(Task* task) {
    try {
        task->run();
        task->deRegisterThreadAndFinalizeTask();
    } catch (std::exception& e) {
        task->setException(std::current_exception());
        task->deRegisterThreadAndFinalizeTask();
    }
}
} // namespace common
} // namespace kuzu
