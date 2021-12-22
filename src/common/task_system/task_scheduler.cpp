#include "src/common/include/task_system/task_scheduler.h"

using namespace graphflow::common;

namespace graphflow {
namespace common {

TaskScheduler::TaskScheduler(uint64_t numThreads)
    : nextScheduledTaskID{0}, logger{LoggerUtils::getOrCreateSpdLogger("taskscheduler")} {
    for (auto n = 0u; n < numThreads; ++n) {
        threads.emplace_back([&] { runWorkerThread(); });
    }
}

TaskScheduler::~TaskScheduler() {
    stopThreads = true;
    for (auto& thread : threads) {
        thread.join();
    }
}

shared_ptr<ScheduledTask> TaskScheduler::scheduleTask(const shared_ptr<Task>& task) {
    lock_t lck{mtx};
    auto scheduledTask = make_shared<ScheduledTask>(task, nextScheduledTaskID++);
    taskQueue.push_back(scheduledTask);
    return scheduledTask;
}

void TaskScheduler::waitAllTasksToCompleteOrError() {
    logger->info("Thread {} called waitAllTasksToCompleteOrError. Beginning to wait.",
        StringUtils::getThreadIDString());
    while (true) {
        lock_t lck{mtx};
        if (taskQueue.empty()) {
            logger->info("Thread {} successfully waited all tasks to be complete. Returning from "
                         "waitAllTasksToCompleteOrError.",
                StringUtils::getThreadIDString());
            return;
        }
        for (auto it = taskQueue.begin(); it != taskQueue.end(); ++it) {
            auto task = (*it)->task;
            if (task->hasException()) {
                taskQueue.erase(it);
                std::rethrow_exception(task->getExceptionPtr());
            }
            // TODO(Semih): We can optimize to stops after finding a registrable task. This is
            // because tasks after the first registrable task in the queue cannot have any thread
            // yet registered to them, so they cannot have errored.
        }
        lck.unlock();
        this_thread::sleep_for(chrono::microseconds(THREAD_WAIT_TIME_IN_MICROS));
    }
}

void TaskScheduler::scheduleTaskAndWaitOrError(const shared_ptr<Task>& task) {
    logger->info("Thread {} called scheduleTaskAndWaitOrError. Scheduling task.",
        StringUtils::getThreadIDString());
    for (auto& dependency : task->children) {
        scheduleTaskAndWaitOrError(dependency);
    }
    auto scheduledTask = scheduleTask(task);
    while (!task->isCompletedOrHasException()) {
        this_thread::sleep_for(chrono::microseconds(THREAD_WAIT_TIME_IN_MICROS));
    }
    if (task->hasException()) {
        logger->info("Thread {} found a task with exception. Will call removeErroringTask.",
            StringUtils::getThreadIDString());
        removeErroringTask(scheduledTask->ID);
        std::rethrow_exception(task->getExceptionPtr());
    }
    logger->info("Thread {} exiting scheduleTaskAndWaitOrError (task was successfully complete)",
        StringUtils::getThreadIDString());
}

shared_ptr<ScheduledTask> TaskScheduler::getTaskAndRegister() {
    lock_t lck{mtx};
    if (taskQueue.empty()) {
        return nullptr;
    }
    auto it = taskQueue.begin();
    while (it != taskQueue.end()) {
        auto task = (*it)->task;
        if (!task->registerThread()) {
            // If we cannot register for a thread it is because of two possibilities:
            // (i) maximum number of threads have registered for task and the task is completed
            // (without an exception)the task has an exception; or (ii) same as (i) but the task
            // has not yet successfully completed; or (iii) task has an exception; Only in (i)
            // we remove the task from the queue. For (ii) and (iii) we keep the task in queue.
            // Recall erroring tasks need to be manually removed.
            if (task->isCompletedSuccessfully()) { // option (i)
                logger->info("Thread {} is removing completed schedule task {} from queue.",
                    StringUtils::getThreadIDString(), (*it)->ID);
                it = taskQueue.erase(it);
            } else { // option (ii) or (iii): keep the task in the queue.
                ++it;
            }
        } else {
            logger->info("Registered thread {} to schedule task {}.",
                StringUtils::getThreadIDString(), (*it)->ID);
            return *it;
        }
    }
    return nullptr;
}

void TaskScheduler::removeErroringTask(uint64_t scheduledTaskID) {
    lock_t lck{mtx};
    logger->info("RemovErroringTask is called.Thread {}", StringUtils::getThreadIDString());
    for (auto it = taskQueue.begin(); it != taskQueue.end(); ++it) {
        if (scheduledTaskID == (*it)->ID) {
            logger->info(
                "Inside removeErroringTask.Thread {} is removing an erroring task {} from queue.",
                StringUtils::getThreadIDString(), (*it)->ID);
            taskQueue.erase(it);
            return;
        }
    }
    logger->info(
        "Inside removeErroringTask. Thread {} could not find the task to remove from queue.",
        StringUtils::getThreadIDString());
}

void TaskScheduler::runWorkerThread() {
    while (true) {
        if (stopThreads) {
            break;
        }
        auto scheduledTask = getTaskAndRegister();
        if (!scheduledTask) {
            this_thread::sleep_for(chrono::microseconds(THREAD_WAIT_TIME_IN_MICROS));
            continue;
        }
        try {
            scheduledTask->task->run();
            scheduledTask->task->deRegisterThreadAndFinalizeTaskIfNecessary();
            logger->info(
                "Thread {} completed task successfully.", StringUtils::getThreadIDString());
        } catch (exception& e) {
            logger->info("Thread {} caught an exception. Setting the exception of the task.",
                StringUtils::getThreadIDString());
            scheduledTask->task->setException(current_exception());
            continue;
        }
    }
}

} // namespace common
} // namespace graphflow
