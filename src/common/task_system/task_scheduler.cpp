#include "common/task_system/task_scheduler.h"

#include "common/configs.h"
#include "spdlog/spdlog.h"

using namespace kuzu::common;

namespace kuzu {
namespace common {

TaskScheduler::TaskScheduler(uint64_t numThreads)
    : logger{LoggerUtils::getOrCreateLogger("processor")}, nextScheduledTaskID{0} {
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

shared_ptr<ScheduledTask> TaskScheduler::scheduleTask(const shared_ptr<Task>& task) {
    lock_t lck{mtx};
    auto scheduledTask = make_shared<ScheduledTask>(task, nextScheduledTaskID++);
    taskQueue.push_back(scheduledTask);
    return scheduledTask;
}

void TaskScheduler::waitAllTasksToCompleteOrError() {
    while (true) {
        lock_t lck{mtx};
        if (taskQueue.empty()) {
            return;
        }
        for (auto it = taskQueue.begin(); it != taskQueue.end(); ++it) {
            auto task = (*it)->task;
            if (task->hasException()) {
                taskQueue.erase(it);
                std::rethrow_exception(task->getExceptionPtr());
            }
            // TODO(Semih): We can optimize to stop after finding a registrable task. This is
            // because tasks after the first registrable task in the queue cannot have any thread
            // yet registered to them, so they cannot have errored.
        }
        lck.unlock();
        this_thread::sleep_for(chrono::microseconds(THREAD_SLEEP_TIME_WHEN_WAITING_IN_MICROS));
    }
}

void TaskScheduler::scheduleTaskAndWaitOrError(const shared_ptr<Task>& task) {
    for (auto& dependency : task->children) {
        scheduleTaskAndWaitOrError(dependency);
    }
    auto scheduledTask = scheduleTask(task);
    while (!task->isCompleted()) {
        this_thread::sleep_for(chrono::microseconds(THREAD_SLEEP_TIME_WHEN_WAITING_IN_MICROS));
    }
    if (task->hasException()) {
        removeErroringTask(scheduledTask->ID);
        std::rethrow_exception(task->getExceptionPtr());
    }
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
            this_thread::sleep_for(chrono::microseconds(THREAD_SLEEP_TIME_WHEN_WAITING_IN_MICROS));
            continue;
        }
        try {
            scheduledTask->task->run();
            scheduledTask->task->deRegisterThreadAndFinalizeTaskIfNecessary();
        } catch (exception& e) {
            scheduledTask->task->setException(current_exception());
            scheduledTask->task->deRegisterThreadAndFinalizeTaskIfNecessary();
            continue;
        }
    }
}

} // namespace common
} // namespace kuzu
