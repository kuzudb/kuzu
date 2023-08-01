#pragma once

#include <atomic>
#include <deque>
#include <thread>

#include "common/task_system/task.h"
#include "common/utils.h"
#include "processor/execution_context.h"

namespace kuzu {
namespace common {

struct ScheduledTask {
    ScheduledTask(std::shared_ptr<Task> task, uint64_t ID) : task{std::move(task)}, ID{ID} {};
    std::shared_ptr<Task> task;
    uint64_t ID;
};

/**
 * TaskScheduler is a library that manages a set of worker threads that can execute tasks that are
 * put into a task queue. Each task accepts a maximum number of threads. Users of TaskScheduler
 * schedule tasks to be executed by calling schedule functions, e.g., scheduleTask or
 * scheduleTaskAndWaitOrError. New tasks are put at the end of the queue. Workers grab the first
 * task from the beginning of the queue that they can register themselves to work on. Any task that
 * is completed is removed automatically from the queue. If there is a task that raises an
 * exception, the worker threads catch it and store it with the tasks. The user thread that is
 * waiting on the completion of the task (or tasks) will throw the exception (the user thread could
 * be waiting on a tasks through a function that waits, e.g., scheduleTaskAndWaitOrError.
 *
 * Currently there is one way the TaskScheduler can be used:
 * Schedule one task T and wait for T to finish or error if there was an exception raised by
 * one of the threads working on T that errored. This is simply done by the call:
 *      scheduleTaskAndWaitOrError(T);
 *
 * TaskScheduler guarantees that workers will register themselves to tasks in FIFO order. However
 * this does not guarantee that the tasks will be completed in FIFO order: a long running task
 * that is not accepting more registration can stay in the queue for an unlimited time until
 * completion.
 */
class TaskScheduler {
public:
    explicit TaskScheduler(uint64_t numThreads);
    ~TaskScheduler();

    // Functions for the users of the task scheduler, e.g., processor to use.

    // Schedules the dependencies of the given task and finally the task one after another (so
    // not concurrently), and throws an exception if any of the tasks errors. Regardless of
    // whether or not the given task or one of its dependencies errors, when this function
    // returns, no task related to the given task will be in the task queue. Further no worker
    // thread will be working on the given task.
    void scheduleTaskAndWaitOrError(
        const std::shared_ptr<Task>& task, processor::ExecutionContext* context);

    std::shared_ptr<ScheduledTask> scheduleTask(const std::shared_ptr<Task>& task);

private:
    void removeErroringTask(uint64_t scheduledTaskID);

    // Functions to launch worker threads and for the worker threads to use to grab task from queue.
    void runWorkerThread();
    std::shared_ptr<ScheduledTask> getTaskAndRegister();

    void interruptTaskIfTimeOutNoLock(processor::ExecutionContext* context);

private:
    std::mutex mtx;
    std::deque<std::shared_ptr<ScheduledTask>> taskQueue;
    std::atomic<bool> stopThreads{false};
    std::vector<std::thread> threads;
    uint64_t nextScheduledTaskID;
};

} // namespace common
} // namespace kuzu
