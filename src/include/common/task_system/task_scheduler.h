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
 * be waiting on a tasks through a function that waits, e.g., scheduleTaskAndWaitOrError or
 * waitAllTasksToCompleteOrError.
 *
 * Currently there are two ways the TaskScheduler can be used:
 * (1) Schedule a set of tasks T1, ..., Tk, and wait for all of them to be completed or error
 * if any one them has raised an exception (but only the exception of one (the earliest scheduled)
 * task will be raised. We do not currently re-throw multiple exceptions from multiple tasks. Ex:
 *      schedule(T1);
 *      ...;
 *      schedule(Tk);
 *      waitAllTasksToCompleteOrError();
 * (2) Schedule one task T and wait for T to finish or error if there was an exception raised by
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

    // If a user, e.g., currently the copier, adds a set of tasks T1, ..., Tk, to the task scheduler
    // without waiting for them to finish, the user needs to call waitAllTasksToCompleteOrError() if
    // it wants to catch the errors that may have happened in T1, ..., Tk. If this function is not
    // called and a T_i errors, there will be two side effects: (1) the user will never be aware
    // that there was an error in some of the scheduled tasks; (2) the erroring task will not be
    // removed from the task queue, so it will remain there permanently. We only remove erroring
    // tasks inside waitAllTasksToCompleteOrError and scheduleTaskAndWaitOrError. Also, see the note
    // below in waitAllTasksToCompleteOrError for details of the behavior when multiple tasks fail.
    std::shared_ptr<ScheduledTask> scheduleTask(const std::shared_ptr<Task>& task);

    // Also note that if a user has scheduled multiple concrete tasks and calls
    // waitAllTasksToCompleteOrError and multiple tasks error, then waitAllTasksToCompleteOrError
    // will rethrow only the exception of the first task that it observes to have an error and
    // it will remove only that one. Other tasks that may have failed many not be removed
    // from the task queue and remain in the queue. So for now, use this function if you
    // want the system to crash if any of the tasks fails.
    void waitAllTasksToCompleteOrError();

    void waitUntilEnoughTasksFinish(int64_t minimumNumTasksToScheduleMore);

    // Checks if there is an erroring task in the queue and if so, errors.
    void errorIfThereIsAnException();

    bool isTaskQueueEmpty() { return taskQueue.empty(); }
    uint64_t getNumTasks() { return taskQueue.size(); }

private:
    void removeErroringTask(uint64_t scheduledTaskID);

    void errorIfThereIsAnExceptionNoLock();

    // Functions to launch worker threads and for the worker threads to use to grab task from queue.
    void runWorkerThread();
    std::shared_ptr<ScheduledTask> getTaskAndRegister();

    void interruptTaskIfTimeOutNoLock(processor::ExecutionContext* context);

private:
    std::shared_ptr<spdlog::logger> logger;
    std::mutex mtx;
    std::deque<std::shared_ptr<ScheduledTask>> taskQueue;
    std::atomic<bool> stopThreads{false};
    std::vector<std::thread> threads;
    uint64_t nextScheduledTaskID;
};

} // namespace common
} // namespace kuzu
