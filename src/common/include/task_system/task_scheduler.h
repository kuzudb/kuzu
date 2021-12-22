#pragma once

#include <deque>
#include <thread>

#include "src/common/include/task_system/task.h"
#include "src/common/include/utils.h"

namespace graphflow {
namespace common {

struct ScheduledTask {
public:
    ScheduledTask(shared_ptr<Task> task, uint64_t ID) : task{task}, ID{ID} {};
    shared_ptr<Task> task;
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
    const uint64_t THREAD_WAIT_TIME_IN_MICROS = 100;
    explicit TaskScheduler(uint64_t numThreads);
    ~TaskScheduler();

    // Functions for the users of the task scheduler, e.g., loader and processor to use.
    void scheduleTaskAndWaitOrError(const shared_ptr<Task>& task);
    shared_ptr<ScheduledTask> scheduleTask(const shared_ptr<Task>& task);
    void waitAllTasksToCompleteOrError();

private:
    void removeErroringTask(uint64_t scheduledTaskID);

    // Functions to launch worker threads and for the worker threads to use to grab task from queue.
    void runWorkerThread();
    shared_ptr<ScheduledTask> getTaskAndRegister();

private:
    shared_ptr<spdlog::logger> logger;
    mutex mtx;
    deque<shared_ptr<ScheduledTask>> taskQueue;
    bool stopThreads{false};
    vector<thread> threads;
    uint64_t nextScheduledTaskID;
};

} // namespace common
} // namespace graphflow
