#pragma once

#include <deque>

#include "src/processor/include/task_system/task.h"

namespace graphflow {
namespace processor {

// TaskQueue manages Tasks. At present, we have the invariant that there cannot be 2 tasks that
// are running concurrently.
class TaskQueue {

public:
    Task* getTask();

    void push(Task* task);

private:
    mutex mtx;
    deque<Task*> taskQueue;
};

} // namespace processor
} // namespace graphflow
