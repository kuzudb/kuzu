#pragma once

#include <deque>

#include "src/processor/include/task_system/task.h"

namespace graphflow {
namespace processor {

// TaskQueue manages Tasks. At present, we have the invariant that there cannot be 2 tasks that
// are running concurrently.
class TaskQueue {

public:
    shared_ptr<Task> getTask();

    void push(shared_ptr<Task> task);

private:
    mutex mtx;
    deque<shared_ptr<Task>> taskQueue;
};

} // namespace processor
} // namespace graphflow
