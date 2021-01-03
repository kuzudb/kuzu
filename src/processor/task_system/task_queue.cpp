#include "src/processor/include/task_system/task_queue.h"

namespace graphflow {
namespace processor {

shared_ptr<Task> TaskQueue::getTask() {
    lock_t lck{mtx};
    if (taskQueue.empty()) {
        return nullptr;
    }
    if (!taskQueue[0]->canRegister()) {
        taskQueue.pop_front();
        return nullptr;
    }
    return taskQueue[0];
}

void TaskQueue::push(shared_ptr<Task> task) {
    lock_t lock{mtx};
    taskQueue.push_back(task);
}

} // namespace processor
} // namespace graphflow
