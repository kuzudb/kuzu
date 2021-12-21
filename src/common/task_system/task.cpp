#include "src/common/include/task_system/task.h"

namespace graphflow {
namespace common {

Task::Task(uint64_t maxNumThreads) : maxNumThreads{maxNumThreads} {
    numThreadsFinished = 0;
    numThreadsRegistered = 0;
    parent = nullptr;
}

bool Task::registerThread() {
    lock_t lck{mtx};
    if (!hasException() && canRegisterInternalNoLock()) {
        numThreadsRegistered++;
        return true;
    }
    return false;
}

void Task::deRegisterThreadAndFinalizeTaskIfNecessary() {
    lock_t lck{mtx};
    ++numThreadsFinished;
    if (isCompletedNoLock()) {
        finalizeIfNecessary();
    }
}

} // namespace common
} // namespace graphflow
