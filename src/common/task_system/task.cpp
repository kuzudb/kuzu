#include "src/common/include/task_system/task.h"

namespace graphflow {
namespace common {

Task::Task(uint64_t maxNumThreads) : maxNumThreads{maxNumThreads} {}

bool Task::registerThread() {
    lock_t lck{mtx};
    if (!hasExceptionNoLock() && canRegisterInternalNoLock()) {
        numThreadsRegistered++;
        return true;
    }
    return false;
}

void Task::deRegisterThreadAndFinalizeTaskIfNecessary() {
    lock_t lck{mtx};
    ++numThreadsFinished;
    if (!hasExceptionNoLock() && isCompletedNoLock()) {
        finalizeIfNecessary();
    }
}

} // namespace common
} // namespace graphflow
