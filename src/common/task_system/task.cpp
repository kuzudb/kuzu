#include "common/task_system/task.h"

namespace kuzu {
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
        try {
            finalizeIfNecessary();
        } catch (std::exception& e) { setExceptionNoLock(std::current_exception()); }
    }
}

} // namespace common
} // namespace kuzu
