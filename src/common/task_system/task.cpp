#include "common/task_system/task.h"

namespace kuzu {
namespace common {

Task::Task(uint64_t maxNumThreads) : maxNumThreads{maxNumThreads} {}

/*
 * If registered threads is 0, try to register, return true if successful
 */
bool Task::tryRegisterIfUnregistered() {
    lock_t lck{mtx};
    if (numThreadsRegistered > 0) {
        return false;
    }
    if (!hasExceptionNoLock() && canRegisterNoLock()) {
        numThreadsRegistered++;
        return true;
    }
    return false;
}

bool Task::registerThread() {
    lock_t lck{mtx};
    if (!hasExceptionNoLock() && canRegisterNoLock()) {
        numThreadsRegistered++;
        return true;
    }
    return false;
}

void Task::deRegisterThreadAndFinalizeTask() {
    lock_t lck{mtx};
    ++numThreadsFinished;
    if (!hasExceptionNoLock() && isCompletedNoLock()) {
        try {
            finalizeIfNecessary();
        } catch (std::exception& e) {
            setExceptionNoLock(std::current_exception());
        }
    }
    if (isCompletedNoLock()) {
        lck.unlock();
        cv.notify_all();
    }
}

} // namespace common
} // namespace kuzu
