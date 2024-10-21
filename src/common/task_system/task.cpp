#include "common/task_system/task.h"

namespace kuzu {
namespace common {

Task::Task(uint64_t maxNumThreads)
    : parent{nullptr}, maxNumThreads{maxNumThreads}, numThreadsFinished{0}, numThreadsRegistered{0},
      exceptionsPtr{nullptr}, ID{UINT64_MAX} {}

bool Task::registerThread() {
    lock_t lck{taskMtx};
    if (!hasExceptionNoLock() && canRegisterNoLock()) {
        numThreadsRegistered++;
        return true;
    }
    return false;
}

void Task::deRegisterThreadAndFinalizeTask() {
    lock_t lck{taskMtx};
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
