#include "common/task_system/task.h"

namespace kuzu {
namespace common {

Task::Task(uint64_t maxNumThreads)
    : parent{nullptr}, maxNumThreads{maxNumThreads}, numThreadsFinished{0}, numThreadsRegistered{0},
      exceptionsPtr{nullptr}, ID{UINT64_MAX} {}

bool Task::registerThread() {
    #ifndef __SINGLE_THREADED__
    lock_t lck{taskMtx};
    #endif
    if (!hasExceptionNoLock() && canRegisterNoLock()) {
        numThreadsRegistered++;
        return true;
    }
    return false;
}

void Task::deRegisterThreadAndFinalizeTask() {
    #ifndef __SINGLE_THREADED__
    lock_t lck{taskMtx};
    #endif
    ++numThreadsFinished;
    if (!hasExceptionNoLock() && isCompletedNoLock()) {
        try {
            finalizeIfNecessary();
        } catch (std::exception& e) {
            setExceptionNoLock(std::current_exception());
        }
    }
    if (isCompletedNoLock()) {
        #ifndef __SINGLE_THREADED__
        lck.unlock();
        cv.notify_all();
        #endif
    }
}

} // namespace common
} // namespace kuzu
