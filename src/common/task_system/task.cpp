#include "common/task_system/task.h"

namespace kuzu {
namespace common {

Task::Task(uint64_t maxNumThreads) : maxNumThreads{maxNumThreads} {}

bool Task::registerThread() {
    lock_t lck{mtx};
    if (!hasExceptionNoLock() && canRegisterNoLock()) {
        numThreadsRegistered++;
        printf("Task::registerThread() numThreadsRegistered: %llu\n", numThreadsRegistered);
        return true;
    }
    return false;
}

void Task::deRegisterThreadAndFinalizeTask() {
    lock_t lck{mtx};
    ++numThreadsFinished;
    printf("Task::registerThread() numThreadsRegistered: %llu\n", numThreadsRegistered);
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
