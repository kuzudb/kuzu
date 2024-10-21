#pragma once

#ifndef __SINGLE_THREADED__
#include <condition_variable>
#include <mutex>
#endif

#include <memory>
#include <utility>
#include <vector>

namespace kuzu {
namespace common {

using lock_t = std::unique_lock<std::mutex>;

/**
 * Task represents a task that can be executed by multiple threads in the TaskScheduler. Task is a
 * virtual class. Users of TaskScheduler need to extend the Task class and implement at
 * least a virtual run() function. Users can assume that before T.run() is called, a worker thread W
 * has grabbed task T from the TaskScheduler's queue and registered itself to T. They can also
 * assume that after run() is called W will deregister itself from T. When deregistering, if W is
 * the last worker to finish on T, i.e., once W finishes, T will be completed, the
 * finalizeIfNecessary() will be called. So the run() and finalizeIfNecessary() calls are separate
 * calls and if there is some state from the run() function execution that will be needed by
 * finalizeIfNecessary, users should save it somewhere that can be accessed in
 * finalizeIfNecessary(). See ProcessorTask for an example of this.
 */
class Task {
    friend class TaskScheduler;

public:
    explicit Task(uint64_t maxNumThreads);
    virtual ~Task() = default;
    virtual void run() = 0;
    //     This function is called from inside deRegisterThreadAndFinalizeTaskIfNecessary() only
    //     once by the last registered worker that is completing this task. So the task lock is
    //     already acquired. So do not attempt to acquire the task lock inside. If needed we can
    //     make the deregister function release the lock before calling finalizeIfNecessary and
    //     drop this assumption.
    virtual void finalizeIfNecessary() {};

    void addChildTask(std::unique_ptr<Task> child) {
        child->parent = this;
        children.push_back(std::move(child));
    }

    inline bool isCompletedSuccessfully() {
#ifndef __SINGLE_THREADED__
        lock_t lck{taskMtx};
#endif
        return isCompletedNoLock() && !hasExceptionNoLock();
    }

    inline bool isCompletedNoLock() const {
        return (numThreadsRegistered > 0 && numThreadsFinished == numThreadsRegistered);
    }

    inline void setSingleThreadedTask() { maxNumThreads = 1; }

    bool registerThread();

    void deRegisterThreadAndFinalizeTask();

    inline void setException(std::exception_ptr exceptionPtr) {
#ifndef __SINGLE_THREADED__
        lock_t lck{taskMtx};
#endif
        setExceptionNoLock(std::move(exceptionPtr));
    }

    inline bool hasException() {
#ifndef __SINGLE_THREADED__
        lock_t lck{taskMtx};
#endif
        return exceptionsPtr != nullptr;
    }

    std::exception_ptr getExceptionPtr() {
#ifndef __SINGLE_THREADED__
        lock_t lck{taskMtx};
#endif
        return exceptionsPtr;
    }

private:
    bool canRegisterNoLock() const {
        return 0 == numThreadsFinished && maxNumThreads > numThreadsRegistered;
    }

    inline bool hasExceptionNoLock() const { return exceptionsPtr != nullptr; }

    inline void setExceptionNoLock(std::exception_ptr exceptionPtr) {
        if (exceptionsPtr == nullptr) {
            exceptionsPtr = std::move(exceptionPtr);
        }
    }

public:
    Task* parent;
    std::vector<std::shared_ptr<Task>>
        children; // Dependency tasks that needs to be executed first.

protected:
#ifndef __SINGLE_THREADED__
    std::mutex taskMtx;
    std::condition_variable cv;
#endif
    uint64_t maxNumThreads, numThreadsFinished, numThreadsRegistered;
    std::exception_ptr exceptionsPtr;
    uint64_t ID;
};

} // namespace common
} // namespace kuzu
