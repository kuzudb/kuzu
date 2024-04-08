#pragma once

#include <condition_variable>
#include <memory>
#include <mutex>
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
    //     make the deregister function release the lock before calling finalize and drop this
    //     assumption.
    virtual void finalizeIfNecessary() {};

    void addChildTask(std::unique_ptr<Task> child) {
        child->parent = this;
        children.push_back(std::move(child));
    }

    inline bool isCompletedSuccessfully() {
        lock_t lck{mtx};
        return isCompletedNoLock() && !hasExceptionNoLock();
    }

    inline bool isCompletedNoLock() const {
        return (numThreadsRegistered > 0 && numThreadsFinished == numThreadsRegistered);
    }

    inline void setSingleThreadedTask() { maxNumThreads = 1; }

    bool registerThread();

    void deRegisterThreadAndFinalizeTask();

    inline void setException(std::exception_ptr exceptionPtr) {
        lock_t lck{mtx};
        setExceptionNoLock(std::move(exceptionPtr));
    }

    inline bool hasException() {
        lock_t lck{mtx};
        return exceptionsPtr != nullptr;
    }

    std::exception_ptr getExceptionPtr() {
        lock_t lck{mtx};
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
    Task* parent = nullptr;
    std::vector<std::shared_ptr<Task>>
        children; // Dependency tasks that needs to be executed first.

protected:
    std::mutex mtx;
    std::condition_variable cv;
    uint64_t maxNumThreads, numThreadsFinished{0}, numThreadsRegistered{0};
    std::exception_ptr exceptionsPtr = nullptr;
    uint64_t ID;
};

} // namespace common
} // namespace kuzu
