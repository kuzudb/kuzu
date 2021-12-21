#pragma once

#include <mutex>
#include <vector>

using namespace std;

namespace graphflow {
namespace common {

using lock_t = unique_lock<mutex>;

/**
 * Task represents a task that can be executed by multiple threads in the TaskScheduler. Task is a
 * virtual class that and users of TaskScheduler need to extend the Task class and implement at
 * least virtual run(). Users can assume that before T.run() is called, a worker thread W has
 * grabbed task T from the TaskScheduler's queue and registered itself to T. However,
 * workers need to dereFurther once run()
 * returns, W will deregister itself. Right before returning from run(), implementers of extended
 * Task classes can check if W is the final thread that's about to finish and deregister itself by
 * calling isAllButOneRegisteredThreadFinished(). This can be helpful if the last finishing worker
 * thread on T should do some finalization work before returning from run().
 */
class Task {

public:
    Task(uint64_t maxNumThreads);
    virtual ~Task() = default;
    virtual void run() = 0;
    //     This function is called from inside deregister() only once by the last registered worker
    //     that is completing this task. So the task lock is already acquired. So do not attempt to
    //     acquire the task lock inside. If needed we can make the deregister function release the
    //     lock before calling finalize and drop this assumption.
    virtual void finalizeIfNecessary(){};

    void addChildTask(unique_ptr<Task> child) {
        child->parent = this;
        children.push_back(move(child));
    }

    inline bool isCompletedOrHasException() {
        lock_t lck{mtx};
        return hasException() || isCompletedNoLock();
    }

    inline bool isCompletedSuccessfully() {
        lock_t lck{mtx};
        return isCompletedNoLock() && !hasException();
    }

    inline bool isCompletedNoLock() {
        return (numThreadsRegistered > 0 && numThreadsFinished == numThreadsRegistered);
    }

    bool registerThread();

    void deRegisterThreadAndFinalizeTaskIfNecessary();

    inline void setException(std::exception_ptr exceptionPtr) {
        lock_t lck{mtx};
        if (this->exceptionsPtr == NULL) {
            this->exceptionsPtr = exceptionPtr;
        }
    }

    bool hasException() const { return exceptionsPtr != NULL; }

    std::exception_ptr getExceptionPtr() {
        lock_t lck{mtx};
        return exceptionsPtr;
    }

private:
    bool canRegisterInternalNoLock() const {
        return 0 == numThreadsFinished && maxNumThreads > numThreadsRegistered;
    }

public:
    Task* parent;
    vector<shared_ptr<Task>> children; // Dependency tasks that needs to be executed first.

protected:
    mutex mtx;
    uint64_t maxNumThreads, numThreadsFinished, numThreadsRegistered;
    std::exception_ptr exceptionsPtr = NULL;
    uint64_t ID;
};

} // namespace common
} // namespace graphflow
