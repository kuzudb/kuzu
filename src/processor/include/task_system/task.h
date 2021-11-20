#pragma once

#include <mutex>

#include "src/processor/include/execution_context.h"
#include "src/processor/include/physical_plan/operator/sink.h"

using namespace std;

namespace graphflow {
namespace processor {

using lock_t = unique_lock<mutex>;

class Task {

public:
    Task(Sink* sinkOp, uint64_t numThreads);

    void run();

    void addChildTask(unique_ptr<Task> child) {
        child->parent = this;
        children.push_back(move(child));
    }

    inline bool isCompleted() {
        lock_t lck{mtx};
        return numThreadsRegistered > 0 && numThreadsFinished == numThreadsRegistered;
    }

    inline bool canRegister() {
        lock_t lck{mtx};
        return canRegisterInternal();
    }

    inline int64_t getNumDependenciesFinished() {
        lock_t lck{mtx};
        return numDependenciesFinished;
    }

    inline void incrementNumDependenciesFinished() {
        lock_t lck{mtx};
        numDependenciesFinished++;
    }

private:
    unique_ptr<PhysicalOperator> registerThread();
    void deRegisterThread(unique_ptr<PhysicalOperator> taskSinkOp);
    bool canRegisterInternal() const {
        return 0 == numThreadsFinished && maxNumThreads > numThreadsRegistered;
    }

public:
    mutex mtx;
    Sink* sinkOp;
    Task* parent;
    vector<shared_ptr<Task>> children; // Dependency tasks that needs to be executed first.

private:
    uint64_t maxNumThreads, numThreadsFinished, numThreadsRegistered, numDependenciesFinished;
};

} // namespace processor
} // namespace graphflow
