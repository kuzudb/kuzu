#pragma once

#include "src/common/include/timer.h"
#include "src/processor/include/execution_context.h"
#include "src/processor/include/physical_plan/physical_plan.h"
#include "src/processor/include/physical_plan/query_result.h"

using namespace std;

namespace graphflow {
namespace processor {

using lock_t = unique_lock<mutex>;

class Task {

public:
    Task(Sink* sinkOp, const uint64_t& maxNumThreads)
        : sinkOp{sinkOp}, maxNumThreads{maxNumThreads}, numThreadsFinished{0},
          numThreadsRegistered{0}, numDependenciesFinished{0}, parent{nullptr} {}

    void run();

    inline bool isCompleted() {
        return numThreadsRegistered > 0 && numThreadsFinished == numThreadsRegistered;
    }

    inline bool canRegister() {
        return 0 == numThreadsFinished && maxNumThreads > numThreadsRegistered;
    }

private:
    bool registerThread();
    void deregisterThread(unique_ptr<PhysicalOperator> taskSinkOp);

public:
    mutex mtx;
    Sink* sinkOp;
    atomic_uint64_t maxNumThreads, numThreadsFinished, numThreadsRegistered,
        numDependenciesFinished;
    Task* parent;
    vector<unique_ptr<Task>> children; // Dependency tasks that needs to be executed first.
};

} // namespace processor
} // namespace graphflow
