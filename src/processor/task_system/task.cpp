#include "src/processor/include/task_system/task.h"

#include "src/processor/include/physical_plan/operator/sink/result_collector.h"

namespace graphflow {
namespace processor {

Task::Task(Sink* sinkOp, uint64_t numThreads) : sinkOp{sinkOp}, maxNumThreads{numThreads} {
    numThreadsFinished = 0;
    numThreadsRegistered = 0;
    numDependenciesFinished = 0;
    parent = nullptr;
}

void Task::run() {
    auto pipelineSinkCopy = registerThread();
    if (pipelineSinkCopy == nullptr) {
        return;
    }
    do {
        pipelineSinkCopy->getNextTuples();
    } while (pipelineSinkCopy->getResultSet()->getNumTuples() > 0);
    deregisterThread(move(pipelineSinkCopy));
}

unique_ptr<PhysicalOperator> Task::registerThread() {
    lock_t lck{mtx};
    if (canRegisterInternal()) {
        numThreadsRegistered++;
        return sinkOp->clone();
    }
    return nullptr;
}

void Task::deregisterThread(unique_ptr<PhysicalOperator> taskSinkOp) {
    lock_t lck{mtx};
    if (numThreadsFinished == numThreadsRegistered - 1) {
        sinkOp->finalize();
        if (parent) {
            parent->incrementNumDependenciesFinished();
        }
    }
    if (parent == nullptr) {
        auto resultCollector = reinterpret_cast<ResultCollector*>(sinkOp);
        auto threadResultCollector = reinterpret_cast<ResultCollector*>(taskSinkOp.get());
        resultCollector->queryResult->appendQueryResult(move(threadResultCollector->queryResult));
    }
    ++numThreadsFinished;
}

} // namespace processor
} // namespace graphflow
