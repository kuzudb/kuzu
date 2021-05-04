#include "src/processor/include/task_system/task.h"

#include "src/processor/include/physical_plan/operator/sink/result_collector.h"

namespace graphflow {
namespace processor {

void Task::run() {
    if (!registerThread()) {
        return;
    }
    auto pipelineSinkCopy = sinkOp->clone();
    do {
        pipelineSinkCopy->getNextTuples();
    } while (pipelineSinkCopy->getResultSet()->getNumTuples() > 0);
    deregisterThread(move(pipelineSinkCopy));
}

bool Task::registerThread() {
    lock_t lck{mtx};
    if (canRegister()) {
        numThreadsRegistered++;
        return true;
    }
    return false;
}

void Task::deregisterThread(unique_ptr<PhysicalOperator> taskSinkOp) {
    lock_t lck{mtx};
    if (numThreadsFinished == numThreadsRegistered - 1) {
        sinkOp->finalize();
        if (parent) {
            parent->numDependenciesFinished++;
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
