#include "src/processor/include/physical_plan/operator/skip/skip.h"

namespace graphflow {
namespace processor {

Skip::Skip(uint64_t skipNumber, shared_ptr<atomic_uint64_t> counter,
    unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id)
    : PhysicalOperator{move(prevOperator), SKIP, context, id}, skipNumber{skipNumber},
      counter(move(counter)) {
    resultSet = this->prevOperator->getResultSet();
}

void Skip::getNextTuples() {
    metrics->executionTime.start();
    resultSet->resetStartOffSet();
    auto numTupleSkippedBefore = 0u;
    auto numTupleInResultSet = 0u;
    do {
        prevOperator->getNextTuples();
        numTupleInResultSet = resultSet->getNumTuples();
        // end of execution due to no more input
        if (numTupleInResultSet == 0) {
            return;
        }
        numTupleSkippedBefore = counter->fetch_add(numTupleInResultSet);
    } while (numTupleSkippedBefore + numTupleInResultSet <= skipNumber);
    int64_t numTupleToSkipInCurrentResultSet = skipNumber - numTupleSkippedBefore;
    if (numTupleToSkipInCurrentResultSet < 0) {
        // Other thread has finished skipping. Process everything in current result set.
        metrics->numOutputTuple.increase(numTupleInResultSet);
    } else {
        resultSet->setNumTupleToSkip(numTupleToSkipInCurrentResultSet);
        metrics->numOutputTuple.increase(numTupleInResultSet - numTupleToSkipInCurrentResultSet);
    }
    metrics->executionTime.stop();
}

} // namespace processor
} // namespace graphflow