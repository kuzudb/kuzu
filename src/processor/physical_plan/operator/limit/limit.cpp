#include "src/processor/include/physical_plan/operator/limit/limit.h"

namespace graphflow {
namespace processor {

Limit::Limit(uint64_t limitNumber, shared_ptr<atomic_uint64_t> counter,
    unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id)
    : PhysicalOperator{move(prevOperator), LIMIT, context, id},
      limitNumber{limitNumber}, counter{move(counter)} {
    resultSet = this->prevOperator->getResultSet();
}

void Limit::getNextTuples() {
    metrics->executionTime.start();
    resultSet->setNumTuplesToIterate(UINT64_MAX);
    prevOperator->getNextTuples();
    auto numTuplesInResultSet = resultSet->getNumTuples();
    // end of execution due to no more input
    if (numTuplesInResultSet == 0) {
        return;
    }
    auto numTupleProcessedBefore = counter->fetch_add(numTuplesInResultSet);
    if (numTupleProcessedBefore + numTuplesInResultSet > limitNumber) {
        int64_t numTupleToProcessInCurrentResultSet = limitNumber - numTupleProcessedBefore;
        // end of execution due to limit has reached
        if (numTupleToProcessInCurrentResultSet < 0) {
            for (auto& dataChunk : resultSet->dataChunks) {
                dataChunk->state->size = 0;
            }
            return;
        } else {
            resultSet->setNumTuplesToIterate(numTupleToProcessInCurrentResultSet);
        }
    }
    metrics->executionTime.stop();
}

} // namespace processor
} // namespace graphflow
