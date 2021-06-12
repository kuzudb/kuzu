#include "src/processor/include/physical_plan/operator/flatten/flatten.h"

namespace graphflow {
namespace processor {

Flatten::Flatten(uint64_t dataChunkPos, unique_ptr<PhysicalOperator> prevOperator,
    ExecutionContext& context, uint32_t id)
    : PhysicalOperator{move(prevOperator), FLATTEN, context, id}, dataChunkToFlattenPos{
                                                                      dataChunkPos} {
    resultSet = this->prevOperator->getResultSet();
    dataChunkToFlatten = resultSet->dataChunks[dataChunkPos];
}

void Flatten::getNextTuples() {
    executionTime->start();
    if (dataChunkToFlatten->state->size == 0ul ||
        dataChunkToFlatten->state->size == dataChunkToFlatten->state->currPos + 1ul) {
        dataChunkToFlatten->state->currPos = -1;
        prevOperator->getNextTuples();
        if (dataChunkToFlatten->state->size == 0) {
            executionTime->stop();
            return;
        }
    }
    dataChunkToFlatten->state->currPos++;
    executionTime->stop();
    numOutputTuple->increase(1ul);
}

} // namespace processor
} // namespace graphflow
