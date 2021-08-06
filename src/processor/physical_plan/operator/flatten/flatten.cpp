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
    metrics->executionTime.start();
    if (dataChunkToFlatten->state->selectedSize == 0ul ||
        dataChunkToFlatten->state->selectedSize == dataChunkToFlatten->state->currIdx + 1ul) {
        dataChunkToFlatten->state->currIdx = -1;
        prevOperator->getNextTuples();
        if (dataChunkToFlatten->state->selectedSize == 0) {
            metrics->executionTime.stop();
            return;
        }
    }
    dataChunkToFlatten->state->currIdx++;
    metrics->executionTime.stop();
    metrics->numOutputTuple.incrementByOne();
}

} // namespace processor
} // namespace graphflow
