#include "src/processor/include/physical_plan/operator/flatten/flatten.h"

namespace graphflow {
namespace processor {

Flatten::Flatten(uint64_t dataChunkPos, unique_ptr<PhysicalOperator> prevOperator)
    : PhysicalOperator{move(prevOperator), FLATTEN}, dataChunkToFlattenPos{dataChunkPos} {
    resultSet = this->prevOperator->getResultSet();
    dataChunkToFlatten = resultSet->dataChunks[dataChunkPos];
}

void Flatten::getNextTuples() {
    if (dataChunkToFlatten->state->size == 0ul ||
        dataChunkToFlatten->state->size == dataChunkToFlatten->state->currPos + 1ul) {
        dataChunkToFlatten->state->currPos = UINT64_MAX;
        prevOperator->getNextTuples();
        if (dataChunkToFlatten->state->size == 0) {
            return;
        }
    }
    dataChunkToFlatten->state->currPos++;
}

} // namespace processor
} // namespace graphflow
