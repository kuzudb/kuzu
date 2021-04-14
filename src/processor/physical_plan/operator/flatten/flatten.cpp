#include "src/processor/include/physical_plan/operator/flatten/flatten.h"

namespace graphflow {
namespace processor {

Flatten::Flatten(uint64_t dataChunkPos, unique_ptr<PhysicalOperator> prevOperator)
    : PhysicalOperator{move(prevOperator), FLATTEN}, dataChunkToFlattenPos{dataChunkToFlattenPos} {
    dataChunks = this->prevOperator->getDataChunks();
    dataChunkToFlatten = dataChunks->getDataChunk(dataChunkPos);
}

void Flatten::getNextTuples() {
    if (dataChunkToFlatten->state->size == 0ul ||
        dataChunkToFlatten->state->size == dataChunkToFlatten->state->currPos + 1ul) {
        dataChunkToFlatten->state->currPos = -1;
        prevOperator->getNextTuples();
        if (dataChunkToFlatten->state->size == 0) {
            dataChunkToFlatten->state->size = 0;
            return;
        }
    }
    dataChunkToFlatten->state->currPos++;
}

} // namespace processor
} // namespace graphflow
