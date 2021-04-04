#include "src/processor/include/physical_plan/operator/flatten/flatten.h"

namespace graphflow {
namespace processor {

Flatten::Flatten(uint64_t dataChunkPos, unique_ptr<PhysicalOperator> prevOperator)
    : PhysicalOperator{move(prevOperator), FLATTEN}, dataChunkToFlattenPos{dataChunkToFlattenPos} {
    dataChunks = this->prevOperator->getDataChunks();
    dataChunkToFlatten = dataChunks->getDataChunk(dataChunkPos);
}

bool Flatten::hasNextMorsel() {
    return (dataChunkToFlatten->size > 0ul &&
               dataChunkToFlatten->size > dataChunkToFlatten->currPos + 1l) ||
           prevOperator->hasNextMorsel();
}

void Flatten::getNextTuples() {
    if (dataChunkToFlatten->size == 0ul ||
        dataChunkToFlatten->size == dataChunkToFlatten->currPos + 1ul) {
        dataChunkToFlatten->currPos = -1;
        prevOperator->getNextTuples();
        if (dataChunkToFlatten->size == 0) {
            dataChunkToFlatten->size = 0;
            return;
        }
    }
    dataChunkToFlatten->currPos++;
}

} // namespace processor
} // namespace graphflow
