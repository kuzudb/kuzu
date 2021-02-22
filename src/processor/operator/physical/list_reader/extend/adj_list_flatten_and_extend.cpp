#include "src/processor/include/operator/physical/list_reader/extend/adj_list_flatten_and_extend.h"

namespace graphflow {
namespace processor {

bool AdjListFlattenAndExtend::hasNextMorsel() {
    return (inDataChunk->size > 0 && inDataChunk->size > inDataChunk->currPos + 1) ||
           prevOperator->hasNextMorsel();
}

void AdjListFlattenAndExtend::getNextTuples() {
    if (handle->hasMoreToRead()) {
        readValuesFromList();
        return;
    }
    if (inDataChunk->size == 0 || inDataChunk->size == inDataChunk->currPos + 1) {
        inDataChunk->currPos = -1;
        prevOperator->getNextTuples();
        if (inDataChunk->size == 0) {
            outDataChunk->size = 0;
            return;
        }
    }
    inDataChunk->currPos += 1;
    readValuesFromList();
}

} // namespace processor
} // namespace graphflow
