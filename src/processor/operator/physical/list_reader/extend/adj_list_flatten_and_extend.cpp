#include "src/processor/include/operator/physical/list_reader/extend/adj_list_flatten_and_extend.h"

namespace graphflow {
namespace processor {

bool AdjListFlattenAndExtend::hasNextMorsel() {
    return (inDataChunk->size > 0 && inDataChunk->size > inDataChunk->curr_idx + 1) ||
           prevOperator->hasNextMorsel();
}

void AdjListFlattenAndExtend::getNextTuples() {
    if (handle->hasMoreToRead()) {
        readValuesFromList();
        return;
    }
    if (inDataChunk->size == 0 || inDataChunk->size == inDataChunk->curr_idx + 1) {
        inDataChunk->curr_idx = 0;
        prevOperator->getNextTuples();
    } else {
        inDataChunk->curr_idx++;
    }
    if (inDataChunk->size > 0) {
        readValuesFromList();
    } else {
        outDataChunk->size = 0;
    }
}

} // namespace processor
} // namespace graphflow
