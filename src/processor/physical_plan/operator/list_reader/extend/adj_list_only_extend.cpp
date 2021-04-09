#include "src/processor/include/physical_plan/operator/list_reader/extend/adj_list_only_extend.h"

namespace graphflow {
namespace processor {

void AdjListOnlyExtend::getNextTuples() {
    if (handle->hasMoreToRead()) {
        readValuesFromList();
        return;
    }
    prevOperator->getNextTuples();
    if (inDataChunk->size > 0) {
        readValuesFromList();
    } else {
        outDataChunk->size = outDataChunk->numSelectedValues = 0;
    }
}

} // namespace processor
} // namespace graphflow
