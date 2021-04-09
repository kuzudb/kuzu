#include "src/processor/include/physical_plan/operator/read_list/extend/extend.h"

namespace graphflow {
namespace processor {

void Extend::getNextTuples() {
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
