#include "src/processor/include/physical_plan/operator/read_list/extend/flatten_and_extend.h"

namespace graphflow {
namespace processor {

template<bool IS_OUT_DATACHUNK_FILTERED>
void FlattenAndExtend<IS_OUT_DATACHUNK_FILTERED>::getNextTuples() {
    if (handle->hasMoreToRead()) {
        readValuesFromList();
        outDataChunk->state->numSelectedValues = outDataChunk->state->size;
        if constexpr (IS_OUT_DATACHUNK_FILTERED) {
            outDataChunk->state->initializeSelector();
        }
        return;
    }
    while (true) {
        if (inDataChunk->state->numSelectedValues == 0ul ||
            inDataChunk->state->numSelectedValues == inDataChunk->state->currPos + 1ul) {
            do {
                inDataChunk->state->currPos = -1;
                prevOperator->getNextTuples();
            } while (inDataChunk->state->size > 0 && inDataChunk->state->numSelectedValues == 0);
            if (inDataChunk->state->size == 0) {
                outDataChunk->state->numSelectedValues = outDataChunk->state->size = 0;
                return;
            }
        }
        do {
            inDataChunk->state->currPos++;
            readValuesFromList();
        } while (outDataChunk->state->size == 0 &&
                 inDataChunk->state->numSelectedValues > inDataChunk->state->currPos + 1ul);
        if (outDataChunk->state->size > 0) {
            outDataChunk->state->numSelectedValues = outDataChunk->state->size;
            if constexpr (IS_OUT_DATACHUNK_FILTERED) {
                outDataChunk->state->initializeSelector();
            }
            return;
        }
    }
}

template class FlattenAndExtend<true>;
template class FlattenAndExtend<false>;
} // namespace processor
} // namespace graphflow
