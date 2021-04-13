#include "src/processor/include/physical_plan/operator/read_list/extend/extend.h"

namespace graphflow {
namespace processor {

template<bool IS_OUT_DATACHUNK_FILTERED>
void Extend<IS_OUT_DATACHUNK_FILTERED>::getNextTuples() {
    if (handle->hasMoreToRead()) {
        readValuesFromList();
        outDataChunk->state->numSelectedValues = outDataChunk->state->size;
        if constexpr (IS_OUT_DATACHUNK_FILTERED) {
            outDataChunk->state->initializeSelector();
        }
        return;
    }
    while (true) {
        do {
            prevOperator->getNextTuples();
        } while (inDataChunk->state->size > 0 && inDataChunk->state->numSelectedValues == 0);
        if (inDataChunk->state->size > 0) {
            readValuesFromList();
            if (outDataChunk->state->size > 0) {
                outDataChunk->state->numSelectedValues = outDataChunk->state->size;
                if constexpr (IS_OUT_DATACHUNK_FILTERED) {
                    outDataChunk->state->initializeSelector();
                }
                return;
            }
        } else {
            outDataChunk->state->size = outDataChunk->state->numSelectedValues = 0;
            return;
        }
    }
}

template class Extend<true>;
template class Extend<false>;
} // namespace processor
} // namespace graphflow
