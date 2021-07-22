#include "src/common/include/data_chunk/data_chunk_state.h"

namespace graphflow {
namespace common {

SharedVectorState::SharedVectorState(uint64_t capacity) : currIdx{-1}, size{0} {
    selectedPositionsBuffer = make_unique<sel_t[]>(capacity);
    resetSelectorToUnselected();
    selectedPositions = nullptr;
    multiplicity = nullptr;
}

uint64_t SharedVectorState::getNumSelectedValues() const {
    if (isFlat()) {
        return multiplicity == nullptr ? 1 : multiplicity[getPositionOfCurrIdx()];
    } else if (multiplicity == nullptr) {
        return size;
    } else {
        auto numSelectedValuesSum = 0u;
        for (auto i = 0u; i < size; i++) {
            numSelectedValuesSum += multiplicity[getSelectedPositionAtIdx(i)];
        }
        return numSelectedValuesSum;
    }
}

shared_ptr<SharedVectorState> SharedVectorState::getSingleValueDataChunkState() {
    auto state = make_shared<SharedVectorState>(1);
    state->size = 1;
    state->currIdx = 0;
    return state;
}

shared_ptr<SharedVectorState> SharedVectorState::clone() {
    auto newState = make_shared<SharedVectorState>(size);
    newState->currIdx = currIdx;
    newState->size = size;
    memcpy(newState->selectedPositionsBuffer.get(), selectedPositionsBuffer.get(),
        size * sizeof(sel_t));
    isUnfiltered() ? newState->resetSelectorToUnselected() :
                     newState->resetSelectorToValuePosBuffer();
    return newState;
}

} // namespace common
} // namespace graphflow
