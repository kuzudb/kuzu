#pragma once

#include "src/common/include/data_chunk/data_chunk.h"
#include "src/common/include/data_chunk/data_chunk_state.h"

using namespace graphflow::common;
using namespace std;

namespace graphflow {
namespace processor {

class FilteringOperator {

public:
    FilteringOperator()
        : prevNumSelectedValues{0ul}, prevSelectedValues{nullptr},
          prevSelectedValuesBuffer{make_unique<sel_t[]>(DEFAULT_VECTOR_CAPACITY)} {};

protected:
    inline void reInitToRerunSubPlan() {
        prevNumSelectedValues = 0ul;
        prevSelectedValues = nullptr;
    }

    inline void restoreDataChunkSelectorState(const shared_ptr<DataChunk>& dataChunkToSelect) {
        if (prevSelectedValues == nullptr) {
            return;
        }
        dataChunkToSelect->state->selectedSize = prevNumSelectedValues;
        if (prevSelectedValues == (sel_t*)&DataChunkState::INCREMENTAL_SELECTED_POS) {
            dataChunkToSelect->state->resetSelectorToUnselected();
        } else {
            dataChunkToSelect->state->resetSelectorToValuePosBuffer();
            memcpy(dataChunkToSelect->state->selectedPositions, prevSelectedValuesBuffer.get(),
                prevNumSelectedValues * sizeof(sel_t));
        }
    };

    inline void saveDataChunkSelectorState(const shared_ptr<DataChunk>& dataChunkToSelect) {
        prevNumSelectedValues = dataChunkToSelect->state->selectedSize;
        if (dataChunkToSelect->state->isUnfiltered()) {
            prevSelectedValues = (sel_t*)&DataChunkState::INCREMENTAL_SELECTED_POS;
        } else {
            memcpy(prevSelectedValuesBuffer.get(),
                dataChunkToSelect->state->selectedPositionsBuffer.get(),
                prevNumSelectedValues * sizeof(sel_t));
            prevSelectedValues = prevSelectedValuesBuffer.get();
        }
    };

protected:
    uint64_t prevNumSelectedValues;
    sel_t* prevSelectedValues;
    unique_ptr<sel_t[]> prevSelectedValuesBuffer;
};
} // namespace processor
} // namespace graphflow
