#pragma once
#include <memory>

#include "common/constants.h"
#include "common/types/types.h"

namespace kuzu {
namespace common {

class SelectionVector {
public:
    explicit SelectionVector(sel_t capacity) : selectedSize{0} {
        selectedPositionsBuffer = std::make_unique<sel_t[]>(capacity);
        resetSelectorToUnselected();
    }

    inline bool isUnfiltered() const {
        return selectedPositions == (sel_t*)&INCREMENTAL_SELECTED_POS;
    }
    inline void resetSelectorToUnselected() {
        selectedPositions = (sel_t*)&INCREMENTAL_SELECTED_POS;
    }
    inline void resetSelectorToUnselectedWithSize(sel_t size) {
        selectedPositions = (sel_t*)&INCREMENTAL_SELECTED_POS;
        selectedSize = size;
    }
    inline void resetSelectorToValuePosBuffer() {
        selectedPositions = selectedPositionsBuffer.get();
    }
    inline void resetSelectorToValuePosBufferWithSize(sel_t size) {
        selectedPositions = selectedPositionsBuffer.get();
        selectedSize = size;
    }
    inline sel_t* getSelectedPositionsBuffer() { return selectedPositionsBuffer.get(); }

    KUZU_API static const sel_t INCREMENTAL_SELECTED_POS[DEFAULT_VECTOR_CAPACITY];

public:
    sel_t* selectedPositions;
    // TODO: type of `selectedSize` was changed from `sel_t` to `uint64_t`, which should be reverted
    // when we removed arrow array in ValueVector. Currently, we need to keep size of arrow array,
    // which could be larger than MAX of `sel_t`.
    uint64_t selectedSize;

private:
    std::unique_ptr<sel_t[]> selectedPositionsBuffer;
};

} // namespace common
} // namespace kuzu
