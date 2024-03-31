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
        setToUnfiltered();
    }

    bool isUnfiltered() const { return selectedPositions == (sel_t*)&INCREMENTAL_SELECTED_POS; }
    void setToUnfiltered() { selectedPositions = (sel_t*)&INCREMENTAL_SELECTED_POS; }
    void setToUnfiltered(sel_t size) {
        selectedPositions = (sel_t*)&INCREMENTAL_SELECTED_POS;
        selectedSize = size;
    }

    // Set to filtered is not very accurate. It sets selectedPositions to a mutable array.
    void setToFiltered() { selectedPositions = selectedPositionsBuffer.get(); }
    void setToFiltered(sel_t size) {
        selectedPositions = selectedPositionsBuffer.get();
        selectedSize = size;
    }
    sel_t* getMultableBuffer() { return selectedPositionsBuffer.get(); }

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
