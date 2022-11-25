#pragma once

#include <cstring>
#include <memory>
#include <vector>

#include "common/configs.h"
#include "common/types/types.h"

using namespace std;

namespace kuzu {
namespace common {

class SelectionVector {
public:
    explicit SelectionVector(sel_t capacity) : selectedSize{0} {
        selectedPositionsBuffer = make_unique<sel_t[]>(capacity);
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

    static const sel_t INCREMENTAL_SELECTED_POS[DEFAULT_VECTOR_CAPACITY];

public:
    sel_t* selectedPositions;
    sel_t selectedSize;

private:
    unique_ptr<sel_t[]> selectedPositionsBuffer;
};

class DataChunkState {

public:
    DataChunkState() : DataChunkState(DEFAULT_VECTOR_CAPACITY) {}
    explicit DataChunkState(uint64_t capacity) : currIdx{-1}, originalSize{0} {
        selVector = make_unique<SelectionVector>(capacity);
    }

    // returns a dataChunkState for vectors holding a single value.
    static shared_ptr<DataChunkState> getSingleValueDataChunkState();

    inline void initOriginalAndSelectedSize(uint64_t size) {
        originalSize = size;
        selVector->selectedSize = size;
    }
    inline bool isFlat() const { return currIdx != -1; }
    inline bool isCurrIdxLast() const { return currIdx == selVector->selectedSize - 1; }
    inline uint64_t getPositionOfCurrIdx() const {
        assert(isFlat());
        return selVector->selectedPositions[currIdx];
    }
    inline uint64_t getNumSelectedValues() const { return isFlat() ? 1 : selVector->selectedSize; }

public:
    // The currIdx is >= 0 when vectors are flattened and -1 if the vectors are unflat.
    int64_t currIdx;
    // We need to keep track of originalSize of DataChunks to perform consistent scans of vectors
    // or lists. This is because all the vectors in a data chunk has to be the same length as they
    // share the same selectedPositions array.Therefore, if there is a scan after a filter on the
    // data chunk, the selectedSize of selVector might decrease, so the scan cannot know how much it
    // has to scan to generate a vector that is consistent with the rest of the vectors in the
    // data chunk.
    uint64_t originalSize;
    unique_ptr<SelectionVector> selVector;
};

} // namespace common
} // namespace kuzu
