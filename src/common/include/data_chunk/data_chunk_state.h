#pragma once

#include <cstring>
#include <memory>
#include <vector>

#include "src/common/include/configs.h"
#include "src/common/types/include/types.h"

using namespace std;

namespace graphflow {
namespace common {

class DataChunkState {

public:
    DataChunkState() : DataChunkState(DEFAULT_VECTOR_CAPACITY) {}
    explicit DataChunkState(uint64_t capacity);

    // returns a dataChunkState for vectors holding a single value.
    static shared_ptr<DataChunkState> getSingleValueDataChunkState();

    inline bool isUnfiltered() const {
        return selectedPositions == (sel_t*)&INCREMENTAL_SELECTED_POS;
    }

    inline void resetSelectorToUnselected() {
        selectedPositions = (sel_t*)&INCREMENTAL_SELECTED_POS;
    }

    inline void resetSelectorToValuePosBuffer() {
        selectedPositions = selectedPositionsBuffer.get();
    }

    inline void setSelectedSize(uint64_t size) { selectedSize = size; }

    void initOriginalAndSelectedSize(uint64_t size) {
        originalSize = size;
        selectedSize = size;
    }

    inline bool isFlat() const { return currIdx != -1; }

    inline uint64_t getPositionOfCurrIdx() const { return selectedPositions[currIdx]; }

    uint64_t getNumSelectedValues() const;

public:
    static const sel_t INCREMENTAL_SELECTED_POS[DEFAULT_VECTOR_CAPACITY];

    // The currIdx is >= 0 when vectors are flattened and -1 if the vectors are unflat.
    int64_t currIdx;
    // We need to keep track of originalSize of DataChunks to perform consistent scans of vectors
    // or lists. This is because all of the vectors in a datachunk has to be the same length as they
    // share the same selectedPositions array.Therefore if there is a scan after a filter on the
    // datachunk, the selectedSize might decrease, so the scan cannot know how much it has to
    // scan to generate a vector that is consistent with the rest of the vectors in the datachunk.
    uint64_t originalSize;
    uint64_t selectedSize;
    sel_t* selectedPositions;
    unique_ptr<sel_t[]> selectedPositionsBuffer;
};

} // namespace common
} // namespace graphflow
