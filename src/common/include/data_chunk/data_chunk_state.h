#pragma once

#include <cstring>
#include <memory>
#include <vector>

#include "src/common/include/types.h"

using namespace std;

namespace graphflow {
namespace common {

class DataChunkState {

public:
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

    void initMultiplicity() {
        multiplicityBuffer = make_unique<uint64_t[]>(DEFAULT_VECTOR_CAPACITY);
        multiplicity = multiplicityBuffer.get();
    }

    inline bool isFlat() const { return currIdx != -1; }

    inline uint64_t getPositionOfCurrIdx() const { return selectedPositions[currIdx]; }

    uint64_t getNumSelectedValues() const;

    shared_ptr<DataChunkState> clone();

public:
    static const sel_t INCREMENTAL_SELECTED_POS[DEFAULT_VECTOR_CAPACITY];

    // The currIdx is >= 0 when vectors are flattened and -1 if the vectors are unflat.
    int64_t currIdx;
    uint64_t size;
    sel_t* selectedPositions;
    uint64_t* multiplicity;
    unique_ptr<sel_t[]> selectedPositionsBuffer;

private:
    unique_ptr<uint64_t[]> multiplicityBuffer;
};

} // namespace common
} // namespace graphflow
