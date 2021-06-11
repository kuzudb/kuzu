#pragma once

#include <cstring>
#include <memory>
#include <vector>

#include "src/common/include/types.h"

using namespace std;

namespace graphflow {
namespace common {

class VectorState {
public:
    // returns a dataChunkState for vectors holding a single value.
    static shared_ptr<VectorState> getSingleValueDataChunkState();

    VectorState(bool initializeSelectedValuesPos, uint64_t capacity);

    void initializeSelector() {
        for (auto i = 0u; i < size; i++) {
            valuesPos[i] = i;
        }
    }

    void initMultiplicity() {
        multiplicityBuffer = make_unique<uint64_t[]>(DEFAULT_VECTOR_CAPACITY);
        multiplicity = multiplicityBuffer.get();
    }

    inline bool isFlat() const { return currPos != -1; }

    inline uint64_t getCurrSelectedValuesPos() const { return selectedValuesPos[currPos]; }

    uint64_t getNumSelectedValues() const;

    shared_ptr<VectorState> clone();

public:
    // The current position when vectors are flattened.
    int64_t currPos;
    uint64_t size;
    uint64_t* selectedValuesPos;
    uint64_t* multiplicity;

private:
    unique_ptr<uint64_t[]> valuesPos;
    unique_ptr<uint64_t[]> multiplicityBuffer;
};

} // namespace common
} // namespace graphflow
