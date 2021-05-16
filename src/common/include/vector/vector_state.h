#pragma once

#include <cstring>
#include <memory>
#include <vector>

using namespace std;

namespace graphflow {
namespace common {

class VectorState {
public:
    // returns a dataChunkState for vectors holding a single value.
    static shared_ptr<VectorState> getSingleValueDataChunkState();

    VectorState(bool initializeSelectedValuesPos, uint64_t capacity);

    void initializeSelector() {
        for (auto i = 0u; i < numSelectedValues; i++) {
            valuesPos[i] = i;
        }
    }

    inline bool isFlat() const { return currPos != -1; }

    inline uint64_t getCurrSelectedValuesPos() { return selectedValuesPos[currPos]; }

    shared_ptr<VectorState> clone();

public:
    uint64_t size;
    // The current position when vectors are flattened.
    int64_t currPos;
    uint64_t numSelectedValues;
    uint64_t* selectedValuesPos;

private:
    unique_ptr<uint64_t[]> valuesPos;
};

} // namespace common
} // namespace graphflow
