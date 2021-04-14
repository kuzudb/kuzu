#pragma once

#include <memory>
#include <vector>

#include "src/common/include/types.h"

using namespace std;

namespace graphflow {
namespace common {

class DataChunkState {
public:
    DataChunkState(bool initializeSelectedValuesPos, uint64_t capacity)
        : size{0}, currPos{-1}, selectedValuesPos{make_unique<uint64_t[]>(capacity)},
          numSelectedValues{0} {
        if (initializeSelectedValuesPos) {
            // If the dataChunk won't be filtered, we initialize selectedValuesPos such as values at
            // all positions are selected.
            for (auto i = 0u; i < capacity; i++) {
                selectedValuesPos[i] = i;
            }
        }
    }

public:
    uint64_t size;
    // The current position when vectors are flattened.
    int64_t currPos;
    unique_ptr<uint64_t[]> selectedValuesPos;
    uint64_t numSelectedValues;
};

} // namespace common
} // namespace graphflow
