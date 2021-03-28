#pragma once

#include <memory>
#include <vector>

#include "src/common/include/types.h"

using namespace std;

namespace graphflow {
namespace common {

class ValueVector;

// A DataChunk represents tuples as a set of value vectors and a selector array.
/* The data chunk represents a subset of a relation i.e., a set of tuples as
  lists of the same length. Data chunks are passed as intermediate
  representations between operators. A data chunk further contains a currPos
  which is used when flattening and implies the value vector only contains the
  elements at currPos of each value vector. */
class DataChunk {

public:
    DataChunk() : DataChunk(true){};

    DataChunk(bool initializeSelectedValuesPos)
        : DataChunk(initializeSelectedValuesPos, MAX_VECTOR_SIZE) {}

    DataChunk(bool initializeSelectedValuesPos, uint64_t capacity)
        : size{0}, currPos{-1l}, selectedValuesPos{make_unique<uint64_t[]>(capacity)},
          numSelectedValues{0} {
        if (initializeSelectedValuesPos) {
            // If the dataChunk won't be filtered, we initialize selectedValuesPos such as values at
            // all positions are selected.
            for (auto i = 0u; i < capacity; i++) {
                selectedValuesPos[i] = i;
            }
        }
    };

    void append(shared_ptr<ValueVector> valueVector) { valueVectors.push_back(valueVector); }

    inline uint64_t getNumAttributes() const { return valueVectors.size(); }

    inline shared_ptr<ValueVector> getValueVector(uint64_t valueVectorPos) {
        return valueVectors[valueVectorPos];
    }

    inline uint64_t getCurrSelectedValuesPos() { return selectedValuesPos[currPos]; }

    inline bool isFlat() const { return currPos != -1; }

public:
    vector<shared_ptr<ValueVector>> valueVectors;
    uint64_t size;
    // The current position when vectors are flattened.
    int64_t currPos;
    unique_ptr<uint64_t[]> selectedValuesPos;
    uint64_t numSelectedValues;
};

} // namespace common
} // namespace graphflow
