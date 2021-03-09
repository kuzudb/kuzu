#pragma once

#include <memory>
#include <vector>

using namespace std;

namespace graphflow {
namespace common {

class ValueVector;

//! A DataChunk represents tuples as a set of value vectors and a selector array.
/*!
    The data chunk represents a subset of a relation i.e., a set of tuples as
   lists of the same length. Data chunks are passed as intermediate
   representations between operators. A data chunk further contains a currPos
   which is used when flatting and implies the value vector only contains the
   elements at currPos of each value vector.
*/
class DataChunk {

public:
    DataChunk() : size{0}, currPos{int64_t(-1)} {};

    void append(shared_ptr<ValueVector> valueVector) { valueVectors.push_back(valueVector); }

    inline uint64_t getNumTuples() const { return size; }

    inline uint64_t getNumAttributes() const { return valueVectors.size(); }

    inline shared_ptr<ValueVector> getValueVector(const uint64_t& valueVectorPos) {
        return valueVectors[valueVectorPos];
    }

    inline bool isFlat() const { return currPos != -1; }

    inline int64_t getCurrPos() const { return currPos; }

public:
    vector<shared_ptr<ValueVector>> valueVectors;
    uint64_t size;
    uint64_t numTruesInSelector;
    // The current position when vectors are flattened.
    int64_t currPos;
    shared_ptr<uint8_t[]> selector;
};

} // namespace common
} // namespace graphflow
