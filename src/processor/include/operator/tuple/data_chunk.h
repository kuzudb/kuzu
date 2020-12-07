#pragma once

#include <vector>

#include "src/common/include/vector/value_vector.h"

using namespace std;

namespace graphflow {
namespace processor {

//! A DataChunk represents tuples as a set of value vectors and a selector array.
/*!
    The data chunk represents a subset of a relation i.e., a set of tuples as
   lists of the same length. Data chunks are passed as intermediate
   representations between operators. A data chunk further contains a curr_idx
   which is used when flatting and implies the value vector only contains the
   elements at curr_idx of each value vector.
*/
class DataChunk {
public:
    vector<ValueVector> valueVectors;
    uint64_t size;
    //! The current position when vectors are flattened.
    uint64_t curr_idx;

    void append(DataChunk& dataChunk);
    void append(ValueVector& valueVector) { valueVectors.push_back(valueVector); }

    uint64_t getNumTuples() const { return size; }
    uint64_t getNumAttributes() const { return valueVectors.size(); }
};

} // namespace processor
} // namespace graphflow
