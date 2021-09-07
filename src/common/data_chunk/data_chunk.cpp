#include "src/common/include/data_chunk/data_chunk.h"

namespace graphflow {
namespace common {

unique_ptr<DataChunk> DataChunk::clone() {
    auto newChunk = make_unique<DataChunk>(valueVectors.size(), state->clone());
    for (auto i = 0; i < valueVectors.size(); ++i) {
        newChunk->insert(i, valueVectors[i]->clone());
    }
    return newChunk;
}

} // namespace common
} // namespace graphflow
