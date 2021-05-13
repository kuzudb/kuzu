#include "src/common/include/data_chunk/data_chunk.h"

namespace graphflow {
namespace common {

unique_ptr<DataChunk> DataChunk::clone() {
    auto newChunk = make_unique<DataChunk>();
    newChunk->state = state->clone();
    for (auto& valueVector : valueVectors) {
        newChunk->append(valueVector->clone());
    }
    return newChunk;
}

} // namespace common
} // namespace graphflow
