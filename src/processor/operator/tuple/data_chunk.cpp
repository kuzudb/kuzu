#include "src/processor/include/operator/tuple/data_chunk.h"

namespace graphflow {
namespace processor {

void DataChunk::append(DataChunk& dataChunk) {
    valueVectors.reserve(dataChunk.getNumAttributes() + 1);
    valueVectors.insert(
        valueVectors.end(), dataChunk.valueVectors.begin(), dataChunk.valueVectors.end());
}

} // namespace processor
} // namespace graphflow
