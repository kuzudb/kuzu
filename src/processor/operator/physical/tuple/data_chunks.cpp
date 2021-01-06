#include "src/processor/include/operator/physical/tuple/data_chunks.h"

namespace graphflow {
namespace processor {

uint64_t DataChunks::getNumTuples() {
    uint64_t numTuples = 1;
    for (auto& dataChunk : dataChunks) {
        numTuples *= dataChunk->size;
    }
    return numTuples;
}

} // namespace processor
} // namespace graphflow
