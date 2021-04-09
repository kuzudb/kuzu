#include "src/processor/include/physical_plan/operator/tuple/data_chunks.h"

namespace graphflow {
namespace processor {

uint64_t DataChunks::getNumTuples() {
    uint64_t numTuples = 1;
    for (auto& dataChunk : dataChunks) {
        if (!dataChunk->isFlat()) {
            numTuples *= dataChunk->state->numSelectedValues;
        }
    }
    return numTuples;
}

} // namespace processor
} // namespace graphflow
