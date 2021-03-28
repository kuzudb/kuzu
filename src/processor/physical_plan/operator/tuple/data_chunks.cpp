#include "src/processor/include/physical_plan/operator/tuple/data_chunks.h"

namespace graphflow {
namespace processor {

uint64_t DataChunks::getNumTuples() {
    uint64_t numTuples = 1;
    // TODO: loop only over the non-flattened data chunks.
    for (auto& dataChunk : dataChunks) {
        if (!dataChunk->isFlat()) {
            numTuples *= dataChunk->numSelectedValues;
        }
    }
    return numTuples;
}

} // namespace processor
} // namespace graphflow
