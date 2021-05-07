#include "src/processor/include/physical_plan/operator/result/result_set.h"

namespace graphflow {
namespace processor {

uint64_t ResultSet::getNumTuples() {
    uint64_t numTuples = 1;
    for (auto& dataChunk : dataChunks) {
        if (!dataChunk->state->isFlat()) {
            numTuples *= dataChunk->state->numSelectedValues;
        }
    }
    return numTuples;
}

} // namespace processor
} // namespace graphflow
