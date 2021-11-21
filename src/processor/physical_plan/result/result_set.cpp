#include "src/processor/include/physical_plan/result/result_set.h"

namespace graphflow {
namespace processor {

uint64_t ResultSet::getNumTuples() {
    uint64_t numTuples = 1;
    for (auto i = 0u; i < dataChunks.size(); ++i) {
        if (!dataChunksMask[i]) {
            continue;
        }
        numTuples *= dataChunks[i]->state->getNumSelectedValues();
    }
    return numTuples * multiplicity;
}

} // namespace processor
} // namespace graphflow
