#include "src/processor/include/physical_plan/result/result_set.h"

namespace graphflow {
namespace processor {

uint64_t ResultSet::getNumTuples(const unordered_set<uint32_t>& dataChunksPosInScope) {
    assert(!dataChunksPosInScope.empty());
    uint64_t numTuples = 1;
    for (auto& dataChunkPos : dataChunksPosInScope) {
        numTuples *= dataChunks[dataChunkPos]->state->getNumSelectedValues();
    }
    return numTuples * multiplicity;
}

} // namespace processor
} // namespace graphflow
