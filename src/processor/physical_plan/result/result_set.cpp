#include "src/processor/include/physical_plan/result/result_set.h"

namespace graphflow {
namespace processor {

uint64_t ResultSet::getNumTuplesWithoutMultiplicity(
    const unordered_set<uint32_t>& dataChunksPosInScope) {
    assert(!dataChunksPosInScope.empty());
    uint64_t numTuples = 1;
    for (auto& dataChunkPos : dataChunksPosInScope) {
        auto state = dataChunks[dataChunkPos]->state;
        numTuples *= state->getNumSelectedValues();
    }
    return numTuples;
}

} // namespace processor
} // namespace graphflow
