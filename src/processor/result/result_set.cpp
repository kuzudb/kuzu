#include "processor/result/result_set.h"

namespace kuzu {
namespace processor {

uint64_t ResultSet::getNumTuplesWithoutMultiplicity(
    const std::unordered_set<uint32_t>& dataChunksPosInScope) {
    assert(!dataChunksPosInScope.empty());
    uint64_t numTuples = 1;
    for (auto& dataChunkPos : dataChunksPosInScope) {
        auto state = dataChunks[dataChunkPos]->state;
        numTuples *= state->getNumSelectedValues();
    }
    return numTuples;
}

} // namespace processor
} // namespace kuzu
