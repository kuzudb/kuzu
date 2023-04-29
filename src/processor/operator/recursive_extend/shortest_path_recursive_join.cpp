#include "processor/operator/recursive_extend/shortest_path_recursive_join.h"

namespace kuzu {
namespace processor {

void ShortestPathRecursiveJoin::initLocalStateInternal(
    ResultSet* resultSet_, ExecutionContext* context) {
    BaseRecursiveJoin::initLocalStateInternal(resultSet_, context);
    distanceVector = resultSet->getValueVector(distanceVectorPos);
    auto maxNodeOffset = nodeTable->getMaxNodeOffset(transaction);
    bfsMorsel = std::make_unique<ShortestPathBFSMorsel>(
        maxNodeOffset, lowerBound, upperBound, sharedState->semiMask.get());
    bfsMorsel->resetState();
}

bool ShortestPathRecursiveJoin::scanOutput() {
    auto morsel = (ShortestPathBFSMorsel*)bfsMorsel.get();
    if (outputCursor == morsel->dstNodeOffsets.size()) {
        return false;
    }
    auto vectorSize = 0u;
    while (vectorSize != common::DEFAULT_VECTOR_CAPACITY &&
           outputCursor < morsel->dstNodeOffsets.size()) {
        auto offset = morsel->dstNodeOffsets[outputCursor];
        dstNodeIDVector->setValue<common::nodeID_t>(
            vectorSize, common::nodeID_t{offset, nodeTable->getTableID()});
        distanceVector->setValue<int64_t>(vectorSize, morsel->dstNodeOffset2PathLength.at(offset));
        vectorSize++;
        outputCursor++;
    }
    dstNodeIDVector->state->initOriginalAndSelectedSize(vectorSize);
    return true;
}

} // namespace processor
} // namespace kuzu
