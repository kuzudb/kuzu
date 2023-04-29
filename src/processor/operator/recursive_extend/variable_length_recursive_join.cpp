#include "processor/operator/recursive_extend/variable_length_recursive_join.h"

namespace kuzu {
namespace processor {

void VariableLengthRecursiveJoin::initLocalStateInternal(
    ResultSet* resultSet_, ExecutionContext* context) {
    BaseRecursiveJoin::initLocalStateInternal(resultSet_, context);
    auto maxNodeOffset = nodeTable->getMaxNodeOffset(transaction);
    bfsMorsel = std::make_unique<VariableLengthBFSMorsel>(
        maxNodeOffset, lowerBound, upperBound, sharedState->semiMask.get());
    bfsMorsel->resetState();
}

bool VariableLengthRecursiveJoin::scanOutput() {
    auto morsel = (VariableLengthBFSMorsel*)bfsMorsel.get();
    if (outputCursor == morsel->dstNodeOffsets.size()) {
        return false;
    }
    auto vectorSize = 0u;
    while (vectorSize != common::DEFAULT_VECTOR_CAPACITY &&
           outputCursor < morsel->dstNodeOffsets.size()) {
        auto offset = morsel->dstNodeOffsets[outputCursor];
        auto& numPath = morsel->dstNodeOffset2NumPath.at(offset);
        auto numPathFitInCurrentVector =
            std::min<uint64_t>(common::DEFAULT_VECTOR_CAPACITY - vectorSize, numPath);
        for (auto i = 0u; i < numPathFitInCurrentVector; ++i) {
            dstNodeIDVector->setValue<common::nodeID_t>(
                vectorSize, common::nodeID_t{offset, nodeTable->getTableID()});
            vectorSize++;
        }
        numPath -= numPathFitInCurrentVector;
        outputCursor += numPath == 0; // No more path to scan under current cursor
    }
    dstNodeIDVector->state->initOriginalAndSelectedSize(vectorSize);
    return true;
}

} // namespace processor
} // namespace kuzu
