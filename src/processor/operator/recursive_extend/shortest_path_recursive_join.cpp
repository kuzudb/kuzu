#include "processor/operator/recursive_extend/shortest_path_recursive_join.h"

namespace kuzu {
namespace processor {

void ShortestPathRecursiveJoin::initLocalStateInternal(
    ResultSet* resultSet_, ExecutionContext* context) {
    BaseRecursiveJoin::initLocalStateInternal(resultSet_, context);
    auto maxNodeOffset = nodeTable->getMaxNodeOffset(transaction);
    std::vector<std::unique_ptr<BaseFrontierScanner>> scanners;
    switch (dataInfo->joinType) {
    case planner::RecursiveJoinType::TRACK_PATH: {
        bfsMorsel = std::make_unique<ShortestPathMorsel<true /* trackPath */>>(
            maxNodeOffset, lowerBound, upperBound, sharedState->semiMask.get());
        for (auto i = lowerBound; i <= upperBound; ++i) {
            scanners.push_back(std::make_unique<PathScanner>(bfsMorsel->targetDstNodeOffsets, i));
        }
    } break;
    case planner::RecursiveJoinType::TRACK_NONE: {
        bfsMorsel = std::make_unique<ShortestPathMorsel<false /* trackPath */>>(
            maxNodeOffset, lowerBound, upperBound, sharedState->semiMask.get());
        for (auto i = lowerBound; i <= upperBound; ++i) {
            scanners.push_back(
                std::make_unique<DstNodeScanner>(bfsMorsel->targetDstNodeOffsets, i));
        }
    } break;
    default:
        throw common::NotImplementedException("ShortestPathRecursiveJoin::initLocalStateInternal");
    }
    frontiersScanner = std::make_unique<FrontiersScanner>(std::move(scanners));
}

} // namespace processor
} // namespace kuzu
