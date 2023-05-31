#include "processor/operator/recursive_extend/shortest_path_recursive_join.h"

#include "processor/operator/recursive_extend/shortest_path_state.h"

namespace kuzu {
namespace processor {

void ShortestPathRecursiveJoin::initLocalStateInternal(
    ResultSet* resultSet_, ExecutionContext* context) {
    BaseRecursiveJoin::initLocalStateInternal(resultSet_, context);
    std::vector<std::unique_ptr<BaseFrontierScanner>> scanners;
    switch (dataInfo->joinType) {
    case planner::RecursiveJoinType::TRACK_PATH: {
        bfsState = std::make_unique<ShortestPathState<true /* TRACK_PATH */>>(
            upperBound, targetDstNodes.get());
        for (auto i = lowerBound; i <= upperBound; ++i) {
            scanners.push_back(std::make_unique<PathScanner>(targetDstNodes.get(), i));
        }
    } break;
    case planner::RecursiveJoinType::TRACK_NONE: {
        bfsState = std::make_unique<ShortestPathState<false /* TRACK_PATH */>>(
            upperBound, targetDstNodes.get());
        for (auto i = lowerBound; i <= upperBound; ++i) {
            scanners.push_back(std::make_unique<DstNodeScanner>(targetDstNodes.get(), i));
        }
    } break;
    default:
        throw common::NotImplementedException("ShortestPathRecursiveJoin::initLocalStateInternal");
    }
    frontiersScanner = std::make_unique<FrontiersScanner>(std::move(scanners));
}

} // namespace processor
} // namespace kuzu
