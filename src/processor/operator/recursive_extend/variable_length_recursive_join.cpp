#include "processor/operator/recursive_extend/variable_length_recursive_join.h"

#include "processor/operator/recursive_extend/variable_length_state.h"

namespace kuzu {
namespace processor {

void VariableLengthRecursiveJoin::initLocalStateInternal(
    ResultSet* resultSet_, ExecutionContext* context) {
    BaseRecursiveJoin::initLocalStateInternal(resultSet_, context);
    std::vector<std::unique_ptr<BaseFrontierScanner>> scanners;
    switch (dataInfo->joinType) {
    case planner::RecursiveJoinType::TRACK_PATH: {
        bfsState = std::make_unique<VariableLengthState<true /* TRACK_PATH */>>(
            upperBound, targetDstNodes.get());
        for (auto i = lowerBound; i <= upperBound; ++i) {
            scanners.push_back(std::make_unique<PathScanner>(targetDstNodes.get(), i));
        }
    } break;
    case planner::RecursiveJoinType::TRACK_NONE: {
        bfsState = std::make_unique<VariableLengthState<false /* TRACK_PATH */>>(
            upperBound, targetDstNodes.get());
        for (auto i = lowerBound; i <= upperBound; ++i) {
            scanners.push_back(
                std::make_unique<DstNodeWithMultiplicityScanner>(targetDstNodes.get(), i));
        }
    } break;
    default:
        throw common::NotImplementedException(
            "VariableLengthRecursiveJoin::initLocalStateInternal");
    }
    frontiersScanner = std::make_unique<FrontiersScanner>(std::move(scanners));
}

} // namespace processor
} // namespace kuzu
