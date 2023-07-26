#include "planner/logical_plan/logical_operator/logical_merge.h"

#include "planner/logical_plan/logical_operator/flatten_resolver.h"

namespace kuzu {
namespace planner {

f_group_pos_set LogicalMerge::getGroupsPosToFlatten() {
    auto childSchema = children[0]->getSchema();
    return factorization::FlattenAll::getGroupsPosToFlatten(
        childSchema->getGroupsPosInScope(), childSchema);
}

std::unique_ptr<LogicalOperator> LogicalMerge::copy() {
    std::vector<std::unique_ptr<LogicalCreateNodeInfo>> createNodeInfosCopy;
    createNodeInfosCopy.reserve(createNodeInfos.size());
    for (auto& info : createNodeInfos) {
        createNodeInfosCopy.push_back(info->copy());
    }
    std::vector<std::unique_ptr<LogicalCreateRelInfo>> createRelInfosCopy;
    createRelInfosCopy.reserve(createRelInfos.size());
    for (auto& info : createRelInfos) {
        createRelInfosCopy.push_back(info->copy());
    }
    return std::make_unique<LogicalMerge>(
        mark, std::move(createNodeInfosCopy), std::move(createRelInfosCopy), children[0]->copy());
}

} // namespace planner
} // namespace kuzu
