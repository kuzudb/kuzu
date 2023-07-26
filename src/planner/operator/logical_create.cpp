#include "planner/logical_plan/logical_operator/logical_create.h"

#include "planner/logical_plan/logical_operator/flatten_resolver.h"

namespace kuzu {
namespace planner {

void LogicalCreateNode::computeFactorizedSchema() {
    copyChildSchema(0);
    for (auto& info : infos) {
        auto groupPos = schema->createGroup();
        schema->setGroupAsSingleState(groupPos);
        schema->insertToGroupAndScope(info->node->getInternalIDProperty(), groupPos);
    }
}

void LogicalCreateNode::computeFlatSchema() {
    copyChildSchema(0);
    for (auto& info : infos) {
        schema->insertToGroupAndScope(info->node->getInternalIDProperty(), 0);
    }
}

f_group_pos_set LogicalCreateNode::getGroupsPosToFlatten() {
    // Flatten all inputs. E.g. MATCH (a) CREATE (b). We need to create b for each tuple in the
    // match clause. This is to simplify operator implementation.
    auto childSchema = children[0]->getSchema();
    return factorization::FlattenAll::getGroupsPosToFlatten(
        childSchema->getGroupsPosInScope(), childSchema);
}

std::unique_ptr<LogicalOperator> LogicalCreateNode::copy() {
    std::vector<std::unique_ptr<LogicalCreateNodeInfo>> infosCopy;
    infosCopy.reserve(infos.size());
    for (auto& info : infos) {
        infosCopy.push_back(info->copy());
    }
    return std::make_unique<LogicalCreateNode>(std::move(infosCopy), children[0]->copy());
}

f_group_pos_set LogicalCreateRel::getGroupsPosToFlatten() {
    auto childSchema = children[0]->getSchema();
    return factorization::FlattenAll::getGroupsPosToFlatten(
        childSchema->getGroupsPosInScope(), childSchema);
}

std::unique_ptr<LogicalOperator> LogicalCreateRel::copy() {
    std::vector<std::unique_ptr<LogicalCreateRelInfo>> infosCopy;
    infosCopy.reserve(infos.size());
    for (auto& info : infos) {
        infosCopy.push_back(info->copy());
    }
    return std::make_unique<LogicalCreateRel>(std::move(infosCopy), children[0]->copy());
}

} // namespace planner
} // namespace kuzu
