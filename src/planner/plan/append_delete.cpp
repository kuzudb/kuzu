#include "binder/query/updating_clause/bound_delete_info.h"
#include "common/enums/clause_type.h"
#include "planner/operator/persistent/logical_delete.h"
#include "planner/query_planner.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

void QueryPlanner::appendDeleteNode(
    const std::vector<BoundDeleteInfo*>& boundInfos, LogicalPlan& plan) {
    std::vector<std::unique_ptr<LogicalDeleteNodeInfo>> infos;
    for (auto& boundInfo : boundInfos) {
        infos.push_back(std::make_unique<LogicalDeleteNodeInfo>(
            // TODO: Add kuzu_dynamic_pointer_cast, and should use that instead for safety checks.
            std::static_pointer_cast<NodeExpression>(boundInfo->nodeOrRel),
            boundInfo->deleteClauseType == common::DeleteClauseType::DETACH_DELETE ?
                common::DeleteNodeType::DETACH_DELETE :
                common::DeleteNodeType::DELETE));
    }
    auto deleteNode = std::make_shared<LogicalDeleteNode>(std::move(infos), plan.getLastOperator());
    appendFlattens(deleteNode->getGroupsPosToFlatten(), plan);
    deleteNode->setChild(0, plan.getLastOperator());
    deleteNode->computeFactorizedSchema();
    plan.setLastOperator(std::move(deleteNode));
}

void QueryPlanner::appendDeleteRel(
    const std::vector<BoundDeleteInfo*>& boundInfos, LogicalPlan& plan) {
    std::vector<std::shared_ptr<RelExpression>> rels;
    for (auto& info : boundInfos) {
        auto rel = std::static_pointer_cast<RelExpression>(info->nodeOrRel);
        rels.push_back(rel);
    }
    auto deleteRel = std::make_shared<LogicalDeleteRel>(rels, plan.getLastOperator());
    for (auto i = 0u; i < boundInfos.size(); ++i) {
        appendFlattens(deleteRel->getGroupsPosToFlatten(i), plan);
        deleteRel->setChild(0, plan.getLastOperator());
    }
    deleteRel->computeFactorizedSchema();
    plan.setLastOperator(std::move(deleteRel));
}

} // namespace planner
} // namespace kuzu
