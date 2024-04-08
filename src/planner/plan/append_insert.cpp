#include "binder/query/updating_clause/bound_insert_info.h"
#include "planner/operator/persistent/logical_insert.h"
#include "planner/planner.h"

using namespace kuzu::common;
using namespace kuzu::binder;

namespace kuzu {
namespace planner {

std::unique_ptr<LogicalInsertInfo> Planner::createLogicalInsertInfo(const BoundInsertInfo* info) {
    auto insertInfo = std::make_unique<LogicalInsertInfo>(info->tableType, info->pattern,
        info->columnExprs, info->columnDataExprs, info->conflictAction);
    if (info->iriReplaceExpr != nullptr) { // See bound_insert_info.h for explanation.
        insertInfo->columnExprs = expression_vector{info->iriReplaceExpr};
        KU_ASSERT(insertInfo->columnExprs.size() == 1);
        auto projectIri = false;
        for (auto& property : propertiesToScan) {
            if (*property == *info->iriReplaceExpr) {
                projectIri = true;
            }
        }
        insertInfo->isReturnColumnExprs.push_back(projectIri);
        return insertInfo;
    }
    binder::expression_set propertyExprSet;
    for (auto& expr : getProperties(*info->pattern)) {
        propertyExprSet.insert(expr);
    }
    for (auto& expr : insertInfo->columnExprs) {
        insertInfo->isReturnColumnExprs.push_back(propertyExprSet.contains(expr));
    }
    return insertInfo;
}

void Planner::appendInsertNode(const std::vector<const binder::BoundInsertInfo*>& boundInsertInfos,
    LogicalPlan& plan) {
    std::vector<LogicalInsertInfo> logicalInfos;
    logicalInfos.reserve(boundInsertInfos.size());
    for (auto& boundInfo : boundInsertInfos) {
        logicalInfos.push_back(createLogicalInsertInfo(boundInfo)->copy());
    }
    auto insertNode =
        std::make_shared<LogicalInsert>(std::move(logicalInfos), plan.getLastOperator());
    appendFlattens(insertNode->getGroupsPosToFlatten(), plan);
    insertNode->setChild(0, plan.getLastOperator());
    insertNode->computeFactorizedSchema();
    plan.setLastOperator(insertNode);
}

void Planner::appendInsertRel(const std::vector<const binder::BoundInsertInfo*>& boundInsertInfos,
    LogicalPlan& plan) {
    std::vector<LogicalInsertInfo> logicalInfos;
    logicalInfos.reserve(boundInsertInfos.size());
    for (auto& boundInfo : boundInsertInfos) {
        logicalInfos.push_back(createLogicalInsertInfo(boundInfo)->copy());
    }
    auto insertRel =
        std::make_shared<LogicalInsert>(std::move(logicalInfos), plan.getLastOperator());
    appendFlattens(insertRel->getGroupsPosToFlatten(), plan);
    insertRel->setChild(0, plan.getLastOperator());
    insertRel->computeFactorizedSchema();
    plan.setLastOperator(insertRel);
}
} // namespace planner
} // namespace kuzu
