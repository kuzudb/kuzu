#include "binder/expression/property_expression.h"
#include "catalog/catalog.h"
#include "planner/operator/logical_hash_join.h"
#include "planner/operator/scan/logical_scan_node_table.h"
#include "planner/planner.h"

using namespace kuzu::common;
using namespace kuzu::binder;
using namespace kuzu::catalog;

namespace kuzu {
namespace planner {

static expression_vector removeInternalIDProperty(const expression_vector& expressions) {
    expression_vector result;
    for (auto expr : expressions) {
        if (expr->constCast<PropertyExpression>().isInternalID()) {
            continue;
        }
        result.push_back(expr);
    }
    return result;
}

void Planner::appendScanNodeTable(const Expression& expr, const expression_vector& properties,
    LogicalPlan& plan) {
    auto& node = expr.constCast<NodeExpression>();
    if (node.hasExternalTableInfo()) {
        auto externalTableInfo = node.getExternalTableInfo();
        appendScanFile(&externalTableInfo->fileScanInfo, plan);
        auto buildPlan = LogicalPlan();
        appendScanNodeTable(node.getInternalID(), node.getTableIDs(),
            {externalTableInfo->internalColumn}, buildPlan);
        auto joinCondition =
            std::make_pair(externalTableInfo->externalColumn, externalTableInfo->internalColumn);
        std::vector<join_condition_t> joinConditions;
        joinConditions.push_back(joinCondition);
        appendHashJoin(joinConditions, JoinType::INNER, nullptr, plan, buildPlan, plan);
        return;
    }
    appendScanNodeTable(node.getInternalID(), node.getTableIDs(), properties, plan);
}

void Planner::appendScanNodeTable(std::shared_ptr<Expression> nodeID,
    std::vector<table_id_t> tableIDs, const expression_vector& properties, LogicalPlan& plan) {
    auto propertiesToScan_ = removeInternalIDProperty(properties);
    auto scan = make_shared<LogicalScanNodeTable>(std::move(nodeID), std::move(tableIDs),
        propertiesToScan_);
    scan->computeFactorizedSchema();
    plan.setCardinality(cardinalityEstimator.estimateScanNode(scan.get()));
    plan.setLastOperator(std::move(scan));
}

} // namespace planner
} // namespace kuzu
