#include "planner/logical_plan/scan/logical_scan_node.h"
#include "planner/logical_plan/scan/logical_scan_node_property.h"
#include "planner/query_planner.h"

namespace kuzu {
namespace planner {

void QueryPlanner::appendScanNodeID(std::shared_ptr<NodeExpression>& node, LogicalPlan& plan) {
    assert(plan.isEmpty());
    auto scan = make_shared<LogicalScanNode>(node);
    scan->computeFactorizedSchema();
    // update cardinality
    plan.setCardinality(cardinalityEstimator->estimateScanNode(scan.get()));
    plan.setLastOperator(std::move(scan));
}

void QueryPlanner::appendScanNodeProperties(const expression_vector& propertyExpressions,
    std::shared_ptr<NodeExpression> node, LogicalPlan& plan) {
    expression_vector propertyExpressionToScan;
    for (auto& propertyExpression : propertyExpressions) {
        if (plan.getSchema()->isExpressionInScope(*propertyExpression)) {
            continue;
        }
        propertyExpressionToScan.push_back(propertyExpression);
    }
    if (propertyExpressionToScan.empty()) { // all properties have been scanned before
        return;
    }
    auto scanNodeProperty = make_shared<LogicalScanNodeProperty>(
        std::move(node), std::move(propertyExpressionToScan), plan.getLastOperator());
    scanNodeProperty->computeFactorizedSchema();
    plan.setLastOperator(std::move(scanNodeProperty));
}

} // namespace planner
} // namespace kuzu
