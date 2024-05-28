#include "binder/expression/property_expression.h"
#include "planner/operator/scan/logical_scan_node_table.h"
#include "planner/planner.h"

using namespace kuzu::common;
using namespace kuzu::binder;

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
