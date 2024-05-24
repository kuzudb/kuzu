#include "binder/expression/property_expression.h"
#include "planner/operator/scan/logical_scan_node_table.h"
#include "planner/planner.h"

using namespace kuzu::common;
using namespace kuzu::binder;

namespace kuzu {
namespace planner {

void Planner::appendScanNodeTable(std::shared_ptr<Expression> nodeID,
    std::vector<table_id_t> tableIDs, const expression_vector& properties, LogicalPlan& plan) {
    expression_vector propertiesToScan_;
    for (auto& property : properties) {
        if (ku_dynamic_cast<Expression&, PropertyExpression&>(*property).isInternalID()) {
            continue;
        }
        propertiesToScan_.push_back(property);
    }
    auto scanNodeProperty = make_shared<LogicalScanNodeTable>(std::move(nodeID),
        std::move(tableIDs), propertiesToScan_);
    scanNodeProperty->computeFactorizedSchema();
    plan.setLastOperator(std::move(scanNodeProperty));
}

} // namespace planner
} // namespace kuzu
