#include "planner/operator/logical_cross_product.h"
#include "planner/query_planner.h"

using namespace kuzu::common;

namespace kuzu {
namespace planner {

void QueryPlanner::appendCrossProduct(
    AccumulateType accumulateType, LogicalPlan& probePlan, LogicalPlan& buildPlan) {
    auto crossProduct = make_shared<LogicalCrossProduct>(
        accumulateType, probePlan.getLastOperator(), buildPlan.getLastOperator());
    crossProduct->computeFactorizedSchema();
    // update cost
    probePlan.setCost(probePlan.getCardinality() + buildPlan.getCardinality());
    // update cardinality
    probePlan.setCardinality(cardinalityEstimator->estimateCrossProduct(probePlan, buildPlan));
    probePlan.setLastOperator(std::move(crossProduct));
}

} // namespace planner
} // namespace kuzu
