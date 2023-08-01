#include "planner/join_order_enumerator.h"
#include "planner/logical_plan/logical_operator/logical_cross_product.h"
#include "planner/query_planner.h"

namespace kuzu {
namespace planner {

void JoinOrderEnumerator::appendCrossProduct(
    common::AccumulateType accumulateType, LogicalPlan& probePlan, LogicalPlan& buildPlan) {
    auto crossProduct = make_shared<LogicalCrossProduct>(
        accumulateType, probePlan.getLastOperator(), buildPlan.getLastOperator());
    crossProduct->computeFactorizedSchema();
    // update cost
    probePlan.setCost(probePlan.getCardinality() + buildPlan.getCardinality());
    // update cardinality
    probePlan.setCardinality(
        queryPlanner->cardinalityEstimator->estimateCrossProduct(probePlan, buildPlan));
    probePlan.setLastOperator(std::move(crossProduct));
}

} // namespace planner
} // namespace kuzu
