#include "planner/operator/logical_cross_product.h"
#include "planner/planner.h"

using namespace kuzu::common;

namespace kuzu {
namespace planner {

void Planner::appendCrossProduct(AccumulateType accumulateType, const LogicalPlan& probePlan,
    const LogicalPlan& buildPlan, LogicalPlan& resultPlan) {
    auto crossProduct = make_shared<LogicalCrossProduct>(accumulateType,
        probePlan.getLastOperator(), buildPlan.getLastOperator());
    crossProduct->computeFactorizedSchema();
    // update cost
    resultPlan.setCost(probePlan.getCardinality() + buildPlan.getCardinality());
    // update cardinality
    resultPlan.setCardinality(cardinalityEstimator.estimateCrossProduct(probePlan, buildPlan));
    resultPlan.setLastOperator(std::move(crossProduct));
}

} // namespace planner
} // namespace kuzu
