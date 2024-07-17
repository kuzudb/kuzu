#include "planner/operator/logical_cross_product.h"
#include "planner/planner.h"

using namespace kuzu::common;

namespace kuzu {
namespace planner {

void Planner::appendCrossProduct(const LogicalPlan& probePlan,
    const LogicalPlan& buildPlan, LogicalPlan& resultPlan) {
    appendCrossProduct(AccumulateType::REGULAR, nullptr /* mark */, probePlan, buildPlan, resultPlan);
}

void Planner::appendCrossProduct(common::AccumulateType accumulateType,
    std::shared_ptr<binder::Expression> mark, const LogicalPlan& probePlan,
    const LogicalPlan& buildPlan, LogicalPlan& resultPlan) {
    auto info = LogicalAccumulateInfo(accumulateType, mark);
    auto crossProduct = make_shared<LogicalCrossProduct>(info,
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
