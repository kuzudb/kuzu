#include "planner/operator/logical_cross_product.h"
#include "planner/planner.h"

using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace planner {

void Planner::appendCrossProduct(const LogicalPlan& probePlan, const LogicalPlan& buildPlan,
    LogicalPlan& resultPlan) {
    appendCrossProduct(AccumulateType::REGULAR, nullptr /* mark */, probePlan, buildPlan,
        resultPlan);
}

void Planner::appendOptionalCrossProduct(std::shared_ptr<Expression> mark,
    const LogicalPlan& probePlan, const LogicalPlan& buildPlan, LogicalPlan& resultPlan) {
    appendCrossProduct(AccumulateType::OPTIONAL_, mark, probePlan, buildPlan, resultPlan);
}

void Planner::appendAccOptionalCrossProduct(std::shared_ptr<Expression> mark,
    LogicalPlan& probePlan, const LogicalPlan& buildPlan, LogicalPlan& resultPlan) {
    KU_ASSERT(probePlan.hasUpdate());
    tryAppendAccumulate(probePlan);
    appendCrossProduct(AccumulateType::OPTIONAL_, mark, probePlan, buildPlan, resultPlan);
    auto& sipInfo = resultPlan.getLastOperator()->cast<LogicalCrossProduct>().getSIPInfoUnsafe();
    sipInfo.direction = SIPDirection::PROBE_TO_BUILD;
}

void Planner::appendCrossProduct(AccumulateType accumulateType, std::shared_ptr<Expression> mark,
    const LogicalPlan& probePlan, const LogicalPlan& buildPlan, LogicalPlan& resultPlan) {
    auto crossProduct = make_shared<LogicalCrossProduct>(accumulateType, mark,
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
