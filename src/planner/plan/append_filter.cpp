#include "planner/operator/logical_filter.h"
#include "planner/planner.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

void Planner::appendFilters(const binder::expression_vector& predicates,
    kuzu::planner::LogicalPlan& plan) {
    for (auto& predicate : predicates) {
        appendFilter(predicate, plan);
    }
}

void Planner::appendFilter(const std::shared_ptr<Expression>& predicate, LogicalPlan& plan) {
    planSubqueryIfNecessary(predicate, plan);
    auto filter = make_shared<LogicalFilter>(predicate, plan.getLastOperator());
    appendFlattens(filter->getGroupsPosToFlatten(), plan);
    filter->setChild(0, plan.getLastOperator());
    filter->computeFactorizedSchema();
    // estimate cardinality
    filter->setCardinality(cardinalityEstimator.estimateFilter(plan, *predicate));
    plan.setCardinality(filter->getCardinality());
    plan.setLastOperator(std::move(filter));
}

} // namespace planner
} // namespace kuzu
