#include "planner/operator/logical_filter.h"
#include "planner/query_planner.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

void QueryPlanner::appendFilters(
    const binder::expression_vector& predicates, kuzu::planner::LogicalPlan& plan) {
    for (auto& predicate : predicates) {
        appendFilter(predicate, plan);
    }
}

void QueryPlanner::appendFilter(const std::shared_ptr<Expression>& predicate, LogicalPlan& plan) {
    planSubqueryIfNecessary(predicate, plan);
    auto filter = make_shared<LogicalFilter>(predicate, plan.getLastOperator());
    QueryPlanner::appendFlattens(filter->getGroupsPosToFlatten(), plan);
    filter->setChild(0, plan.getLastOperator());
    filter->computeFactorizedSchema();
    // estimate cardinality
    plan.setCardinality(cardinalityEstimator->estimateFilter(plan, *predicate));
    plan.setLastOperator(std::move(filter));
}

} // namespace planner
} // namespace kuzu
