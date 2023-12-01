#include "planner/operator/factorization/flatten_resolver.h"
#include "planner/operator/logical_projection.h"
#include "planner/query_planner.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

void QueryPlanner::appendProjection(
    const expression_vector& expressionsToProject, LogicalPlan& plan) {
    for (auto& expression : expressionsToProject) {
        planSubqueryIfNecessary(expression, plan);
    }
    for (auto& expression : expressionsToProject) {
        auto dependentGroupsPos = plan.getSchema()->getDependentGroupsPos(expression);
        auto groupsPosToFlatten = factorization::FlattenAllButOne::getGroupsPosToFlatten(
            dependentGroupsPos, plan.getSchema());
        appendFlattens(groupsPosToFlatten, plan);
    }
    auto projection = make_shared<LogicalProjection>(expressionsToProject, plan.getLastOperator());
    projection->computeFactorizedSchema();
    plan.setLastOperator(std::move(projection));
}

} // namespace planner
} // namespace kuzu
