#include "planner/operator/logical_multiplcity_reducer.h"
#include "planner/query_planner.h"

namespace kuzu {
namespace planner {

void QueryPlanner::appendMultiplicityReducer(LogicalPlan& plan) {
    auto multiplicityReducer = make_shared<LogicalMultiplicityReducer>(plan.getLastOperator());
    multiplicityReducer->computeFactorizedSchema();
    plan.setLastOperator(std::move(multiplicityReducer));
}

} // namespace planner
} // namespace kuzu
