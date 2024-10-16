#include "planner/operator/logical_multiplcity_reducer.h"
#include "planner/planner.h"

namespace kuzu {
namespace planner {

void Planner::appendMultiplicityReducer(LogicalPlan& plan) {
    auto printInfo = std::make_unique<OPPrintInfo>();
    auto multiplicityReducer =
        make_shared<LogicalMultiplicityReducer>(plan.getLastOperator(), std::move(printInfo));
    multiplicityReducer->computeFactorizedSchema();
    plan.setLastOperator(std::move(multiplicityReducer));
}

} // namespace planner
} // namespace kuzu
