#include "planner/operator/scan/logical_expressions_scan.h"
#include "planner/query_planner.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

void QueryPlanner::appendExpressionsScan(const expression_vector& expressions, LogicalPlan& plan) {
    auto expressionsScan = std::make_shared<LogicalExpressionsScan>(expressions);
    expressionsScan->computeFactorizedSchema();
    plan.setLastOperator(expressionsScan);
}

} // namespace planner
} // namespace kuzu
