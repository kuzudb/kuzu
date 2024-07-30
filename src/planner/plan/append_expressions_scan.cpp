#include "planner/operator/scan/logical_expressions_scan.h"
#include "planner/planner.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

void Planner::appendExpressionsScan(const expression_vector& expressions, LogicalPlan& plan) {
    auto printInfo = std::make_unique<OPPrintInfo>();
    auto expressionsScan =
        std::make_shared<LogicalExpressionsScan>(expressions, std::move(printInfo));
    expressionsScan->computeFactorizedSchema();
    plan.setLastOperator(expressionsScan);
}

} // namespace planner
} // namespace kuzu
