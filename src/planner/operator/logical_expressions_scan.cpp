#include "planner/logical_plan/logical_operator/logical_expressions_scan.h"

namespace kuzu {
namespace planner {

void LogicalExpressionScan::computeFactorizedSchema() {
    createEmptySchema();
    auto groupPos = schema->createGroup();
    schema->setGroupAsSingleState(groupPos); // Mark group holding constant as single state.
    schema->insertToGroupAndScope(expression, groupPos);
}

void LogicalExpressionScan::computeFlatSchema() {
    createEmptySchema();
    schema->createGroup();
    schema->insertToGroupAndScope(expression, 0);
}

} // namespace planner
} // namespace kuzu
