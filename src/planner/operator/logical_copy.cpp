#include "planner/logical_plan/logical_operator/logical_copy.h"

namespace kuzu {
namespace planner {

void LogicalCopy::computeFactorizedSchema() {
    createEmptySchema();
    auto groupPos = schema->createGroup();
    schema->insertToGroupAndScope(arrowColumnExpressions, groupPos);
    schema->insertToGroupAndScope(offsetExpression, groupPos);
    schema->insertToGroupAndScope(outputExpression, groupPos);
    schema->setGroupAsSingleState(groupPos);
}

void LogicalCopy::computeFlatSchema() {
    createEmptySchema();
    schema->createGroup();
    schema->insertToGroupAndScope(arrowColumnExpressions, 0);
    schema->insertToGroupAndScope(offsetExpression, 0);
    schema->insertToGroupAndScope(outputExpression, 0);
}

} // namespace planner
} // namespace kuzu
