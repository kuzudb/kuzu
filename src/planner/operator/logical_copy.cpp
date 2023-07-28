#include "planner/logical_plan/logical_operator/logical_copy.h"

namespace kuzu {
namespace planner {

void LogicalCopy::computeFactorizedSchema() {
    createEmptySchema();
    auto groupPos = schema->createGroup();
    schema->insertToGroupAndScope(nodeOffsetExpression, groupPos);
    schema->insertToGroupAndScope(dataColumnExpressions, groupPos);
    schema->insertToGroupAndScope(outputExpression, groupPos);
    schema->setGroupAsSingleState(groupPos);
}

void LogicalCopy::computeFlatSchema() {
    createEmptySchema();
    schema->createGroup();
    schema->insertToGroupAndScope(nodeOffsetExpression, 0);
    schema->insertToGroupAndScope(dataColumnExpressions, 0);
    schema->insertToGroupAndScope(outputExpression, 0);
}

} // namespace planner
} // namespace kuzu
