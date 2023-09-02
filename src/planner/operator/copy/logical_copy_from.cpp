#include "planner/logical_plan/copy/logical_copy_from.h"

namespace kuzu {
namespace planner {

void LogicalCopyFrom::computeFactorizedSchema() {
    createEmptySchema();
    auto groupPos = schema->createGroup();
    schema->insertToGroupAndScope(columnExpressions, groupPos);
    schema->insertToGroupAndScope(nodeOffsetExpression, groupPos);
    schema->insertToGroupAndScope(outputExpression, groupPos);
    schema->setGroupAsSingleState(groupPos);
}

void LogicalCopyFrom::computeFlatSchema() {
    createEmptySchema();
    schema->createGroup();
    schema->insertToGroupAndScope(columnExpressions, 0);
    schema->insertToGroupAndScope(nodeOffsetExpression, 0);
    schema->insertToGroupAndScope(outputExpression, 0);
}

} // namespace planner
} // namespace kuzu
