#include "planner/operator/ddl/logical_ddl.h"

namespace kuzu {
namespace planner {

void LogicalDDL::computeFlatSchema() {
    createEmptySchema();
    schema->createGroup();
    schema->insertToGroupAndScope(outputExpression, 0);
}

void LogicalDDL::computeFactorizedSchema() {
    createEmptySchema();
    auto groupPos = schema->createGroup();
    schema->insertToGroupAndScope(outputExpression, groupPos);
    schema->setGroupAsSingleState(groupPos);
}

} // namespace planner
} // namespace kuzu
