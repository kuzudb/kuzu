#include "planner/logical_plan/logical_operator/logical_explain.h"

namespace kuzu {
namespace planner {

void LogicalExplain::computeFlatSchema() {
    createEmptySchema();
    schema->createGroup();
    schema->insertToGroupAndScope(outputExpression, 0);
}

void LogicalExplain::computeFactorizedSchema() {
    createEmptySchema();
    auto groupPos = schema->createGroup();
    schema->insertToGroupAndScope(outputExpression, groupPos);
    schema->setGroupAsSingleState(groupPos);
}

} // namespace planner
} // namespace kuzu
